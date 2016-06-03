
/*-------------------------------------------------------------------------
 *
 * cdbdisp_non_thread.c
 *	  Functions for non-threaded implementation of dispatching
 *	  commands to QExecutors.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <limits.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "storage/ipc.h"		/* For proc_exit_inprogress  */
#include "tcop/tcopprot.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_non_thread.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/gp_atomic.h"

#define DISPATCH_WAIT_TIMEOUT_SEC 2

#define LOG_DISPATCHER_DEBUG(...) do { \
	elog(DEBUG5, __VA_ARGS__); \
    } while(false);


/*
 * Keeps state of all the dispatch command threads.
 */
typedef struct CdbDispatchCmdThreads
{

	/*
	 * dispatchResultPtrArray: Array[0..db_count-1] of CdbDispatchResult*
	 * Each CdbDispatchResult object points to a SegmentDatabaseDescriptor
	 * that this thread is responsible for dispatching the command to.
	 */
	struct CdbDispatchResult **dispatchResultPtrArray;

	int dispatchCount;

	/*
	 * Depending on this mode, we may send query cancel or query finish
	 * message to QE while we are waiting it to complete.  NONE means
	 * we expect QE to complete without any instruction.
	 */
	volatile DispatchWaitMode waitMode;

	char *query_text;
	int query_text_len;

}   CdbDispatchParmsNonThread;



static bool
shouldStillDispatchCommand(CdbDispatchParmsNonThread * pParms,
						   CdbDispatchResult * dispatchResult);

static bool
dispatchCommand(CdbDispatchResult * dispatchResult,
				const char *query_text,
				int query_text_len);

/* returns true if command complete */
static bool processResults(CdbDispatchResult * dispatchResult);

static void
cdbdisp_checkCancel(CdbDispatchParmsNonThread* pParms);

static void
cdbdisp_DispatchWait(CdbDispatcherState *ds,
					 bool wait);

static void
cdbdisp_checkConnectionAlive(CdbDispatchParmsNonThread * pParms);
static void
cdbdisp_checkSegmentDBAlive(CdbDispatchParmsNonThread * pParms);
static void
handlePollSuccess(CdbDispatchParmsNonThread* pParms, struct pollfd *fds);


static void *
cdbdisp_makeNonThreadedParms(int maxSlices, char *queryText, int len);

static void
CdbCheckDispatchResult_internal(struct CdbDispatcherState *ds,
								DispatchWaitMode waitMode);

static void
cdbdisp_dispatchToGang_internal(struct CdbDispatcherState *ds,
								struct Gang *gp,
								int sliceIndex,
								CdbDispatchDirectDesc * dispDirect);
static bool
cdbdisp_shouldCancel(struct CdbDispatcherState *ds);

DispatcherInternalFuncs NonThreadedFuncs =
{
	NULL,
	cdbdisp_shouldCancel,
	cdbdisp_makeNonThreadedParms,
	CdbCheckDispatchResult_internal,
	cdbdisp_dispatchToGang_internal
};

static void
cdbdisp_dispatchOut(CdbDispatchParmsNonThread* pParms,
					CdbDispatchResult* qeResult,
					char* newQueryText,
					int newQueryTextLen)
{
	SegmentDatabaseDescriptor *segdbDesc = qeResult->segdbDesc;

	pParms->dispatchResultPtrArray[pParms->dispatchCount++] = qeResult;
	/*
	 * This CdbDispatchResult/SegmentDatabaseDescriptor pair will be
	 * dispatched and monitored by a thread to be started below. Only that
	 * thread should touch them until the thread is finished with them and
	 * resets the stillRunning flag. Caller must CdbCheckDispatchResult()
	 * to wait for completion.
	 */
	qeResult->stillRunning = true;

	qeResult->hasDispatched = false;
	qeResult->sentSignal = DISPATCH_WAIT_NONE;
	qeResult->wasCanceled = false;

	if (cdbconn_isBadConnection(segdbDesc))
	{
		qeResult->stillRunning = false;
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("Lost connection before dispatch to %s",
						 segdbDesc->whoami)));
	}
	else if (!shouldStillDispatchCommand(pParms, qeResult))
	{
		/*
		 * Don't dispatch if cancellation pending or no connection.
		 */
		qeResult->wasCanceled = true;
		qeResult->stillRunning = false;
		if (PQisBusy(qeResult->segdbDesc->conn))
			elog(LOG, " We thought we were done, because !shouldStillDispatchCommand(),"
					  " but libpq says we are still busy");
	}
	else
	{
		/*
		 * Kick off the command over the libpq connection.
		 * If unsuccessful, proceed anyway, and check for lost connection below.
		 */
		if (PQisBusy(qeResult->segdbDesc->conn))
			elog(LOG, "Trying to send to busy connection (%s) asyncStatus %d",
				qeResult->segdbDesc->whoami,
				qeResult->segdbDesc->conn->asyncStatus);

		if (!dispatchCommand(qeResult, newQueryText, newQueryTextLen))
		{
			qeResult->stillRunning = false;
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("Command could not be dispatch to segment db %s",
							 qeResult->segdbDesc->whoami)));
		}
		else
		{
			/*
			 * We'll keep monitoring this QE -- whether or not the command
			 * was dispatched -- in order to check for a lost connection
			 * or any other errors that libpq might have in store for us.
			 */
			LOG_DISPATCHER_DEBUG("Command dispatched to QE (%s)",
								 qeResult->segdbDesc->whoami);
			qeResult->hasDispatched = true;
		}
	}
}

static void
cdbdisp_dispatchToGang_internal(struct CdbDispatcherState *ds,
								struct Gang *gp,
								int sliceIndex,
								CdbDispatchDirectDesc * dispDirect)
{
	int	i = 0;
	char *newQueryText = NULL;
	int newQueryTextLen = 0;

	CdbDispatchParmsNonThread *pParms = (CdbDispatchParmsNonThread*)ds->dispatchParams;

	newQueryText = sliceIndex < 0 ? pParms->query_text :
				   dupQueryTextAndSetSliceId(ds->dispatchStateContext, pParms->query_text,
						   pParms->query_text_len, sliceIndex);

	newQueryTextLen = pParms->query_text_len;
	/*
	 * Create the threads. (which also starts the dispatching).
	 */
	for (i = 0; i < gp->size; i++)
	{
		CdbDispatchResult* qeResult = NULL;

		SegmentDatabaseDescriptor *segdbDesc = &gp->db_descriptors[i];
		Assert(segdbDesc != NULL);

		if (dispDirect->directed_dispatch)
		{
			/* We can direct dispatch to one segment DB only */
			Assert(dispDirect->count == 1);
			if (dispDirect->content[0] != segdbDesc->segindex)
				continue;
		}

		/*
		 * Initialize the QE's CdbDispatchResult object.
		 */
		qeResult = cdbdisp_makeResult(ds->primaryResults, segdbDesc, sliceIndex);
		if (qeResult == NULL)
		{
			/*
			 * writer_gang could be NULL if this is an extended query.
			 */
			if (ds->primaryResults->writer_gang)
				ds->primaryResults->writer_gang->dispatcherActive = true;

			elog(FATAL, "could not allocate resources for segworker communication");
		}

		cdbdisp_dispatchOut(pParms, qeResult, newQueryText, newQueryTextLen);
	}
}

static void
CdbCheckDispatchResult_internal(struct CdbDispatcherState *ds,
								DispatchWaitMode waitMode)
{
	Assert(ds != NULL);
	CdbDispatchParmsNonThread *pParms = (CdbDispatchParmsNonThread*)ds->dispatchParams;

	/* cdbdisp_destroyDispatcherState is called */
	if(pParms == NULL)
		return;

	/* Don't overwrite DISPATCH_WAIT_CANCEL or DISPATCH_WAIT_FINISH with DISPATCH_WAIT_NONE */
	if (waitMode != DISPATCH_WAIT_NONE)
		pParms->waitMode = waitMode;

	cdbdisp_DispatchWait(ds, true);

	/*
	 * It looks like everything went fine, make sure we don't miss a
	 * user cancellation?
	 *
	 * The waitMode argument is NONE when we are doing "normal work".
	 */
	if (waitMode == DISPATCH_WAIT_NONE || waitMode == DISPATCH_WAIT_FINISH)
		CHECK_FOR_INTERRUPTS();
}

/*
 * cdbdisp_makeDispatchThreads:
 * Allocates memory for a CdbDispatchCmdThreads structure and the memory
 * needed inside. Do the initialization.
 * Will be freed in function cdbdisp_destroyDispatcherState by deleting the
 * memory context.
 */
static void *
cdbdisp_makeNonThreadedParms(int maxSlices, char *queryText, int len)
{
	int	maxResults = maxSlices * getgpsegmentCount();
	int	size = 0;

	CdbDispatchParmsNonThread *pParms = palloc0(sizeof(CdbDispatchParmsNonThread));

	size = maxResults * sizeof(CdbDispatchResult *);
	pParms->dispatchResultPtrArray = (CdbDispatchResult **) palloc0(size);
	pParms->dispatchCount = 0;
	pParms->waitMode = DISPATCH_WAIT_NONE;
	pParms->query_text = queryText;
	pParms->query_text_len = len;

	return (void*)pParms;
}


static void
cdbdisp_DispatchWait(CdbDispatcherState *ds,
					 bool wait)
{
	CdbDispatchParmsNonThread *pParms = (CdbDispatchParmsNonThread*)ds->dispatchParams;
	SegmentDatabaseDescriptor *segdbDesc;
	CdbDispatchResult *dispatchResult;
	int	i;
	int db_count = 0;
	int	timeoutCounter = 0;
	struct pollfd *fds;

	db_count = pParms->dispatchCount;
	fds = (struct pollfd *) palloc(db_count * sizeof(struct pollfd));

	/*
	 * OK, we are finished submitting the command to the segdbs.
	 * Now, we have to wait for them to finish.
	 */
	for (;;)
	{
		int	sock;
		int	n;
		int	nfds = 0;

		/*
		 * Which QEs are still running and could send results to us?
		 */
		for (i = 0; i < db_count; i++)
		{
			dispatchResult = pParms->dispatchResultPtrArray[i];
			segdbDesc = dispatchResult->segdbDesc;

			/*
			 * Already finished with this QE?
			 */
			if (!dispatchResult->stillRunning)
				continue;

			if (cdbconn_isBadConnection(segdbDesc))
			{
				dispatchResult->stillRunning = false;
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("Lost connection during dispatch to %s",
								 segdbDesc->whoami)));
			}

			/*
			 * Add socket to fd_set if still connected.
			 */
			sock = PQsocket(segdbDesc->conn);
			Assert(sock >= 0);
			fds[nfds].fd = sock;
			fds[nfds].events = POLLIN;
			nfds++;
		}

		/*
		 * Break out when no QEs still running.
		 */
		if (nfds <= 0)
			break;

		/*
		 * bail-out if we are dying.  We should not do much of cleanup
		 * as the main thread is waiting on this thread to finish.	Once
		 * QD dies, QE will recognize it shortly anyway.
		 */
		if (proc_exit_inprogress)
			break;

		/*
		 * Wait for results from QEs. Block here until input is available.
		 */
		n = poll(fds, nfds, wait ? DISPATCH_WAIT_TIMEOUT_SEC * 1000 : 0);

		/* poll returns with an error, including one due to an interrupted call */
		if (n < 0)
		{
			int	sock_errno = SOCK_ERRNO;
			if (sock_errno == EINTR)
				continue;

			LOG_DISPATCHER_DEBUG("handlePollError poll() failed; errno=%d", sock_errno);

			cdbdisp_checkConnectionAlive(pParms);
			cdbdisp_checkCancel(pParms);
			cdbdisp_checkSegmentDBAlive(pParms);
		}
		/* If the time limit expires, poll() returns 0 */
		else if (n == 0)
		{
			if (!wait)
			{
				pfree(fds);
				return;
			}

			cdbdisp_checkCancel(pParms);
			if (timeoutCounter++ > 30)
			{
				cdbdisp_checkSegmentDBAlive(pParms);
				timeoutCounter = 0;
			}
		}
		/* We have data waiting on one or more of the connections. */
		else
			handlePollSuccess(pParms, fds);
	}

	pfree(fds);
}



/*
 * Helper function to thread_DispatchCommand that actually kicks off the
 * command on the libpq connection.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
static bool
dispatchCommand(CdbDispatchResult * dispatchResult,
				const char *query_text,
				int query_text_len)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	TimestampTz beforeSend = 0;
	long secs;
	int	usecs;

	if (DEBUG1 >= log_min_messages)
		beforeSend = GetCurrentTimestamp();

	/*
	 * Submit the command asynchronously.
	 */
	if (!cdbconn_sendGpQuery(segdbDesc, (char *) query_text, query_text_len))
		return false;


	if (DEBUG1 >= log_min_messages)
	{
		TimestampDifference(beforeSend, GetCurrentTimestamp(), &secs, &usecs);

		if (secs != 0 || usecs > 1000)	/* Time > 1ms? */
			elog(LOG, "time for PQsendGpQuery_shared %ld.%06d", secs, usecs);
	}

	return true;
}

static void
handlePollSuccess(CdbDispatchParmsNonThread* pParms,
				  struct pollfd *fds)
{
	int cur_fds_num = 0;
	int i = 0;

	/*
	 * We have data waiting on one or more of the connections.
	 */
	for (i = 0; i < pParms->dispatchCount; i++)
	{
		bool finished;
		int sock;
		CdbDispatchResult *dispatchResult = pParms->dispatchResultPtrArray[i];
		SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;

		/*
		 * Skip if already finished or didn't dispatch.
		 */
		if (!dispatchResult->stillRunning)
			continue;

		LOG_DISPATCHER_DEBUG("looking for results from %d of %d (%s)",
							 i + 1, pParms->dispatchCount, segdbDesc->whoami);

		sock = PQsocket(segdbDesc->conn);
		Assert(sock >= 0);
		Assert(sock == fds[cur_fds_num].fd);

		/*
		 * Skip this connection if it has no input available.
		 */
		if (!(fds[cur_fds_num++].revents & POLLIN))
			continue;

		LOG_DISPATCHER_DEBUG("PQsocket says there are results from %d", i + 1);

		/*
		 * Receive and process results from this QE.
		 */
		finished = processResults(dispatchResult);
		/*
		 * Are we through with this QE now?
		 */
		if (finished)
		{
			dispatchResult->stillRunning = false;

			LOG_DISPATCHER_DEBUG("processResults says we are finished with %d: %s",
								 i + 1, segdbDesc->whoami);

			if (DEBUG1 >= log_min_messages)
			{
				char msec_str[32];
				switch (check_log_duration(msec_str, false))
				{
					case 1:
					case 2:
						elog(LOG, "duration to dispatch result received from thread %d (seg %d): %s ms",
								  i + 1, dispatchResult->segdbDesc->segindex, msec_str);
						break;
				}
			}

			if (PQisBusy(dispatchResult->segdbDesc->conn))
				elog(LOG, "We thought we were done, because finished==true, but libpq says we are still busy");
		}
		else
			LOG_DISPATCHER_DEBUG("processResults says we have more to do with %d: %s",
								 i + 1, segdbDesc->whoami);
	}
}

/*
 * Helper function to thread_DispatchCommand that handles errors that occur
 * during the poll() call.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 *		 The cleanup of the connections will be performed by handlePollTimeout().
 */
static void
cdbdisp_checkConnectionAlive(CdbDispatchParmsNonThread * pParms)
{
	int	i;

	/*
	 * Based on the select man page, we could get here with
	 * errno == EBADF (bad descriptor), EINVAL (highest descriptor negative or negative timeout)
	 * or ENOMEM (out of memory).
	 * This is most likely a programming error or a bad system failure, but we'll try to 
	 * clean up a bit anyhow.
	 *
	 * We *can* get here as a result of some hardware issues. the timeout code
	 * knows how to clean up if we've lost contact with one of our peers.
	 *
	 * We should check a connection's integrity before calling PQisBusy().
	 */
	for (i = 0; i < pParms->dispatchCount; i++)
	{
		CdbDispatchResult *dispatchResult = pParms->dispatchResultPtrArray[i];

		/*
		 * Skip if already finished or didn't dispatch. 
		 */
		if (!dispatchResult->stillRunning)
			continue;

		/*
		 * We're done with this QE, sadly. 
		 */
		if (cdbconn_isBadConnection(dispatchResult->segdbDesc))
		{
			dispatchResult->stillRunning = false;
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("Lost connection during dispatch to %s",
							 dispatchResult->segdbDesc->whoami)));
		}
	}

	/*
	 * No point in trying to cancel the other QEs with select() broken.
	 */
}

static void
cdbdisp_checkCancel(CdbDispatchParmsNonThread* pParms)
{
	int i;

	for (i = 0; i < pParms->dispatchCount; i++)
	{
		DispatchWaitMode waitMode;
		CdbDispatchResult *dispatchResult = pParms->dispatchResultPtrArray[i];
		Assert(dispatchResult != NULL);
		SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
		CdbDispatchResults *meleeResults = dispatchResult->meleeResults;

		/*
		 * Already finished with this QE?
		 */
		if (!dispatchResult->stillRunning)
			continue;

		waitMode = DISPATCH_WAIT_NONE;
		/*
		 * Send query finish to this QE if QD is already done.
		 */
		if (pParms->waitMode == DISPATCH_WAIT_FINISH)
			waitMode = DISPATCH_WAIT_FINISH;

		/*
		 * However, escalate it to cancel if:
		 *	 - user interrupt has occurred,
		 *	 - or I'm told to send cancel,
		 *	 - or an error has been reported by another QE,
		 *	 - in case the caller wants cancelOnError and it was not canceled
		 */
		if ((InterruptPending || pParms->waitMode == DISPATCH_WAIT_CANCEL || meleeResults->errcode) &&
			(meleeResults->cancelOnError && !dispatchResult->wasCanceled))
			waitMode = DISPATCH_WAIT_CANCEL;

		/*
		 * Finally, don't send the signal if
		 *	 - no action needed (NONE)
		 *	 - the signal was already sent
		 *	 - connection is dead
		 */
		if (waitMode != DISPATCH_WAIT_NONE &&
			waitMode != dispatchResult->sentSignal &&
			!cdbconn_isBadConnection(segdbDesc))
			dispatchResult->sentSignal = cdbdisp_signalQE(segdbDesc, waitMode);
	}
}

/*
 * Helper function to thread_DispatchCommand that handles timeouts that occur
 * during the poll() call.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
static void
cdbdisp_checkSegmentDBAlive(CdbDispatchParmsNonThread * pParms)
{
	int i;
	bool forceScan = true;

	/*
	 * check the connection still valid, set 1 min time interval
	 * this may affect performance, should turn it off if required.
	 */
	for (i = 0; i < pParms->dispatchCount; i++)
	{
		CdbDispatchResult *dispatchResult = pParms->dispatchResultPtrArray[i];
		SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;

		LOG_DISPATCHER_DEBUG("checking status %d of %d %s stillRunning %d",
				i + 1, pParms->dispatchCount, segdbDesc->whoami, dispatchResult->stillRunning);

		/*
		 * Skip if already finished or didn't dispatch.
		 */
		if (!dispatchResult->stillRunning)
			continue;

		/*
		 * Skip the entry db.
		 */
		if (segdbDesc->segindex < 0)
			continue;

		LOG_DISPATCHER_DEBUG("testing connection %d of %d %s stillRunning %d",
				i + 1, pParms->dispatchCount, segdbDesc->whoami, dispatchResult->stillRunning);

		if (!FtsTestConnection(segdbDesc->segment_database_info, forceScan))
		{
			dispatchResult->stillRunning = false;
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("FTS detected connection lost during dispatch to %s",
							 dispatchResult->segdbDesc->whoami)));
		}

		forceScan = false;
	}
}

/*
 * Helper function to thread_DispatchCommand that decides if we should dispatch
 * to this segment database.
 *
 * (1) don't dispatch if there is already a query cancel notice pending.
 * (2) make sure our libpq connection is still good.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
static bool
shouldStillDispatchCommand(CdbDispatchParmsNonThread * pParms,
						   CdbDispatchResult * dispatchResult)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	CdbDispatchResults *gangResults = dispatchResult->meleeResults;

	/*
	 * Don't submit if already encountered an error. The error has already
	 * been noted, so just keep quiet.
	 */
	if (pParms->waitMode == DISPATCH_WAIT_CANCEL)
	{
		if (Debug_cancel_print || DEBUG4 >= log_min_messages)
			elog(LOG, "Cancellation in progress; command not sent to %s", segdbDesc->whoami);
		return false;
	}

	if (gangResults->errcode && gangResults->cancelOnError)
	{
		if (Debug_cancel_print || DEBUG4 >= log_min_messages)
			elog(LOG, "Error cleanup in progress; command not sent to %s", segdbDesc->whoami);
		return false;
	}

	/*
	 * Don't submit if client told us to cancel. The cancellation request has
	 * already been noted, so hush.
	 */
	if (InterruptPending && gangResults->cancelOnError)
	{
		if (Debug_cancel_print || DEBUG4 >= log_min_messages)
			elog(LOG, "Cancellation request pending; command not sent to %s", segdbDesc->whoami);
		return false;
	}

	return true;
}

static bool
processResults(CdbDispatchResult * dispatchResult)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	char *msg;

	/*
	 * Receive input from QE.
	 */
	if (PQconsumeInput(segdbDesc->conn) == 0)
	{
		dispatchResult->stillRunning = false;
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("Error on receive from %s",
						segdbDesc->whoami)));
	}

	/*
	 * If we have received one or more complete messages, process them.
	 */
	while (!PQisBusy(segdbDesc->conn))
	{
		/* loop to call PQgetResult; won't block */
		PGresult *pRes;
		ExecStatusType resultStatus;
		int	resultIndex;

		/*
		 * PQisBusy() does some error handling, which can
		 * cause the connection to die -- we can't just continue on as
		 * if the connection is happy without checking first.
		 * 
		 * For example, cdbdisp_numPGresult() will return a completely
		 * bogus value!
		 */
		if (cdbconn_isBadConnection(segdbDesc))
		{
			dispatchResult->stillRunning = false;
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("Lost connection during receive from %s",
							segdbDesc->whoami)));
		}

		/*
		 * Get one message.
		 */
		LOG_DISPATCHER_DEBUG("PQgetResult");
		pRes = PQgetResult(segdbDesc->conn);

		/*
		 * Command is complete when PGgetResult() returns NULL. It is critical
		 * that for any connection that had an asynchronous command sent thru
		 * it, we call PQgetResult until it returns NULL. Otherwise, the next
		 * time a command is sent to that connection, it will return an error
		 * that there's a command pending.
		 */
		if (!pRes)
		{
			LOG_DISPATCHER_DEBUG("%s -> idle", segdbDesc->whoami);
			/* this is normal end of command */
			return true;
		}

		/* update writer transaction information */
		CollectQEWriterTransactionInformation(segdbDesc, dispatchResult);

		/*
		 * Attach the PGresult object to the CdbDispatchResult object.
		 */
		resultIndex = cdbdisp_numPGresult(dispatchResult);
		cdbdisp_appendResult(dispatchResult, pRes);

		/*
		 * Did a command complete successfully?
		 */
		resultStatus = PQresultStatus(pRes);
		if (resultStatus == PGRES_COMMAND_OK ||
			resultStatus == PGRES_TUPLES_OK ||
			resultStatus == PGRES_COPY_IN ||
			resultStatus == PGRES_COPY_OUT)
		{
			LOG_DISPATCHER_DEBUG("%s -> ok %s",
					  	  	  	 segdbDesc->whoami,
								 PQcmdStatus(pRes) ? PQcmdStatus(pRes) : "(no cmdStatus)");

			/*
			 * Save the index of the last successful PGresult. Can be given to
			 * cdbdisp_getPGresult() to get tuple count, etc.
			 */
			dispatchResult->okindex = resultIndex;

			/*
			 * SREH - get number of rows rejected from QE if any
			 */
			if (pRes->numRejected > 0)
				dispatchResult->numrowsrejected += pRes->numRejected;

			if (resultStatus == PGRES_COPY_IN ||
				resultStatus == PGRES_COPY_OUT)
				return true;
		}
		/*
		 * Note QE error. Cancel the whole statement if requested.
		 */
		else
		{
			/* QE reported an error */
			char	   *sqlstate = PQresultErrorField(pRes, PG_DIAG_SQLSTATE);
			int			errcode = 0;

			msg = PQresultErrorMessage(pRes);

			LOG_DISPATCHER_DEBUG("%s -> %s %s  %s",
					  	  	  	 segdbDesc->whoami,
								 PQresStatus(resultStatus),
								 sqlstate ? sqlstate : "(no SQLSTATE)",
								 msg);

			/*
			 * Convert SQLSTATE to an error code (ERRCODE_xxx). Use a generic
			 * nonzero error code if no SQLSTATE.
			 */
			if (sqlstate && strlen(sqlstate) == 5)
				errcode = sqlstate_to_errcode(sqlstate);

			/*
			 * Save first error code and the index of its PGresult buffer
			 * entry.
			 */
			cdbdisp_seterrcode(errcode, resultIndex, dispatchResult);
		}
	}

	return false; /* we must keep on monitoring this socket */
}



static bool
cdbdisp_shouldCancel(struct CdbDispatcherState *ds)
{
	struct CdbDispatchResults  *meleeResults;

	Assert(ds);

	cdbdisp_DispatchWait(ds, false);

	meleeResults = ds->primaryResults;

	if (meleeResults == NULL) /* cleanup ? */
	{
		return false;
	}

	Assert(meleeResults);

	if (meleeResults->errcode)
	{
		return true;
	}

	return false;
}

