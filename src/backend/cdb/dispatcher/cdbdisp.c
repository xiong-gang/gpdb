/*-------------------------------------------------------------------------
 *
 * cdbdisp.c
 *	  Functions to dispatch commands to QExecutors.
 *
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <pthread.h>
#include <limits.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "catalog/catquery.h"
#include "executor/execdesc.h"	/* QueryDesc */
#include "storage/ipc.h"		/* For proc_exit_inprogress  */
#include "miscadmin.h"
#include "utils/memutils.h"

#include "utils/tqual.h" 			/*for the snapshot */
#include "storage/proc.h"  			/* MyProc */
#include "storage/procarray.h"      /* updateSharedLocalSnapshot */
#include "access/xact.h"  			/*for GetCurrentTransactionId */


#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "executor/executor.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "tcop/tcopprot.h"
#include "cdb/cdbplan.h"
#include "postmaster/syslogger.h"

#include "cdb/cdbselect.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbsubplan.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbdtxcontextinfo.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbrelsize.h"
#include "gp-libpq-fe.h"
#include "libpq/libpq-be.h"
#include "cdb/cdbutil.h"

#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "utils/gp_atomic.h"
#include "utils/builtins.h"
#include "utils/portal.h"

#define DISPATCH_WAIT_TIMEOUT_SEC 2
extern bool Test_print_direct_dispatch_info;

extern pthread_t main_tid;
#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#else
#define mythread() ((unsigned long) pthread_self().p)
#endif 

/*
 * default directed-dispatch parameters: don't direct anything.
 */
CdbDispatchDirectDesc default_dispatch_direct_desc = {false, 0, {0}};

/*
 * Static Helper functions
 */

static void *thread_DispatchCommand(void *arg);
static void thread_DispatchOut(DispatchCommandParms		*pParms);
static void thread_DispatchWait(DispatchCommandParms	*pParms);
static void thread_DispatchWaitSingle(DispatchCommandParms		*pParms);

static void CdbCheckDispatchResultInt(struct CdbDispatcherState *ds,
						  struct SegmentDatabaseDescriptor ***failedSegDB,
						  int *numOfFailed,
						  DispatchWaitMode waitMode);

static bool
shouldStillDispatchCommand(DispatchCommandParms *pParms, CdbDispatchResult * dispatchResult);

							
static void
handlePollError(DispatchCommandParms *pParms,
                  int                   db_count,
                  int                   sock_errno);							

static void
handlePollTimeout(DispatchCommandParms   *pParms,
                    int                     db_count,
                    int                    *timeoutCounter,
                    bool                    useSampling);

static void
CollectQEWriterTransactionInformation(SegmentDatabaseDescriptor *segdbDesc, CdbDispatchResult * dispatchResult);


static bool                     /* returns true if command complete */
processResults(CdbDispatchResult   *dispatchResult);

static void
bindCurrentOfParams(char *cursor_name, 
					Oid target_relid, 
					ItemPointer ctid, 
					int *gp_segment_id, 
					Oid *tableoid);

static int getMaxThreadsPerGang();
static CdbDispatchCmdThreads *
cdbdisp_makeDispatchThreads(int maxThreads);
static CdbDispatchResults *
cdbdisp_makeDispatchResults(int resultCapacity, int sliceCapacity,
		bool cancelOnError);

static char *dupQueryTextAndSetSliceId(MemoryContext cxt, char *queryText, int len,
		int sliceId);
static void cdbdisp_dtxParmsInit(struct CdbDispatcherState *ds,
		DispatchCommandDtxProtocolParms *pDtxProtocolParms);
static void cdbdisp_queryParmsInit(struct CdbDispatcherState *ds,
		DispatchCommandQueryParms *pQueryParms);
static void dispatchCommand(CdbDispatchResult *dispatchResult,
		const char *query_text, int query_text_len);


#define GP_PARTITION_SELECTION_OID 6084
#define GP_PARTITION_EXPANSION_OID 6085
#define GP_PARTITION_INVERSE_OID 6086

 
/*
 * Clear our "active" flags; so that we know that the writer gangs are busy -- and don't stomp on
 * internal dispatcher structures. See MPP-6253 and MPP-6579.
 */
static void
cdbdisp_clearGangActiveFlag(CdbDispatcherState *ds)
{
	if (ds && ds->primaryResults && ds->primaryResults->writer_gang)
	{
		ds->primaryResults->writer_gang->dispatcherActive = false;
	}
}

/*
 * We need an array describing the relationship between a slice and
 * the number of "child" slices which depend on it.
 */
typedef struct {
	int sliceIndex;
	int children;
	Slice *slice;
} sliceVec;

static int fillSliceVector(SliceTable * sliceTable, int sliceIndex, sliceVec *sliceVector, int len);

/* determines which dispatchOptions need to be set. */
static int generateTxnOptions(bool needTwoPhase);

typedef struct
{
	plan_tree_base_prefix base; /* Required prefix for plan_tree_walker/mutator */
	bool		single_row_insert;
}	pre_dispatch_function_evaluation_context;

static Node *pre_dispatch_function_evaluation_mutator(Node *node,
						 pre_dispatch_function_evaluation_context * context);

static void
CdbDispatchUtilityStatement_Internal(struct Node *stmt, bool needTwoPhase, char* debugCaller);

/* 
 * ====================================================
 * STATIC STATE VARIABLES should not be declared!
 * global state will break the ability to run cursors.
 * only globals with a higher granularity than a running
 * command (i.e: transaction, session) are ok.
 * ====================================================
 */

static DtxContextInfo TempQDDtxContextInfo = DtxContextInfo_StaticInit;

/*
 * Counter to indicate there are some dispatch threads running.  This will
 * be incremented at the beginning of dispatch threads and decremented at
 * the end of them.
 */
static volatile int32 RunningThreadCount = 0;

/*
 * cdbdisp_dispatchToGang:
 * Send the strCommand SQL statement to the subset of all segdbs in the cluster
 * specified by the gang parameter.  cancelOnError indicates whether an error
 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
 * on other qExecs. Normally this would be true.  The commands are sent over the libpq
 * connections that were established during cdblink_setup.	They are run inside of threads.
 * The number of segdbs handled by any one thread is determined by the
 * guc variable gp_connections_per_thread.
 *
 * The caller must provide a CdbDispatchResults object having available
 * resultArray slots sufficient for the number of QEs to be dispatched:
 * i.e., resultCapacity - resultCount >= gp->size.	This function will
 * assign one resultArray slot per QE of the Gang, paralleling the Gang's
 * db_descriptors array.  Success or failure of each QE will be noted in
 * the QE's CdbDispatchResult entry; but before examining the results, the
 * caller must wait for execution to end by calling CdbCheckDispatchResult().
 *
 * The CdbDispatchResults object owns some malloc'ed storage, so the caller
 * must make certain to free it by calling cdbdisp_destroyDispatcherState().
 *
 * When dispatchResults->cancelOnError is false, strCommand is to be
 * dispatched to every connected gang member if possible, despite any
 * cancellation requests, QE errors, connection failures, etc.
 *
 * NB: This function should return normally even if there is an error.
 * It should not longjmp out via elog(ERROR, ...), ereport(ERROR, ...),
 * PG_THROW, CHECK_FOR_INTERRUPTS, etc.
 */
void cdbdisp_dispatchToGang(struct CdbDispatcherState *ds, struct Gang *gp,
		int sliceIndex, CdbDispatchDirectDesc *disp_direct)
{
	struct CdbDispatchResults	*dispatchResults = ds->primaryResults;
	SegmentDatabaseDescriptor	*segdbDesc;
	int	i,
		max_threads,
		segdbs_in_thread_pool = 0,
		newThreads = 0;	
	int gangSize = 0;
	SegmentDatabaseDescriptor *db_descriptors;
	char *newQueryText = NULL;
	DispatchCommandParms *pParms = NULL;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(gp && gp->size > 0);
	Assert(dispatchResults && dispatchResults->resultArray);

	if (dispatchResults->writer_gang)
	{
		/* Are we dispatching to the writer-gang when it is already busy ? */
		if (gp == dispatchResults->writer_gang)
		{
			if (dispatchResults->writer_gang->dispatcherActive)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("query plan with multiple segworker groups is not supported"),
						 errhint("likely caused by a function that reads or modifies data in a distributed table")));
			}

			dispatchResults->writer_gang->dispatcherActive = true;
		}
	}

	gangSize = gp->size;
	Assert(gangSize <= largestGangsize());
	db_descriptors = gp->db_descriptors;
	
	/*
	 * The most threads we could have is segdb_count / gp_connections_per_thread, rounded up.
	 * This is equivalent to 1 + (segdb_count-1) / gp_connections_per_thread.
	 * We allocate enough memory for this many DispatchCommandParms structures,
	 * even though we may not use them all.
	 *
	 * We can only use gp->size here if we're not dealing with a
	 * singleton gang. It is safer to always use the max number of segments we are
	 * controlling (largestGangsize).
	 */
	Assert(gp_connections_per_thread >= 0);
	Assert(ds->dispatchThreads != NULL);
	/*
	 * If we attempt to reallocate, there is a race here: we
	 * know that we have threads running using the
	 * dispatchCommandParamsAr! If we reallocate we
	 * potentially yank it out from under them! Don't do
	 * it!
	 */
	max_threads = getMaxThreadsPerGang();
	if (ds->dispatchThreads->dispatchCommandParmsArSize < (ds->dispatchThreads->threadCount + max_threads))
	{
		elog(ERROR, "Attempted to reallocate dispatchCommandParmsAr while other threads still running size %d new threadcount %d",
			 ds->dispatchThreads->dispatchCommandParmsArSize, ds->dispatchThreads->threadCount + max_threads);
	}

	pParms = &ds->dispatchThreads->dispatchCommandParmsAr[0];
	newQueryText = dupQueryTextAndSetSliceId(ds->dispatchStateContext, pParms->query_text, pParms->query_text_len, sliceIndex);
	/*
	 * Create the thread parms structures based targetSet parameter.
	 * This will add the segdbDesc pointers appropriate to the
	 * targetSet into the thread Parms structures, making sure that each thread
	 * handles gp_connections_per_thread segdbs.
	 */
	for (i = 0; i < gangSize; i++)
	{
		CdbDispatchResult *qeResult;
		segdbDesc = &db_descriptors[i];
		int parmsIndex = 0;

		Assert(segdbDesc != NULL);

		if (disp_direct->directed_dispatch)
		{
			Assert (disp_direct->count == 1); /* currently we allow direct-to-one dispatch, only */

			if (disp_direct->content[0] != segdbDesc->segment_database_info->segindex)
				continue;
		}

		/* Initialize the QE's CdbDispatchResult object. */
		qeResult = cdbdisp_makeResult(dispatchResults, segdbDesc, sliceIndex);

		if (qeResult == NULL)
		{
			/* writer_gang could be NULL if this is an extended query. */
			if (dispatchResults->writer_gang)
				dispatchResults->writer_gang->dispatcherActive = true;
			elog(FATAL, "could not allocate resources for segworker communication");
		}

		/* Transfer any connection errors from segdbDesc. */
		if (segdbDesc->errcode ||
			segdbDesc->error_message.len)
			cdbdisp_mergeConnectionErrors(qeResult, segdbDesc);

		parmsIndex = gp_connections_per_thread == 0 ? 0 : segdbs_in_thread_pool / gp_connections_per_thread;
		pParms = ds->dispatchThreads->dispatchCommandParmsAr + ds->dispatchThreads->threadCount + parmsIndex;
		pParms->dispatchResultPtrArray[pParms->db_count++] = qeResult;
		if (newQueryText != NULL)
			pParms->query_text = newQueryText;

		/*
		 * This CdbDispatchResult/SegmentDatabaseDescriptor pair will be
		 * dispatched and monitored by a thread to be started below. Only that
		 * thread should touch them until the thread is finished with them and
		 * resets the stillRunning flag. Caller must CdbCheckDispatchResult()
		 * to wait for completion.
		 */
		qeResult->stillRunning = true;

		segdbs_in_thread_pool++;
	}

	/*
	 * Compute the thread count based on how many segdbs were added into the
	 * thread pool, knowing that each thread handles gp_connections_per_thread
	 * segdbs.
	 */
	if (segdbs_in_thread_pool == 0)
		newThreads = 0;
	else if (gp_connections_per_thread == 0)
		newThreads = 1;
	else
		newThreads = 1
				+ (segdbs_in_thread_pool - 1) / gp_connections_per_thread;

	/*
	 * Create the threads. (which also starts the dispatching).
	 */
	for (i = 0; i < newThreads; i++)
	{
		DispatchCommandParms *pParms = &(ds->dispatchThreads->dispatchCommandParmsAr + ds->dispatchThreads->threadCount)[i];
		
		Assert(pParms != NULL);
		
	    if (gp_connections_per_thread==0)
	    {
	    	Assert(newThreads <= 1);
	    	thread_DispatchOut(pParms);
	    }
	    else
	    {
	    	int		pthread_err = 0;

			pParms->thread_valid = true;
			pthread_err = gp_pthread_create(&pParms->thread, thread_DispatchCommand, pParms, "dispatchToGang");

			if (pthread_err != 0)
			{
				int j;
	
				pParms->thread_valid = false;
	
				/*
				 * Error during thread create (this should be caused by
				 * resource constraints). If we leave the threads running,
				 * they'll immediately have some problems -- so we need to
				 * join them, and *then* we can issue our FATAL error
				 */
				pParms->waitMode = DISPATCH_WAIT_CANCEL;

				for (j = 0; j < ds->dispatchThreads->threadCount + (i - 1); j++)
				{
					DispatchCommandParms *pParms;
	
					pParms = &ds->dispatchThreads->dispatchCommandParmsAr[j];

					pParms->waitMode = DISPATCH_WAIT_CANCEL;
					pParms->thread_valid = false;
					pthread_join(pParms->thread, NULL);
				}
	
				ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("could not create thread %d of %d", i + 1, newThreads),
								errdetail("pthread_create() failed with err %d", pthread_err)));
			}
	    }

	}

	ds->dispatchThreads->threadCount += newThreads;
	elog(DEBUG4, "dispatchToGang: Total threads now %d", ds->dispatchThreads->threadCount);
}	/* cdbdisp_dispatchToGang */


/*
 * CdbCheckDispatchResult:
 *
 * Waits for completion of threads launched by cdbdisp_dispatchToGang().
 *
 * QEs that were dispatched with 'cancelOnError' true and are not yet idle
 * will be canceled/finished according to waitMode.
 */
void
CdbCheckDispatchResult(struct CdbDispatcherState *ds,
					   DispatchWaitMode waitMode)
{
	PG_TRY();
	{
		CdbCheckDispatchResultInt(ds, NULL, NULL, waitMode);
	}
	PG_CATCH();
	{
		cdbdisp_clearGangActiveFlag(ds);
		PG_RE_THROW();
	}
	PG_END_TRY();

	cdbdisp_clearGangActiveFlag(ds);

	if (log_dispatch_stats)
		ShowUsage("DISPATCH STATISTICS");
	
	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];

		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG,  (errmsg("duration to dispatch result received from all QEs: %s ms", msec_str)));
				break;
		}					 
	}
}

static void
CdbCheckDispatchResultInt(struct CdbDispatcherState *ds,
						  struct SegmentDatabaseDescriptor *** failedSegDB,
						  int *numOfFailed,
						  DispatchWaitMode waitMode)
{
	int			i;
	int			j;
	int			nFailed = 0;
	DispatchCommandParms *pParms;
	CdbDispatchResult *dispatchResult;
	SegmentDatabaseDescriptor *segdbDesc;

	Assert(ds != NULL);

	if (failedSegDB)
		*failedSegDB = NULL;
	if (numOfFailed)
		*numOfFailed = 0;

	/* No-op if no work was dispatched since the last time we were called.	*/
	if (!ds->dispatchThreads || ds->dispatchThreads->threadCount == 0)
	{
		elog(DEBUG5, "CheckDispatchResult: no threads active");
		return;
	}

	/*
	 * Wait for threads to finish.
	 */
	for (i = 0; i < ds->dispatchThreads->threadCount; i++)
	{							/* loop over threads */		
		pParms = &ds->dispatchThreads->dispatchCommandParmsAr[i];
		Assert(pParms != NULL);

		/* Does caller want to stop short? */
		switch (waitMode)
		{
		case DISPATCH_WAIT_CANCEL:
		case DISPATCH_WAIT_FINISH:
			pParms->waitMode = waitMode;
			break;
		default:
			/* none */
			break;
		}

		if (gp_connections_per_thread==0)
		{
			thread_DispatchWait(pParms);
		}
		else
		{
			elog(DEBUG4, "CheckDispatchResult: Joining to thread %d of %d",
				 i + 1, ds->dispatchThreads->threadCount);
	
			if (pParms->thread_valid)
			{
				int			pthread_err = 0;
				pthread_err = pthread_join(pParms->thread, NULL);
				if (pthread_err != 0)
					elog(FATAL, "CheckDispatchResult: pthread_join failed on thread %d (%lu) of %d (returned %d attempting to join to %lu)",
						 i + 1, 
#ifndef _WIN32
						 (unsigned long) pParms->thread, 
#else
						 (unsigned long) pParms->thread.p,
#endif
						 ds->dispatchThreads->threadCount, pthread_err, (unsigned long)mythread());
			}
		}
		HOLD_INTERRUPTS();
		pParms->thread_valid = false;
		MemSet(&pParms->thread, 0, sizeof(pParms->thread));
		RESUME_INTERRUPTS();

		/*
		 * Examine the CdbDispatchResult objects containing the results
		 * from this thread's QEs.
		 */
		for (j = 0; j < pParms->db_count; j++)
		{						/* loop over QEs managed by one thread */
			dispatchResult = pParms->dispatchResultPtrArray[j];

			if (dispatchResult == NULL)
			{
				elog(LOG, "CheckDispatchResult: result object is NULL ? skipping.");
				continue;
			}

			if (dispatchResult->segdbDesc == NULL)
			{
				elog(LOG, "CheckDispatchResult: result object segment descriptor is NULL ? skipping.");
				continue;
			}

			segdbDesc = dispatchResult->segdbDesc;

			/* segdbDesc error message is unlikely here, but check anyway. */
			if (segdbDesc->errcode ||
				segdbDesc->error_message.len)
				cdbdisp_mergeConnectionErrors(dispatchResult, segdbDesc);

			/* Log the result */
			if (DEBUG2 >= log_min_messages)
				cdbdisp_debugDispatchResult(dispatchResult, DEBUG2, DEBUG3);

			/* Notify FTS to reconnect if connection lost or never connected. */
			if (failedSegDB &&
				PQstatus(segdbDesc->conn) == CONNECTION_BAD)
			{
				/* Allocate storage.  Caller should pfree() it. */
				if (!*failedSegDB)
					*failedSegDB = palloc(sizeof(**failedSegDB) *
										  (2 * getgpsegmentCount() + 1));

				/* Append to broken connection list. */
				(*failedSegDB)[nFailed++] = segdbDesc;
				(*failedSegDB)[nFailed] = NULL;

				if (numOfFailed)
					*numOfFailed = nFailed;
			}

			/*
			 * Zap our SegmentDatabaseDescriptor ptr because it may be
			 * invalidated by the call to FtsHandleNetFailure() below.
			 * Anything we need from there, we should get before this.
			 */
			dispatchResult->segdbDesc = NULL;

		}						/* loop over QEs managed by one thread */
	}							/* loop over threads */

	/* reset thread state (will be destroyed later on in finishCommand) */
    ds->dispatchThreads->threadCount = 0;
			
	/*
	 * It looks like everything went fine, make sure we don't miss a
	 * user cancellation?
	 *
	 * The waitMode argument is NONE when we are doing "normal work".
	 */
	if (waitMode == DISPATCH_WAIT_NONE || waitMode == DISPATCH_WAIT_FINISH)
		CHECK_FOR_INTERRUPTS();
}	/* cdbdisp_checkDispatchResult */


/*--------------------------------------------------------------------*/

/*
 * I refactored this code out of the two routines
 *    cdbdisp_dispatchRMCommand  and  cdbdisp_dispatchDtxProtocolCommand
 * when I thought I might need it in a third place.
 * 
 * Not sure if this makes things cleaner or not
 */
struct pg_result **
cdbdisp_returnResults(CdbDispatchResults *primaryResults,
					  StringInfo errmsgbuf,
					  int *numresults)
{
	CdbDispatchResults *gangResults;
	CdbDispatchResult *dispatchResult;
	PGresult  **resultSets = NULL;
	int			nslots;
	int			nresults = 0;
	int			i;
	int			totalResultCount=0;

	/*
	 * Allocate result set ptr array. Make room for one PGresult ptr per
	 * primary segment db, plus a null terminator slot after the
	 * last entry. The caller must PQclear() each PGresult and free() the
	 * array.
	 */
	nslots = 2 * largestGangsize() + 1;
	resultSets = (struct pg_result **)calloc(nslots, sizeof(*resultSets));

	if (!resultSets)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg("cdbdisp_returnResults failed: out of memory")));

	/* Collect results from primary gang. */
	gangResults = primaryResults;
	if (gangResults)
	{
		totalResultCount = gangResults->resultCount;

		for (i = 0; i < gangResults->resultCount; ++i)
		{
			dispatchResult = &gangResults->resultArray[i];

			/* Append error messages to caller's buffer. */
			cdbdisp_dumpDispatchResult(dispatchResult, false, errmsgbuf);

			/* Take ownership of this QE's PGresult object(s). */
			nresults += cdbdisp_snatchPGresults(dispatchResult,
												resultSets + nresults,
												nslots - nresults - 1);
		}
	}

	/* Put a stopper at the end of the array. */
	Assert(nresults < nslots);
	resultSets[nresults] = NULL;

	/* If our caller is interested, tell them how many sets we're returning. */
	if (numresults != NULL)
		*numresults = totalResultCount;

	return resultSets;
}



/*--------------------------------------------------------------------*/




/* Wait for all QEs to finish, then report any errors from the given
 * CdbDispatchResults objects and free them.  If not all QEs in the
 * associated gang(s) executed the command successfully, throws an
 * error and does not return.  No-op if both CdbDispatchResults ptrs are NULL.
 * This is a convenience function; callers with unusual requirements may
 * instead call CdbCheckDispatchResult(), etc., directly.
 */
void
cdbdisp_finishCommand(struct CdbDispatcherState *ds,
					  void (*handle_results_callback)(CdbDispatchResults *primaryResults, void *ctx),
					  void *ctx)
{
	StringInfoData buf;
	int			errorcode = 0;

	/* If cdbdisp_dispatchToGang() wasn't called, don't wait. */
	if (!ds || !ds->primaryResults)
		return;

	/*
	 * If we are called in the dying sequence, don't touch QE connections.
	 * Anything below could cause ERROR in which case we would miss a chance
	 * to clean up shared memory as this is from AbortTransaction.
	 * QE may stay a bit longer, but since we can consider QD as libpq
	 * client to QE, they will notice that we as a client do not
	 * appear anymore and will finish soon.  Also ERROR report doesn't
	 * go to the client anyway since we are in proc_exit.
	 */
	if (proc_exit_inprogress)
		return;

	/* Wait for all QEs to finish. Don't cancel them. */
	CdbCheckDispatchResult(ds, DISPATCH_WAIT_NONE);

	/* If no errors, free the CdbDispatchResults objects and return. */
	if (ds->primaryResults)
		errorcode = ds->primaryResults->errcode;

	if (!errorcode)
	{
		/* Call the callback function to handle the results */
		if (handle_results_callback != NULL)
			handle_results_callback(ds->primaryResults, ctx);

		cdbdisp_destroyDispatcherState(ds);
		return;
	}

	/* Format error messages from the primary gang. */
	initStringInfo(&buf);
	cdbdisp_dumpDispatchResults(ds->primaryResults, &buf, false);

	cdbdisp_destroyDispatcherState(ds);

	/* Too bad, our gang got an error. */
	PG_TRY();
	{
		ereport(ERROR, (errcode(errorcode),
                        errOmitLocation(true),
						errmsg("%s", buf.data)));
	}
	PG_CATCH();
	{
		pfree(buf.data);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* not reached */
}	/* cdbdisp_finishCommand */

/*
 * cdbdisp_handleError
 *
 * When caller catches an error, the PG_CATCH handler can use this
 * function instead of cdbdisp_finishCommand to wait for all QEs
 * to finish, clean up, and report QE errors if appropriate.
 * This function should be called only from PG_CATCH handlers.
 *
 * This function destroys and frees the given CdbDispatchResults objects.
 * It is a no-op if both CdbDispatchResults ptrs are NULL.
 *
 * On return, the caller is expected to finish its own cleanup and
 * exit via PG_RE_THROW().
 */
void
cdbdisp_handleError(struct CdbDispatcherState *ds)
{
	int     qderrcode;
	bool    useQeError = false;

	qderrcode = elog_geterrcode();

	/* If cdbdisp_dispatchToGang() wasn't called, don't wait. */
	if (!ds || !ds->primaryResults)
		return;

	/*
	 * Request any remaining commands executing on qExecs to stop.
	 * We need to wait for the threads to finish.  This allows for proper
	 * cleanup of the results from the async command executions.
	 * Cancel any QEs still running.
	 */
	CdbCheckDispatchResult(ds, DISPATCH_WAIT_CANCEL);

    /*
     * When a QE stops executing a command due to an error, as a
     * consequence there can be a cascade of interconnect errors
     * (usually "sender closed connection prematurely") thrown in
     * downstream processes (QEs and QD).  So if we are handling
     * an interconnect error, and a QE hit a more interesting error,
     * we'll let the QE's error report take precedence.
     */
	if (qderrcode == ERRCODE_GP_INTERCONNECTION_ERROR)
	{
		bool qd_lost_flag = false;
		char *qderrtext = elog_message();

		if (qderrtext && strcmp(qderrtext, CDB_MOTION_LOST_CONTACT_STRING) == 0)
			qd_lost_flag = true;

		if (ds->primaryResults && ds->primaryResults->errcode)
		{
			if (qd_lost_flag && ds->primaryResults->errcode == ERRCODE_GP_INTERCONNECTION_ERROR)
				useQeError = true;
			else if (ds->primaryResults->errcode != ERRCODE_GP_INTERCONNECTION_ERROR)
				useQeError = true;
		}
	}

    if (useQeError)
    {
        /*
         * Throw the QE's error, catch it, and fall thru to return
         * normally so caller can finish cleaning up.  Afterwards
         * caller must exit via PG_RE_THROW().
         */
        PG_TRY();
        {
            cdbdisp_finishCommand(ds, NULL, NULL);
        }
        PG_CATCH();
        {}                      /* nop; fall thru */
        PG_END_TRY();
    }
    else
    {
        /*
         * Discard any remaining results from QEs; don't confuse matters by
         * throwing a new error.  Any results of interest presumably should
         * have been examined before raising the error that the caller is
         * currently handling.
         */
    	cdbdisp_destroyDispatcherState(ds);
    }
}                               /* cdbdisp_handleError */

bool
cdbdisp_check_estate_for_cancel(struct EState *estate)
{
	struct CdbDispatchResults  *meleeResults;

	Assert(estate);
	Assert(estate->dispatcherState);

	meleeResults = estate->dispatcherState->primaryResults;

	if (meleeResults == NULL) /* cleanup ? */
	{
		return false;
	}

	Assert(meleeResults);

//	if (pleaseCancel || meleeResults->errcode)
	if (meleeResults->errcode)
	{
		return true;
	}

	return false;
}


/*--------------------------------------------------------------------*/


static void
thread_DispatchOut(DispatchCommandParms *pParms)
{
	CdbDispatchResult			*dispatchResult;
	int							i, db_count = pParms->db_count;

	/*
	 * The pParms contains an array of SegmentDatabaseDescriptors
	 * to send commands through to.
	 */
	for (i = 0; i < db_count; i++)
	{
		dispatchResult = pParms->dispatchResultPtrArray[i];
		
		/* Don't use elog, it's not thread-safe */
		if (DEBUG5 >= log_min_messages)
		{
			if (dispatchResult->segdbDesc->conn)
			{
				write_log("thread_DispatchCommand working on %d of %d commands.  asyncStatus %d",
						  i + 1, db_count, dispatchResult->segdbDesc->conn->asyncStatus);
			}
		}

        dispatchResult->hasDispatched = false;
        dispatchResult->sentSignal = DISPATCH_WAIT_NONE;
        dispatchResult->wasCanceled = false;

        if (!shouldStillDispatchCommand(pParms, dispatchResult))
        {
            /* Don't dispatch if cancellation pending or no connection. */
            dispatchResult->stillRunning = false;
            if (PQisBusy(dispatchResult->segdbDesc->conn))
				write_log(" We thought we were done, because !shouldStillDispatchCommand(), but libpq says we are still busy");
			if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
				write_log(" We thought we were done, because !shouldStillDispatchCommand(), but libpq says the connection died?");
        }
        else
        {
            /* Kick off the command over the libpq connection.
             * If unsuccessful, proceed anyway, and check for lost connection below.
             */
			if (PQisBusy(dispatchResult->segdbDesc->conn))
			{
				write_log("Trying to send to busy connection %s  %d %d asyncStatus %d",
					dispatchResult->segdbDesc->whoami, i, db_count, dispatchResult->segdbDesc->conn->asyncStatus);
			}

			if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
			{
				char *msg;

				msg = PQerrorMessage(dispatchResult->segdbDesc->conn);

				write_log("Dispatcher noticed a problem before query transmit: %s (%s)", msg ? msg : "unknown error", dispatchResult->segdbDesc->whoami);

				/* Save error info for later. */
				cdbdisp_appendMessage(dispatchResult, LOG,
									  ERRCODE_GP_INTERCONNECTION_ERROR,
									  "Error before transmit from %s: %s",
									  dispatchResult->segdbDesc->whoami,
									  msg ? msg : "unknown error");

				PQfinish(dispatchResult->segdbDesc->conn);
				dispatchResult->segdbDesc->conn = NULL;
				dispatchResult->stillRunning = false;

				continue;
			}
#ifdef USE_NONBLOCKING
			/* 
			 * In 2000, Tom Lane said:
			 * "I believe that the nonblocking-mode code is pretty buggy, and don't
			 *  recommend using it unless you really need it and want to help debug
			 *  it.."
			 * 
			 * Reading through the code, I'm not convinced the situation has
			 * improved in 2007... I still see some very questionable things
			 * about nonblocking mode, so for now, I'm disabling it.
			 */
			PQsetnonblocking(dispatchResult->segdbDesc->conn, TRUE);
#endif 
			
			dispatchCommand(dispatchResult, pParms->query_text, pParms->query_text_len);
        }
	}
        
#ifdef USE_NONBLOCKING     
        
    /*
     * Is everything sent?  Well, if the network stack was too busy, and we are using
     * nonblocking mode, some of the sends
     * might not have completed.  We can't use SELECT to wait unless they have
     * received their work, or we will wait forever.    Make sure they do.
     */

	{
		bool allsent=true;
		
		/*
		 * debug loop to check to see if this really is needed
		 */
		for (i = 0; i < db_count; i++)
    	{
    		dispatchResult = pParms->dispatchResultPtrArray[i];
    		if (!dispatchResult->stillRunning || !dispatchResult->hasDispatched)
    			continue;
    		if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
    			continue;
    		if (dispatchResult->segdbDesc->conn->outCount > 0)
    		{
    			write_log("Yes, extra flushing is necessary %d",i);
    			break;
    		}
    	}
		
		/*
		 * Check to see if any needed extra flushing.
		 */
		for (i = 0; i < db_count; i++)
    	{
        	int			flushResult;

    		dispatchResult = pParms->dispatchResultPtrArray[i];
    		if (!dispatchResult->stillRunning || !dispatchResult->hasDispatched)
    			continue;
    		if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
    			continue;
    		/*
			 * If data remains unsent, send it.  Else we might be waiting for the
			 * result of a command the backend hasn't even got yet.
			 */
    		flushResult = PQflush(dispatchResult->segdbDesc->conn);
    		/*
    		 * First time, go through the loop without waiting if we can't 
    		 * flush, in case we are using multiple network adapters, and 
    		 * other connections might be able to flush
    		 */
    		if (flushResult > 0)
    		{
    			allsent=false;
    			write_log("flushing didn't finish the work %d",i);
    		}
    		
    	}

        /*
         * our first attempt at doing more flushes didn't get everything out,
         * so we need to continue to try.
         */

		for (i = 0; i < db_count; i++)
    	{
    		dispatchResult = pParms->dispatchResultPtrArray[i];
    		while (PQisnonblocking(dispatchResult->segdbDesc->conn))
    		{
    			PQflush(dispatchResult->segdbDesc->conn);
    			PQsetnonblocking(dispatchResult->segdbDesc->conn, FALSE);
    		}
		}

	}
#endif
}

static void
thread_DispatchWait(DispatchCommandParms		*pParms)
{
	SegmentDatabaseDescriptor	*segdbDesc;
	CdbDispatchResult			*dispatchResult;
	int							i, db_count = pParms->db_count;
	int							timeoutCounter = 0;

	/*
	 * OK, we are finished submitting the command to the segdbs.
	 * Now, we have to wait for them to finish.
	 */
	for (;;)
	{							/* some QEs running */
		int			sock;
		int			n;
		int			nfds = 0;
		int			cur_fds_num = 0;

		/*
		 * Which QEs are still running and could send results to us?
		 */

		for (i = 0; i < db_count; i++)
		{						/* loop to check connection status */
			dispatchResult = pParms->dispatchResultPtrArray[i];
			segdbDesc = dispatchResult->segdbDesc;

			/* Already finished with this QE? */
			if (!dispatchResult->stillRunning)
				continue;

			/* Add socket to fd_set if still connected. */
			sock = PQsocket(segdbDesc->conn);
			if (sock >= 0 &&
				PQstatus(segdbDesc->conn) != CONNECTION_BAD)
			{
				pParms->fds[nfds].fd = sock;
				pParms->fds[nfds].events = POLLIN;
				nfds++;
				Assert(nfds <= pParms->nfds);
			}

			/* Lost the connection. */
			else
			{
				char	   *msg = PQerrorMessage(segdbDesc->conn);

				/* Save error info for later. */
				cdbdisp_appendMessage(dispatchResult, DEBUG1,
									  ERRCODE_GP_INTERCONNECTION_ERROR,
									  "Lost connection to %s.  %s",
									  segdbDesc->whoami,
									  msg ? msg : "");

				/* Free the PGconn object. */
				PQfinish(segdbDesc->conn);
				segdbDesc->conn = NULL;
				dispatchResult->stillRunning = false;	/* he's dead, Jim */
			}
		}						/* loop to check connection status */

		/* Break out when no QEs still running. */
		if (nfds <= 0)
			break;

		/*
		 * bail-out if we are dying.  We should not do much of cleanup
		 * as the main thread is waiting on this thread to finish.  Once
		 * QD dies, QE will recognize it shortly anyway.
		 */
		if (proc_exit_inprogress)
			break;

		/*
		 * Wait for results from QEs.
		 */

		/* Block here until input is available. */
		n = poll(pParms->fds, nfds, DISPATCH_WAIT_TIMEOUT_SEC * 1000);

		if (n < 0)
		{
			int			sock_errno = SOCK_ERRNO;

			if (sock_errno == EINTR)
				continue;

			handlePollError(pParms, db_count, sock_errno);
			continue;
		}

		if (n == 0)
		{
			handlePollTimeout(pParms, db_count, &timeoutCounter, true);
			continue;
		}

		cur_fds_num = 0;
		/*
		 * We have data waiting on one or more of the connections.
		 */
		for (i = 0; i < db_count; i++)
		{						/* input available; receive and process it */
			bool		finished;

			dispatchResult = pParms->dispatchResultPtrArray[i];
			segdbDesc = dispatchResult->segdbDesc;

			/* Skip if already finished or didn't dispatch. */
			if (!dispatchResult->stillRunning)
				continue;

			if (DEBUG4 >= log_min_messages)
				write_log("looking for results from %d of %d",i+1,db_count);

			/* Skip this connection if it has no input available. */
			sock = PQsocket(segdbDesc->conn);
			if (sock >= 0)
				/*
				 * The fds array is shorter than conn array, so the following
				 * match method will use this assumtion.
				 */
				Assert(sock == pParms->fds[cur_fds_num].fd);
			if (sock >= 0 && (sock == pParms->fds[cur_fds_num].fd))
			{
				cur_fds_num++;
				if (!(pParms->fds[cur_fds_num - 1].revents & POLLIN))
					continue;
			}

			if (DEBUG4 >= log_min_messages)
				write_log("PQsocket says there are results from %d",i+1);
			/* Receive and process results from this QE. */
			finished = processResults(dispatchResult);

			/* Are we through with this QE now? */
			if (finished)
			{
				if (DEBUG4 >= log_min_messages)
					write_log("processResults says we are finished with %d:  %s",i+1,segdbDesc->whoami);
				dispatchResult->stillRunning = false;
				if (DEBUG1 >= log_min_messages)
				{
					char		msec_str[32];
					switch (check_log_duration(msec_str, false))
					{
						case 1:
						case 2:
							write_log("duration to dispatch result received from thread %d (seg %d): %s ms", i+1 ,dispatchResult->segdbDesc->segindex,msec_str);
							break;
					}					 
				}
				if (PQisBusy(dispatchResult->segdbDesc->conn))
					write_log("We thought we were done, because finished==true, but libpq says we are still busy");
				
			}
			else
				if (DEBUG4 >= log_min_messages)
					write_log("processResults says we have more to do with %d: %s",i+1,segdbDesc->whoami);
		}						/* input available; receive and process it */

	}							/* some QEs running */
	
}

static void
thread_DispatchWaitSingle(DispatchCommandParms		*pParms)
{
	SegmentDatabaseDescriptor	*segdbDesc;
	CdbDispatchResult			*dispatchResult;
	char * msg = NULL;
	
	/* Assert() cannot be used in threads */
	if (pParms->db_count != 1)
		write_log("Bug... thread_dispatchWaitSingle called with db_count %d",pParms->db_count);
	
	dispatchResult = pParms->dispatchResultPtrArray[0];
	segdbDesc = dispatchResult->segdbDesc;
	
	if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
		/* Lost the connection. */
	{
		msg = PQerrorMessage(segdbDesc->conn);

		/* Save error info for later. */
		cdbdisp_appendMessage(dispatchResult, DEBUG1,
							  ERRCODE_GP_INTERCONNECTION_ERROR,
							  "Lost connection to %s.  %s",
							  segdbDesc->whoami,
							  msg ? msg : "");

		/* Free the PGconn object. */
		PQfinish(segdbDesc->conn);
		segdbDesc->conn = NULL;
		dispatchResult->stillRunning = false;	/* he's dead, Jim */
	}
	else
	{

		PQsetnonblocking(segdbDesc->conn,FALSE);  /* Not necessary, I think */
 
		for(;;)
		{							/* loop to call PQgetResult; will block */
			PGresult   *pRes;
			ExecStatusType resultStatus;
			int			resultIndex = cdbdisp_numPGresult(dispatchResult);

			if (DEBUG4 >= log_min_messages)
				write_log("PQgetResult, resultIndex = %d",resultIndex);
			/* Get one message. */
			pRes = PQgetResult(segdbDesc->conn);

			CollectQEWriterTransactionInformation(segdbDesc, dispatchResult);

			/*
			 * Command is complete when PGgetResult() returns NULL. It is critical
			 * that for any connection that had an asynchronous command sent thru
			 * it, we call PQgetResult until it returns NULL. Otherwise, the next
			 * time a command is sent to that connection, it will return an error
			 * that there's a command pending.
			 */
			if (!pRes)
			{						/* end of results */
				if (DEBUG4 >= log_min_messages)
				{
					/* Don't use elog, it's not thread-safe */
					write_log("%s -> idle", segdbDesc->whoami);
				}
				break;		/* this is normal end of command */
			}						/* end of results */

			/* Attach the PGresult object to the CdbDispatchResult object. */
			cdbdisp_appendResult(dispatchResult, pRes);

			/* Did a command complete successfully? */
			resultStatus = PQresultStatus(pRes);
			if (resultStatus == PGRES_COMMAND_OK ||
				resultStatus == PGRES_TUPLES_OK ||
				resultStatus == PGRES_COPY_IN ||
				resultStatus == PGRES_COPY_OUT)
			{						/* QE reported success */

				/*
				 * Save the index of the last successful PGresult. Can be given to
				 * cdbdisp_getPGresult() to get tuple count, etc.
				 */
				dispatchResult->okindex = resultIndex;

				if (DEBUG3 >= log_min_messages)
				{
					/* Don't use elog, it's not thread-safe */
					char	   *cmdStatus = PQcmdStatus(pRes);

					write_log("%s -> ok %s",
							  segdbDesc->whoami,
							  cmdStatus ? cmdStatus : "(no cmdStatus)");
				}
				
				if (resultStatus == PGRES_COPY_IN ||
					resultStatus == PGRES_COPY_OUT)
					return;
			}						/* QE reported success */

			/* Note QE error.  Cancel the whole statement if requested. */
			else
			{						/* QE reported an error */
				char	   *sqlstate = PQresultErrorField(pRes, PG_DIAG_SQLSTATE);
				int			errcode = 0;

				msg = PQresultErrorMessage(pRes);

				if (DEBUG2 >= log_min_messages)
				{
					/* Don't use elog, it's not thread-safe */
					write_log("%s -> %s %s  %s",
							  segdbDesc->whoami,
							  PQresStatus(resultStatus),
							  sqlstate ? sqlstate : "(no SQLSTATE)",
							  msg ? msg : "");
				}

				/*
				 * Convert SQLSTATE to an error code (ERRCODE_xxx). Use a generic
				 * nonzero error code if no SQLSTATE.
				 */
				if (sqlstate &&
					strlen(sqlstate) == 5)
					errcode = cdbdisp_sqlstate_to_errcode(sqlstate);

				/*
				 * Save first error code and the index of its PGresult buffer
				 * entry.
				 */
				cdbdisp_seterrcode(errcode, resultIndex, dispatchResult);
			}						/* QE reported an error */
		}							/* loop to call PQgetResult; won't block */
		 
		
		if (DEBUG4 >= log_min_messages)
			write_log("processResultsSingle says we are finished with :  %s",segdbDesc->whoami);
		dispatchResult->stillRunning = false;
		if (DEBUG1 >= log_min_messages)
		{
			char		msec_str[32];
			switch (check_log_duration(msec_str, false))
			{
				case 1:
				case 2:
					write_log("duration to dispatch result received from thread (seg %d): %s ms", dispatchResult->segdbDesc->segindex,msec_str);
					break;
			}					 
		}
		if (PQisBusy(dispatchResult->segdbDesc->conn))
			write_log("We thought we were done, because finished==true, but libpq says we are still busy");
	}
}

/*
 * Cleanup routine for the dispatching thread.  This will indicate the thread
 * is not running any longer.
 */
static void
DecrementRunningCount(void *arg)
{
	pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&RunningThreadCount, 1);
}

/*
 * thread_DispatchCommand is the thread proc used to dispatch the command to one or more of the qExecs.
 *
 * NOTE: This function MUST NOT contain elog or ereport statements. (or most any other backend code)
 *		 elog is NOT thread-safe.  Developers should instead use something like:
 *
 *	if (DEBUG3 >= log_min_messages)
 *			write_log("my brilliant log statement here.");
 *
 * NOTE: In threads, we cannot use palloc, because it's not thread safe.
 */
void *
thread_DispatchCommand(void *arg)
{
	DispatchCommandParms		*pParms = (DispatchCommandParms *) arg;

	gp_set_thread_sigmasks();

	/*
	 * Mark that we are runnig a new thread.  The main thread will check
	 * it to see if there is still alive one.  Let's do this after we block
	 * signals so that nobody will intervent and mess up the value.
	 * (should we actually block signals before spawning a thread, as much
	 * like we do in fork??)
	 */
	pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&RunningThreadCount, 1);

	/*
	 * We need to make sure the value will be decremented once the thread
	 * finishes.  Currently there is not such case but potentially we could
	 * have pthread_exit or thread cancellation in the middle of code, in
	 * which case we would miss to decrement value if we tried to do this
	 * without the cleanup callback facility.
	 */
	pthread_cleanup_push(DecrementRunningCount, NULL);
	{
		thread_DispatchOut(pParms);
		/*
		 * thread_DispatchWaitSingle might have a problem with interupts
		 */
		if (pParms->db_count == 1 && false)
			thread_DispatchWaitSingle(pParms);
		else
			thread_DispatchWait(pParms);
	}
	pthread_cleanup_pop(1);

	return (NULL);
}	/* thread_DispatchCommand */


/* Helper function to thread_DispatchCommand that decides if we should dispatch
 * to this segment database.
 *
 * (1) don't dispatch if there is already a query cancel notice pending.
 * (2) make sure our libpq connection is still good.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
bool
shouldStillDispatchCommand(DispatchCommandParms *pParms, CdbDispatchResult * dispatchResult)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	CdbDispatchResults *gangResults = dispatchResult->meleeResults;

	/* Don't dispatch to a QE that is not connected. Note, that PQstatus() correctly
	 * handles the case where segdbDesc->conn is NULL, and we *definitely* want to
	 * produce an error for that case. */
	if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
	{
		char	   *msg = PQerrorMessage(segdbDesc->conn);

		/* Save error info for later. */
		cdbdisp_appendMessage(dispatchResult, LOG,
							  ERRCODE_GP_INTERCONNECTION_ERROR,
							  "Lost connection to %s.  %s",
							  segdbDesc->whoami,
							  msg ? msg : "");

		if (DEBUG4 >= log_min_messages)
		{
			/* Don't use elog, it's not thread-safe */
			write_log("Lost connection: %s", segdbDesc->whoami);
		}

		/* Free the PGconn object at once whenever we notice it's gone bad. */
		PQfinish(segdbDesc->conn);
		segdbDesc->conn = NULL;
		dispatchResult->stillRunning = false;

		return false;
	}

	/*
	 * Don't submit if already encountered an error. The error has already
	 * been noted, so just keep quiet.
	 */
	if (pParms->waitMode == DISPATCH_WAIT_CANCEL || gangResults->errcode)
	{
		if (gangResults->cancelOnError)
		{
			dispatchResult->wasCanceled = true;

			if (Debug_cancel_print || DEBUG4 >= log_min_messages)
			{
				/* Don't use elog, it's not thread-safe */
				write_log("Error cleanup in progress; command not sent to %s", 
						  segdbDesc->whoami);
			}
			return false;
		}
	}

	/*
	 * Don't submit if client told us to cancel. The cancellation request has
	 * already been noted, so hush.
	 */
	if (InterruptPending &&
		gangResults->cancelOnError)
	{
		dispatchResult->wasCanceled = true;
		if (Debug_cancel_print || DEBUG4 >= log_min_messages)
			write_log("Cancellation request pending; command not sent to %s",
					  segdbDesc->whoami);
		return false;
	}

	return true;
}	/* shouldStillDispatchCommand */


/* Helper function to thread_DispatchCommand that actually kicks off the
 * command on the libpq connection.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
static void dispatchCommand(CdbDispatchResult *dispatchResult,
		const char *query_text, int query_text_len)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	PGconn	   *conn = segdbDesc->conn;
	TimestampTz beforeSend = 0;
	long		secs;
	int			usecs;

	if (DEBUG1 >= log_min_messages)
		beforeSend = GetCurrentTimestamp();

	/*
	 * Submit the command asynchronously.
	 */
	if (PQsendGpQuery_shared(conn, (char *)query_text, query_text_len) == 0)
	{
		char	   *msg = PQerrorMessage(segdbDesc->conn);

		if (DEBUG3 >= log_min_messages)
			write_log("PQsendMPPQuery_shared error %s %s",
					  segdbDesc->whoami, msg ? msg : "");

		/* Note the error. */
		cdbdisp_appendMessage(dispatchResult, LOG,
							  ERRCODE_GP_INTERCONNECTION_ERROR,
							  "Command could not be sent to segment db %s;  %s",
							  segdbDesc->whoami, msg ? msg : "");
		PQfinish(conn);
		segdbDesc->conn = NULL;
		dispatchResult->stillRunning = false;
	}
	
	if (DEBUG1 >= log_min_messages)
	{
		TimestampDifference(beforeSend,
							GetCurrentTimestamp(),
							&secs, &usecs);
		
		if (secs != 0 || usecs > 1000) /* Time > 1ms? */
			write_log("time for PQsendGpQuery_shared %ld.%06d", secs, usecs);
	}

    dispatchResult->hasDispatched = true;
	/*
	 * We'll keep monitoring this QE -- whether or not the command
	 * was dispatched -- in order to check for a lost connection
	 * or any other errors that libpq might have in store for us.
	 */
}	/* dispatchCommand */


/* Helper function to thread_DispatchCommand that handles errors that occur
 * during the poll() call.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 *
 * NOTE: The cleanup of the connections will be performed by handlePollTimeout().
 */
void
handlePollError(DispatchCommandParms *pParms,
				  int db_count,
				  int sock_errno)
{
	int			i;
	int			forceTimeoutCount;

	if (LOG >= log_min_messages)
	{
		/* Don't use elog, it's not thread-safe */
		write_log("handlePollError poll() failed; errno=%d", sock_errno);
	}

	/*
	 * Based on the select man page, we could get here with
	 * errno == EBADF (bad descriptor), EINVAL (highest descriptor negative or negative timeout)
	 * or ENOMEM (out of memory).
	 * This is most likely a programming error or a bad system failure, but we'll try to 
	 * clean up a bit anyhow.
	 *
	 * MPP-3551: We *can* get here as a result of some hardware issues. the timeout code
	 * knows how to clean up if we've lost contact with one of our peers.
	 *
	 * We should check a connection's integrity before calling PQisBusy().
	 */
	for (i = 0; i < db_count; i++)
	{
		CdbDispatchResult *dispatchResult = pParms->dispatchResultPtrArray[i];

		/* Skip if already finished or didn't dispatch. */
		if (!dispatchResult->stillRunning)
			continue;

		/* We're done with this QE, sadly. */
		if (PQstatus(dispatchResult->segdbDesc->conn) == CONNECTION_BAD)
		{
			char *msg;

			msg = PQerrorMessage(dispatchResult->segdbDesc->conn);
			if (msg)
				write_log("Dispatcher encountered connection error on %s: %s",
						  dispatchResult->segdbDesc->whoami, msg);

			write_log("Dispatcher noticed bad connection in handlePollError()");

			/* Save error info for later. */
			cdbdisp_appendMessage(dispatchResult, LOG,
								  ERRCODE_GP_INTERCONNECTION_ERROR,
								  "Error after dispatch from %s: %s",
								  dispatchResult->segdbDesc->whoami,
								  msg ? msg : "unknown error");

			PQfinish(dispatchResult->segdbDesc->conn);
			dispatchResult->segdbDesc->conn = NULL;
			dispatchResult->stillRunning = false;
		}
	}

	forceTimeoutCount = 60; /* anything bigger than 30 */
	handlePollTimeout(pParms, db_count, &forceTimeoutCount, false);

	return;

	/* No point in trying to cancel the other QEs with select() broken. */
}	/* handlePollError */

/*
 * Send cancel/finish signal to still-running QE through libpq.
 * waitMode is either CANCEL or FINISH.  Returns true if we successfully
 * sent a signal (not necessarily received by the target process).
 */
static DispatchWaitMode
cdbdisp_signalQE(SegmentDatabaseDescriptor *segdbDesc,
				 DispatchWaitMode waitMode)
{
	char errbuf[256];
	PGcancel *cn = PQgetCancel(segdbDesc->conn);
	int		ret = 0;

	if (cn == NULL)
		return DISPATCH_WAIT_NONE;

	/*
	 * PQcancel uses some strcpy/strcat functions; let's
	 * clear this for safety.
	 */
	MemSet(errbuf, 0, sizeof(errbuf));

	if (Debug_cancel_print || DEBUG4 >= log_min_messages)
		write_log("Calling PQcancel for %s", segdbDesc->whoami);

	/*
	 * Send query-finish, unless the client really wants to cancel the
	 * query.  This could happen if cancel comes after we sent finish.
	 */
	if (waitMode == DISPATCH_WAIT_CANCEL)
		ret = PQcancel(cn, errbuf, 256);
	else if (waitMode == DISPATCH_WAIT_FINISH)
		ret = PQrequestFinish(cn, errbuf, 256);
	else
		write_log("unknown waitMode: %d", waitMode);

	if (ret == 0 && (Debug_cancel_print || LOG >= log_min_messages))
		write_log("Unable to cancel: %s", errbuf);

	PQfreeCancel(cn);

	return (ret != 0 ? waitMode : DISPATCH_WAIT_NONE);
}

/* Helper function to thread_DispatchCommand that handles timeouts that occur
 * during the poll() call.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
void
handlePollTimeout(DispatchCommandParms * pParms,
					int db_count,
					int *timeoutCounter, bool useSampling)
{
	CdbDispatchResult *dispatchResult;
	CdbDispatchResults *meleeResults;
	SegmentDatabaseDescriptor *segdbDesc;
	int			i;

	/*
	 * Are there any QEs that should be canceled?
	 *
	 * CDB TODO: PQcancel() is expensive, and we do them
	 *			 serially.	Just do a few each time; save some
	 *			 for the next timeout.
	 */
	for (i = 0; i < db_count; i++)
	{							/* loop to check connection status */
		DispatchWaitMode		waitMode;

		dispatchResult = pParms->dispatchResultPtrArray[i];
		if (dispatchResult == NULL)
			continue;
		segdbDesc = dispatchResult->segdbDesc;
		meleeResults = dispatchResult->meleeResults;

		/* Already finished with this QE? */
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
		 *   - user interrupt has occurred,
		 *   - or I'm told to send cancel,
		 *   - or an error has been reported by another QE,
		 *   - in case the caller wants cancelOnError and it was not canceled
		 */
		if ((InterruptPending ||
			pParms->waitMode == DISPATCH_WAIT_CANCEL ||
			meleeResults->errcode) &&
				(meleeResults->cancelOnError &&
				 !dispatchResult->wasCanceled))
			waitMode = DISPATCH_WAIT_CANCEL;

		/*
		 * Finally, don't send the signal if
		 *   - no action needed (NONE)
		 *   - the signal was already sent
		 *   - connection is dead
		 */
		if (waitMode != DISPATCH_WAIT_NONE &&
			waitMode != dispatchResult->sentSignal &&
			PQstatus(segdbDesc->conn) != CONNECTION_BAD)
		{
			dispatchResult->sentSignal =
				cdbdisp_signalQE(segdbDesc, waitMode);
		}
	}

	/*
	 * check the connection still valid, set 1 min time interval
	 * this may affect performance, should turn it off if required.
	 */
	if ((*timeoutCounter)++ > 30)
	{
		*timeoutCounter = 0;

		for (i = 0; i < db_count; i++)
		{
			dispatchResult = pParms->dispatchResultPtrArray[i];
			segdbDesc = dispatchResult->segdbDesc;

			if (DEBUG5 >= log_min_messages)
				write_log("checking status %d of %d     %s stillRunning %d",
						  i+1, db_count, segdbDesc->whoami, dispatchResult->stillRunning);

			/* Skip if already finished or didn't dispatch. */
			if (!dispatchResult->stillRunning)
				continue;

			/*
			 * If we hit the timeout, and the query has already been
			 * cancelled we'll try to re-cancel here.
			 *
			 * XXX we may not need this anymore.  It might be harmful
			 * rather than helpful, as it creates another connection.
			 */
			if (dispatchResult->sentSignal == DISPATCH_WAIT_CANCEL &&
				PQstatus(segdbDesc->conn) != CONNECTION_BAD)
			{
				dispatchResult->sentSignal =
					cdbdisp_signalQE(segdbDesc, DISPATCH_WAIT_CANCEL);
			}

			/* Skip the entry db. */
			if (segdbDesc->segindex < 0)
				continue;

			if (DEBUG5 >= log_min_messages)
				write_log("testing connection %d of %d     %s stillRunning %d",
						  i+1, db_count, segdbDesc->whoami, dispatchResult->stillRunning);

			if (!FtsTestConnection(segdbDesc->segment_database_info, false))
			{
				/* Note the error. */
				cdbdisp_appendMessage(dispatchResult, DEBUG1,
									  ERRCODE_GP_INTERCONNECTION_ERROR,
									  "Lost connection to one or more segments - fault detector checking for segment failures. (%s)",
									  segdbDesc->whoami);

				/*
				 * Not a good idea to store into the PGconn object. Instead,
				 * just close it.
				 */
				PQfinish(segdbDesc->conn);
				segdbDesc->conn = NULL;

				/* This connection is hosed. */
				dispatchResult->stillRunning = false;
			}
		}
	}

}	/* handlePollTimeout */

void
CollectQEWriterTransactionInformation(SegmentDatabaseDescriptor *segdbDesc, CdbDispatchResult *dispatchResult)
{
	PGconn *conn = segdbDesc->conn;
	
	if (conn && conn->QEWriter_HaveInfo)
	{
		dispatchResult->QEIsPrimary = true;
		dispatchResult->QEWriter_HaveInfo = true;
		dispatchResult->QEWriter_DistributedTransactionId = conn->QEWriter_DistributedTransactionId;
		dispatchResult->QEWriter_CommandId = conn->QEWriter_CommandId;
		if (conn && conn->QEWriter_Dirty)
		{
			dispatchResult->QEWriter_Dirty = true;
		}
	}
}

bool							/* returns true if command complete */
processResults(CdbDispatchResult *dispatchResult)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	char	   *msg;
	int			rc;

	/* MPP-2518: PQisBusy() has side-effects */
	if (DEBUG5 >= log_min_messages)
	{
		write_log("processResults.  isBusy = %d", PQisBusy(segdbDesc->conn));

		if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
			goto connection_error;
	}

	/* Receive input from QE. */
	rc = PQconsumeInput(segdbDesc->conn);

	/* If PQconsumeInput fails, we're hosed. */
	if (rc == 0)
	{ /* handle PQconsumeInput error */
		goto connection_error;
	}

	/* MPP-2518: PQisBusy() has side-effects */
	if (DEBUG4 >= log_min_messages && PQisBusy(segdbDesc->conn))
		write_log("PQisBusy");
			
	/* If we have received one or more complete messages, process them. */
	while (!PQisBusy(segdbDesc->conn))
	{							/* loop to call PQgetResult; won't block */
		PGresult   *pRes;
		ExecStatusType resultStatus;
		int			resultIndex;

		/* MPP-2518: PQisBusy() does some error handling, which can
		 * cause the connection to die -- we can't just continue on as
		 * if the connection is happy without checking first. 
		 *
		 * For example, cdbdisp_numPGresult() will return a completely
		 * bogus value! */
		if (PQstatus(segdbDesc->conn) == CONNECTION_BAD || segdbDesc->conn->sock == -1)
		{ /* connection is dead. */
			goto connection_error;
		}

		resultIndex = cdbdisp_numPGresult(dispatchResult);

		if (DEBUG4 >= log_min_messages)
			write_log("PQgetResult");
		/* Get one message. */
		pRes = PQgetResult(segdbDesc->conn);
		
		CollectQEWriterTransactionInformation(segdbDesc, dispatchResult);

		/*
		 * Command is complete when PGgetResult() returns NULL. It is critical
		 * that for any connection that had an asynchronous command sent thru
		 * it, we call PQgetResult until it returns NULL. Otherwise, the next
		 * time a command is sent to that connection, it will return an error
		 * that there's a command pending.
		 */
		if (!pRes)
		{						/* end of results */
			if (DEBUG4 >= log_min_messages)
			{
				/* Don't use elog, it's not thread-safe */
				write_log("%s -> idle", segdbDesc->whoami);
			}
						
			return true;		/* this is normal end of command */
		}						/* end of results */

		
		/* Attach the PGresult object to the CdbDispatchResult object. */
		cdbdisp_appendResult(dispatchResult, pRes);

		/* Did a command complete successfully? */
		resultStatus = PQresultStatus(pRes);
		if (resultStatus == PGRES_COMMAND_OK ||
			resultStatus == PGRES_TUPLES_OK ||
			resultStatus == PGRES_COPY_IN ||
			resultStatus == PGRES_COPY_OUT)
		{						/* QE reported success */

			/*
			 * Save the index of the last successful PGresult. Can be given to
			 * cdbdisp_getPGresult() to get tuple count, etc.
			 */
			dispatchResult->okindex = resultIndex;

			if (DEBUG3 >= log_min_messages)
			{
				/* Don't use elog, it's not thread-safe */
				char	   *cmdStatus = PQcmdStatus(pRes);

				write_log("%s -> ok %s",
						  segdbDesc->whoami,
						  cmdStatus ? cmdStatus : "(no cmdStatus)");
			}
			
			/* SREH - get number of rows rejected from QE if any */
			if(pRes->numRejected > 0)
				dispatchResult->numrowsrejected += pRes->numRejected;

			if (resultStatus == PGRES_COPY_IN ||
				resultStatus == PGRES_COPY_OUT)
				return true;
		}						/* QE reported success */

		/* Note QE error.  Cancel the whole statement if requested. */
		else
		{						/* QE reported an error */
			char	   *sqlstate = PQresultErrorField(pRes, PG_DIAG_SQLSTATE);
			int			errcode = 0;

			msg = PQresultErrorMessage(pRes);

			if (DEBUG2 >= log_min_messages)
			{
				/* Don't use elog, it's not thread-safe */
				write_log("%s -> %s %s  %s",
						  segdbDesc->whoami,
						  PQresStatus(resultStatus),
						  sqlstate ? sqlstate : "(no SQLSTATE)",
						  msg ? msg : "");
			}

			/*
			 * Convert SQLSTATE to an error code (ERRCODE_xxx). Use a generic
			 * nonzero error code if no SQLSTATE.
			 */
			if (sqlstate &&
				strlen(sqlstate) == 5)
				errcode = cdbdisp_sqlstate_to_errcode(sqlstate);

			/*
			 * Save first error code and the index of its PGresult buffer
			 * entry.
			 */
			cdbdisp_seterrcode(errcode, resultIndex, dispatchResult);
		}						/* QE reported an error */
	}							/* loop to call PQgetResult; won't block */
	
	return false;				/* we must keep on monitoring this socket */

connection_error:
	msg = PQerrorMessage(segdbDesc->conn);

	if (msg)
		write_log("Dispatcher encountered connection error on %s: %s", segdbDesc->whoami, msg);

	/* Save error info for later. */
	cdbdisp_appendMessage(dispatchResult, LOG,
						  ERRCODE_GP_INTERCONNECTION_ERROR,
						  "Error on receive from %s: %s",
						  segdbDesc->whoami,
						  msg ? msg : "unknown error");

	/* Can't recover, so drop the connection. */
	PQfinish(segdbDesc->conn);
	segdbDesc->conn = NULL;
	dispatchResult->stillRunning = false;

	return true; /* connection is gone! */	
}	/* processResults */


/*--------------------------------------------------------------------*/




	

/*--------------------------------------------------------------------*/



/*
 * Let's evaluate all STABLE functions that have constant args before dispatch, so we get a consistent
 * view across QEs
 *
 * Also, if this is a single_row insert, let's evaluate nextval() and currval() before dispatching
 *
 */

static Node *
pre_dispatch_function_evaluation_mutator(Node *node,
										 pre_dispatch_function_evaluation_context * context)
{
	Node * new_node = 0;
	
	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		/* Not replaceable, so just copy the Param (no need to recurse) */
		return (Node *) copyObject(param);
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;
		List	   *args;
		ListCell   *arg;
		Expr	   *simple;
		FuncExpr   *newexpr;
		bool		has_nonconst_input;

		Form_pg_proc funcform;
		EState	   *estate;
		ExprState  *exprstate;
		MemoryContext oldcontext;
		Datum		const_val;
		bool		const_is_null;
		int16		resultTypLen;
		bool		resultTypByVal;

		Oid			funcid;
		HeapTuple	func_tuple;


		/*
		 * Reduce constants in the FuncExpr's arguments.  We know args is
		 * either NIL or a List node, so we can call expression_tree_mutator
		 * directly rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
												pre_dispatch_function_evaluation_mutator,
												(void *) context);
										
		funcid = expr->funcid;

		newexpr = makeNode(FuncExpr);
		newexpr->funcid = expr->funcid;
		newexpr->funcresulttype = expr->funcresulttype;
		newexpr->funcretset = expr->funcretset;
		newexpr->funcformat = expr->funcformat;
		newexpr->args = args;

		/*
		 * Check for constant inputs
		 */
		has_nonconst_input = false;
		
		foreach(arg, args)
		{
			if (!IsA(lfirst(arg), Const))
			{
				has_nonconst_input = true;
				break;
			}
		}
		
		if (!has_nonconst_input)
		{
			bool is_seq_func = false;
			bool tup_or_set;
			cqContext	*pcqCtx;

			pcqCtx = caql_beginscan(
					NULL,
					cql("SELECT * FROM pg_proc "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(funcid)));

			func_tuple = caql_getnext(pcqCtx);

			if (!HeapTupleIsValid(func_tuple))
				elog(ERROR, "cache lookup failed for function %u", funcid);

			funcform = (Form_pg_proc) GETSTRUCT(func_tuple);

			/* can't handle set returning or row returning functions */
			tup_or_set = (funcform->proretset || 
						  type_is_rowtype(funcform->prorettype));

			caql_endscan(pcqCtx);
			
			/* can't handle it */
			if (tup_or_set)
			{
				/* 
				 * We haven't mutated this node, but we still return the
				 * mutated arguments.
				 *
				 * If we don't do this, we'll miss out on transforming function
				 * arguments which are themselves functions we need to mutated.
				 * For example, select foo(now()).
				 *
				 * See MPP-3022 for what happened when we didn't do this.
				 */
				return (Node *)newexpr;
			}

			/* 
			 * Ignored evaluation of gp_partition stable functions.
			 * TODO: garcic12 - May 30, 2013, refactor gp_partition stable functions to be truly
			 * stable (JIRA: MPP-19541).
			 */
			if (funcid == GP_PARTITION_SELECTION_OID 
				|| funcid == GP_PARTITION_EXPANSION_OID 
				|| funcid == GP_PARTITION_INVERSE_OID)
			{
				return (Node *)newexpr;
			}

			/* 
			 * Related to MPP-1429.  Here we want to mark any statement that is
			 * going to use a sequence as dirty.  Doing this means that the
			 * QD will flush the xlog which will also flush any xlog writes that
			 * the sequence server might do. 
			 */
			if (funcid == NEXTVAL_FUNC_OID || funcid == CURRVAL_FUNC_OID ||
				funcid == SETVAL_FUNC_OID)
			{
				ExecutorMarkTransactionUsesSequences();
				is_seq_func = true;
			}

			if (funcform->provolatile == PROVOLATILE_IMMUTABLE)
				/* okay */ ;
			else if (funcform->provolatile == PROVOLATILE_STABLE)
				/* okay */ ;
			else if (context->single_row_insert && is_seq_func)
				;				/* Volatile, but special sequence function */
			else
				return (Node *)newexpr;

			/*
			 * Ok, we have a function that is STABLE (or IMMUTABLE), with
			 * constant args. Let's try to evaluate it.
			 */

			/*
			 * To use the executor, we need an EState.
			 */
			estate = CreateExecutorState();

			/* We can use the estate's working context to avoid memory leaks. */
			oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

			/*
			 * Prepare expr for execution.
			 */
			exprstate = ExecPrepareExpr((Expr *) newexpr, estate);

			/*
			 * And evaluate it.
			 *
			 * It is OK to use a default econtext because none of the
			 * ExecEvalExpr() code used in this situation will use econtext.
			 * That might seem fortuitous, but it's not so unreasonable --- a
			 * constant expression does not depend on context, by definition,
			 * n'est-ce pas?
			 */
			const_val =
				ExecEvalExprSwitchContext(exprstate,
										  GetPerTupleExprContext(estate),
										  &const_is_null, NULL);

			/* Get info needed about result datatype */
			get_typlenbyval(expr->funcresulttype, &resultTypLen, &resultTypByVal);

			/* Get back to outer memory context */
			MemoryContextSwitchTo(oldcontext);

			/* Must copy result out of sub-context used by expression eval */
			if (!const_is_null)
				const_val = datumCopy(const_val, resultTypByVal, resultTypLen);

			/* Release all the junk we just created */
			FreeExecutorState(estate);

			/*
			 * Make the constant result node.
			 */
			simple = (Expr *) makeConst(expr->funcresulttype, -1, resultTypLen,
										const_val, const_is_null,
										resultTypByVal);

			if (simple)			/* successfully simplified it */
				return (Node *) simple;
		}

		/*
		 * The expression cannot be simplified any further, so build and
		 * return a replacement FuncExpr node using the possibly-simplified
		 * arguments.
		 */
		return (Node *) newexpr;
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;
		List	   *args;

		OpExpr	   *newexpr;

		/*
		 * Reduce constants in the OpExpr's arguments.  We know args is either
		 * NIL or a List node, so we can call expression_tree_mutator directly
		 * rather than recursing to self.
		 */
		args = (List *) expression_tree_mutator((Node *) expr->args,
												pre_dispatch_function_evaluation_mutator,
												(void *) context);

		/*
		 * Need to get OID of underlying function.	Okay to scribble on input
		 * to this extent.
		 */
		set_opfuncid(expr);

		newexpr = makeNode(OpExpr);
		newexpr->opno = expr->opno;
		newexpr->opfuncid = expr->opfuncid;
		newexpr->opresulttype = expr->opresulttype;
		newexpr->opretset = expr->opretset;
		newexpr->args = args;

		return (Node *) newexpr;
	}
	else if (IsA(node, CurrentOfExpr))
	{
		/*
		 * updatable cursors 
		 *
		 * During constant folding, the CurrentOfExpr's gp_segment_id, ctid, 
		 * and tableoid fields are filled in with observed values from the 
		 * referenced cursor. For more detail, see bindCurrentOfParams below.
		 */
		CurrentOfExpr *expr = (CurrentOfExpr *) node,
					  *newexpr = copyObject(expr);

		bindCurrentOfParams(newexpr->cursor_name,
							newexpr->target_relid,
			   		   		&newexpr->ctid,
					  	   	&newexpr->gp_segment_id,
					  	   	&newexpr->tableoid);
		return (Node *) newexpr;
	}
	
	/*
	 * For any node type not handled above, we recurse using
	 * plan_tree_mutator, which will copy the node unchanged but try to
	 * simplify its arguments (if any) using this routine.
	 */
	new_node =  plan_tree_mutator(node, pre_dispatch_function_evaluation_mutator,
								  (void *) context);

	return new_node;
}

/*
 * bindCurrentOfParams
 *
 * During constant folding, we evaluate STABLE functions to give QEs a consistent view
 * of the query. At this stage, we will also bind observed values of 
 * gp_segment_id/ctid/tableoid into the CurrentOfExpr.
 * This binding must happen only after planning, otherwise we disrupt prepared statements.
 * Furthermore, this binding must occur before dispatch, because a QE lacks the 
 * the information needed to discern whether it's responsible for the currently 
 * positioned tuple.
 *
 * The design of this parameter binding is very tightly bound to the parse/analyze
 * and subsequent planning of DECLARE CURSOR. We depend on the "is_simply_updatable"
 * calculation of parse/analyze to decide whether CURRENT OF makes sense for the
 * referenced cursor. Moreover, we depend on the ensuing planning of DECLARE CURSOR
 * to provide the junk metadata of gp_segment_id/ctid/tableoid (per tuple).
 *
 * This function will lookup the portal given by "cursor_name". If it's simply updatable,
 * we'll glean gp_segment_id/ctid/tableoid from the portal's most recently fetched 
 * (raw) tuple. We bind this information into the CurrentOfExpr to precisely identify
 * the currently scanned tuple, ultimately for consumption of TidScan/execQual by the QEs.
 */
static void
bindCurrentOfParams(char *cursor_name, Oid target_relid, ItemPointer ctid, int *gp_segment_id, Oid *tableoid)
{
	char 			*table_name;
	Portal			portal;
	QueryDesc		*queryDesc;
	AttrNumber		gp_segment_id_attno;
	AttrNumber		ctid_attno;
	AttrNumber		tableoid_attno;
	bool			isnull;
	Datum			value;

	portal = GetPortalByName(cursor_name);
	if (!PortalIsValid(portal))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" does not exist", cursor_name)));

	queryDesc = PortalGetQueryDesc(portal);
	if (queryDesc == NULL)
		ereport(ERROR, 
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is held from a previous transaction", cursor_name)));

	/* obtain table_name for potential error messages */
	table_name = get_rel_name(target_relid);

	/* 
	 * The referenced cursor must be simply updatable. This has already
	 * been discerned by parse/analyze for the DECLARE CURSOR of the given
	 * cursor. This flag assures us that gp_segment_id, ctid, and tableoid (if necessary)
 	 * will be available as junk metadata, courtesy of preprocess_targetlist.
	 */
	if (!portal->is_simply_updatable)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not a simply updatable scan of table \"%s\"",
						cursor_name, table_name)));

	/* 
	 * The target relation must directly match the cursor's relation. This throws out
	 * the simple case in which a cursor is declared against table X and the update is
	 * issued against Y. Moreover, this disallows some subtler inheritance cases where
	 * Y inherits from X. While such cases could be implemented, it seems wiser to
	 * simply error out cleanly.
	 */
	Index varno = extractSimplyUpdatableRTEIndex(queryDesc->plannedstmt->rtable);
	Oid cursor_relid = getrelid(varno, queryDesc->plannedstmt->rtable);
	if (target_relid != cursor_relid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not a simply updatable scan of table \"%s\"",
						cursor_name, table_name)));
	/* 
	 * The cursor must have a current result row: per the SQL spec, it's 
	 * an error if not.
	 */
	if (portal->atStart || portal->atEnd)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_STATE),
				 errmsg("cursor \"%s\" is not positioned on a row", cursor_name)));

	/*
	 * As mentioned above, if parse/analyze recognized this cursor as simply
	 * updatable during DECLARE CURSOR, then its subsequent planning must have
	 * made gp_segment_id, ctid, and tableoid available as junk for each tuple.
	 *
	 * To retrieve this junk metadeta, we leverage the EState's junkfilter against
	 * the raw tuple yielded by the highest most node in the plan.
	 */
	TupleTableSlot *slot = queryDesc->planstate->ps_ResultTupleSlot;
	Insist(!TupIsNull(slot));
	Assert(queryDesc->estate->es_junkFilter);

	/* extract gp_segment_id metadata */
	gp_segment_id_attno = ExecFindJunkAttribute(queryDesc->estate->es_junkFilter, "gp_segment_id");
	if (!AttributeNumberIsValid(gp_segment_id_attno))
		elog(ERROR, "could not find junk gp_segment_id column");
	value = ExecGetJunkAttribute(slot, gp_segment_id_attno, &isnull);
	if (isnull)
		elog(ERROR, "gp_segment_id is NULL");
	*gp_segment_id = DatumGetInt32(value);

	/* extract ctid metadata */
	ctid_attno = ExecFindJunkAttribute(queryDesc->estate->es_junkFilter, "ctid");
	if (!AttributeNumberIsValid(ctid_attno))
		elog(ERROR, "could not find junk ctid column");
	value = ExecGetJunkAttribute(slot, ctid_attno, &isnull);
	if (isnull)
		elog(ERROR, "ctid is NULL");
	ItemPointerCopy(DatumGetItemPointer(value), ctid);

	/* 
	 * extract tableoid metadata
	 *
	 * DECLARE CURSOR planning only includes tableoid metadata when
	 * scrolling a partitioned table, as this is the only case in which
	 * gp_segment_id/ctid alone do not suffice to uniquely identify a tuple.
	 */
	tableoid_attno = ExecFindJunkAttribute(queryDesc->estate->es_junkFilter,
										   "tableoid");
	if (AttributeNumberIsValid(tableoid_attno))
	{
		value = ExecGetJunkAttribute(slot, tableoid_attno, &isnull);
		if (isnull)
			elog(ERROR, "tableoid is NULL");
		*tableoid = DatumGetObjectId(value);

		/*
		 * This is our last opportunity to verify that the physical table given
		 * by tableoid is, indeed, simply updatable.
		 */
		if (!isSimplyUpdatableRelation(*tableoid))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("%s is not updatable",
							get_rel_name_partition(*tableoid))));
	} else
		*tableoid = InvalidOid;

	pfree(table_name);
}


/*
 * Evaluate functions to constants.
 */
Node *
exec_make_plan_constant(struct PlannedStmt *stmt, bool is_SRI)
{
	pre_dispatch_function_evaluation_context pcontext;

	Assert(stmt);
	exec_init_plan_tree_base(&pcontext.base, stmt);
	pcontext.single_row_insert = is_SRI;

	return plan_tree_mutator((Node *)stmt->planTree, pre_dispatch_function_evaluation_mutator, &pcontext);
}

Node *
planner_make_plan_constant(struct PlannerInfo *root, Node *n, bool is_SRI)
{
	pre_dispatch_function_evaluation_context pcontext;

	planner_init_plan_tree_base(&pcontext.base, root);
	pcontext.single_row_insert = is_SRI;

	return plan_tree_mutator(n, pre_dispatch_function_evaluation_mutator, &pcontext);
}






/*
 * Synchronize threads to finish for this process to die.  Dispatching
 * threads need to acknowledge that we are dying, otherwise the main
 * thread will cleanup memory contexts which could cause process crash
 * while the threads are touching stale pointers.  Threads will check
 * proc_exit_inprogress and immediately stops once it's found to be true.
 */
void
cdbdisp_waitThreads(void)
{
	int i, max_retry;
	long interval = 10 * 1000; /* 10 msec */

	/*
	 * Just in case to avoid to be stuck in the final stage of process
	 * lifecycle, insure by setting time limit.  If it exceeds, it probably
	 * means some threads are stuck and not progressing, in which case
	 * we can go ahead and cleanup things anyway.  The duration should be
	 * longer than the select timeout in thread_DispatchWait.
	 */
	max_retry = (DISPATCH_WAIT_TIMEOUT_SEC + 10) * 1000000L / interval;

	/* This is supposed to be called after the flag is set. */
	Assert(proc_exit_inprogress);

	for (i = 0; i < max_retry; i++)
	{
		if (RunningThreadCount == 0)
			break;
		pg_usleep(interval);
	}
}


static int getMaxThreadsPerGang()
{
	int maxThreads = 0;
	if (gp_connections_per_thread == 0)
		maxThreads = 1;	/* one, not zero, because we need to allocate one param block */
	else
		maxThreads = 1 + (largestGangsize() - 1) / gp_connections_per_thread;
	return maxThreads;
}

/*
 * cdbdisp_makeDispatchThreads:
 * Allocates memory for a CdbDispatchCmdThreads structure and the memory
 * needed inside. Do the initialization.
 * Will be freed in function cdbdisp_destroyDispatcherState by deleting the
 * memory context.
 */
static CdbDispatchCmdThreads *
cdbdisp_makeDispatchThreads(int maxThreads)
{
	int maxConn =
			gp_connections_per_thread == 0 ?
					largestGangsize() : gp_connections_per_thread;
	int size = 0;
	int i = 0;
	CdbDispatchCmdThreads *dThreads = palloc0(sizeof(*dThreads));

	size = maxThreads * sizeof(DispatchCommandParms);
	dThreads->dispatchCommandParmsAr = (DispatchCommandParms *) palloc0(size);
	dThreads->dispatchCommandParmsArSize = maxThreads;
	dThreads->threadCount = 0;

	for (i = 0; i < maxThreads; i++)
	{
		DispatchCommandParms *pParms = &dThreads->dispatchCommandParmsAr[i];

		pParms->nfds = maxConn;
		MemSet(&pParms->thread, 0, sizeof(pthread_t));

		size = maxConn * sizeof(CdbDispatchResult *);
		pParms->dispatchResultPtrArray = (CdbDispatchResult **) palloc0(size);

		size = sizeof(struct pollfd) * maxConn;
		pParms->fds = (struct pollfd *) palloc0(size);
	}

	return dThreads;
} /* cdbdisp_makeDispatchThreads */

/*
 * cdbdisp_makeDispatchResults:
 * Allocates a CdbDispatchResults object in the current memory context.
 * Will be freed in function cdbdisp_destroyDispatcherState by deleting the
 * memory context.
 */
static CdbDispatchResults *
cdbdisp_makeDispatchResults(int resultCapacity, int sliceCapacity,
		bool cancelOnError)
{
	CdbDispatchResults *results = palloc0(sizeof(*results));
	int nbytes = resultCapacity * sizeof(results->resultArray[0]);

	results->resultArray = palloc0(nbytes);
	results->resultCapacity = resultCapacity;
	results->resultCount = 0;
	results->iFirstError = -1;
	results->errcode = 0;
	results->cancelOnError = cancelOnError;

	results->sliceMap = NULL;
	results->sliceCapacity = sliceCapacity;
	if (sliceCapacity > 0)
	{
		nbytes = sliceCapacity * sizeof(results->sliceMap[0]);
		results->sliceMap = palloc0(nbytes);
	}

	return results;
} /* cdbdisp_makeDispatchResults */


/*
 * Allocate memory and initialize CdbDispatcherState.
 *
 * Call cdbdisp_destroyDispatcherState to free it.
 *
 *   maxResults: max number of results, normally equals to max number of QEs.
 *   maxSlices: max number of slices of the query/command.
 */
void
cdbdisp_makeDispatcherState(CdbDispatcherState *ds, int maxResults,
		int maxSlices, bool cancelOnError)
{
	int maxThreadsPerGang = getMaxThreadsPerGang();
	/* the maximum number of command parameter blocks we'll possibly need is
	 * one for each slice on the primary gang. Max sure that we
	 * have enough -- once we've created the command block we're stuck with it
	 * for the duration of this statement (including CDB-DTM ).
	 * X 2 for good measure ? */
	int maxThreads = maxThreadsPerGang * 4 * Max(maxSlices, 5);
	MemoryContext oldContext = NULL;

	Assert(ds != NULL);
	Assert(ds->dispatchStateContext == NULL);
	Assert(ds->dispatchThreads == NULL);
	Assert(ds->primaryResults == NULL);

	ds->dispatchStateContext = AllocSetContextCreate(TopMemoryContext,
			"Dispatch Context",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(ds->dispatchStateContext);
	ds->primaryResults = cdbdisp_makeDispatchResults(maxResults, maxSlices,
			cancelOnError);
	ds->dispatchThreads = cdbdisp_makeDispatchThreads(maxThreads);
	MemoryContextSwitchTo(oldContext);
}

/*
 * Free memory in CdbDispatcherState
 *
 * Free the PQExpBufferData allocated in libpq.
 * Free dispatcher memory context.
 */
void cdbdisp_destroyDispatcherState(CdbDispatcherState *ds)
{
	CdbDispatchResults * results = ds->primaryResults;
    if (results != NULL && results->resultArray != NULL)
    {
        int i;
        for (i = 0; i < results->resultCount; i++)
        {
            cdbdisp_termResult(&results->resultArray[i]);
        }
        results->resultArray = NULL;
    }

    if (ds->dispatchStateContext != NULL)
    {
        MemoryContextDelete(ds->dispatchStateContext);
        ds->dispatchStateContext = NULL;
    }

	ds->dispatchStateContext = NULL;
	ds->dispatchThreads = NULL;
	ds->primaryResults = NULL;
}

/*
 * Set slice in query text
 *
 * Make a new copy of query text and set the slice id in the right place.
 *
 */
static char *dupQueryTextAndSetSliceId(MemoryContext cxt, char *queryText, int len,
		int sliceId)
{
	/* DTX command and RM command don't need slice id */
	if (sliceId < 0)
		return NULL;

	int tmp = htonl(sliceId);
	char *newQuery = MemoryContextAlloc(cxt, len);
	memcpy(newQuery, queryText, len);

	/*
	 * the first byte is 'M' and followed by the length, which is an integer.
	 * see function PQbuildGpQueryString.
	 */
	memcpy(newQuery + 1 + sizeof(int), &tmp, sizeof(tmp));
	return newQuery;
}



/*
 * Initialize CdbDispatcherState using DispatchCommandQueryParms
 *
 * Allocate query text in memory context, initialize it and assign it to
 * all DispatchCommandQueryParms in this dispatcher state.
 *
 * For now, there's only one field (localSlice) which is different to each
 * dispatcher thread, we set it it later.
 *
 * Also, we free the DispatchCommandQueryParms memory.
 */
static void cdbdisp_queryParmsInit(struct CdbDispatcherState *ds,
		DispatchCommandQueryParms *pQueryParms)
{
	int i = 0;
	int len = 0;
	MemoryContext oldContext = NULL;

	CdbDispatchCmdThreads *dThreads = ds->dispatchThreads;
	DispatchCommandParms *pParms = &dThreads->dispatchCommandParmsAr[0];

	Assert(pQueryParms->strCommand != NULL);

	char *queryText = PQbuildGpQueryString(ds->dispatchStateContext, pParms, pQueryParms, &len);

	if (pQueryParms->serializedQuerytree != NULL)
	{
		pfree(pQueryParms->serializedQuerytree);
		pQueryParms->serializedQuerytree = NULL;
	}

	if (pQueryParms->serializedPlantree != NULL)
	{
		pfree(pQueryParms->serializedPlantree);
		pQueryParms->serializedPlantree = NULL;
	}

	if (pQueryParms->serializedParams != NULL)
	{
		pfree(pQueryParms->serializedParams);
		pQueryParms->serializedParams = NULL;
	}

	if (pQueryParms->serializedSliceInfo != NULL)
	{
		pfree(pQueryParms->serializedSliceInfo);
		pQueryParms->serializedSliceInfo = NULL;
	}

	if (pQueryParms->serializedDtxContextInfo != NULL)
	{
		pfree(pQueryParms->serializedDtxContextInfo);
		pQueryParms->serializedDtxContextInfo = NULL;
	}

	if (pQueryParms->seqServerHost != NULL)
	{
		pfree(pQueryParms->seqServerHost);
		pQueryParms->seqServerHost = NULL;
	}

	for (i = 0; i < dThreads->dispatchCommandParmsArSize; i++)
	{
		pParms = &dThreads->dispatchCommandParmsAr[i];
		pParms->query_text = queryText;
		pParms->query_text_len = len;
	}
}




/*
 * Initialize CdbDispatcherState using DispatchCommandDtxProtocolParms
 *
 * Allocate query text in memory context, initialize it and assign it to
 * all DispatchCommandQueryParms in this dispatcher state.
 */
static void cdbdisp_dtxParmsInit(struct CdbDispatcherState *ds,
		DispatchCommandDtxProtocolParms *pDtxProtocolParms)
{
	CdbDispatchCmdThreads *dThreads = ds->dispatchThreads;
	int i = 0;
	int len = 0;
	DispatchCommandParms *pParms = NULL;
	MemoryContext oldContext = NULL;

	Assert(pDtxProtocolParms->dtxProtocolCommandLoggingStr != NULL);
	Assert(pDtxProtocolParms->gid != NULL);

	oldContext = MemoryContextSwitchTo(ds->dispatchStateContext);

	char *queryText = PQbuildGpDtxProtocolCommand(ds->dispatchStateContext, pDtxProtocolParms, &len);

	MemoryContextSwitchTo(oldContext);

	for (i = 0; i < dThreads->dispatchCommandParmsArSize; i++)
	{
		pParms = &dThreads->dispatchCommandParmsAr[i];
		pParms->query_text = queryText;
		pParms->query_text_len = len;
	}
}
