/*-------------------------------------------------------------------------
 *
 * cdbtm.c
 *	  Provides routines for performing distributed transaction
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbtm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "catalog/pg_authid.h"
#include "cdb/cdbtm.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdisp_dtx.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbdtxcontextinfo.h"

#include "cdb/cdbvars.h"
#include "access/transam.h"
#include "access/xact.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbfts.h"
#include "lib/stringinfo.h"
#include "access/twophase.h"
#include "access/distributedlog.h"
#include "postmaster/postmaster.h"
#include "port/atomics.h"
#include "storage/procarray.h"

#include "cdb/cdbllize.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#include "utils/sharedsnapshot.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"

extern bool Test_print_direct_dispatch_info;

#define DTX_PHASE2_SLEEP_TIME_BETWEEN_RETRIES_MSECS 100

volatile DistributedTransactionTimeStamp *shmDistribTimeStamp;
volatile DistributedTransactionId *shmGIDSeq;

uint32 *shmNextSnapshotId;

int	max_tm_gxacts = 100;


#define TM_ERRDETAIL (errdetail("gid=%u-%.10u, state=%s", \
		MyTmGxact->distribTimeStamp, MyTmGxact->gxid, DtxStateToString(MyTmGxactLocal->state)))
/* here are some flag options relationed to the txnOptions field of
 * PQsendGpQuery
 */

/*
 * bit 1 is for statement wants DTX transaction
 * bits 2-4 for iso level
 * bit 5 is for read-only
 */
#define GP_OPT_NEED_TWO_PHASE                           0x0001

#define GP_OPT_ISOLATION_LEVEL_MASK   					0x000E
#define GP_OPT_READ_UNCOMMITTED							(1 << 1)
#define GP_OPT_READ_COMMITTED							(2 << 1)
#define GP_OPT_REPEATABLE_READ							(3 << 1)
#define GP_OPT_SERIALIZABLE								(4 << 1)

#define GP_OPT_READ_ONLY         						0x0010

#define GP_OPT_EXPLICT_BEGIN      						0x0020

/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */
static void clearAndResetGxact(void);
static void doPrepareTransaction(void);
static void doInsertForgetCommitted(void);
static void doNotifyingOnePhaseCommit(void);
static void doNotifyingCommitPrepared(void);
static void doNotifyingAbort(void);
static void retryAbortPrepared(void);
static void doQEDistributedExplicitBegin();
static void currentDtxActivateTwoPhase(void);
static void setCurrentDtxState(DtxState state);

static bool isDtxQueryDispatcher(void);
static void performDtxProtocolCommitPrepared(const char *gid, bool raiseErrorIfNotFound);
static void performDtxProtocolAbortPrepared(const char *gid, bool raiseErrorIfNotFound);

extern void CheckForResetSession(void);

/**
 * All assignments of the global DistributedTransactionContext should go through this function
 *   (so we can add logging here to see all assignments)
 *
 * @param context the new value for DistributedTransactionContext
 */
static void
setDistributedTransactionContext(DtxContext context)
{
	/*
	 * elog(INFO, "Setting DistributedTransactionContext to '%s'",
	 * DtxContextToString(context));
	 */
	DistributedTransactionContext = context;
}

static void
requireDistributedTransactionContext(DtxContext requiredCurrentContext)
{
	if (DistributedTransactionContext != requiredCurrentContext)
	{
		elog(FATAL, "Expected segment distributed transaction context to be '%s', found '%s'",
			 DtxContextToString(requiredCurrentContext),
			 DtxContextToString(DistributedTransactionContext));
	}
}

/**
 * Does DistributedTransactionContext indicate that this is acting as a QD?
 */
static bool
isQDContext(void)
{
	switch (DistributedTransactionContext)
	{
		case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
		case DTX_CONTEXT_QD_RETRY_PHASE_2:
			return true;
		default:
			return false;
	}
}

/**
 * Does DistributedTransactionContext indicate that this is acting as a QE?
 */
static bool
isQEContext()
{
	switch (DistributedTransactionContext)
	{
		case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
		case DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT:
		case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
		case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
		case DTX_CONTEXT_QE_READER:
			return true;
		default:
			return false;
	}
}

/*=========================================================================
 * VISIBLE FUNCTIONS
 */

DistributedTransactionTimeStamp
getDtxStartTime(void)
{
	Assert(shmDistribTimeStamp != NULL);
	return *shmDistribTimeStamp;
}

DistributedTransactionId
getDistributedTransactionId(void)
{
	if (isQDContext())
	{
		return GetCurrentDistributedTransactionId();
	}
	else if (isQEContext())
	{
		return QEDtxContextInfo.distributedXid;
	}
	else
	{
		return InvalidDistributedTransactionId;
	}
}

bool
getDistributedTransactionIdentifier(char *id)
{
	if (isQDContext())
	{
		DistributedTransactionId gxid = GetCurrentDistributedTransactionId();
		if (gxid != InvalidDistributedTransactionId)
		{
			/*
			 * The length check here requires the identifer have a trailing
			 * NUL character.
			 */
			dtxFormGID(id, getDtxStartTime(), gxid);
			return true;
		}
	}
	else if (isQEContext())
	{
		if (QEDtxContextInfo.distributedXid != InvalidDistributedTransactionId)
		{
			dtxFormGID(id, QEDtxContextInfo.distributedTimeStamp, QEDtxContextInfo.distributedXid);
			return true;
		}
	}

	MemSet(id, 0, TMGIDSIZE);
	return false;
}

bool
isPreparedDtxTransaction(void)
{
	AssertImply(MyTmGxactLocal->state == DTX_STATE_PREPARED,
				(Gp_role == GP_ROLE_DISPATCH &&
				DistributedTransactionContext == DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE));

	return (MyTmGxactLocal->state == DTX_STATE_PREPARED);
}

/*
 * The executor can avoid starting a distributed transaction if it knows that
 * the current dtx is clean and we aren't in a user-started global transaction.
 */
bool
isCurrentDtxTwoPhaseActivated(void)
{
	return MyTmGxactLocal->state != DTX_STATE_NONE;
}

static void
currentDtxActivateTwoPhase(void)
{
	setCurrentDtxState(DTX_STATE_ACTIVE_DISTRIBUTED);
}

static void
setCurrentDtxState(DtxState state)
{
	MyTmGxactLocal->state = state;
}

DtxState
getCurrentDtxState(void)
{
	return MyTmGxactLocal ? MyTmGxactLocal->state : DTX_STATE_NONE;
}

bool
notifyCommittedDtxTransactionIsNeeded(void)
{
	if (DistributedTransactionContext != DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE)
	{
		elog(DTM_DEBUG5, "notifyCommittedDtxTransaction nothing to do (DistributedTransactionContext = '%s')",
			 DtxContextToString(DistributedTransactionContext));
		return false;
	}

	if (!isCurrentDtxTwoPhaseActivated())
	{
		elog(DTM_DEBUG5, "notifyCommittedDtxTransaction nothing to do (two phase not activated)");
		return false;
	}

	return true;
}

/*
 * Notify commited a global transaction, called by user commit
 * or by CommitTransaction
 */
void
notifyCommittedDtxTransaction(void)
{
	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(DistributedTransactionContext == DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE);
	Assert(isCurrentDtxTwoPhaseActivated());

	switch(MyTmGxactLocal->state)
	{
		case DTX_STATE_PREPARED:
		case DTX_STATE_INSERTED_COMMITTED:
			doNotifyingCommitPrepared();
			break;
		case DTX_STATE_ONE_PHASE_COMMIT:
			doNotifyingOnePhaseCommit();
			break;
		default:
			ereport(ERROR,
					(errmsg("Unexpected DTX state"),
					 TM_ERRDETAIL));
	}
}

void
setupTwoPhaseTransaction(void)
{
	if (!IsTransactionState())
		elog(ERROR, "DTM transaction is not active");

	if (!isCurrentDtxTwoPhaseActivated())
		currentDtxActivateTwoPhase();

	if (MyTmGxactLocal->state != DTX_STATE_ACTIVE_DISTRIBUTED)
		elog(ERROR, "DTM transaction state (%s) is invalid", DtxStateToString(MyTmGxactLocal->state));
}


/*
 * Routine to dispatch internal sub-transaction calls from UDFs to segments.
 * The calls are BeginInternalSubTransaction, ReleaseCurrentSubTransaction and
 * RollbackAndReleaseCurrentSubTransaction.
 */
bool
doDispatchSubtransactionInternalCmd(DtxProtocolCommand cmdType)
{
	char	   *serializedDtxContextInfo = NULL;
	int			serializedDtxContextInfoLen = 0;
	char		gid[TMGIDSIZE];
	bool		succeeded = false;

	if (currentGxactWriterGangLost())
	{
		ereport(WARNING,
				(errmsg("writer gang of current global transaction is lost")));
		return false;
	}

	if (cmdType == DTX_PROTOCOL_COMMAND_SUBTRANSACTION_BEGIN_INTERNAL &&
		!isCurrentDtxTwoPhaseActivated())
	{
		currentDtxActivateTwoPhase();
	}

	serializedDtxContextInfo = qdSerializeDtxContextInfo(&serializedDtxContextInfoLen,
														 false /* wantSnapshot */ ,
														 false /* inCursor */ ,
														 mppTxnOptions(true),
														 "doDispatchSubtransactionInternalCmd");

	dtxFormGID(gid, MyTmGxact->distribTimeStamp, MyTmGxact->gxid);
	succeeded = doDispatchDtxProtocolCommand(cmdType,
											 gid,
											 NULL, /* raiseError */ true,
											 cdbcomponent_getCdbComponentsList(),
											 serializedDtxContextInfo, serializedDtxContextInfoLen);

	/* send a DTM command to others to tell them about the transaction */
	if (!succeeded)
	{
		ereport(ERROR,
				(errmsg("dispatching subtransaction internal command failed for gid = \"%s\" due to error", gid)));
	}

	return succeeded;
}

static void
doPrepareTransaction(void)
{
	bool		succeeded;

	CHECK_FOR_INTERRUPTS();

	elog(DTM_DEBUG5, "doPrepareTransaction entering in state = %s",
		 DtxStateToString(MyTmGxactLocal->state));

	/*
	 * Don't allow a cancel while we're dispatching our prepare (we wrap our
	 * state change as well; for good measure.
	 */
	HOLD_INTERRUPTS();

	Assert(MyTmGxactLocal->state == DTX_STATE_ACTIVE_DISTRIBUTED);
	setCurrentDtxState(DTX_STATE_PREPARING);

	elog(DTM_DEBUG5, "doPrepareTransaction moved to state = %s", DtxStateToString(MyTmGxactLocal->state));

	Assert(MyTmGxactLocal->twophaseSegments != NIL);
	succeeded = currentDtxDispatchProtocolCommand(DTX_PROTOCOL_COMMAND_PREPARE, true);

	/*
	 * Now we've cleaned up our dispatched statement, cancels are allowed
	 * again.
	 */
	RESUME_INTERRUPTS();

	if (!succeeded)
	{
		elog(DTM_DEBUG5, "doPrepareTransaction error finds badPrimaryGangs = %s",
			 (MyTmGxactLocal->badPrepareGangs ? "true" : "false"));

		ereport(ERROR,
				(errmsg("The distributed transaction 'Prepare' broadcast failed to one or more segments"),
				TM_ERRDETAIL));
	}
	ereport(DTM_DEBUG5,
			(errmsg("The distributed transaction 'Prepare' broadcast succeeded to the segments"),
			TM_ERRDETAIL));

	Assert(MyTmGxactLocal->state == DTX_STATE_PREPARING);
	setCurrentDtxState(DTX_STATE_PREPARED);

	SIMPLE_FAULT_INJECTOR("dtm_broadcast_prepare");

	elog(DTM_DEBUG5, "doPrepareTransaction leaving in state = %s", DtxStateToString(MyTmGxactLocal->state));
}

/*
 * Insert FORGET COMMITTED into the xlog.
 */
static void
doInsertForgetCommitted(void)
{
	TMGXACT_LOG gxact_log;

	elog(DTM_DEBUG5, "doInsertForgetCommitted entering in state = %s", DtxStateToString(MyTmGxactLocal->state));

	setCurrentDtxState(DTX_STATE_INSERTING_FORGET_COMMITTED);

	dtxFormGID(gxact_log.gid, MyTmGxact->distribTimeStamp, MyTmGxact->gxid);
	gxact_log.gxid = MyTmGxact->gxid;

	RecordDistributedForgetCommitted(&gxact_log);

	setCurrentDtxState(DTX_STATE_INSERTED_FORGET_COMMITTED);
	MyTmGxact->needIncludedInCkpt = false;
}

void
ClearTransactionState(TransactionId latestXid)
{
	/*
	 * These two actions must be performed for a distributed transaction under
	 * the same locking of ProceArrayLock so the visibility of the transaction
	 * changes for local master readers (e.g. those using  SnapshotNow for
	 * reading) the same as for distributed transactions.
	 *
	 *
	 * In upstream Postgres, proc->xid is cleared in ProcArrayEndTransaction.
	 * But there would have a small window in Greenplum that allows inconsistency
	 * between ProcArrayEndTransaction and notifying prepared commit to segments.
	 * In between, the master has new tuple visible while the segments are seeing
	 * old tuples.
	 *
	 * For example, session 1 runs:
	 *    RENAME from a_new to a;
	 * session 2 runs:
	 *    DROP TABLE a;
	 *
	 * When session 1 goes to just before notifyCommittedDtxTransaction, the new
	 * coming session 2 can see a new tuple for renamed table "a" in pg_class,
	 * and can drop it in master. However, dispatching DROP to segments, at this
	 * point of time segments still have old tuple for "a_new" visible in
	 * pg_class and DROP process just fails to drop "a". Then DTX is notified
	 * later and committed in the segments, the new tuple for "a" is visible
	 * now, but nobody wants to DROP it anymore, so the master has no tuple for
	 * "a" while the segments have it.
	 *
	 * To fix this, transactions require two-phase commit should defer clear 
	 * proc->xid here with ProcArryLock held.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	ProcArrayEndTransaction(MyProc, latestXid, true);
	ProcArrayEndGxact();
	LWLockRelease(ProcArrayLock);
}

static void
doNotifyingOnePhaseCommit(void)
{
	bool		succeeded;
	volatile int savedInterruptHoldoffCount;

	if (MyTmGxactLocal->twophaseSegments == NULL)
		return;

	elog(DTM_DEBUG5, "doNotifyingOnePhaseCommit entering in state = %s", DtxStateToString(MyTmGxactLocal->state));

	Assert(MyTmGxactLocal->state == DTX_STATE_ONE_PHASE_COMMIT);
	setCurrentDtxState(DTX_STATE_PERFORMING_ONE_PHASE_COMMIT);

	savedInterruptHoldoffCount = InterruptHoldoffCount;

	Assert(MyTmGxactLocal->twophaseSegments != NIL);

	succeeded = currentDtxDispatchProtocolCommand(DTX_PROTOCOL_COMMAND_COMMIT_ONEPHASE, true);
	if (!succeeded)
	{
		Assert(MyTmGxactLocal->state == DTX_STATE_PERFORMING_ONE_PHASE_COMMIT);
		elog(ERROR, "one phase commit failed");
	}
}

static void
doNotifyingCommitPrepared(void)
{
	bool		succeeded;
	int			retry = 0;
	volatile int savedInterruptHoldoffCount;
	MemoryContext oldcontext = CurrentMemoryContext;;

	elog(DTM_DEBUG5, "doNotifyingCommitPrepared entering in state = %s", DtxStateToString(MyTmGxactLocal->state));

	Assert(MyTmGxactLocal->state == DTX_STATE_INSERTED_COMMITTED);
	setCurrentDtxState(DTX_STATE_NOTIFYING_COMMIT_PREPARED);

	SIMPLE_FAULT_INJECTOR("dtm_broadcast_commit_prepared");
	savedInterruptHoldoffCount = InterruptHoldoffCount;

	Assert(MyTmGxactLocal->twophaseSegments != NIL);
	PG_TRY();
	{
		succeeded = currentDtxDispatchProtocolCommand(DTX_PROTOCOL_COMMAND_COMMIT_PREPARED, true);
	}
	PG_CATCH();
	{
		/*
		 * restore the previous value, which is reset to 0 in errfinish.
		 */
		MemoryContextSwitchTo(oldcontext);
		InterruptHoldoffCount = savedInterruptHoldoffCount;
		succeeded = false;
		FlushErrorState();
	}
	PG_END_TRY();

	if (!succeeded)
	{
		Assert(MyTmGxactLocal->state == DTX_STATE_NOTIFYING_COMMIT_PREPARED);
		ereport(DTM_DEBUG5,
				(errmsg("marking retry needed for distributed transaction "
						"'Commit Prepared' broadcast to the segments"),
				TM_ERRDETAIL));

		setCurrentDtxState(DTX_STATE_RETRY_COMMIT_PREPARED);
		setDistributedTransactionContext(DTX_CONTEXT_QD_RETRY_PHASE_2);
	}

	while (!succeeded && dtx_phase2_retry_count > retry++)
	{
		/*
		 * sleep for brief duration before retry, to increase chances of
		 * success if first try failed due to segment panic/restart. Otherwise
		 * all the retries complete in less than a sec, defeating the purpose
		 * of the retry.
		 */
		pg_usleep(DTX_PHASE2_SLEEP_TIME_BETWEEN_RETRIES_MSECS * 1000);

		ereport(WARNING,
				(errmsg("the distributed transaction 'Commit Prepared' broadcast "
						"failed to one or more segments. Retrying ... try %d", retry),
				TM_ERRDETAIL));

		/*
		 * We must succeed in delivering the commit to all segment instances,
		 * or any failed segment instances must be marked INVALID.
		 */
		elog(NOTICE, "Releasing segworker group to retry broadcast.");
		DisconnectAndDestroyAllGangs(true);

		/*
		 * This call will at a minimum change the session id so we will not
		 * have SharedSnapshotAdd colissions.
		 */
		CheckForResetSession();
		savedInterruptHoldoffCount = InterruptHoldoffCount;

		PG_TRY();
		{
			succeeded = currentDtxDispatchProtocolCommand(DTX_PROTOCOL_COMMAND_RETRY_COMMIT_PREPARED, true);
		}
		PG_CATCH();
		{
			/*
			 * restore the previous value, which is reset to 0 in errfinish.
			 */
			MemoryContextSwitchTo(oldcontext);
			InterruptHoldoffCount = savedInterruptHoldoffCount;
			succeeded = false;
			FlushErrorState();
		}
		PG_END_TRY();
	}

	if (!succeeded)
		ereport(PANIC,
				(errmsg("unable to complete 'Commit Prepared' broadcast"),
				TM_ERRDETAIL));

	ereport(DTM_DEBUG5,
			(errmsg("the distributed transaction 'Commit Prepared' broadcast succeeded to all the segments"),
			TM_ERRDETAIL));

	doInsertForgetCommitted();
}

static void
retryAbortPrepared(void)
{
	int			retry = 0;
	bool		succeeded = false;
	volatile int savedInterruptHoldoffCount;
	MemoryContext oldcontext = CurrentMemoryContext;;

	while (!succeeded && dtx_phase2_retry_count > retry++)
	{
		/*
		 * By deallocating the gang, we will force a new gang to connect to
		 * all the segment instances.  And, we will abort the transactions in
		 * the segments. What's left are possibily prepared transactions.
		 */
		if (retry > 1)
		{
			elog(NOTICE, "Releasing segworker groups to retry broadcast.");
			/*
			 * sleep for brief duration before retry, to increase chances of
			 * success if first try failed due to segment panic/restart. Otherwise
			 * all the retries complete in less than a sec, defeating the purpose
			 * of the retry.
			 */
			pg_usleep(DTX_PHASE2_SLEEP_TIME_BETWEEN_RETRIES_MSECS * 1000);
		}
		DisconnectAndDestroyAllGangs(true);

		/*
		 * This call will at a minimum change the session id so we will not
		 * have SharedSnapshotAdd colissions.
		 */
		CheckForResetSession();

		savedInterruptHoldoffCount = InterruptHoldoffCount;

		PG_TRY();
		{
			MyTmGxactLocal->twophaseSegments = cdbcomponent_getCdbComponentsList();
			succeeded = currentDtxDispatchProtocolCommand(DTX_PROTOCOL_COMMAND_RETRY_ABORT_PREPARED, true);
			if (!succeeded)
				ereport(WARNING,
						(errmsg("the distributed transaction 'Abort' broadcast "
								"failed to one or more segments. Retrying ... try %d", retry),
						TM_ERRDETAIL));
		}
		PG_CATCH();
		{
			/*
			 * restore the previous value, which is reset to 0 in errfinish.
			 */
			MemoryContextSwitchTo(oldcontext);
			InterruptHoldoffCount = savedInterruptHoldoffCount;
			succeeded = false;
			FlushErrorState();
		}
		PG_END_TRY();
	}

	if (!succeeded)
		ereport(PANIC,
				(errmsg("unable to complete 'Abort' broadcast"),
				TM_ERRDETAIL));

	ereport(DTM_DEBUG5,
			(errmsg("The distributed transaction 'Abort' broadcast succeeded to all the segments"),
			TM_ERRDETAIL));
}


static void
doNotifyingAbort(void)
{
	bool		succeeded;
	volatile int savedInterruptHoldoffCount;
	MemoryContext oldcontext = CurrentMemoryContext;

	elog(DTM_DEBUG5, "doNotifyingAborted entering in state = %s", DtxStateToString(MyTmGxactLocal->state));

	Assert(MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_NO_PREPARED ||
			MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED ||
			MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_PREPARED);

	if (MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_NO_PREPARED)
	{
		/*
		 * In some cases, dtmPreCommand said two phase commit is needed, but some errors
		 * occur before the command is actually dispatched, no need to dispatch DTX for
		 * such cases.
		 */ 
		if (!MyTmGxactLocal->writerGangLost && MyTmGxactLocal->twophaseSegments)
		{
			succeeded = currentDtxDispatchProtocolCommand(DTX_PROTOCOL_COMMAND_ABORT_NO_PREPARED, false);

			if (!succeeded)
			{
				ereport(WARNING,
						(errmsg("The distributed transaction 'Abort' broadcast failed to one or more segments"),
						TM_ERRDETAIL));

				/*
				 * Reset the dispatch logic and disconnect from any segment
				 * that didn't respond to our abort.
				 */
				elog(NOTICE, "Releasing segworker groups to finish aborting the transaction.");
				DisconnectAndDestroyAllGangs(true);

				/*
				 * This call will at a minimum change the session id so we
				 * will not have SharedSnapshotAdd colissions.
				 */
				CheckForResetSession();
			}
			else
			{
				ereport(DTM_DEBUG5,
						(errmsg("The distributed transaction 'Abort' broadcast succeeded to all the segments"),
						TM_ERRDETAIL));
			}
		}
		else
		{
			ereport(DTM_DEBUG5,
					(errmsg("The distributed transaction 'Abort' broadcast was omitted (segworker group already dead)"),
					TM_ERRDETAIL));
		}
	}
	else
	{
		DtxProtocolCommand dtxProtocolCommand;
		char	   *abortString;

		Assert(MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED ||
				MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_PREPARED);

		if (MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED)
		{
			dtxProtocolCommand = DTX_PROTOCOL_COMMAND_ABORT_SOME_PREPARED;
			abortString = "Abort [Prepared]";
		}
		else
		{
			dtxProtocolCommand = DTX_PROTOCOL_COMMAND_ABORT_PREPARED;
			abortString = "Abort Prepared";
		}

		savedInterruptHoldoffCount = InterruptHoldoffCount;

		PG_TRY();
		{
			succeeded = currentDtxDispatchProtocolCommand(dtxProtocolCommand, true);
		}
		PG_CATCH();
		{
			/*
			 * restore the previous value, which is reset to 0 in errfinish.
			 */
			MemoryContextSwitchTo(oldcontext);
			InterruptHoldoffCount = savedInterruptHoldoffCount;
			succeeded = false;
			FlushErrorState();
		}
		PG_END_TRY();

		if (!succeeded)
		{
			ereport(WARNING,
					(errmsg("the distributed transaction broadcast failed "
							"to one or more segments"),
					TM_ERRDETAIL));

			setCurrentDtxState(DTX_STATE_RETRY_ABORT_PREPARED);
			setDistributedTransactionContext(DTX_CONTEXT_QD_RETRY_PHASE_2);
			retryAbortPrepared();
		}
	}

	SIMPLE_FAULT_INJECTOR("dtm_broadcast_abort_prepared");

	Assert(MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_NO_PREPARED ||
			MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED ||
			MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_PREPARED ||
			MyTmGxactLocal->state == DTX_STATE_RETRY_ABORT_PREPARED);
}

/*
 * prepare a global transaction, called by user commit
 * or by CommitTransaction
 */
void
prepareDtxTransaction(void)
{
	TransactionId xid = GetTopTransactionIdIfAny();
	bool		markXidCommitted = TransactionIdIsValid(xid);

	if (DistributedTransactionContext != DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE)
	{
		elog(DTM_DEBUG5, "prepareDtxTransaction nothing to do (DistributedTransactionContext = '%s')",
			 DtxContextToString(DistributedTransactionContext));
		return;
	}

	if (!isCurrentDtxTwoPhaseActivated())
	{
		Assert(MyTmGxact->gxid == InvalidDistributedTransactionId);
		Assert(MyTmGxactLocal->state == DTX_STATE_NONE);
		resetGxact();
		return;
	}

	/*
	 * If only one segment was involved in the transaction, and no local XID
	 * has been assigned on the QD either, we can perform one-phase commit
	 * on that one segment. Otherwise, broadcast PREPARE TRANSACTION to the
	 * segments.
	 */
	if (!ExecutorDidWriteXLog() ||
	    (!markXidCommitted && list_length(MyTmGxactLocal->twophaseSegments) < 2))
	{
		setCurrentDtxState(DTX_STATE_ONE_PHASE_COMMIT);
		return;
	}

	elog(DTM_DEBUG5,
		 "prepareDtxTransaction called with state = %s",
		 DtxStateToString(MyTmGxactLocal->state));

	Assert(MyTmGxactLocal->state == DTX_STATE_ACTIVE_DISTRIBUTED);
	Assert(MyTmGxact->gxid > FirstDistributedTransactionId);

	doPrepareTransaction();
}

/*
 * rollback a global transaction, called by user rollback
 * or by AbortTransaction during Postgres automatic rollback
 */
void
rollbackDtxTransaction(void)
{
	if (DistributedTransactionContext != DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE)
	{
		elog(DTM_DEBUG5, "rollbackDtxTransaction nothing to do (DistributedTransactionContext = '%s')",
			 DtxContextToString(DistributedTransactionContext));
		return;
	}
	if (!isCurrentDtxTwoPhaseActivated())
	{
		elog(DTM_DEBUG5, "rollbackDtxTransaction nothing to do (two phase not activate)");
		return;
	}

	ereport(DTM_DEBUG5,
			(errmsg("rollbackDtxTransaction called"),
			TM_ERRDETAIL));

	switch (MyTmGxactLocal->state)
	{
		case DTX_STATE_ACTIVE_DISTRIBUTED:
			setCurrentDtxState(DTX_STATE_NOTIFYING_ABORT_NO_PREPARED);
			break;

		case DTX_STATE_PREPARING:
			if (MyTmGxactLocal->badPrepareGangs)
			{
				setCurrentDtxState(DTX_STATE_RETRY_ABORT_PREPARED);

				/*
				 * DisconnectAndDestroyAllGangs and ResetSession happens
				 * inside retryAbortPrepared.
				 */
				retryAbortPrepared();
				clearAndResetGxact();
				return;
			}
			setCurrentDtxState(DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED);
			break;

		case DTX_STATE_PREPARED:
			setCurrentDtxState(DTX_STATE_NOTIFYING_ABORT_PREPARED);
			break;

		case DTX_STATE_ONE_PHASE_COMMIT:
		case DTX_STATE_PERFORMING_ONE_PHASE_COMMIT:
			setCurrentDtxState(DTX_STATE_NOTIFYING_ABORT_NO_PREPARED);
			break;

		case DTX_STATE_NOTIFYING_ABORT_NO_PREPARED:

			/*
			 * By deallocating the gang, we will force a new gang to connect
			 * to all the segment instances.  And, we will abort the
			 * transactions in the segments.
			 */
			elog(NOTICE, "Releasing segworker groups to finish aborting the transaction.");
			DisconnectAndDestroyAllGangs(true);

			/*
			 * This call will at a minimum change the session id so we will
			 * not have SharedSnapshotAdd colissions.
			 */
			CheckForResetSession();

			clearAndResetGxact();
			return;

		case DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED:
		case DTX_STATE_NOTIFYING_ABORT_PREPARED:
			ereport(FATAL,
					(errmsg("Unable to complete the 'Abort Prepared' broadcast"),
					TM_ERRDETAIL));
			break;

		case DTX_STATE_NOTIFYING_COMMIT_PREPARED:
		case DTX_STATE_INSERTING_COMMITTED:
		case DTX_STATE_INSERTED_COMMITTED:
		case DTX_STATE_INSERTING_FORGET_COMMITTED:
		case DTX_STATE_INSERTED_FORGET_COMMITTED:
		case DTX_STATE_RETRY_COMMIT_PREPARED:
		case DTX_STATE_RETRY_ABORT_PREPARED:
			elog(DTM_DEBUG5, "rollbackDtxTransaction dtx state \"%s\" not expected here",
				 DtxStateToString(MyTmGxactLocal->state));
			clearAndResetGxact();
			return;

		default:
			elog(PANIC, "Unrecognized dtx state: %d",
				 (int) MyTmGxactLocal->state);
			break;
	}


	Assert(MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_NO_PREPARED ||
			MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED ||
			MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_PREPARED);

	/*
	 * if the process is in the middle of blowing up... then we don't do
	 * anything here.  we can resolve any in-doubt transactions later.
	 *
	 * We can't dispatch -- but we *do* need to free up shared-memory entries.
	 */
	if (proc_exit_inprogress)
	{
		/*
		 * Unable to complete distributed abort broadcast with possible
		 * prepared transactions...
		 */
		if (MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_SOME_PREPARED ||
			MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_PREPARED)
		{
			ereport(FATAL,
					(errmsg("Unable to complete the 'Abort Prepared' broadcast"),
					TM_ERRDETAIL));
		}

		Assert(MyTmGxactLocal->state == DTX_STATE_NOTIFYING_ABORT_NO_PREPARED);

		/*
		 * By deallocating the gang, we will force a new gang to connect to
		 * all the segment instances.  And, we will abort the transactions in
		 * the segments.
		 */
		DisconnectAndDestroyAllGangs(true);

		/*
		 * This call will at a minimum change the session id so we will not
		 * have SharedSnapshotAdd colissions.
		 */
		CheckForResetSession();

		clearAndResetGxact();
		return;
	}

	doNotifyingAbort();
	clearAndResetGxact();

	return;
}

/* get tm share memory size */
int
tmShmemSize(void)
{
	if ((Gp_role != GP_ROLE_DISPATCH) && (Gp_role != GP_ROLE_UTILITY))
		return 0;

	return
		MAXALIGN(TMCONTROLBLOCK_BYTES(max_tm_gxacts));
}


/*
 * tmShmemInit - should be called only once from postmaster and inherit by all
 * postgres processes
 */
void
tmShmemInit(void)
{
	bool		found;
	TmControlBlock *shared;

	/*
	 * max_prepared_xacts is a guc which is postmaster-startup setable -- it
	 * can only be updated by restarting the system. Global transactions will
	 * all use two-phase commit, so the number of global transactions is bound
	 * to the number of prepared.
	 */
	max_tm_gxacts = max_prepared_xacts;

	if ((Gp_role != GP_ROLE_DISPATCH) && (Gp_role != GP_ROLE_UTILITY))
		return;

	shared = (TmControlBlock *) ShmemInitStruct("Transaction manager", tmShmemSize(), &found);
	if (!shared)
		elog(FATAL, "could not initialize transaction manager share memory");

	shmDistribTimeStamp = &shared->distribTimeStamp;
	shmGIDSeq = &shared->seqno;
	/* Only initialize this if we are the creator of the shared memory */
	if (!found)
	{
		time_t		t = time(NULL);

		if (t == (time_t) -1)
		{
			elog(PANIC, "cannot generate global transaction id");
		}

		*shmDistribTimeStamp = (DistributedTransactionTimeStamp) t;
		elog(DEBUG1, "DTM start timestamp %u", *shmDistribTimeStamp);

		*shmGIDSeq = FirstDistributedTransactionId;
		ShmemVariableCache->latestCompletedDxid = InvalidDistributedTransactionId;
	}
	shmDtmStarted = &shared->DtmStarted;
	shmNextSnapshotId = &shared->NextSnapshotId;
	shmNumCommittedGxacts = &shared->num_committed_xacts;
	shmCommittedGxactArray = &shared->committed_gxact_array[0];

	if (!IsUnderPostmaster)
		/* Initialize locks and shared memory area */
	{
		*shmNextSnapshotId = 0;
		*shmDtmStarted = false;
		*shmNumCommittedGxacts = 0;
	}
}

/* mppTxnOptions:
 * Generates an int containing the appropriate flags to direct the remote
 * segdb QE process to perform any needed transaction commands before or
 * after the statement.
 */
int
mppTxnOptions(bool needTwoPhase)
{
	int			options = 0;

	elog(DTM_DEBUG5,
		 "mppTxnOptions DefaultXactIsoLevel = %s, DefaultXactReadOnly = %s, XactIsoLevel = %s, XactReadOnly = %s.",
		 IsoLevelAsUpperString(DefaultXactIsoLevel), (DefaultXactReadOnly ? "true" : "false"),
		 IsoLevelAsUpperString(XactIsoLevel), (XactReadOnly ? "true" : "false"));

	if (needTwoPhase)
		options |= GP_OPT_NEED_TWO_PHASE;

	if (XactIsoLevel == XACT_READ_COMMITTED)
		options |= GP_OPT_READ_COMMITTED;
	else if (XactIsoLevel == XACT_REPEATABLE_READ)
		options |= GP_OPT_REPEATABLE_READ;
	else if (XactIsoLevel == XACT_SERIALIZABLE)
		options |= GP_OPT_SERIALIZABLE;
	else if (XactIsoLevel == XACT_READ_UNCOMMITTED)
		options |= GP_OPT_READ_UNCOMMITTED;

	if (XactReadOnly)
		options |= GP_OPT_READ_ONLY;

	if (isCurrentDtxTwoPhaseActivated() && MyTmGxactLocal->explicitBeginRemembered)
		options |= GP_OPT_EXPLICT_BEGIN;

	elog(DTM_DEBUG5,
		 "mppTxnOptions txnOptions = 0x%x, needTwoPhase = %s, explicitBegin = %s, isoLevel = %s, readOnly = %s.",
		 options,
		 (isMppTxOptions_NeedTwoPhase(options) ? "true" : "false"), (isMppTxOptions_ExplicitBegin(options) ? "true" : "false"),
		 IsoLevelAsUpperString(mppTxOptions_IsoLevel(options)), (isMppTxOptions_ReadOnly(options) ? "true" : "false"));

	return options;

}

int
mppTxOptions_IsoLevel(int txnOptions)
{
	if ((txnOptions & GP_OPT_ISOLATION_LEVEL_MASK) == GP_OPT_SERIALIZABLE)
		return XACT_SERIALIZABLE;
	else if ((txnOptions & GP_OPT_ISOLATION_LEVEL_MASK) == GP_OPT_REPEATABLE_READ)
		return XACT_REPEATABLE_READ;
	else if ((txnOptions & GP_OPT_ISOLATION_LEVEL_MASK) == GP_OPT_READ_COMMITTED)
		return XACT_READ_COMMITTED;
	else if ((txnOptions & GP_OPT_ISOLATION_LEVEL_MASK) == GP_OPT_READ_UNCOMMITTED)
		return XACT_READ_UNCOMMITTED;
	/* QD must set transaction isolation level */
	elog(ERROR, "transaction options from QD did not include isolation level");
}

bool
isMppTxOptions_ReadOnly(int txnOptions)
{
	return ((txnOptions & GP_OPT_READ_ONLY) != 0);
}

bool
isMppTxOptions_NeedTwoPhase(int txnOptions)
{
	return ((txnOptions & GP_OPT_NEED_TWO_PHASE) != 0);
}

/* isMppTxOptions_ExplicitBegin:
 * Return the ExplicitBegin flag.
 */
bool
isMppTxOptions_ExplicitBegin(int txnOptions)
{
	return ((txnOptions & GP_OPT_EXPLICT_BEGIN) != 0);
}

/*=========================================================================
 * HELPER FUNCTIONS
 */
bool
currentDtxDispatchProtocolCommand(DtxProtocolCommand dtxProtocolCommand, bool raiseError)
{
	char gid[TMGIDSIZE];
	bool *badgang = (MyTmGxactLocal->state == DTX_STATE_PREPARING) ? &MyTmGxactLocal->badPrepareGangs : NULL;

	dtxFormGID(gid, MyTmGxact->distribTimeStamp, MyTmGxact->gxid);
	return doDispatchDtxProtocolCommand(dtxProtocolCommand, gid, badgang, raiseError,
										MyTmGxactLocal->twophaseSegments, NULL, 0);
}

bool
doDispatchDtxProtocolCommand(DtxProtocolCommand dtxProtocolCommand,
							 char *gid,
							 bool *badGangs, bool raiseError,
							 List *twophaseSegments,
							 char *serializedDtxContextInfo,
							 int serializedDtxContextInfoLen)
{
	int			i,
				resultCount,
				numOfFailed = 0;

	char	   *dtxProtocolCommandStr = 0;

	struct pg_result **results;

	if (!twophaseSegments)
		return true;

	dtxProtocolCommandStr = DtxProtocolCommandToString(dtxProtocolCommand);

	if (Test_print_direct_dispatch_info)
		elog(INFO, "Distributed transaction command '%s' to %s",
			 								dtxProtocolCommandStr,
											segmentsToContentStr(twophaseSegments));

	ereport(DTM_DEBUG5,
			(errmsg("dispatchDtxProtocolCommand: %d ('%s'), direct content #: %s",
					dtxProtocolCommand, dtxProtocolCommandStr,
					segmentsToContentStr(twophaseSegments))));

	ErrorData *qeError;
	results = CdbDispatchDtxProtocolCommand(dtxProtocolCommand,
											dtxProtocolCommandStr,
											gid,
											&qeError, &resultCount, badGangs, twophaseSegments,
											serializedDtxContextInfo, serializedDtxContextInfoLen);

	if (qeError)
	{
		if (!raiseError)
		{
			ereport(LOG,
					(errmsg("DTM error (gathered results from cmd '%s')", dtxProtocolCommandStr),
					 errdetail("QE reported error: %s", qeError->message)));
		}
		else
			ReThrowError(qeError);
		return false;
	}

	if (results == NULL)
	{
		numOfFailed++;			/* If we got no results, we need to treat it
								 * as an error! */
	}

	for (i = 0; i < resultCount; i++)
	{
		char	   *cmdStatus;
		ExecStatusType resultStatus;

		/*
		 * note: PQresultStatus() is smart enough to deal with results[i] ==
		 * NULL
		 */
		resultStatus = PQresultStatus(results[i]);
		if (resultStatus != PGRES_COMMAND_OK &&
			resultStatus != PGRES_TUPLES_OK)
		{
			numOfFailed++;
		}
		else
		{
			/*
			 * success ? If an error happened during a transaction which
			 * hasn't already been caught when we try a prepare we'll get a
			 * rollback from our prepare ON ONE SEGMENT: so we go look at the
			 * status, otherwise we could issue a COMMIT when we don't want
			 * to!
			 */
			cmdStatus = PQcmdStatus(results[i]);

			elog(DEBUG3, "DTM: status message cmd '%s' [%d] result '%s'", dtxProtocolCommandStr, i, cmdStatus);
			if (strncmp(cmdStatus, dtxProtocolCommandStr, strlen(cmdStatus)) != 0)
			{
				/* failed */
				numOfFailed++;
			}
		}
	}

	for (i = 0; i < resultCount; i++)
		PQclear(results[i]);

	if (results)
		free(results);

	return (numOfFailed == 0);
}


bool
dispatchDtxCommand(const char *cmd)
{
	int			i,
				numOfFailed = 0;

	CdbPgResults cdb_pgresults = {NULL, 0};

	elog(DTM_DEBUG5, "dispatchDtxCommand: '%s'", cmd);

	if (currentGxactWriterGangLost())
	{
		ereport(WARNING,
				(errmsg("writer gang of current global transaction is lost")));
		return false;
	}

	CdbDispatchCommand(cmd, DF_NEED_TWO_PHASE, &cdb_pgresults);

	if (cdb_pgresults.numResults == 0)
	{
		return false;			/* If we got no results, we need to treat it
								 * as an error! */
	}

	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		char	   *cmdStatus;
		ExecStatusType resultStatus;

		/*
		 * note: PQresultStatus() is smart enough to deal with results[i] ==
		 * NULL
		 */
		resultStatus = PQresultStatus(cdb_pgresults.pg_results[i]);
		if (resultStatus != PGRES_COMMAND_OK &&
			resultStatus != PGRES_TUPLES_OK)
		{
			numOfFailed++;
		}
		else
		{
			/*
			 * success ? If an error happened during a transaction which
			 * hasn't already been caught when we try a prepare we'll get a
			 * rollback from our prepare ON ONE SEGMENT: so we go look at the
			 * status, otherwise we could issue a COMMIT when we don't want
			 * to!
			 */
			cmdStatus = PQcmdStatus(cdb_pgresults.pg_results[i]);

			elog(DEBUG3, "DTM: status message cmd '%s' [%d] result '%s'", cmd, i, cmdStatus);
			if (strncmp(cmdStatus, cmd, strlen(cmdStatus)) != 0)
			{
				/* failed */
				numOfFailed++;
			}
		}
	}

	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return (numOfFailed == 0);
}

/* reset global transaction context */
void
resetGxact()
{
	if (Gp_role == GP_ROLE_DISPATCH && MyTmGxact->gxid != InvalidDistributedTransactionId)
	{
		/* As QE doesn't take distributed snapshot, we don't have to hold ProcArrayLock to clear gxid */
		Assert(LWLockHeldByMe(ProcArrayLock));
		MyTmGxact->gxid = InvalidDistributedTransactionId;
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
		MyTmGxact->gxid = InvalidDistributedTransactionId;

	MyTmGxact->distribTimeStamp = 0;
	MyTmGxact->xminDistributedSnapshot = InvalidDistributedTransactionId;
	MyTmGxact->needIncludedInCkpt = false;
	MyTmGxact->sessionId = 0;

	MyTmGxactLocal->explicitBeginRemembered = false;
	MyTmGxactLocal->badPrepareGangs = false;
	MyTmGxactLocal->writerGangLost = false;
	MyTmGxactLocal->twophaseSegmentsMap = NULL;
	MyTmGxactLocal->twophaseSegments = NIL;
	MyTmGxactLocal->isOnePhaseCommit = false;
	setCurrentDtxState(DTX_STATE_NONE);
}

bool
getNextDistributedXactStatus(TMGALLXACTSTATUS *allDistributedXactStatus, TMGXACTSTATUS **distributedXactStatus)
{
	if (allDistributedXactStatus->next >= allDistributedXactStatus->count)
	{
		return false;
	}

	*distributedXactStatus = &allDistributedXactStatus->statusArray[allDistributedXactStatus->next];
	allDistributedXactStatus->next++;

	return true;
}

static void
clearAndResetGxact(void)
{
	Assert(isCurrentDtxTwoPhaseActivated());

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	ProcArrayEndGxact();
	LWLockRelease(ProcArrayLock);
}

/*
 * serializes commits with checkpoint info using PGPROC->inCommit
 * Change state to DTX_STATE_INSERTING_COMMITTED.
 */
void
insertingDistributedCommitted(void)
{
	elog(DTM_DEBUG5,
		 "insertingDistributedCommitted entering in state = %s",
		 DtxStateToString(MyTmGxactLocal->state));

	Assert(MyTmGxactLocal->state == DTX_STATE_PREPARED);
	setCurrentDtxState(DTX_STATE_INSERTING_COMMITTED);
}

/*
 * Change state to DTX_STATE_INSERTED_COMMITTED.
 */
void
insertedDistributedCommitted(void)
{
	ereport(DTM_DEBUG5,
			(errmsg("entering insertedDistributedCommitted"),
			TM_ERRDETAIL));

	Assert(MyTmGxactLocal->state == DTX_STATE_INSERTING_COMMITTED);
	setCurrentDtxState(DTX_STATE_INSERTED_COMMITTED);

	/*
	 * We don't have to hold ProcArrayLock here because needIncludedInCkpt is used
	 * during creating checkpoint and we already set delayChkpt before we got here.
	 */
	Assert(MyPgXact->delayChkpt);
	if (IS_QUERY_DISPATCHER())
		MyTmGxact->needIncludedInCkpt = true;
}

/* generate global transaction id */
DistributedTransactionId
generateGID(void)
{
	DistributedTransactionId gxid;

	gxid = pg_atomic_add_fetch_u32((pg_atomic_uint32*)shmGIDSeq, 1);
	if (gxid == LastDistributedTransactionId)
		ereport(PANIC,
				(errmsg("reached the limit of %u global transactions per start",
						LastDistributedTransactionId)));

	return gxid;
}

/*
 * When called, a SET command is dispatched and the writer gang
 * writes the shared snapshot. This function actually does nothing
 * useful besides making sure that a writer gang is alive and has
 * set the shared snapshot so that the readers could access it.
 *
 * At this point this function is added as a helper for cursor
 * query execution since in MPP cursor queries don't use writer
 * gangs. However, it could be used for other purposes as well.
 *
 * See declaration of assign_gp_write_shared_snapshot(...) for more
 * information.
 */
void
verify_shared_snapshot_ready(void)
{
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchCommand("set gp_write_shared_snapshot=true",
						   DF_CANCEL_ON_ERROR |
						   DF_WITH_SNAPSHOT |
						   DF_NEED_TWO_PHASE,
						   NULL);

		dumpSharedLocalSnapshot_forCursor();

		/*
		 * To keep our readers in sync (since they will be dispatched
		 * separately) we need to rewind the segmate synchronizer.
		 */
		DtxContextInfo_RewindSegmateSync();
	}
}

/*
 * Force the writer QE to write the shared snapshot. Will get called
 * after a "set gp_write_shared_snapshot=<true/false>" is executed
 * in dispatch mode.
 *
 * See verify_shared_snapshot_ready(...) for additional information.
 */
void
assign_gp_write_shared_snapshot(bool newval, void *extra)
{

#if FALSE
	elog(DEBUG1, "SET gp_write_shared_snapshot: %s",
		 (newval ? "true" : "false"));
#endif

	/*
	 * Make sure newval is "true". if it's "false" this could be a part of a
	 * ROLLBACK so we don't want to set the snapshot then.
	 */
	if (newval)
	{
		if (Gp_role == GP_ROLE_EXECUTE)
		{
			PushActiveSnapshot(GetTransactionSnapshot());

			if (Gp_is_writer)
			{
				dumpSharedLocalSnapshot_forCursor();
			}

			PopActiveSnapshot();
		}
	}
}

static void
doQEDistributedExplicitBegin()
{
	/*
	 * Start a command.
	 */
	StartTransactionCommand();

	/* Here is the explicit BEGIN. */
	BeginTransactionBlock();

	/*
	 * Finish the BEGIN command.  It will leave the explict transaction
	 * in-progress.
	 */
	CommitTransactionCommand();
}

static bool
isDtxQueryDispatcher(void)
{
	bool		isDtmStarted;
	bool		isSharedLocalSnapshotSlotPresent;

	isDtmStarted = (shmDtmStarted != NULL && *shmDtmStarted);
	isSharedLocalSnapshotSlotPresent = (SharedLocalSnapshotSlot != NULL);

	return (Gp_role == GP_ROLE_DISPATCH &&
			isDtmStarted &&
			isSharedLocalSnapshotSlotPresent);
}

/*
 * Called prior to handling a requested that comes to the QD, or a utility request to a QE.
 *
 * Sets up the distributed transaction context value and does some basic error checking.
 *
 * Essentially:
 *     if the DistributedTransactionContext is already QD_DISTRIBUTED_CAPABLE then leave it
 *     else if the DistributedTransactionContext is already QE_TWO_PHASE_EXPLICIT_WRITER then leave it
 *     else it MUST be a LOCAL_ONLY, and is converted to QD_DISTRIBUTED_CAPABLE if this process is acting
 *          as a QE.
 */
void
setupRegularDtxContext(void)
{
	switch (DistributedTransactionContext)
	{
		case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
			/* Continue in this context.  Do not touch QEDtxContextInfo, etc. */
			break;

		case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
			/* Allow this for copy...???  Do not touch QEDtxContextInfo, etc. */
			break;

		default:
			if (DistributedTransactionContext != DTX_CONTEXT_LOCAL_ONLY)
			{
				/*
				 * we must be one of:
				 *
				 * DTX_CONTEXT_QD_RETRY_PHASE_2,
				 * DTX_CONTEXT_QE_ENTRY_DB_SINGLETON,
				 * DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT,
				 * DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER,
				 * DTX_CONTEXT_QE_READER, DTX_CONTEXT_QE_PREPARED
				 */

				elog(ERROR, "setupRegularDtxContext finds unexpected DistributedTransactionContext = '%s'",
					 DtxContextToString(DistributedTransactionContext));
			}

			/* DistributedTransactionContext is DTX_CONTEXT_LOCAL_ONLY */

			Assert(QEDtxContextInfo.distributedXid == InvalidDistributedTransactionId);

			/*
			 * Determine if we are strictly local or a distributed capable QD.
			 */
			Assert(DistributedTransactionContext == DTX_CONTEXT_LOCAL_ONLY);

			if (isDtxQueryDispatcher())
			{
				setDistributedTransactionContext(DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE);
			}
			break;
	}

	elog(DTM_DEBUG5, "setupRegularDtxContext leaving with DistributedTransactionContext = '%s'.",
		 DtxContextToString(DistributedTransactionContext));
}

/**
 * Called on the QE when a query to process has been received.
 *
 * This will set up all distributed transaction information and set the state appropriately.
 */
void
setupQEDtxContext(DtxContextInfo *dtxContextInfo)
{
	DistributedSnapshot *distributedSnapshot;
	int			txnOptions;
	bool		needTwoPhase;
	bool		explicitBegin;
	bool		haveDistributedSnapshot;
	bool		isEntryDbSingleton = false;
	bool		isReaderQE = false;
	bool		isWriterQE = false;
	bool		isSharedLocalSnapshotSlotPresent;

	Assert(dtxContextInfo != NULL);

	/*
	 * DTX Context Info (even when empty) only comes in QE requests.
	 */
	distributedSnapshot = &dtxContextInfo->distributedSnapshot;
	txnOptions = dtxContextInfo->distributedTxnOptions;

	needTwoPhase = isMppTxOptions_NeedTwoPhase(txnOptions);
	explicitBegin = isMppTxOptions_ExplicitBegin(txnOptions);

	haveDistributedSnapshot = dtxContextInfo->haveDistributedSnapshot;
	isSharedLocalSnapshotSlotPresent = (SharedLocalSnapshotSlot != NULL);

	if (DEBUG5 >= log_min_messages || Debug_print_full_dtm)
	{
		elog(DTM_DEBUG5,
			 "setupQEDtxContext inputs (part 1): Gp_role = %s, Gp_is_writer = %s, "
			 "txnOptions = 0x%x, needTwoPhase = %s, explicitBegin = %s, isoLevel = %s, readOnly = %s, haveDistributedSnapshot = %s.",
			 role_to_string(Gp_role), (Gp_is_writer ? "true" : "false"), txnOptions,
			 (needTwoPhase ? "true" : "false"), (explicitBegin ? "true" : "false"),
			 IsoLevelAsUpperString(mppTxOptions_IsoLevel(txnOptions)), (isMppTxOptions_ReadOnly(txnOptions) ? "true" : "false"),
			 (haveDistributedSnapshot ? "true" : "false"));
		elog(DTM_DEBUG5,
			 "setupQEDtxContext inputs (part 2): distributedXid = %u, isSharedLocalSnapshotSlotPresent = %s.",
			 dtxContextInfo->distributedXid,
			 (isSharedLocalSnapshotSlotPresent ? "true" : "false"));

		if (haveDistributedSnapshot)
		{
			elog(DTM_DEBUG5,
				 "setupQEDtxContext inputs (part 2a): distributedXid = %u, "
				 "distributedSnapshotData (xmin = %u, xmax = %u, xcnt = %u), distributedCommandId = %d",
				 dtxContextInfo->distributedXid,
				 distributedSnapshot->xmin, distributedSnapshot->xmax,
				 distributedSnapshot->count,
				 dtxContextInfo->curcid);
		}
		if (isSharedLocalSnapshotSlotPresent)
		{
			if (DTM_DEBUG5 >= log_min_messages)
			{
				LWLockAcquire(SharedLocalSnapshotSlot->slotLock, LW_SHARED);
				elog(DTM_DEBUG5,
					 "setupQEDtxContext inputs (part 2b):  shared local snapshot xid = %u "
					 "(xmin: %u xmax: %u xcnt: %u) curcid: %d, QDxid = %u/%u, QDcid = %u",
					 SharedLocalSnapshotSlot->xid,
					 SharedLocalSnapshotSlot->snapshot.xmin,
					 SharedLocalSnapshotSlot->snapshot.xmax,
					 SharedLocalSnapshotSlot->snapshot.xcnt,
					 SharedLocalSnapshotSlot->snapshot.curcid,
					 SharedLocalSnapshotSlot->QDxid,
					 SharedLocalSnapshotSlot->segmateSync,
					 SharedLocalSnapshotSlot->QDcid);
				LWLockRelease(SharedLocalSnapshotSlot->slotLock);
			}
		}
	}

	switch (Gp_role)
	{
		case GP_ROLE_EXECUTE:
			if (IS_QUERY_DISPATCHER() && !Gp_is_writer)
			{
				isEntryDbSingleton = true;
			}
			else
			{
				/*
				 * NOTE: this is a bit hackish. It appears as though
				 * StartTransaction() gets called during connection setup
				 * before we even have time to setup our shared snapshot slot.
				 */
				if (SharedLocalSnapshotSlot == NULL)
				{
					if (explicitBegin || haveDistributedSnapshot)
					{
						elog(ERROR, "setupQEDtxContext not expecting distributed begin or snapshot when no Snapshot slot exists");
					}
				}
				else
				{
					if (Gp_is_writer)
					{
						isWriterQE = true;
					}
					else
					{
						isReaderQE = true;
					}
				}
			}
			break;

		default:
			Assert(DistributedTransactionContext == DTX_CONTEXT_LOCAL_ONLY);
			elog(DTM_DEBUG5,
				 "setupQEDtxContext leaving context = 'Local Only' for Gp_role = %s", role_to_string(Gp_role));
			return;
	}

	elog(DTM_DEBUG5,
		 "setupQEDtxContext intermediate result: isEntryDbSingleton = %s, isWriterQE = %s, isReaderQE = %s.",
		 (isEntryDbSingleton ? "true" : "false"),
		 (isWriterQE ? "true" : "false"), (isReaderQE ? "true" : "false"));

	/*
	 * Copy to our QE global variable.
	 */
	DtxContextInfo_Copy(&QEDtxContextInfo, dtxContextInfo);

	switch (DistributedTransactionContext)
	{
		case DTX_CONTEXT_LOCAL_ONLY:
			if (isEntryDbSingleton && haveDistributedSnapshot)
			{
				/*
				 * Later, in GetSnapshotData, we will adopt the QD's
				 * transaction and snapshot information.
				 */

				setDistributedTransactionContext(DTX_CONTEXT_QE_ENTRY_DB_SINGLETON);
			}
			else if (isReaderQE && haveDistributedSnapshot)
			{
				/*
				 * Later, in GetSnapshotData, we will adopt the QE Writer's
				 * transaction and snapshot information.
				 */

				setDistributedTransactionContext(DTX_CONTEXT_QE_READER);
			}
			else if (isWriterQE && (explicitBegin || needTwoPhase))
			{
				if (!haveDistributedSnapshot)
				{
					elog(DTM_DEBUG5,
						 "setupQEDtxContext Segment Writer is involved in a distributed transaction without a distributed snapshot...");
				}

				if (IsTransactionOrTransactionBlock())
				{
					elog(ERROR, "Starting an explicit distributed transaction in segment -- cannot already be in a transaction");
				}

				if (explicitBegin)
				{
					/*
					 * We set the DistributedTransactionContext BEFORE we
					 * create the transactions to influence the behavior of
					 * StartTransaction.
					 */
					setDistributedTransactionContext(DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER);

					doQEDistributedExplicitBegin();
				}
				else
				{
					Assert(needTwoPhase);
					setDistributedTransactionContext(DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER);
				}
			}
			else if (haveDistributedSnapshot)
			{
				if (IsTransactionOrTransactionBlock())
				{
					elog(ERROR,
						 "Going to start a local implicit transaction in segment using a distribute "
						 "snapshot -- cannot already be in a transaction");
				}

				/*
				 * Before executing the query, postgres.c make a standard call
				 * to StartTransactionCommand which will begin a local
				 * transaction with StartTransaction.  This is fine.
				 *
				 * However, when the snapshot is created later, the state
				 * below will tell GetSnapshotData to make the local snapshot
				 * from the distributed snapshot.
				 */
				setDistributedTransactionContext(DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT);
			}
			else
			{
				Assert(!haveDistributedSnapshot);

				/*
				 * A local implicit transaction without reference to a
				 * distributed snapshot.  Stay in NONE state.
				 */
				Assert(DistributedTransactionContext == DTX_CONTEXT_LOCAL_ONLY);
			}
			break;

		case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
/*
		elog(NOTICE, "We should have left this transition state '%s' at the end of the previous command...",
			 DtxContextToString(DistributedTransactionContext));
*/
			Assert(IsTransactionOrTransactionBlock());

			if (explicitBegin)
			{
				elog(ERROR, "Cannot have an explicit BEGIN statement...");
			}
			break;

		case DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT:
			elog(ERROR, "We should have left this transition state '%s' at the end of the previous command",
				 DtxContextToString(DistributedTransactionContext));
			break;

		case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
			Assert(IsTransactionOrTransactionBlock());
			break;

		case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
		case DTX_CONTEXT_QE_READER:

			/*
			 * We are playing games with the xact.c code, so we shouldn't test
			 * with the IsTransactionOrTransactionBlock() routine.
			 */
			break;

		case DTX_CONTEXT_QE_PREPARED:
		case DTX_CONTEXT_QE_FINISH_PREPARED:
			elog(ERROR, "We should not be trying to execute a query in state '%s'",
				 DtxContextToString(DistributedTransactionContext));
			break;

		default:
			elog(PANIC, "Unexpected segment distribute transaction context value: %d",
				 (int) DistributedTransactionContext);
			break;
	}

	elog(DTM_DEBUG5, "setupQEDtxContext final result: DistributedTransactionContext = '%s'.",
		 DtxContextToString(DistributedTransactionContext));

	if (haveDistributedSnapshot)
	{
		elog((Debug_print_snapshot_dtm ? LOG : DEBUG5), "[Distributed Snapshot #%u] *Set QE* currcid = %d (gxid = %u, '%s')",
			 dtxContextInfo->distributedSnapshot.distribSnapshotId,
			 dtxContextInfo->curcid,
			 getDistributedTransactionId(),
			 DtxContextToString(DistributedTransactionContext));
	}

}

void
finishDistributedTransactionContext(char *debugCaller, bool aborted)
{
	DistributedTransactionId gxid;

	/*
	 * We let the 2 retry states go up to PostgresMain.c, otherwise everything
	 * MUST be complete.
	 */
	if (isCurrentDtxTwoPhaseActivated() &&
		(MyTmGxactLocal->state != DTX_STATE_RETRY_COMMIT_PREPARED &&
		 MyTmGxactLocal->state != DTX_STATE_RETRY_ABORT_PREPARED))
	{
		ereport(FATAL,
				(errmsg("Unexpected dtx status (caller = %s).", debugCaller),
				TM_ERRDETAIL));
	}

	gxid = getDistributedTransactionId();
	elog(DTM_DEBUG5,
		 "finishDistributedTransactionContext called to change DistributedTransactionContext from %s to %s (caller = %s, gxid = %u)",
		 DtxContextToString(DistributedTransactionContext),
		 DtxContextToString(DTX_CONTEXT_LOCAL_ONLY),
		 debugCaller,
		 gxid);

	setDistributedTransactionContext(DTX_CONTEXT_LOCAL_ONLY);

	DtxContextInfo_Reset(&QEDtxContextInfo);

}

static void
rememberDtxExplicitBegin(void)
{
	Assert (isCurrentDtxTwoPhaseActivated());

	if (!MyTmGxactLocal->explicitBeginRemembered)
	{
		ereport(DTM_DEBUG5,
				(errmsg("rememberDtxExplicitBegin explicit BEGIN"),
				TM_ERRDETAIL));
		MyTmGxactLocal->explicitBeginRemembered = true;
	}
	else
	{
		ereport(DTM_DEBUG5,
				(errmsg("rememberDtxExplicitBegin already an explicit BEGIN"),
				TM_ERRDETAIL));
	}
}

bool
isDtxExplicitBegin(void)
{
	return (isCurrentDtxTwoPhaseActivated() && MyTmGxactLocal->explicitBeginRemembered);
}

/*
 * This is mostly here because
 * cdbcopy doesn't use cdbdisp's services.
 */
void
sendDtxExplicitBegin(void)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	setupTwoPhaseTransaction();
	rememberDtxExplicitBegin();
}

/**
 * On the QD, run the Prepare operation.
 */
static void
performDtxProtocolPrepare(const char *gid)
{
	StartTransactionCommand();

	elog(DTM_DEBUG5, "performDtxProtocolCommand going to call PrepareTransactionBlock for distributed transaction (id = '%s')", gid);
	if (!PrepareTransactionBlock((char *) gid))
	{
		elog(ERROR, "Prepare of distributed transaction %s failed", gid);
		return;
	}

	/*
	 * Calling CommitTransactionCommand will cause the actual COMMIT/PREPARE
	 * work to be performed.
	 */
	CommitTransactionCommand();

	elog(DTM_DEBUG5, "Prepare of distributed transaction succeeded (id = '%s')", gid);

	setDistributedTransactionContext(DTX_CONTEXT_QE_PREPARED);
}


/**
 * On the QE, run the Commit one-phase operation.
 */
static void
performDtxProtocolCommitOnePhase(const char *gid)
{
	elog(DTM_DEBUG5,
		 "performDtxProtocolCommitOnePhase going to call CommitTransaction for distributed transaction %s", gid);

	/* MyTmGxact is now not used on QE for one-phase commit */
	dtxCrackOpenGid(gid, &MyTmGxact->distribTimeStamp, &MyTmGxact->gxid);
	MyTmGxactLocal->isOnePhaseCommit = true;

	StartTransactionCommand();

	if (!EndTransactionBlock())
	{
		elog(ERROR, "One-phase Commit of distributed transaction %s failed", gid);
		return;
	}

	/* Calling CommitTransactionCommand will cause the actual COMMIT work to be performed. */
	CommitTransactionCommand();

	finishDistributedTransactionContext("performDtxProtocolCommitOnePhase -- Commit onephase", false);
	MyProc->localDistribXactData.state = LOCALDISTRIBXACT_STATE_NONE;
}

/**
 * On the QD, run the Commit Prepared operation.
 */
static void
performDtxProtocolCommitPrepared(const char *gid, bool raiseErrorIfNotFound)
{
	elog(DTM_DEBUG5,
		 "performDtxProtocolCommitPrepared going to call FinishPreparedTransaction for distributed transaction %s", gid);

	StartTransactionCommand();

	/*
	 * Since this call may fail, lets setup a handler.
	 */
	PG_TRY();
	{
		FinishPreparedTransaction((char *) gid, /* isCommit */ true, raiseErrorIfNotFound);
	}
	PG_CATCH();
	{
		finishDistributedTransactionContext("performDtxProtocolCommitPrepared -- Commit Prepared (error case)", false);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Calling CommitTransactionCommand will cause the actual COMMIT/PREPARE
	 * work to be performed.
	 */
	CommitTransactionCommand();

	finishDistributedTransactionContext("performDtxProtocolCommitPrepared -- Commit Prepared", false);
}

/**
 * On the QD, run the Abort Prepared operation.
 */
static void
performDtxProtocolAbortPrepared(const char *gid, bool raiseErrorIfNotFound)
{
	elog(DTM_DEBUG5, "performDtxProtocolAbortPrepared going to call FinishPreparedTransaction for distributed transaction %s", gid);

	StartTransactionCommand();

	/*
	 * Since this call may fail, lets setup a handler.
	 */
	PG_TRY();
	{
		FinishPreparedTransaction((char *) gid, /* isCommit */ false, raiseErrorIfNotFound);
	}
	PG_CATCH();
	{
		finishDistributedTransactionContext("performDtxProtocolAbortPrepared -- Commit Prepared (error case)", true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * Calling CommitTransactionCommand will cause the actual COMMIT/PREPARE
	 * work to be performed.
	 */
	CommitTransactionCommand();

	finishDistributedTransactionContext("performDtxProtocolAbortPrepared -- Commit Prepared", true);
}

/**
 * On the QE, handle a DtxProtocolCommand
 */
void
performDtxProtocolCommand(DtxProtocolCommand dtxProtocolCommand,
						  const char *gid,
						  DtxContextInfo *contextInfo)
{
	elog(DTM_DEBUG5,
		 "performDtxProtocolCommand called with DTX protocol = %s, segment distribute transaction context: '%s'",
		 DtxProtocolCommandToString(dtxProtocolCommand), DtxContextToString(DistributedTransactionContext));

	switch (dtxProtocolCommand)
	{
		case DTX_PROTOCOL_COMMAND_ABORT_NO_PREPARED:
			elog(DTM_DEBUG5,
				 "performDtxProtocolCommand going to call AbortOutOfAnyTransaction for distributed transaction %s", gid);
			AbortOutOfAnyTransaction();
			break;

		case DTX_PROTOCOL_COMMAND_PREPARE:
		case DTX_PROTOCOL_COMMAND_COMMIT_ONEPHASE:

			/*
			 * The QD has directed us to read-only commit or prepare an
			 * implicit or explicit distributed transaction.
			 */
			switch (DistributedTransactionContext)
			{
				case DTX_CONTEXT_LOCAL_ONLY:

					/*
					 * Spontaneously aborted while we were back at the QD?
					 */
					elog(ERROR, "Distributed transaction %s not found", gid);
					break;

				case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
				case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
					if (dtxProtocolCommand == DTX_PROTOCOL_COMMAND_COMMIT_ONEPHASE)
						performDtxProtocolCommitOnePhase(gid);
					else
						performDtxProtocolPrepare(gid);
					break;

				case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
				case DTX_CONTEXT_QD_RETRY_PHASE_2:
				case DTX_CONTEXT_QE_PREPARED:
				case DTX_CONTEXT_QE_FINISH_PREPARED:
				case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
				case DTX_CONTEXT_QE_READER:
					elog(FATAL, "Unexpected segment distribute transaction context: '%s'",
						 DtxContextToString(DistributedTransactionContext));

				default:
					elog(PANIC, "Unexpected segment distribute transaction context value: %d",
						 (int) DistributedTransactionContext);
					break;
			}
			break;

		case DTX_PROTOCOL_COMMAND_ABORT_SOME_PREPARED:
			switch (DistributedTransactionContext)
			{
				case DTX_CONTEXT_LOCAL_ONLY:

					/*
					 * Spontaneously aborted while we were back at the QD?
					 *
					 * It's normal if the transaction doesn't exist. The QD will
					 * call abort on us, even if we didn't finish the prepare yet,
					 * if some other QE reported failure already.
					 */
					elog(DTM_DEBUG3, "Distributed transaction %s not found during abort", gid);
					AbortOutOfAnyTransaction();
					break;

				case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
				case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
					AbortOutOfAnyTransaction();
					break;

				case DTX_CONTEXT_QE_PREPARED:
					setDistributedTransactionContext(DTX_CONTEXT_QE_FINISH_PREPARED);
					performDtxProtocolAbortPrepared(gid, /* raiseErrorIfNotFound */ true);
					break;

				case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
				case DTX_CONTEXT_QD_RETRY_PHASE_2:
				case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
				case DTX_CONTEXT_QE_READER:
					elog(PANIC, "Unexpected segment distribute transaction context: '%s'",
						 DtxContextToString(DistributedTransactionContext));

				default:
					elog(PANIC, "Unexpected segment distribute transaction context value: %d",
						 (int) DistributedTransactionContext);
					break;
			}
			break;

		case DTX_PROTOCOL_COMMAND_COMMIT_PREPARED:
			requireDistributedTransactionContext(DTX_CONTEXT_QE_PREPARED);
			setDistributedTransactionContext(DTX_CONTEXT_QE_FINISH_PREPARED);
			performDtxProtocolCommitPrepared(gid, /* raiseErrorIfNotFound */ true);
			break;

		case DTX_PROTOCOL_COMMAND_ABORT_PREPARED:
			requireDistributedTransactionContext(DTX_CONTEXT_QE_PREPARED);
			setDistributedTransactionContext(DTX_CONTEXT_QE_FINISH_PREPARED);
			performDtxProtocolAbortPrepared(gid, /* raiseErrorIfNotFound */ true);
			break;

		case DTX_PROTOCOL_COMMAND_RETRY_COMMIT_PREPARED:
			requireDistributedTransactionContext(DTX_CONTEXT_LOCAL_ONLY);
			performDtxProtocolCommitPrepared(gid, /* raiseErrorIfNotFound */ false);
			break;

		case DTX_PROTOCOL_COMMAND_RETRY_ABORT_PREPARED:
			requireDistributedTransactionContext(DTX_CONTEXT_LOCAL_ONLY);
			performDtxProtocolAbortPrepared(gid, /* raiseErrorIfNotFound */ false);
			break;

		case DTX_PROTOCOL_COMMAND_RECOVERY_COMMIT_PREPARED:
			requireDistributedTransactionContext(DTX_CONTEXT_LOCAL_ONLY);
			performDtxProtocolCommitPrepared(gid, /* raiseErrorIfNotFound */ false);
			break;

		case DTX_PROTOCOL_COMMAND_RECOVERY_ABORT_PREPARED:
			requireDistributedTransactionContext(DTX_CONTEXT_LOCAL_ONLY);
			performDtxProtocolAbortPrepared(gid, /* raiseErrorIfNotFound */ false);
			break;

		case DTX_PROTOCOL_COMMAND_SUBTRANSACTION_BEGIN_INTERNAL:
			switch (DistributedTransactionContext)
			{
				case DTX_CONTEXT_LOCAL_ONLY:

					/*
					 * QE is not aware of DTX yet. A typical case is SELECT
					 * foo(), where foo() opens internal subtransaction
					 */
					setupQEDtxContext(contextInfo);
					StartTransactionCommand();
					break;
				case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:

					/*
					 * We already marked this QE to be writer, and transaction
					 * is open.
					 */
				case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
				case DTX_CONTEXT_QE_READER:
					break;
				default:
					/* Lets flag this situation out, with explicit crash */
					Assert(false);
					elog(DTM_DEBUG5,
						 " SUBTRANSACTION_BEGIN_INTERNAL distributed transaction context invalid: %d",
						 (int) DistributedTransactionContext);
					break;
			}

			BeginInternalSubTransaction(NULL);
			Assert(contextInfo->nestingLevel + 1 == GetCurrentTransactionNestLevel());
			break;

		case DTX_PROTOCOL_COMMAND_SUBTRANSACTION_RELEASE_INTERNAL:
			Assert(contextInfo->nestingLevel == GetCurrentTransactionNestLevel());
			ReleaseCurrentSubTransaction();
			break;

		case DTX_PROTOCOL_COMMAND_SUBTRANSACTION_ROLLBACK_INTERNAL:

			/*
			 * Rollback performs work on master and then dispatches, hence has
			 * nestingLevel its expecting post operation
			 */
			if ((contextInfo->nestingLevel + 1) > GetCurrentTransactionNestLevel())
			{
				ereport(ERROR,
						(errmsg("transaction %s at level %d already processed (current level %d)",
								gid, contextInfo->nestingLevel, GetCurrentTransactionNestLevel())));
			}

			unsigned int i = GetCurrentTransactionNestLevel() - contextInfo->nestingLevel;

			while (i > 0)
			{
				RollbackAndReleaseCurrentSubTransaction();
				i--;
			}

			Assert(contextInfo->nestingLevel == GetCurrentTransactionNestLevel());
			break;

		default:
			elog(ERROR, "Unrecognized dtx protocol command: %d",
				 (int) dtxProtocolCommand);
			break;
	}
	elog(DTM_DEBUG5, "performDtxProtocolCommand successful return for distributed transaction %s", gid);
}

void
markCurrentGxactWriterGangLost(void)
{
	MyTmGxactLocal->writerGangLost = true;
}

bool
currentGxactWriterGangLost(void)
{
	return MyTmGxactLocal->writerGangLost;
}

/*
 * Record which segment involved in the two phase commit.
 */
void
addToGxactTwophaseSegments(Gang *gang)
{
	SegmentDatabaseDescriptor *segdbDesc;
	MemoryContext oldContext;
	int segindex;
	int i;

	if (!isCurrentDtxTwoPhaseActivated())
		return;

	/* skip if all segdbs are in the list */
	if (list_length(MyTmGxactLocal->twophaseSegments) >= getgpsegmentCount())
		return;

	oldContext = MemoryContextSwitchTo(TopTransactionContext);
	for (i = 0; i < gang->size; i++)
	{
		segdbDesc = gang->db_descriptors[i];
		Assert(segdbDesc);
		segindex = segdbDesc->segindex;

		/* entry db is just a reader, will not involve in two phase commit */
		if (segindex == -1)
			continue;

		/* skip if record already */
		if (bms_is_member(segindex, MyTmGxactLocal->twophaseSegmentsMap))
			continue;

		MyTmGxactLocal->twophaseSegmentsMap =
			bms_add_member(MyTmGxactLocal->twophaseSegmentsMap, segindex);

		MyTmGxactLocal->twophaseSegments =
			lappend_int(MyTmGxactLocal->twophaseSegments, segindex);
	}
	MemoryContextSwitchTo(oldContext);
}
