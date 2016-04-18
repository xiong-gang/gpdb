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
#include "utils/guc.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"

#include "storage/procarray.h"      /* updateSharedLocalSnapshot */

#include "gp-libpq-fe.h"


static void dtxDispatchCommand(struct CdbDispatchResult *dispatchResult, DispatchCommandParms *pParms);
static void dtxDispatchDestroy(DispatchCommandParms *pParms);
static void dtxDispatchInit(DispatchCommandParms *pParms, void *inputParms);

DispatchType DtxDispatchType = {
		GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL,
		dtxDispatchCommand,
		dtxDispatchInit,
		dtxDispatchDestroy
};

static DtxContextInfo TempQDDtxContextInfo = DtxContextInfo_StaticInit;
/*
 * cdbdisp_dispatchDtxProtocolCommand:
 * Sends a non-cancelable command to all segment dbs
 *
 * Returns a malloc'ed array containing the PGresult objects thus
 * produced; the caller must PQclear() them and free() the array.
 * A NULL entry follows the last used entry in the array.
 *
 * Any error messages - whether or not they are associated with
 * PGresult objects - are appended to a StringInfo buffer provided
 * by the caller.
 */
struct pg_result **				/* returns ptr to array of PGresult ptrs */
cdbdisp_dispatchDtxProtocolCommand(DtxProtocolCommand dtxProtocolCommand,
								   int	flags,
								   char	*dtxProtocolCommandLoggingStr,
								   char	*gid,
								   DistributedTransactionId	gxid,
								   StringInfo errmsgbuf,
								   int *numresults,
								   bool *badGangs,
								   CdbDispatchDirectDesc *direct,
								   char *argument, int argumentLength)
{
	CdbDispatcherState ds = {NULL, NULL};

	PGresult  **resultSets = NULL;

	DispatchCommandDtxProtocolParms dtxProtocolParms;
	Gang	*primaryGang;
	int		nsegdb = getgpsegmentCount();

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "cdbdisp_dispatchDtxProtocolCommand: %s for gid = %s, direct content #: %d",
		 dtxProtocolCommandLoggingStr, gid, direct->directed_dispatch ? direct->content[0] : -1);

	*badGangs = false;

	MemSet(&dtxProtocolParms, 0, sizeof(dtxProtocolParms));
	dtxProtocolParms.dtxProtocolCommand = dtxProtocolCommand;
	dtxProtocolParms.flags = flags;
	dtxProtocolParms.dtxProtocolCommandLoggingStr = dtxProtocolCommandLoggingStr;
	if (strlen(gid) >= TMGIDSIZE)
		elog(PANIC, "Distribute transaction identifier too long (%d)",
			 (int)strlen(gid));
	memcpy(dtxProtocolParms.gid, gid, TMGIDSIZE);
	dtxProtocolParms.gxid = gxid;
	dtxProtocolParms.argument = argument;
	dtxProtocolParms.argumentLength = argumentLength;

	/*
	 * Allocate a primary QE for every available segDB in the system.
	 */
	primaryGang = allocateWriterGang();

	Assert(primaryGang);

	if (primaryGang->dispatcherActive)
	{
		elog(LOG, "cdbdisp_dispatchDtxProtocolCommand: primary gang marked active re-marking");
		primaryGang->dispatcherActive = false;
	}

	dtxProtocolParms.primary_gang_id = primaryGang->gang_id;

	/*
     * Dispatch the command.
     */
    ds.dispatchThreads = NULL;

	ds.primaryResults = cdbdisp_makeDispatchResults(nsegdb, 0, /* cancelOnError */ false);

	ds.primaryResults->writer_gang = primaryGang;

	cdbdisp_dispatchToGang(&ds, &DtxDispatchType,
						   &dtxProtocolParms,
						   primaryGang, -1, 1, direct);

	/* Wait for all QEs to finish.	Don't cancel. */
	CdbCheckDispatchResult(&ds, DISPATCH_WAIT_NONE);

	if (!gangOK(primaryGang))
	{
		*badGangs = true;

		elog((Debug_print_full_dtm ? LOG : DEBUG5),
			 "cdbdisp_dispatchDtxProtocolCommand: Bad gang from dispatch of %s for gid = %s",
			 dtxProtocolCommandLoggingStr, gid);
	}

	resultSets = cdbdisp_returnResults(ds.primaryResults, errmsgbuf, numresults);

	/* free memory allocated for the dispatch threads struct */
	cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

	return resultSets;
}	/* cdbdisp_dispatchDtxProtocolCommand */





char *
qdSerializeDtxContextInfo(int *size, bool wantSnapshot, bool inCursor, int txnOptions, char *debugCaller)
{
	char *serializedDtxContextInfo;

	Snapshot snapshot;
	int serializedLen;
	DtxContextInfo *pDtxContextInfo = NULL;

	/* If we already have a LatestSnapshot set then no reason to try
	 * and get a new one.  just use that one.  But... there is one important
	 * reason why this HAS to be here.  ROLLBACK stmts get dispatched to QEs
	 * in the abort transaction code.  This code tears down enough stuff such
	 * that you can't call GetTransactionSnapshot() within that code. So we
	 * need to use the LatestSnapshot since we can't re-gen a new one.
	 *
	 * It is also very possible that for a single user statement which may
	 * only generate a single snapshot that we will dispatch multiple statements
	 * to our qExecs.  Something like:
	 *
	 *							QD				QEs
	 *							|  				|
	 * User SQL Statement ----->|	  BEGIN		|
	 *  						|-------------->|
	 *						    | 	  STMT		|
	 *							|-------------->|
	 *						 	|    PREPARE	|
	 *							|-------------->|
	 *							|    COMMIT		|
	 *							|-------------->|
	 *							|				|
	 *
	 * This may seem like a problem because all four of those will dispatch
	 * the same snapshot with the same curcid.  But... this is OK because
	 * BEGIN, PREPARE, and COMMIT don't need Snapshots on the QEs.
	 *
	 * NOTE: This will be a problem if we ever need to dispatch more than one
	 *  	 statement to the qExecs and more than one needs a snapshot!
	 */
	*size = 0;
	snapshot = NULL;

	if (wantSnapshot)
	{

		if (LatestSnapshot == NULL &&
			SerializableSnapshot == NULL &&
			!IsAbortInProgress() )
		{
			/* unfortunately, the dtm issues a select for prepared xacts at the
			 * beginning and this is before a snapshot has been set up.  so we need
			 * one for that but not for when we dont have a valid XID.
			 *
			 * but we CANT do this if an ABORT is in progress... instead we'll send
			 * a NONE since the qExecs dont need the information to do a ROLLBACK.
			 *
			 */
			elog((Debug_print_full_dtm ? LOG : DEBUG5),"qdSerializeDtxContextInfo calling GetTransactionSnapshot to make snapshot");

			GetTransactionSnapshot();
		}

		if (LatestSnapshot != NULL)
		{
			elog((Debug_print_full_dtm ? LOG : DEBUG5),"qdSerializeDtxContextInfo using LatestSnapshot");

			snapshot = LatestSnapshot;
			elog((Debug_print_snapshot_dtm ? LOG : DEBUG5),"[Distributed Snapshot #%u] *QD Use Latest* currcid = %d (gxid = %u, '%s')",
				 LatestSnapshot->distribSnapshotWithLocalMapping.header.distribSnapshotId,
				 LatestSnapshot->curcid,
				 getDistributedTransactionId(),
				 DtxContextToString(DistributedTransactionContext));
		}
		else if (SerializableSnapshot != NULL)
		{
			elog((Debug_print_full_dtm ? LOG : DEBUG5),"qdSerializeDtxContextInfo using SerializableSnapshot");

			snapshot = SerializableSnapshot;
			elog((Debug_print_snapshot_dtm ? LOG : DEBUG5),"[Distributed Snapshot #%u] *QD Use Serializable* currcid = %d (gxid = %u, '%s')",
				 SerializableSnapshot->distribSnapshotWithLocalMapping.header.distribSnapshotId,
				 SerializableSnapshot->curcid,
				 getDistributedTransactionId(),
				 DtxContextToString(DistributedTransactionContext));

		}
	}


	switch (DistributedTransactionContext)
	{
		case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
		case DTX_CONTEXT_LOCAL_ONLY:
			if (snapshot != NULL)
			{
				DtxContextInfo_CreateOnMaster(&TempQDDtxContextInfo,
											  &snapshot->distribSnapshotWithLocalMapping,
											  snapshot->curcid, txnOptions);
			}
			else
			{
				DtxContextInfo_CreateOnMaster(&TempQDDtxContextInfo,
											  NULL, 0, txnOptions);
			}

			TempQDDtxContextInfo.cursorContext = inCursor;

			if (DistributedTransactionContext == DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE &&
				snapshot != NULL)
			{
				updateSharedLocalSnapshot(&TempQDDtxContextInfo, snapshot, "qdSerializeDtxContextInfo");
			}

			pDtxContextInfo = &TempQDDtxContextInfo;
			break;

		case DTX_CONTEXT_QD_RETRY_PHASE_2:
		case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
		case DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT:
		case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
		case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
		case DTX_CONTEXT_QE_READER:
		case DTX_CONTEXT_QE_PREPARED:
		case DTX_CONTEXT_QE_FINISH_PREPARED:
			elog(FATAL, "Unexpected distribute transaction context: '%s'",
				 DtxContextToString(DistributedTransactionContext));

		default:
			elog(FATAL, "Unrecognized DTX transaction context: %d",
				 (int)DistributedTransactionContext);
	}

	serializedLen = DtxContextInfo_SerializeSize(pDtxContextInfo);
	Assert (serializedLen > 0);

	*size = serializedLen;
	serializedDtxContextInfo = palloc(*size);

	DtxContextInfo_Serialize(serializedDtxContextInfo, pDtxContextInfo);

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"qdSerializeDtxContextInfo (called by %s) returning a snapshot of %d bytes (ptr is %s)",
	     debugCaller, *size, (serializedDtxContextInfo != NULL ? "Non-NULL" : "NULL"));
	return serializedDtxContextInfo;
}



static void
dtxDispatchCommand(CdbDispatchResult *dispatchResult, DispatchCommandParms *pParms)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	TimestampTz beforeSend = 0;
	long		secs;
	int			usecs;

	if (Debug_dtm_action == DEBUG_DTM_ACTION_DELAY &&
		Debug_dtm_action_target == DEBUG_DTM_ACTION_TARGET_PROTOCOL &&
		Debug_dtm_action_protocol == pParms->dtxProtocolParms.dtxProtocolCommand &&
		Debug_dtm_action_segment == dispatchResult->segdbDesc->segment_database_info->segindex)
	{
		write_log("Delaying '%s' broadcast for segment %d by %d milliseconds.",
				  DtxProtocolCommandToString(Debug_dtm_action_protocol),
				  Debug_dtm_action_segment,
				  Debug_dtm_action_delay_ms);
		pg_usleep(Debug_dtm_action_delay_ms * 1000);
	}

	/* Don't use elog, it's not thread-safe */
	if (DEBUG3 >= log_min_messages)
		write_log("%s <- dtx protocol command %d", segdbDesc->whoami, (int)pParms->dtxProtocolParms.dtxProtocolCommand);

	if (DEBUG1 >= log_min_messages)
		beforeSend = GetCurrentTimestamp();

	dispatchCommand(dispatchResult, pParms->query_text, pParms->query_text_len);

	if (DEBUG1 >= log_min_messages)
	{
		TimestampDifference(beforeSend,
							GetCurrentTimestamp(),
							&secs, &usecs);

		if (secs != 0 || usecs > 1000) /* Time > 1ms? */
			write_log("time for PQsendGpQuery_shared %ld.%06d", secs, usecs);
	}
}


static void
dtxDispatchDestroy(DispatchCommandParms *pParms)
{
	DispatchCommandDtxProtocolParms *pDtxProtocolParms = &pParms->dtxProtocolParms;
	pDtxProtocolParms->dtxProtocolCommand = 0;
}


static void
dtxDispatchInit(DispatchCommandParms *pParms, void *inputParms)
{
	DispatchCommandDtxProtocolParms *pDtxProtocolParms = (DispatchCommandDtxProtocolParms *) inputParms;

	pParms->dtxProtocolParms.dtxProtocolCommand = pDtxProtocolParms->dtxProtocolCommand;
	pParms->dtxProtocolParms.flags = pDtxProtocolParms->flags;
	pParms->dtxProtocolParms.dtxProtocolCommandLoggingStr = pDtxProtocolParms->dtxProtocolCommandLoggingStr;
	if (strlen(pDtxProtocolParms->gid) >= TMGIDSIZE)
		elog(PANIC, "Distribute transaction identifier too long (%d)",
			 (int)strlen(pDtxProtocolParms->gid));
	memcpy(pParms->dtxProtocolParms.gid, pDtxProtocolParms->gid, TMGIDSIZE);
	pParms->dtxProtocolParms.gxid = pDtxProtocolParms->gxid;
	pParms->dtxProtocolParms.primary_gang_id = pDtxProtocolParms->primary_gang_id;
	pParms->dtxProtocolParms.argument = pDtxProtocolParms->argument;
	pParms->dtxProtocolParms.argumentLength = pDtxProtocolParms->argumentLength;
    pParms->query_text = PQbuildGpDtxProtocolCommand(
            (int)pParms->dtxProtocolParms.dtxProtocolCommand,
            pParms->dtxProtocolParms.flags,
            pParms->dtxProtocolParms.dtxProtocolCommandLoggingStr,
            pParms->dtxProtocolParms.gid,
            pParms->dtxProtocolParms.gxid,
            pParms->dtxProtocolParms.primary_gang_id,
            pParms->dtxProtocolParms.argument,
            pParms->dtxProtocolParms.argumentLength,
            &pParms->query_text_len);
}
