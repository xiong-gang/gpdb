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
#include "cdb/cdbvars.h"

#include "gp-libpq-fe.h"


static void dtxBuildDispatchString(DispatchCommandParms *pParms);
static void dtxDispatchCommand(struct CdbDispatchResult *dispatchResult, DispatchCommandParms *pParms);
static void dtxDispatchDestroy(DispatchCommandParms *pParms);
static void dtxDispatchInit(DispatchCommandParms *pParms, void *inputParms);

DispatchType DtxDispatchType = {
		GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL,
		dtxBuildDispatchString,
		dtxDispatchCommand,
		dtxDispatchInit,
		dtxDispatchDestroy
};


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

/* Helper function to thread_DispatchCommand that actually kicks off the
 * command on the libpq connection.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
static void
dispatchCommandDtxProtocol(CdbDispatchResult	*dispatchResult,
						   const char			*query_text,
						   int					query_text_len,
						   DtxProtocolCommand	dtxProtocolCommand)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	PGconn	   *conn = segdbDesc->conn;

	/* Don't use elog, it's not thread-safe */
	if (DEBUG3 >= log_min_messages)
		write_log("%s <- dtx protocol command %d", segdbDesc->whoami, (int)dtxProtocolCommand);

	/*
	 * Submit the command asynchronously.
	 */
	if (PQsendGpQuery_shared(conn, (char *)query_text, query_text_len) == 0)
	{
		char *msg = PQerrorMessage(segdbDesc->conn);

		if (DEBUG3 >= log_min_messages)
			write_log("PQsendMPPQuery_shared error %s %s",segdbDesc->whoami,
						 msg ? msg : "");
		/* Note the error. */
		cdbdisp_appendMessage(dispatchResult, LOG,
							  ERRCODE_GP_INTERCONNECTION_ERROR,
							  "Command could not be sent to segment db %s;  %s",
							  segdbDesc->whoami,
							  msg ? msg : "");
		PQfinish(conn);
		segdbDesc->conn = NULL;
		dispatchResult->stillRunning = false;
	}

	/*
	 * We'll keep monitoring this QE -- whether or not the command
	 * was dispatched -- in order to check for a lost connection
	 * or any other errors that libpq might have in store for us.
	 */
}	/* dispatchCommand */


static void
dtxBuildDispatchString(DispatchCommandParms *pParms)
{
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

static void
dtxDispatchCommand(CdbDispatchResult *dispatchResult, DispatchCommandParms *pParms)
{
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

	dispatchCommandDtxProtocol(dispatchResult, pParms->query_text, pParms->query_text_len, 
    									   pParms->dtxProtocolParms.dtxProtocolCommand);

	dispatchResult->hasDispatched = true;
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
}
