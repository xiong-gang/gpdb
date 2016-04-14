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
#include "cdb/cdbconn.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbmutate.h"
#include "cdb/cdbrelsize.h"
#include "cdb/cdbsrlz.h"
#include "tcop/tcopprot.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "miscadmin.h"

static void queryBuildDispatchString(DispatchCommandParms *pParms);
static void queryDispatchCommand(CdbDispatchResult *dispatchResult, DispatchCommandParms *pParms);
static void queryDispatchDestroy(DispatchCommandParms *pParms);
static void queryDispatchInit(DispatchCommandParms *pParms, void *inputParms);

/* determines which dispatchOptions need to be set. */
static int generateTxnOptions(bool needTwoPhase);
static void remove_subquery_in_RTEs(Node *node);
static void
CdbDispatchUtilityStatement_Internal(struct Node *stmt, bool needTwoPhase, char* debugCaller);
static void
cdbdisp_dispatchSetCommandToAllGangs(const char	*strCommand,
						char					*serializedQuerytree,
						int						serializedQuerytreelen,
						char					*serializedPlantree,
						int						serializedPlantreelen,
                        bool					cancelOnError,
                        bool					needTwoPhase,
                        struct CdbDispatcherState *ds);

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

extern bool Test_print_direct_dispatch_info;

DispatchType QueryDispatchType = {
		GP_DISPATCH_COMMAND_TYPE_QUERY,
		queryBuildDispatchString,
		queryDispatchCommand,
		queryDispatchInit,
		queryDispatchDestroy
};


/*
 * This code was refactored out of cdbdisp_dispatchPlan.  It's
 * used both for dispatching plans when we are using normal gangs,
 * and for dispatching all statements from Query Dispatch Agents
 * when we are using dispatch agents.
 */
void
cdbdisp_dispatchX(DispatchCommandQueryParms *pQueryParms,
				  bool cancelOnError,
				  struct SliceTable *sliceTbl,
				  struct CdbDispatcherState *ds)
{
	int			oldLocalSlice = 0;
	sliceVec	*sliceVector = NULL;
	int			nSlices = 1;
	int			sliceLim = 1;
	int			iSlice;
	int			rootIdx = pQueryParms->rootIdx;

	if (log_dispatch_stats)
		ResetUsage();

	ds->primaryResults = NULL;
	ds->dispatchThreads = NULL;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	if (sliceTbl)
	{
		Assert(rootIdx == 0 ||
			   (rootIdx > sliceTbl->nMotions &&
				rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));

		/*
		 * Keep old value so we can restore it.  We use this field as a parameter.
		 */
		oldLocalSlice = sliceTbl->localSlice;

		/*
		 * Traverse the slice tree in sliceTbl rooted at rootIdx and build a
		 * vector of slice indexes specifying the order of [potential] dispatch.
		 */
		sliceLim = list_length(sliceTbl->slices);
		sliceVector = palloc0(sliceLim * sizeof(sliceVec));

		nSlices = fillSliceVector(sliceTbl, rootIdx, sliceVector, sliceLim);
	}

	/* Allocate result array with enough slots for QEs of primary gangs. */
	ds->primaryResults = cdbdisp_makeDispatchResults(nSlices * largestGangsize(),
												   sliceLim,
												   cancelOnError);

	cdb_total_plans++;
	cdb_total_slices += nSlices;
	if (nSlices > cdb_max_slices)
		cdb_max_slices = nSlices;

	/* must have somebody to dispatch to. */
	Assert(sliceTbl != NULL || pQueryParms->primary_gang_id > 0);

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to start of dispatch send (root %d): %s ms", pQueryParms->rootIdx, msec_str)));
				break;
		}
	}

	/*
	 * Now we need to call CDBDispatchCommand once per slice.  Each such
	 * call dispatches a MPPEXEC command to each of the QEs assigned to
	 * the slice.
	 *
	 * The QE information goes in two places: (1) in the argument to the
	 * function CDBDispatchCommand, and (2) in the serialized
	 * command sent to the QEs.
	 *
	 * So, for each slice in the tree...
	 */
	for (iSlice = 0; iSlice < nSlices; iSlice++)
	{
		CdbDispatchDirectDesc direct;
		Gang	   *primaryGang = NULL;
		Slice *slice = NULL;
		int si = -1;

		if (sliceVector)
		{
			/*
			 * Sliced dispatch, and we are either the dispatch agent, or we are
			 * the QD and are not using Dispatch Agents.
			 *
			 * So, dispatch to each slice.
			 */
			slice = sliceVector[iSlice].slice;
			si = slice->sliceIndex;

			/*
			 * Is this a slice we should dispatch?
			 */
			if (slice && slice->gangType == GANGTYPE_UNALLOCATED)
			{
				/*
				 * Most slices are dispatched, however, in many  cases the
				 * root runs only on the QD and is not dispatched to the QEs.
				 */
				continue;
			}

			primaryGang = slice->primaryGang;

			/*
			 * If we are on the dispatch agent, the gang pointers aren't filled in.
			 * We must look them up by ID
			 */
			if (primaryGang == NULL)
			{
				elog(DEBUG2,"Dispatch %d, Gangs are %d, type=%d",iSlice, slice->primary_gang_id, slice->gangType);
				primaryGang = findGangById(slice->primary_gang_id);

				Assert(primaryGang != NULL);
				if (primaryGang != NULL)
					Assert(primaryGang->type == slice->gangType || primaryGang->type == GANGTYPE_PRIMARY_WRITER);

				if (primaryGang == NULL)
					continue;
			}

			if (slice->directDispatch.isDirectDispatch)
			{
				direct.directed_dispatch = true;
				direct.count = list_length(slice->directDispatch.contentIds);
				Assert(direct.count == 1); /* only support to single content right now.  If this changes then we need to change from a list to another structure to avoid n^2 cases */
				direct.content[0] = linitial_int(slice->directDispatch.contentIds);

				if (Test_print_direct_dispatch_info)
				{
					elog(INFO, "Dispatch command to SINGLE content");
				}
			}
			else
			{
				direct.directed_dispatch = false;
				direct.count = 0;

				if (Test_print_direct_dispatch_info)
				{
					elog(INFO, "Dispatch command to ALL contents");
				}
			}
		}
		else
		{
			direct.directed_dispatch = false;
			direct.count = 0;

			if (Test_print_direct_dispatch_info)
			{
				elog(INFO, "Dispatch command to ALL contents");
			}

			/*
			 *  Non-sliced, used specified gangs
			 */
			elog(DEBUG2,"primary %d",pQueryParms->primary_gang_id);
			if (pQueryParms->primary_gang_id > 0)
				primaryGang = findGangById(pQueryParms->primary_gang_id);
		}

		Assert(primaryGang != NULL);	/* Must have a gang to dispatch to */
		if (primaryGang)
		{
			Assert(ds->primaryResults && ds->primaryResults->resultArray);
		}

		/* Bail out if already got an error or cancellation request. */
		if (cancelOnError)
		{
			if (ds->primaryResults->errcode)
				break;
			if (InterruptPending)
				break;
		}

		/*
		 * Dispatch the plan to our primaryGang.
		 * Doesn't wait for it to finish.
		 */
		if (primaryGang != NULL)
		{
			if (primaryGang->type == GANGTYPE_PRIMARY_WRITER)
				ds->primaryResults->writer_gang = primaryGang;

			cdbdisp_dispatchToGang(ds,
								   &QueryDispatchType,
								   pQueryParms, primaryGang,
								   si, sliceLim, &direct);
		}
	}

	if (sliceVector)
		pfree(sliceVector);

	if (sliceTbl)
		sliceTbl->localSlice = oldLocalSlice;

	/*
	 * If bailed before completely dispatched, stop QEs and throw error.
	 */
	if (iSlice < nSlices)
	{
		elog(Debug_cancel_print ? LOG : DEBUG2, "Plan dispatch canceled; dispatched %d of %d slices", iSlice, nSlices);

		/* Cancel any QEs still running, and wait for them to terminate. */
		CdbCheckDispatchResult(ds, DISPATCH_WAIT_CANCEL);

		/*
		 * Check and free the results of all gangs. If any QE had an
		 * error, report it and exit via PG_THROW.
		 */
		cdbdisp_finishCommand(ds, NULL, NULL);

		/* Wasn't an error, must have been an interrupt. */
		CHECK_FOR_INTERRUPTS();

		/* Strange!  Not an interrupt either. */
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg_internal("Unable to dispatch plan.")));
	}

	if (DEBUG1 >= log_min_messages)
	{
		char		msec_str[32];
		switch (check_log_duration(msec_str, false))
		{
			case 1:
			case 2:
				ereport(LOG, (errmsg("duration to dispatch out (root %d): %s ms", pQueryParms->rootIdx,msec_str)));
				break;
		}
	}

}	/* cdbdisp_dispatchX */




/*
 * Three Helper functions for CdbDispatchPlan:
 *
 * Used to figure out the dispatch order for the sliceTable by
 * counting the number of dependent child slices for each slice; and
 * then sorting based on the count (all indepenedent slices get
 * dispatched first, then the slice above them and so on).
 *
 * fillSliceVector: figure out the number of slices we're dispatching,
 * and order them.
 *
 * count_dependent_children(): walk tree counting up children.
 *
 * compare_slice_order(): comparison function for qsort(): order the
 * slices by the number of dependent children. Empty slices are
 * sorted last (to make this work with initPlans).
 *
 */
static int
compare_slice_order(const void *aa, const void *bb)
{
	sliceVec *a = (sliceVec *)aa;
	sliceVec *b = (sliceVec *)bb;

	if (a->slice == NULL)
		return 1;
	if (b->slice == NULL)
		return -1;

	/* sort the writer gang slice first, because he sets the shared snapshot */
	if (a->slice->primary_gang_id == 1 && b->slice->primary_gang_id != 1)
		return -1;
	else if (b->slice->primary_gang_id == 1 && a->slice->primary_gang_id != 1)
		return 1;

	if (a->children == b->children)
		return 0;
	else if (a->children > b->children)
		return 1;
	else
		return -1;
}

/* Quick and dirty bit mask operations */
static void
mark_bit(char *bits, int nth)
{
	int nthbyte = nth >> 3;
	char nthbit  = 1 << (nth & 7);
	bits[nthbyte] |= nthbit;
}
static void
or_bits(char* dest, char* src, int n)
{
	int i;

	for(i=0; i<n; i++)
		dest[i] |= src[i];
}

static int
count_bits(char* bits, int nbyte)
{
	int i;
	int nbit = 0;
	int bitcount[] = {
		0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4
	};

	for(i=0; i<nbyte; i++)
	{
		nbit += bitcount[bits[i] & 0x0F];
		nbit += bitcount[(bits[i] >> 4) & 0x0F];
	}

	return nbit;
}

/* We use a bitmask to count the dep. childrens.
 * Because of input sharing, the slices now are DAG.  We cannot simply go down the
 * tree and add up number of children, which will return too big number.
 */
static int markbit_dep_children(SliceTable *sliceTable, int sliceIdx, sliceVec *sliceVec, int bitmasklen, char* bits)
{
	ListCell *sublist;
	Slice *slice = (Slice *) list_nth(sliceTable->slices, sliceIdx);

	foreach(sublist, slice->children)
	{
		int childIndex = lfirst_int(sublist);
		char *newbits = palloc0(bitmasklen);

		markbit_dep_children(sliceTable, childIndex, sliceVec, bitmasklen, newbits);
		or_bits(bits, newbits, bitmasklen);
		mark_bit(bits, childIndex);
		pfree(newbits);
	}

	sliceVec[sliceIdx].sliceIndex = sliceIdx;
	sliceVec[sliceIdx].children = count_bits(bits, bitmasklen);
	sliceVec[sliceIdx].slice = slice;

	return sliceVec[sliceIdx].children;
}

/* Count how many dependent childrens and fill in the sliceVector of dependent childrens. */
static int
count_dependent_children(SliceTable * sliceTable, int sliceIndex, sliceVec *sliceVector, int len)
{
	int 		ret = 0;
	int			bitmasklen = (len+7) >> 3;
	char 	   *bitmask = palloc0(bitmasklen);

	ret = markbit_dep_children(sliceTable, sliceIndex, sliceVector, bitmasklen, bitmask);
	pfree(bitmask);

	return ret;
}

int
fillSliceVector(SliceTable *sliceTbl, int rootIdx, sliceVec *sliceVector, int sliceLim)
{
	int top_count;

	/* count doesn't include top slice add 1 */
	top_count = 1 + count_dependent_children(sliceTbl, rootIdx, sliceVector, sliceLim);

	qsort(sliceVector, sliceLim, sizeof(sliceVec), compare_slice_order);

	return top_count;
}






/*
 * Compose and dispatch the MPPEXEC commands corresponding to a plan tree
 * within a complete parallel plan.  (A plan tree will correspond either
 * to an initPlan or to the main plan.)
 *
 * If cancelOnError is true, then any dispatching error, a cancellation
 * request from the client, or an error from any of the associated QEs,
 * may cause the unfinished portion of the plan to be abandoned or canceled;
 * and in the event this occurs before all gangs have been dispatched, this
 * function does not return, but waits for all QEs to stop and exits to
 * the caller's error catcher via ereport(ERROR,...).  Otherwise this
 * function returns normally and errors are not reported until later.
 *
 * If cancelOnError is false, the plan is to be dispatched as fully as
 * possible and the QEs allowed to proceed regardless of cancellation
 * requests, errors or connection failures from other QEs, etc.
 *
 * The CdbDispatchResults objects allocated for the plan are returned
 * in *pPrimaryResults.  The caller, after calling
 * CdbCheckDispatchResult(), can examine the CdbDispatchResults
 * objects, can keep them as long as needed, and ultimately must free
 * them with cdbdisp_destroyDispatchResults() prior to deallocation of
 * the caller's memory context.  Callers should use PG_TRY/PG_CATCH to
 * ensure proper cleanup.
 *
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 *
 * Note that the slice tree dispatched is the one specified in the EState
 * of the argument QueryDesc as es_cur__slice.
 *
 * Note that the QueryDesc params must include PARAM_EXEC_REMOTE parameters
 * containing the values of any initplans required by the slice to be run.
 * (This is handled by calls to addRemoteExecParamsToParamList() from the
 * functions preprocess_initplans() and ExecutorRun().)
 *
 * Each QE receives its assignment as a message of type 'M' in PostgresMain().
 * The message is deserialized and processed by exec_mpp_query() in postgres.c.
 */
void
cdbdisp_dispatchPlan(struct QueryDesc *queryDesc,
					 bool planRequiresTxn,
					 bool cancelOnError,
					 struct CdbDispatcherState *ds)
{
	char 	*splan,
		*ssliceinfo,
		*sparams;

	int 	splan_len,
		splan_len_uncompressed,
		ssliceinfo_len,
		sparams_len;

	SliceTable *sliceTbl;
	int			rootIdx;
	int			oldLocalSlice;
	PlannedStmt	   *stmt;
	bool		is_SRI;

	DispatchCommandQueryParms queryParms;
	CdbComponentDatabaseInfo *qdinfo;

	ds->primaryResults = NULL;
	ds->dispatchThreads = NULL;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(queryDesc != NULL && queryDesc->estate != NULL);

	/*
	 * Later we'll need to operate with the slice table provided via the
	 * EState structure in the argument QueryDesc.	Cache this information
	 * locally and assert our expectations about it.
	 */
	sliceTbl = queryDesc->estate->es_sliceTable;
	rootIdx = RootSliceIndex(queryDesc->estate);

	Assert(sliceTbl != NULL);
	Assert(rootIdx == 0 ||
		   (rootIdx > sliceTbl->nMotions && rootIdx <= sliceTbl->nMotions + sliceTbl->nInitPlans));

	/*
	 * Keep old value so we can restore it.  We use this field as a parameter.
	 */
	oldLocalSlice = sliceTbl->localSlice;

	/*
	 * This function is called only for planned statements.
	 */
	stmt = queryDesc->plannedstmt;
	Assert(stmt);


	/*
	 * Let's evaluate STABLE functions now, so we get consistent values on the QEs
	 *
	 * Also, if this is a single-row INSERT statement, let's evaluate
	 * nextval() and currval() now, so that we get the QD's values, and a
	 * consistent value for everyone
	 *
	 */

	is_SRI = false;

	if (queryDesc->operation == CMD_INSERT)
	{
		Assert(stmt->commandType == CMD_INSERT);

		/* We might look for constant input relation (instead of SRI), but I'm afraid
		 * that wouldn't scale.
		 */
		is_SRI = IsA(stmt->planTree, Result) && stmt->planTree->lefttree == NULL;
	}

	if (!is_SRI)
		clear_relsize_cache();

	if (queryDesc->operation == CMD_INSERT ||
		queryDesc->operation == CMD_SELECT ||
		queryDesc->operation == CMD_UPDATE ||
		queryDesc->operation == CMD_DELETE)
	{

		MemoryContext oldContext;

		oldContext = CurrentMemoryContext;
		if ( stmt->qdContext ) /* Temporary! See comment in PlannedStmt. */
		{
			oldContext = MemoryContextSwitchTo(stmt->qdContext);
		}
		else /* MPP-8382: memory context of plan tree should not change */
		{
			MemoryContext mc = GetMemoryChunkContext(stmt->planTree);
			oldContext = MemoryContextSwitchTo(mc);
		}

		stmt->planTree = (Plan *) exec_make_plan_constant(stmt, is_SRI);

		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * Cursor queries and bind/execute path queries don't run on the
	 * writer-gang QEs; but they require snapshot-synchronization to
	 * get started.
	 *
	 * initPlans, and other work (see the function pre-evaluation
	 * above) may advance the snapshot "segmateSync" value, so we're
	 * best off setting the shared-snapshot-ready value here. This
	 * will dispatch to the writer gang and force it to set its
	 * snapshot; we'll then be able to serialize the same snapshot
	 * version (see qdSerializeDtxContextInfo() below).
	 *
	 * For details see MPP-6533/MPP-5805. There are a large number of
	 * interesting test cases for segmate-sync.
	 */
	if (queryDesc->extended_query)
	{
		verify_shared_snapshot_ready();
	}

	/*
	 *	MPP-20785:
	 *	remove subquery field from RTE's since it is not needed during query
	 *	execution,
	 *	this is an optimization to reduce size of serialized plan before dispatching
	 */
	remove_subquery_in_RTEs((Node *) (queryDesc->plannedstmt->rtable));

	/*
	 * serialized plan tree. Note that we're called for a single
	 * slice tree (corresponding to an initPlan or the main plan), so the
	 * parameters are fixed and we can include them in the prefix.
	 */
	splan = serializeNode((Node *) queryDesc->plannedstmt, &splan_len, &splan_len_uncompressed);

	/* compute the total uncompressed size of the query plan for all slices */
	int num_slices = queryDesc->plannedstmt->planTree->nMotionNodes + 1;
	uint64 plan_size_in_kb = ((uint64)splan_len_uncompressed * (uint64)num_slices) / (uint64)1024;

	elog(((gp_log_gang >= GPVARS_VERBOSITY_VERBOSE) ? LOG : DEBUG1),
		"Query plan size to dispatch: " UINT64_FORMAT "KB", plan_size_in_kb);

	if (0 < gp_max_plan_size && plan_size_in_kb > gp_max_plan_size)
	{
		ereport(ERROR,
			(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
				(errmsg("Query plan size limit exceeded, current size: " UINT64_FORMAT "KB, max allowed size: %dKB", plan_size_in_kb, gp_max_plan_size),
				 errhint("Size controlled by gp_max_plan_size"))));
	}

	Assert(splan != NULL && splan_len > 0 && splan_len_uncompressed > 0);

	if (queryDesc->params != NULL && queryDesc->params->numParams > 0)
	{
        ParamListInfoData  *pli;
        ParamExternData    *pxd;
        StringInfoData      parambuf;
		Size                length;
        int                 plioff;
		int32               iparam;

        /* Allocate buffer for params */
        initStringInfo(&parambuf);

        /* Copy ParamListInfoData header and ParamExternData array */
        pli = queryDesc->params;
        length = (char *)&pli->params[pli->numParams] - (char *)pli;
        plioff = parambuf.len;
        Assert(plioff == MAXALIGN(plioff));
        appendBinaryStringInfo(&parambuf, pli, length);

        /* Copy pass-by-reference param values. */
        for (iparam = 0; iparam < queryDesc->params->numParams; iparam++)
		{
			int16   typlen;
			bool    typbyval;

            /* Recompute pli each time in case parambuf.data is repalloc'ed */
            pli = (ParamListInfoData *)(parambuf.data + plioff);
			pxd = &pli->params[iparam];

            /* Does pxd->value contain the value itself, or a pointer? */
			get_typlenbyval(pxd->ptype, &typlen, &typbyval);
            if (!typbyval)
            {
				char   *s = DatumGetPointer(pxd->value);

				if (pxd->isnull ||
                    !PointerIsValid(s))
                {
                    pxd->isnull = true;
                    pxd->value = 0;
                }
				else
				{
			        length = datumGetSize(pxd->value, typbyval, typlen);

					/* MPP-1637: we *must* set this before we
					 * append. Appending may realloc, which will
					 * invalidate our pxd ptr. (obviously we could
					 * append first if we recalculate pxd from the new
					 * base address) */
                    pxd->value = Int32GetDatum(length);

                    appendBinaryStringInfo(&parambuf, &iparam, sizeof(iparam));
                    appendBinaryStringInfo(&parambuf, s, length);
				}
            }
		}
        sparams = parambuf.data;
        sparams_len = parambuf.len;
	}
	else
	{
		sparams = NULL;
		sparams_len = 0;
	}

	ssliceinfo = serializeNode((Node *) sliceTbl, &ssliceinfo_len, NULL /*uncompressed_size*/);

	MemSet(&queryParms, 0, sizeof(queryParms));
	queryParms.strCommand = queryDesc->sourceText;
	queryParms.serializedQuerytree = NULL;
	queryParms.serializedQuerytreelen = 0;
	queryParms.serializedPlantree = splan;
	queryParms.serializedPlantreelen = splan_len;
	queryParms.serializedParams = sparams;
	queryParms.serializedParamslen = sparams_len;
	queryParms.serializedSliceInfo = ssliceinfo;
	queryParms.serializedSliceInfolen= ssliceinfo_len;
	queryParms.rootIdx = rootIdx;

	/* sequence server info */
	qdinfo = &(getComponentDatabases()->entry_db_info[0]);
	Assert(qdinfo != NULL && qdinfo->hostip != NULL);
	queryParms.seqServerHost = pstrdup(qdinfo->hostip);
	queryParms.seqServerHostlen = strlen(qdinfo->hostip) + 1;
	queryParms.seqServerPort = seqServerCtl->seqServerPort;

	queryParms.primary_gang_id = 0;	/* We are relying on the slice table to provide gang ids */

	/* serialized a version of our snapshot */
	/*
	 * Generate our transction isolations.  We generally want Plan
	 * based dispatch to be in a global transaction. The executor gets
	 * to decide if the special circumstances exist which allow us to
	 * dispatch without starting a global xact.
	 */
	queryParms.serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&queryParms.serializedDtxContextInfolen, true /* wantSnapshot */, queryDesc->extended_query,
								  generateTxnOptions(planRequiresTxn), "cdbdisp_dispatchPlan");

	Assert(sliceTbl);
	Assert(sliceTbl->slices != NIL);

	cdbdisp_dispatchX(&queryParms, cancelOnError, sliceTbl, ds);

	sliceTbl->localSlice = oldLocalSlice;
}	/* cdbdisp_dispatchPlan */


/*
 * Dispatch SET command to all gangs.
 *
 * Can not dispatch SET commands to busy reader gangs (allocated by cursors) directly because another command is already in progress.
 * Cursors only allocate reader gangs, so primary writer and idle reader gangs can be dispatched to.
 */
static void
cdbdisp_dispatchSetCommandToAllGangs(const char	*strCommand,
								  char			*serializedQuerytree,
								  int			serializedQuerytreelen,
								  char			*serializedPlantree,
								  int			serializedPlantreelen,
								  bool			cancelOnError,
								  bool			needTwoPhase,
								  struct CdbDispatcherState *ds)
{
	DispatchCommandQueryParms queryParms;

	Gang		*primaryGang;
	List		*idleReaderGangs;
	List		*busyReaderGangs;
	ListCell	*le;

	int			nsegdb = getgpsegmentCount();
	int			gangCount;

	ds->primaryResults = NULL;
	ds->dispatchThreads = NULL;

	MemSet(&queryParms, 0, sizeof(queryParms));
	queryParms.strCommand = strCommand;
	queryParms.serializedQuerytree = serializedQuerytree;
	queryParms.serializedQuerytreelen = serializedQuerytreelen;
	queryParms.serializedPlantree = serializedPlantree;
	queryParms.serializedPlantreelen = serializedPlantreelen;

	/*
	 * Allocate a primary QE for every available segDB in the system.
	 */
	primaryGang = allocateWriterGang();

	Assert(primaryGang);

	queryParms.primary_gang_id = primaryGang->gang_id;

	/* serialized a version of our snapshot */
	queryParms.serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&queryParms.serializedDtxContextInfolen, true /* withSnapshot */, false /* cursor*/,
								  generateTxnOptions(needTwoPhase), "cdbdisp_dispatchSetCommandToAllGangs");

	idleReaderGangs = getAllIdleReaderGangs();
	busyReaderGangs = getAllBusyReaderGangs();

	/*
	 * Dispatch the command.
	 */
	gangCount = 1 + list_length(idleReaderGangs);
	ds->primaryResults = cdbdisp_makeDispatchResults(nsegdb * gangCount, 0, cancelOnError);

	ds->primaryResults->writer_gang = primaryGang;
	cdbdisp_dispatchToGang(ds,
						   &QueryDispatchType,
						   &queryParms,
						   primaryGang, -1, gangCount, DEFAULT_DISP_DIRECT);

	foreach(le, idleReaderGangs)
	{
		Gang  *rg = lfirst(le);
		cdbdisp_dispatchToGang(ds,
							   &QueryDispatchType,
							   &queryParms,
							   rg, -1, gangCount, DEFAULT_DISP_DIRECT);
	}

	/*
	 *Can not send set command to busy gangs, so those gangs
	 *can not be reused because their GUC is not set.
	 */
	foreach(le, busyReaderGangs)
	{
		Gang  *rg = lfirst(le);
		rg->noReuse = true;
	}
}	/* cdbdisp_dispatchSetCommandToAllGangs */


void
CdbSetGucOnAllGangs(const char *strCommand,
					   bool cancelOnError,
					   bool needTwoPhase)
{
	volatile CdbDispatcherState ds = {NULL, NULL};
	const bool withSnapshot = true;

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "CdbSetGucOnAllGangs for command = '%s', needTwoPhase = %s",
		 strCommand, (needTwoPhase ? "true" : "false"));

	dtmPreCommand("CdbSetGucOnAllGangs", strCommand, NULL, needTwoPhase, withSnapshot, false /* inCursor */ );

	PG_TRY();
	{
		cdbdisp_dispatchSetCommandToAllGangs(strCommand, NULL, 0, NULL, 0, cancelOnError, needTwoPhase, (struct CdbDispatcherState *)&ds);

		/*
		 * Wait for all QEs to finish. If not all of our QEs were successful,
		 * report the error and throw up.
		 */
		cdbdisp_finishCommand((struct CdbDispatcherState *)&ds, NULL, NULL);
	}
	PG_CATCH();
	{
		/* Something happend, clean up after ourselves */
		CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_CANCEL);

		cdbdisp_destroyDispatchResults(ds.primaryResults);
		cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

		PG_RE_THROW();
		/* not reached */
	}
	PG_END_TRY();
}

/*
 * cdbdisp_dispatchCommand:
 * Send the strCommand SQL statement to all segdbs in the cluster
 * cancelOnError indicates whether an error
 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
 * on other qExecs. Normally this would be true.  The commands are sent over the libpq
 * connections that were established during gang creation.	They are run inside of threads.
 * The number of segdbs handled by any one thread is determined by the
 * guc variable gp_connections_per_thread.
 *
 * The CdbDispatchResults objects allocated for the command
 * are returned in *pPrimaryResults
 * The caller, after calling CdbCheckDispatchResult(), can
 * examine the CdbDispatchResults objects, can keep them as
 * long as needed, and ultimately must free them with
 * cdbdisp_destroyDispatchResults() prior to deallocation
 * of the memory context from which they were allocated.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchResults() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
void
cdbdisp_dispatchCommand(const char                 *strCommand,
						char			   		   *serializedQuerytree,
						int							serializedQuerytreelen,
                        bool                        cancelOnError,
                        bool						needTwoPhase,
                        bool						withSnapshot,
						CdbDispatcherState		*ds)
{
	DispatchCommandQueryParms queryParms;
	Gang	*primaryGang;
	int		nsegdb = getgpsegmentCount();
	CdbComponentDatabaseInfo *qdinfo;

	if (log_dispatch_stats)
		ResetUsage();

	if (DEBUG5 >= log_min_messages)
    	elog(DEBUG3, "cdbdisp_dispatchCommand: %s (needTwoPhase = %s)",
	    	 strCommand, (needTwoPhase ? "true" : "false"));
    else
    	elog((Debug_print_full_dtm ? LOG : DEBUG3), "cdbdisp_dispatchCommand: %.50s (needTwoPhase = %s)",
    	     strCommand, (needTwoPhase ? "true" : "false"));

    ds->primaryResults = NULL;
	ds->dispatchThreads = NULL;

	MemSet(&queryParms, 0, sizeof(queryParms));
	queryParms.strCommand = strCommand;
	queryParms.serializedQuerytree = serializedQuerytree;
	queryParms.serializedQuerytreelen = serializedQuerytreelen;

	/*
	 * Allocate a primary QE for every available segDB in the system.
	 */
	primaryGang = allocateWriterGang();

	Assert(primaryGang);

	queryParms.primary_gang_id = primaryGang->gang_id;

	/* Serialize a version of our DTX Context Info */
	queryParms.serializedDtxContextInfo =
		qdSerializeDtxContextInfo(&queryParms.serializedDtxContextInfolen, withSnapshot, false, generateTxnOptions(needTwoPhase), "cdbdisp_dispatchCommand");

	/* sequence server info */
	qdinfo = &(getComponentDatabases()->entry_db_info[0]);
	Assert(qdinfo != NULL && qdinfo->hostip != NULL);
	queryParms.seqServerHost = pstrdup(qdinfo->hostip);
	queryParms.seqServerHostlen = strlen(qdinfo->hostip) + 1;
	queryParms.seqServerPort = seqServerCtl->seqServerPort;

	/*
	 * Dispatch the command.
	 */
	ds->primaryResults = cdbdisp_makeDispatchResults(nsegdb, 0, cancelOnError);
	ds->primaryResults->writer_gang = primaryGang;

	cdbdisp_dispatchToGang(ds,
						   &QueryDispatchType,
						   &queryParms,
						   primaryGang, -1, 1, DEFAULT_DISP_DIRECT);

	/*
	 * don't pfree serializedShapshot here, it will be pfree'd when
	 * the first thread is destroyed.
	 */

}	/* cdbdisp_dispatchCommand */


/* Helper function to thread_DispatchCommand that actually kicks off the
 * command on the libpq connection.
 *
 * NOTE: since this is called via a thread, the same rules apply as to
 *		 thread_DispatchCommand absolutely no elog'ing.
 */
static void
dispatchCommandQuery(CdbDispatchResult	*dispatchResult,
					 const char			*query_text,
					 int				query_text_len,
					 const char			*strCommand)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;
	PGconn	   *conn = segdbDesc->conn;
	TimestampTz beforeSend = 0;
	long		secs;
	int			usecs;

	/* Don't use elog, it's not thread-safe */
	if (DEBUG3 >= log_min_messages)
		write_log("%s <- %.120s", segdbDesc->whoami, strCommand);

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


	/*
	 * We'll keep monitoring this QE -- whether or not the command
	 * was dispatched -- in order to check for a lost connection
	 * or any other errors that libpq might have in store for us.
	 */
}	/* dispatchCommand */

/*
 * CdbDoCommandNoTxn:
 * Combination of cdbdisp_dispatchCommand and cdbdisp_finishCommand.
 * If not all QEs execute the command successfully, throws an error and
 * does not return.
 *
 * needTwoPhase specifies a desire to include global transaction control
 *  before dispatch.
 */
void
CdbDoCommand(const char *strCommand,
			 bool cancelOnError,
			 bool needTwoPhase)
{
	CdbDispatcherState ds = {NULL, NULL};
	const bool withSnapshot = true;

	elog((Debug_print_full_dtm ? LOG : DEBUG5), "CdbDoCommand for command = '%s', needTwoPhase = %s",
		 strCommand, (needTwoPhase ? "true" : "false"));

	dtmPreCommand("CdbDoCommand", strCommand, NULL, needTwoPhase, withSnapshot, false /* inCursor */);

	cdbdisp_dispatchCommand(strCommand, NULL, 0, cancelOnError, needTwoPhase,
							/* withSnapshot */true, &ds);

	/*
	 * Wait for all QEs to finish. If not all of our QEs were successful,
	 * report the error and throw up.
	 */
	cdbdisp_finishCommand(&ds, NULL, NULL);
}	/* CdbDoCommandNoTxn */

/*
 * Dispatch a command - already parsed and in the form of a Node tree
 * - to all primary segdbs.  Does not wait for completion. Does not
 * start a global transaction.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the CdbDispatchResults objects are destroyed by
 * cdbdisp_destroyDispatchResults() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use cdbdisp_finishCommand().
 */
void
cdbdisp_dispatchUtilityStatement(struct Node *stmt,
								 bool cancelOnError,
								 bool needTwoPhase,
								 bool withSnapshot,
								 struct CdbDispatcherState *ds,
								 char *debugCaller)
{
	char	   *serializedQuerytree;
	int			serializedQuerytree_len;
	Query	   *q = makeNode(Query);
	StringInfoData buffer;

	elog((Debug_print_full_dtm ? LOG : DEBUG5),"cdbdisp_dispatchUtilityStatement debug_query_string = %s (needTwoPhase = %s, debugCaller = %s)",
	     debug_query_string, (needTwoPhase ? "true" : "false"), debugCaller);

	dtmPreCommand("cdbdisp_dispatchUtilityStatement", "(none)", NULL, needTwoPhase,
			withSnapshot, false /* inCursor */ );

	initStringInfo(&buffer);

	q->commandType = CMD_UTILITY;

	Assert(stmt != NULL);
	Assert(stmt->type < 1000);
	Assert(stmt->type > 0);

	q->utilityStmt = stmt;

	q->querySource = QSRC_ORIGINAL;

	/*
	 * We must set q->canSetTag = true.  False would be used to hide a command
	 * introduced by rule expansion which is not allowed to return its
	 * completion status in the command tag (PQcmdStatus/PQcmdTuples). For
	 * example, if the original unexpanded command was SELECT, the status
	 * should come back as "SELECT n" and should not reflect other commands
	 * inserted by rewrite rules.  True means we want the status.
	 */
	q->canSetTag = true;		/* ? */

	/*
	 * serialized the stmt tree, and create the sql statement: mppexec ....
	 */
	serializedQuerytree = serializeNode((Node *) q, &serializedQuerytree_len, NULL /*uncompressed_size*/);

	Assert(serializedQuerytree != NULL);

	cdbdisp_dispatchCommand(debug_query_string, serializedQuerytree, serializedQuerytree_len, cancelOnError, needTwoPhase,
							withSnapshot, ds);

}	/* cdbdisp_dispatchUtilityStatement */

/*
 * cdbdisp_dispatchRMCommand:
 * Sends a non-cancelable command to all segment dbs.
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
cdbdisp_dispatchRMCommand(const char *strCommand,
						  bool withSnapshot,
						  StringInfo errmsgbuf,
						  int *numresults)
{
	volatile struct CdbDispatcherState ds = {NULL, NULL};

	PGresult  **resultSets = NULL;

	/* never want to start a global transaction for these */
	bool		needTwoPhase = false;

	elog(((Debug_print_full_dtm || Debug_print_snapshot_dtm) ? LOG : DEBUG5),
		 "cdbdisp_dispatchRMCommand for command = '%s', withSnapshot = %s",
	     strCommand, (withSnapshot ? "true" : "false"));

	PG_TRY();
	{
		/* Launch the command.	Don't cancel on error. */
		cdbdisp_dispatchCommand(strCommand, NULL, 0,
								/* cancelOnError */false,
								needTwoPhase, withSnapshot,
								(struct CdbDispatcherState *)&ds);

		/* Wait for all QEs to finish.	Don't cancel. */
		CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_NONE);
	}
	PG_CATCH();
	{
		/* Something happend, clean up after ourselves */
		CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_NONE);

		cdbdisp_destroyDispatchResults(ds.primaryResults);
		cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

		PG_RE_THROW();
		/* not reached */
	}
	PG_END_TRY();

	resultSets = cdbdisp_returnResults(ds.primaryResults, errmsgbuf, numresults);

	/* free memory allocated for the dispatch threads struct */
	cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

	return resultSets;
}	/* cdbdisp_dispatchRMCommand */


/*
 * Dispatch a command - already parsed and in the form of a Node tree
 * - to all primary segdbs, and wait for completion.  Starts a global
 * transaction first, if not already started.  If not all QEs in the
 * given gang(s) executed the command successfully, throws an error
 * and does not return.
 */
void
CdbDispatchUtilityStatement(struct Node *stmt, char *debugCaller __attribute__((unused)) )
{
	CdbDispatchUtilityStatement_Internal(stmt, /* needTwoPhase */ true, "CdbDispatchUtilityStatement");
}

void
CdbDispatchUtilityStatement_NoTwoPhase(struct Node *stmt, char *debugCaller __attribute__((unused)) )
{
	CdbDispatchUtilityStatement_Internal(stmt, /* needTwoPhase */ false, "CdbDispatchUtilityStatement_NoTwoPhase");
}

static void
CdbDispatchUtilityStatement_Internal(struct Node *stmt, bool needTwoPhase, char *debugCaller)
{
	volatile struct CdbDispatcherState ds = {NULL, NULL};

	elog((Debug_print_full_dtm ? LOG : DEBUG5),
		 "cdbdisp_dispatchUtilityStatement called (needTwoPhase = %s, debugCaller = %s)",
		 (needTwoPhase ? "true" : "false"), debugCaller);

	PG_TRY();
	{
		cdbdisp_dispatchUtilityStatement(stmt,
										 true /* cancelOnError */,
										 needTwoPhase,
										 true /* withSnapshot */,
										 (struct CdbDispatcherState *)&ds,
										 debugCaller);

		/* Wait for all QEs to finish.	Throw up if error. */
		cdbdisp_finishCommand((struct CdbDispatcherState *)&ds, NULL, NULL);
	}
	PG_CATCH();
	{
		/* Something happend, clean up after ourselves */
		CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_CANCEL);

		cdbdisp_destroyDispatchResults(ds.primaryResults);
		cdbdisp_destroyDispatchThreads(ds.dispatchThreads);

		PG_RE_THROW();
		/* not reached */
	}
	PG_END_TRY();

}	/* CdbDispatchUtilityStatement */
/* generateTxnOptions:
 * Generates an int containing the appropriate flags to direct the remote
 * segdb QE process to perform any needed transaction commands before or
 * after the statement.
 *
 * needTwoPhase - specifies whether this statement even wants a transaction to
 *				 be started.  Certain utility statements dont want to be in a
 *				 distributed transaction.
 */
static int
generateTxnOptions(bool needTwoPhase)
{
	int options;

	options = mppTxnOptions(needTwoPhase);

	return options;

}


/*
 *
 * Remove subquery field in RTE's with subquery kind
 * This is an optimization used to reduce plan size before serialization
 *
 */
static
void remove_subquery_in_RTEs(Node *node)
{
	if (node == NULL)
	{
		return;
	}

 	if (IsA(node, RangeTblEntry))
 	{
 		RangeTblEntry *rte = (RangeTblEntry *)node;
 		if (RTE_SUBQUERY == rte->rtekind && NULL != rte->subquery)
 		{
 			/*
 			 * replace subquery with a dummy subquery
 			 */
 			rte->subquery = makeNode(Query);
 		}

 		return;
 	}

 	if (IsA(node, List))
 	{
 		List *list = (List *) node;
 		ListCell   *lc = NULL;
 		foreach(lc, list)
 		{
 			remove_subquery_in_RTEs((Node *) lfirst(lc));
 		}
 	}
}



static void
queryBuildDispatchString(DispatchCommandParms *pParms)
{
	DispatchCommandQueryParms *pQueryParms = &pParms->queryParms;

	pParms->query_text = PQbuildGpQueryString(
		pQueryParms->strCommand, pQueryParms->strCommandlen, 
		pQueryParms->serializedQuerytree, pQueryParms->serializedQuerytreelen, 
		pQueryParms->serializedPlantree, pQueryParms->serializedPlantreelen, 
		pQueryParms->serializedParams, pQueryParms->serializedParamslen, 
		pQueryParms->serializedSliceInfo, pQueryParms->serializedSliceInfolen, 
		pQueryParms->serializedDtxContextInfo, pQueryParms->serializedDtxContextInfolen, 
		0 /* unused flags*/, pParms->cmdID, pParms->localSlice, pQueryParms->rootIdx,
		pQueryParms->seqServerHost, pQueryParms->seqServerHostlen, pQueryParms->seqServerPort,
		pQueryParms->primary_gang_id,
		GetCurrentStatementStartTimestamp(),
		pParms->sessUserId, pParms->sessUserId_is_super,
		pParms->outerUserId, pParms->outerUserId_is_super, pParms->currUserId,
		&pParms->query_text_len);
}

static void
queryDispatchCommand(CdbDispatchResult *dispatchResult, DispatchCommandParms *pParms)
{
	dispatchCommandQuery(dispatchResult, pParms->query_text, pParms->query_text_len, pParms->queryParms.strCommand);
}


static void
queryDispatchDestroy(DispatchCommandParms *pParms)
{
	DispatchCommandQueryParms *pQueryParms = &pParms->queryParms;

	if (pQueryParms->strCommand)
	{
		/* Caller frees if desired */
		pQueryParms->strCommand = NULL;
	}

	if (pQueryParms->serializedDtxContextInfo)
	{
		pfree(pQueryParms->serializedDtxContextInfo);
		pQueryParms->serializedDtxContextInfo = NULL;
	}

	if (pQueryParms->serializedSliceInfo)
	{
		pfree(pQueryParms->serializedSliceInfo);
		pQueryParms->serializedSliceInfo = NULL;
	}

	if (pQueryParms->serializedQuerytree)
	{
		pfree(pQueryParms->serializedQuerytree);
		pQueryParms->serializedQuerytree = NULL;
	}

	if (pQueryParms->serializedPlantree)
	{
		pfree(pQueryParms->serializedPlantree);
		pQueryParms->serializedPlantree = NULL;
	}

	if (pQueryParms->serializedParams)
	{
		pfree(pQueryParms->serializedParams);
		pQueryParms->serializedParams = NULL;
	}
}


static void
queryDispatchInit(DispatchCommandParms *pParms, void *inputParms)
{

	DispatchCommandQueryParms *pQueryParms = (DispatchCommandQueryParms *) inputParms;

	if (pQueryParms->strCommand == NULL || strlen(pQueryParms->strCommand) == 0)
	{
		pParms->queryParms.strCommand = NULL;
		pParms->queryParms.strCommandlen = 0;
	}
	else
	{
		pParms->queryParms.strCommand = pQueryParms->strCommand;
		pParms->queryParms.strCommandlen = strlen(pQueryParms->strCommand) + 1;
	}

	if (pQueryParms->serializedQuerytree == NULL || pQueryParms->serializedQuerytreelen == 0)
	{
		pParms->queryParms.serializedQuerytree = NULL;
		pParms->queryParms.serializedQuerytreelen = 0;
	}
	else
	{
		pParms->queryParms.serializedQuerytree = pQueryParms->serializedQuerytree;
		pParms->queryParms.serializedQuerytreelen = pQueryParms->serializedQuerytreelen;
	}

	if (pQueryParms->serializedPlantree == NULL || pQueryParms->serializedPlantreelen == 0)
	{
		pParms->queryParms.serializedPlantree = NULL;
		pParms->queryParms.serializedPlantreelen = 0;
	}
	else
	{
		pParms->queryParms.serializedPlantree = pQueryParms->serializedPlantree;
		pParms->queryParms.serializedPlantreelen = pQueryParms->serializedPlantreelen;
	}

	if (pQueryParms->serializedParams == NULL || pQueryParms->serializedParamslen == 0)
	{
		pParms->queryParms.serializedParams = NULL;
		pParms->queryParms.serializedParamslen = 0;
	}
	else
	{
		pParms->queryParms.serializedParams = pQueryParms->serializedParams;
		pParms->queryParms.serializedParamslen = pQueryParms->serializedParamslen;
	}

	if (pQueryParms->serializedSliceInfo == NULL || pQueryParms->serializedSliceInfolen == 0)
	{
		pParms->queryParms.serializedSliceInfo = NULL;
		pParms->queryParms.serializedSliceInfolen = 0;
	}
	else
	{
		pParms->queryParms.serializedSliceInfo = pQueryParms->serializedSliceInfo;
		pParms->queryParms.serializedSliceInfolen = pQueryParms->serializedSliceInfolen;
	}

	if (pQueryParms->serializedDtxContextInfo == NULL || pQueryParms->serializedDtxContextInfolen == 0)
	{
		pParms->queryParms.serializedDtxContextInfo = NULL;
		pParms->queryParms.serializedDtxContextInfolen = 0;
	}
	else
	{
		pParms->queryParms.serializedDtxContextInfo = pQueryParms->serializedDtxContextInfo;
		pParms->queryParms.serializedDtxContextInfolen = pQueryParms->serializedDtxContextInfolen;
	}

	pParms->queryParms.rootIdx = pQueryParms->rootIdx;

	if (pQueryParms->seqServerHost == NULL || pQueryParms->seqServerHostlen == 0)
	{
		pParms->queryParms.seqServerHost = NULL;
		pParms->queryParms.seqServerHostlen = 0;
		pParms->queryParms.seqServerPort = -1;
	}

	else
	{
		pParms->queryParms.seqServerHost = pQueryParms->seqServerHost;
		pParms->queryParms.seqServerHostlen = pQueryParms->seqServerHostlen;
		pParms->queryParms.seqServerPort = pQueryParms->seqServerPort;
	}
	
	pParms->queryParms.primary_gang_id = pQueryParms->primary_gang_id;
}
