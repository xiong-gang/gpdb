/*-------------------------------------------------------------------------
 *
 * distributedlog.c
 *     A GP parallel log to the Postgres clog that records the full distributed
 * xid information for each local transaction id.
 *
 * It is used to determine if the committed xid for a transaction we want to
 * determine the visibility of is for a distributed transaction or a
 * local transaction.
 *
 * By default, entries in the SLRU (Simple LRU) module used to implement this
 * log will be set to zero.  A non-zero entry indicates a committed distributed
 * transaction.
 *
 * We must persist this log and the DTM does reuse the DistributedTransactionId
 * between restarts, so we will be storing the upper half of the whole
 * distributed transaction identifier -- the timestamp -- also so we can
 * be sure which distributed transaction we are looking at.
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/transam/distributedlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/distributedlog.h"
#include "access/transam.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "libpq/libpq-be.h" /* struct Port */


typedef struct DistributedLogMap
{
	TransactionId localXid;
	DistributedTransactionId dxid;
	DistributedTransactionTimeStamp *tstamp;
}DistributedLogMap;


typedef struct DistributedLogShmem
{
	HTAB *mapHash;
	/*
	 * Oldest local XID that is still visible to some distributed snapshot.
	 *
	 * This is initialized by DistributedLog_InitOldestXmin() after
	 * postmaster startup, and advanced whenever we receive a new
	 * distributed snapshot from the QD (or in the QD itself, whenever
	 * we compute a new snapshot).
	 */
	volatile TransactionId	oldestXmin;

} DistributedLogShmem;

static DistributedLogShmem *DistributedLogShared = NULL;


/*
 * Initialize the value for oldest local XID that might still be visible
 * to a distributed snapshot.
 *
 * This gets called once, after postmaster startup. We scan the
 * distributed log, starting from the smallest datfrozenxid, until
 * we find a page that exists.
 *
 * The caller is expected to hold DistributedLogControlLock on entry.
 */
void
DistributedLog_InitOldestXmin(void)
{
	//TODO
	return FirstDistributedTransactionId;
}
/*
 * Advance the "oldest xmin" among distributed snapshots.
 *
 * We track an "oldest xmin" value, which is the oldest XID that is still
 * considered visible by any distributed snapshot in the cluster. The
 * value is a local TransactionId, not a DistributedTransactionId. But
 * it takes into account any snapshots that might still be active in the
 * QD node, even if there are no processes belonging to that distributed
 * transaction running in this segment at the moment.
 *
 * Call this function in the QE, whenever a new distributed snapshot is
 * received from the QD. Pass the 'distribTransactionTimeStamp' and
 * 'xminAllDistributedSnapshots' values from the DistributedSnapshot.
 * 'oldestLocalXmin' is the oldest xmin that is still visible according
 * to local snapshots. (That is the upper bound of how far we can advance
 * the oldest xmin)
 *
 * As a courtesy to callers, this function also returns the new "oldest
 * xmin" value (same as old value, if it was not advanced), just like
 * DistributedLog_GetOldestXmin() would.
 *
 */
TransactionId
DistributedLog_AdvanceOldestXmin(TransactionId oldestLocalXmin,
								 DistributedTransactionTimeStamp distribTransactionTimeStamp,
								 DistributedTransactionId xminAllDistributedSnapshots)
{
	TransactionId oldestXmin;
	TransactionId oldOldestXmin;
	int			currPage;
	int			slotno;
	bool		DistributedLogControlLockHeldByMe = false;
	DistributedLogEntry *entries = NULL;

	Assert(!IS_QUERY_DISPATCHER());
	Assert(TransactionIdIsNormal(oldestLocalXmin));

#ifdef FAULT_INJECTOR
	const char *dbname = NULL;
	if (MyProcPort)
		dbname = MyProcPort->database_name;

	FaultInjector_InjectFaultIfSet("distributedlog_advance_oldest_xmin", DDLNotSpecified,
								   dbname?dbname: "", "");
#endif

	oldestXmin = (TransactionId)pg_atomic_read_u32((pg_atomic_uint32 *)&DistributedLogShared->oldestXmin);
	oldOldestXmin = oldestXmin;
	Assert(oldestXmin != InvalidTransactionId);

	/*
	 * oldestXmin (DistributedLogShared->oldestXmin) can be higher than
	 * oldestLocalXmin (globalXmin in GetSnapshotData()) in concurrent
	 * work-load. This happens due to fact that GetSnapshotData() loops over
	 * procArray and releases the ProcArrayLock before reaching here. So, if
	 * oldestXmin has already bumped ahead of oldestLocalXmin its safe to just
	 * return oldestXmin, as some other process already checked the
	 * distributed log for us.
	 */
	currPage = -1;
	while (TransactionIdPrecedes(oldestXmin, oldestLocalXmin))
	{
		DistributedLogMap *map = hash_search(DistributedLogShared->mapHash, &oldestXmin, HASH_FIND, NULL);
		if (!map)
			continue;
		/*
		 * If this XID is already visible to all distributed snapshots, we can
		 * advance past it. Otherwise stop here. (Local-only transactions will
		 * have zeros in distribXid and distribTimeStamp; this test will also
		 * skip over those.)
		 */
		if (map->tstamp == distribTransactionTimeStamp &&
			!TransactionIdPrecedes(map->dxid, xminAllDistributedSnapshots))
			break;

		TransactionIdAdvance(oldestXmin);
	}


	pg_atomic_write_u32((pg_atomic_uint32 *)&DistributedLogShared->oldestXmin, oldestXmin);


	return oldestXmin;
}

/*
 * Return the "oldest xmin" among distributed snapshots.
 */
TransactionId
DistributedLog_GetOldestXmin(TransactionId oldestLocalXmin)
{
	TransactionId result;

	result = (TransactionId)pg_atomic_read_u32((pg_atomic_uint32 *)&DistributedLogShared->oldestXmin);

	Assert(!IS_QUERY_DISPATCHER());
	elogif(Debug_print_full_dtm, LOG, "distributed oldestXmin is '%u'", result);

	/*
	 * Like in DistributedLog_AdvanceOldestXmin(), the shared oldestXmin
	 * might already have been advanced past oldestLocalXmin.
	 */
	if (!TransactionIdIsValid(result) ||
		TransactionIdFollows(result, oldestLocalXmin))
		result = oldestLocalXmin;

	return result;
}

/*
 * Record that a distributed transaction committed in the distributed log for
 * all transaction ids on a single page. This function is similar to clog
 * function TransactionIdSetTreeStatus().
 */
static void
DistributedLog_SetCommitted_Internal(
	int                                 numLocIds,
	TransactionId 						*localXid,
	DistributedTransactionTimeStamp		distribTimeStamp,
	DistributedTransactionId 			distribXid,
	bool								isRedo)
{
	Assert(!IS_QUERY_DISPATCHER());
	Assert(numLocIds > 0);
	Assert(localXid != NULL);
	Assert(TransactionIdIsValid(localXid[0]));

	if (isRedo)
	{
		//TODO
		return;
	}

	for (int i = 0; i < numLocIds; i++)
	{
		Assert(TransactionIdIsValid(localXid[i]));
		bool found = false;
		DistributedLogMap *map = hash_search(DistributedLogShared->mapHash, &localXid[i], HASH_ENTER, &found);
		if (!found)
		{
			/* Duplicate, so mark the previous occurrence as skippable */
			map->localXid = localXid[i];
			map->dxid = distribXid;
			map->tstamp = distribTimeStamp;
		}
	}
}

/*
 * Record that a distributed transaction and its possible sub-transactions
 * committed, in the distributed log.
 */
void
DistributedLog_SetCommittedTree(TransactionId xid, int nxids, TransactionId *xids,
								DistributedTransactionTimeStamp	distribTimeStamp,
								DistributedTransactionId distribXid,
								bool isRedo)
{
	if (IS_QUERY_DISPATCHER())
		return;

	DistributedLog_SetCommitted_Internal(1, &xid, distribTimeStamp, distribXid, isRedo);
	/* add entry for sub-transaction page at time */
	DistributedLog_SetCommitted_Internal(nxids, xids, distribTimeStamp, distribXid, isRedo);
}

/*
 * Get the corresponding distributed xid and timestamp of a local xid.
 */
void
DistributedLog_GetDistributedXid(
	TransactionId 						localXid,
	DistributedTransactionTimeStamp		*distribTimeStamp,
	DistributedTransactionId 			*distribXid)
{
	Assert(!IS_QUERY_DISPATCHER());

	DistributedLogMap *map = hash_search(DistributedLogShared->mapHash, &localXid, HASH_FIND, NULL);
	if (!map)
		return;

	*distribTimeStamp = map->tstamp;
	*distribXid = map->dxid;
	return;
}

/*
 * Determine if a distributed transaction committed in the distributed log.
 */
bool
DistributedLog_CommittedCheck(
	TransactionId 						localXid,
	DistributedTransactionTimeStamp		*distribTimeStamp,
	DistributedTransactionId 			*distribXid)
{
	Assert(!IS_QUERY_DISPATCHER());

	TransactionId oldestXmin;
	oldestXmin = (TransactionId)pg_atomic_read_u32((pg_atomic_uint32 *)&DistributedLogShared->oldestXmin);
	if (oldestXmin == InvalidTransactionId)
		elog(PANIC, "DistributedLog's OldestXmin not initialized yet");

	if (TransactionIdPrecedes(localXid, oldestXmin))
	{
		*distribTimeStamp = 0;	// Set it to something.
		*distribXid = 0;
		return false;
	}

	DistributedLogMap *map = hash_search(DistributedLogShared->mapHash, &localXid, HASH_FIND, NULL);
	if (!map)
		return false;

	*distribTimeStamp = map->tstamp;
	*distribXid = map->dxid;
	return true;
}

/*
 * TODO:
 * Find the next lowest transaction with a logged or recorded status.
 * Currently on distributed commits are recorded.
 */
bool
DistributedLog_ScanForPrevCommitted(
	TransactionId 						*indexXid,
	DistributedTransactionTimeStamp 	*distribTimeStamp,
	DistributedTransactionId 			*distribXid)
{
	return false;
}

static Size
DistributedLog_SharedShmemSize(void)
{
	Size size;
	size = sizeof(DistributedLogShmem);
	size = add_size(size, hash_estimate_size(MaxConnections, sizeof(DistributedLogMap)));
	return size;
}

/*
 * TODO:
 * Initialization of shared memory for the distributed log.
 */
Size
DistributedLog_ShmemSize(void)
{
	return 0;
}


void
DistributedLog_ShmemInit(void)
{
	bool found;
	Size size = DistributedLog_SharedShmemSize();

	if (IS_QUERY_DISPATCHER())
		return;

	DistributedLogShared = (DistributedLogShmem *)
		ShmemInitStruct("DistributedLogShmem", DistributedLog_SharedShmemSize(), &found);

	if (!DistributedLogShared)
		elog(FATAL, "could not initialize Distributed Log shared memory");

	if (!found)
	{
		HASHCTL hctl;

		DistributedLogShared->oldestXmin = InvalidTransactionId;

		/* Initialize per-query hashtable */
		memset(&hctl, 0, sizeof(hctl));
		hctl.keysize = sizeof(TransactionId);
		hctl.entrysize = sizeof(DistributedLogMap);
		hctl.hash = tag_hash;

		DistributedLogShared->mapHash =
			ShmemInitHash("distributed log hash",
						  MaxConnections,
						  MaxConnections,
						  &hctl,
						  HASH_ELEM | HASH_FUNCTION);
	}
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
DistributedLog_CheckPoint(void)
{
	if (IS_QUERY_DISPATCHER())
		return;

	return;
}

