/*-------------------------------------------------------------------------
 *
 * resgroup.c
 *	  GPDB resource group management code.
 *
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resgroup.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/memquota.h"
#include "commands/resgroupcmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/resgroup-ops.h"
#include "utils/resgroup.h"
#include "utils/resource_manager.h"
#include "utils/resowner.h"
#include "utils/session_state.h"

/* GUC */
int		MaxResourceGroups;

/* Global variables */

/* TODO: init this */
ResGroupProcData _MyResGroupProcData =
{
	InvalidOid, RESGROUP_INVALID_SLOT_ID,
};
ResGroupProcData *MyResGroupProcData = &_MyResGroupProcData;

/* static variables */
/*
 * TODO: update description.
 *
 * Global variable 'CurrentResGroupSharedInfo' is used to optimize the
 * access to the current resource group. This pointer is valid
 * in the lifetime of a transaction as the resource group could
 * be dropped concurrently.
 */
static ResGroup	CurrentResGroupSharedInfo = NULL;

static ResGroupControl *pResGroupControl = NULL;

static bool localResWaiting = false;

/* static functions */
static ResGroup ResGroupHashNew(Oid groupId);
static ResGroup ResGroupHashFind(Oid groupId);
static bool ResGroupHashRemove(Oid groupId);
static void ResGroupWait(ResGroup group, bool isLocked);
static bool ResGroupCreate(Oid groupId);
static void AtProcExit_ResGroup(int code, Datum arg);
static void ResGroupWaitCancel(void);
static int getFreeSlot(void);
static int ResGroupSlotAcquire(void);
static void addTotalQueueDuration(ResGroup group);
static void ResGroupSlotRelease(void);


/*
 * Estimate size the resource group structures will need in
 * shared memory.
 */
Size
ResGroupShmemSize(void)
{
	Size		size = 0;

	/* The hash of groups. */
	size = hash_estimate_size(MaxResourceGroups, sizeof(ResGroupHashEntry));

	/* The control structure. */
	size = add_size(size, sizeof(ResGroupControl) - sizeof(ResGroupData));

	/* The control structure. */
	size = add_size(size, mul_size(MaxResourceGroups, sizeof(ResGroupData)));

	/* Add a safety margin */
	size = add_size(size, size / 10);

	return size;
}

/*
 * Initialize the global ResGroupControl struct of resource groups.
 */
void
ResGroupControlInit(void)
{
	int			i;
    bool        found;
    HASHCTL     info;
    int         hash_flags;
	int			size;

	size = sizeof(*pResGroupControl) - sizeof(ResGroupData);
	size += mul_size(MaxResourceGroups, sizeof(ResGroupData));

    pResGroupControl = ShmemInitStruct("global resource group control",
                                       size, &found);
    if (found)
        return;
    if (pResGroupControl == NULL)
        goto error_out;

    /* Set key and entry sizes of hash table */
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(ResGroupHashEntry);
    info.hash = tag_hash;

    hash_flags = (HASH_ELEM | HASH_FUNCTION);

    LOG_RESGROUP_DEBUG(LOG, "Creating hash table for %d resource groups", MaxResourceGroups);

    pResGroupControl->htbl = ShmemInitHash("Resource Group Hash Table",
                                           MaxResourceGroups,
                                           MaxResourceGroups,
                                           &info, hash_flags);

    if (!pResGroupControl->htbl)
        goto error_out;

    /*
     * No need to acquire LWLock here, since this is expected to be called by
     * postmaster only
     */
    pResGroupControl->loaded = false;
    pResGroupControl->nGroups = MaxResourceGroups;

	for (i = 0; i < MaxResourceGroups; i++)
		pResGroupControl->groups[i].groupId = InvalidOid;

    return;

error_out:
	ereport(FATAL,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("not enough shared memory for resource group control")));
}

/*
 * Allocate a resource group entry from a hash table
 */
void
AllocResGroupEntry(Oid groupId)
{
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	bool groupOK = ResGroupCreate(groupId);
	if (!groupOK)
	{
		LWLockRelease(ResGroupLock);

		ereport(PANIC,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("not enough shared memory for resource groups")));
	}

	LWLockRelease(ResGroupLock);
}

/*
 * Remove a resource group entry from the hash table
 */
void
FreeResGroupEntry(Oid groupId)
{
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

#ifdef USE_ASSERT_CHECKING
	bool groupOK = 
#endif
		ResGroupHashRemove(groupId);
	Assert(groupOK);

	LWLockRelease(ResGroupLock);
}

/*
 * Load the resource groups in shared memory. Note this
 * can only be done after enough setup has been done. This uses
 * heap_open etc which in turn requires shared memory to be set up.
 */
void
InitResGroups(void)
{
	HeapTuple	tuple;
	SysScanDesc	sscan;
	int			numGroups;
	CdbComponentDatabases *cdbComponentDBs;
	CdbComponentDatabaseInfo *qdinfo;

	on_shmem_exit(AtProcExit_ResGroup, 0);
	if (pResGroupControl->loaded)
		return;
	/*
	 * Need a resource owner to keep the heapam code happy.
	 */
	Assert(CurrentResourceOwner == NULL);
	ResourceOwner owner = ResourceOwnerCreate(NULL, "InitResGroups");
	CurrentResourceOwner = owner;

	/*
	 * The resgroup shared mem initialization must be serialized. Only the first session
	 * should do the init.
	 * Serialization is done by LW_EXCLUSIVE ResGroupLock. However, we must obtain all DB
	 * locks before obtaining LWlock to prevent deadlock.
	 */
	Relation relResGroup = heap_open(ResGroupRelationId, AccessShareLock);
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	if (pResGroupControl->loaded)
		goto exit;

	ResGroupOps_Init();

	numGroups = 0;
	sscan = systable_beginscan(relResGroup, InvalidOid, false, SnapshotNow, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		Oid groupId = HeapTupleGetOid(tuple);
		bool groupOK = ResGroupCreate(groupId);
		float cpu_rate_limit = GetCpuRateLimitForResGroup(groupId);

		if (!groupOK)
			ereport(PANIC,
					(errcode(ERRCODE_OUT_OF_MEMORY),
			 		errmsg("not enough shared memory for resource groups")));

		ResGroupOps_CreateGroup(groupId);
		ResGroupOps_SetCpuRateLimit(groupId, cpu_rate_limit);

		numGroups++;
		Assert(numGroups <= MaxResourceGroups);
	}
	systable_endscan(sscan);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		cdbComponentDBs = getCdbComponentDatabases();
		qdinfo = &cdbComponentDBs->entry_db_info[0];
		pResGroupControl->segmentsOnMaster = qdinfo->hostSegs;
	}

	pResGroupControl->loaded = true;
	LOG_RESGROUP_DEBUG(LOG, "initialized %d resource groups", numGroups);

exit:
	LWLockRelease(ResGroupLock);
	heap_close(relResGroup, AccessShareLock);
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(owner);
}

/*
 * Check resource group status when DROP RESOURCE GROUP
 *
 * Errors out if there're running transactions, otherwise lock the resource group.
 * New transactions will be queued if the resource group is locked.
 */
void
ResGroupCheckForDrop(Oid groupId, char *name)
{
	ResGroup group;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	group = ResGroupHashFind(groupId);
	if (group == NULL)
	{
		LWLockRelease(ResGroupLock);

		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Cannot find resource group with Oid %d in shared memory", groupId)));
	}

	if (group->nRunning > 0)
	{
		int nQuery = group->nRunning + group->waitProcs.size;
		LWLockRelease(ResGroupLock);

		Assert(name != NULL);
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("Cannot drop resource group \"%s\"", name),
				 errhint(" The resource group is currently managing %d query(ies) and cannot be dropped.\n"
						 "\tTerminate the queries first or try dropping the group later.\n"
						 "\tThe view pg_stat_activity tracks the queries managed by resource groups.", nQuery)));
	}
	group->lockedForDrop = true;

	LWLockRelease(ResGroupLock);
}

/*
 * Wake up the backends in the wait queue when DROP RESOURCE GROUP finishes.
 * Unlock the resource group if the transaction is abortted.
 * Remove the resource group entry in shared memory if the transaction is committed.
 *
 * This function is called in the callback function of DROP RESOURCE GROUP.
 */
void ResGroupDropCheckForWakeup(Oid groupId, bool isCommit)
{
	int wakeNum;
	PROC_QUEUE	*waitQueue;
	ResGroup	group;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	group = ResGroupHashFind(groupId);
	if (group == NULL)
	{
		LWLockRelease(ResGroupLock);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				errmsg("Cannot find resource group %d in shared memory", groupId)));
	}

	Assert(group->lockedForDrop);

	waitQueue = &(group->waitProcs);
	wakeNum = waitQueue->size;

	while(wakeNum > 0)
	{
		PGPROC *waitProc;

		/* wake up one process in the wait queue */
		waitProc = (PGPROC *) MAKE_PTR(waitQueue->links.next);
		SHMQueueDelete(&(waitProc->links));
		waitQueue->size--;

		waitProc->resWaiting = false;
		waitProc->resGranted = false;
		SetLatch(&waitProc->procLatch);
		wakeNum--;
	}

	if (isCommit)
	{
#ifdef USE_ASSERT_CHECKING
		bool groupOK = 
#endif
			ResGroupHashRemove(groupId);
		Assert(groupOK);
	}
	else
	{
		group->lockedForDrop = false;
	}

	LWLockRelease(ResGroupLock);
}

/*
 * Wake up the backends in the wait queue when 'concurrency' is increased.
 * This function is called in the callback function of ALTER RESOURCE GROUP.
 */
void ResGroupAlterCheckForWakeup(Oid groupId)
{
	int	proposed;
	int wakeNum;
	PROC_QUEUE	*waitQueue;
	ResGroup	group;

	GetConcurrencyForResGroup(groupId, NULL, &proposed);

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	group = ResGroupHashFind(groupId);
	if (group == NULL)
	{
		LWLockRelease(ResGroupLock);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				errmsg("Cannot find resource group %d in shared memory", groupId)));
	}

	waitQueue = &(group->waitProcs);

	if (proposed == RESGROUP_CONCURRENCY_UNLIMITED)
		wakeNum = waitQueue->size;
	else if (proposed <= group->nRunning)
		wakeNum = 0;
	else
		wakeNum = Min(proposed - group->nRunning, waitQueue->size);

	while(wakeNum > 0)
	{
		PGPROC *waitProc;

		/* wake up one process in the wait queue */
		waitProc = (PGPROC *) MAKE_PTR(waitQueue->links.next);
		SHMQueueDelete(&(waitProc->links));
		waitQueue->size--;

		waitProc->resWaiting = false;
		waitProc->resGranted = true;
		SetLatch(&waitProc->procLatch);

		group->nRunning++;
		wakeNum--;
	}

	LWLockRelease(ResGroupLock);
}

/*
 *  Retrieve statistic information of type from resource group
 */
void
ResGroupGetStat(Oid groupId, ResGroupStatType type, char *retStr, int retStrLen, const char *prop)
{
	ResGroup group;

	if (!IsResGroupEnabled())
		return;

	LWLockAcquire(ResGroupLock, LW_SHARED);

	group = ResGroupHashFind(groupId);
	if (group == NULL)
	{
		LWLockRelease(ResGroupLock);

		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Cannot find resource group with Oid %d in shared memory", groupId)));
	}

	switch (type)
	{
		case RES_GROUP_STAT_NRUNNING:
			snprintf(retStr, retStrLen, "%d", group->nRunning);
			break;
		case RES_GROUP_STAT_NQUEUEING:
			snprintf(retStr, retStrLen, "%d", group->waitProcs.size);
			break;
		case RES_GROUP_STAT_TOTAL_EXECUTED:
			snprintf(retStr, retStrLen, "%d", group->totalExecuted);
			break;
		case RES_GROUP_STAT_TOTAL_QUEUED:
			snprintf(retStr, retStrLen, "%d", group->totalQueued);
			break;
		case RES_GROUP_STAT_TOTAL_QUEUE_TIME:
		{
			Datum durationDatum = DirectFunctionCall1(interval_out, IntervalPGetDatum(&group->totalQueuedTime));
			char *durationStr = DatumGetCString(durationDatum);
			strncpy(retStr, durationStr, retStrLen);
			break;
		}
		case RES_GROUP_STAT_MEM_USAGE:
			snprintf(retStr, retStrLen, "%d", group->totalMemoryUsage);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Invalid stat type %s", prop)));
	}

	LWLockRelease(ResGroupLock);
}

/*
 * Check the memory limit of resource group
 */
bool
ResGroupReserveMemory(int32 memoryChunks, int32 overuseChunks)
{
	ResGroupProcData *localInfo = MyResGroupProcData;
	ResGroup sharedInfo = CurrentResGroupSharedInfo;

	if (!IsResGroupEnabled())
		return true;

	Assert(memoryChunks >= 0);

	localInfo->memoryUsed += memoryChunks;

	if (sharedInfo == NULL)
		return true;

	if (localInfo->groupId == InvalidOid || sharedInfo->groupId != localInfo->groupId)
	{
		CurrentResGroupSharedInfo = NULL;
		return true;
	}

	Assert(sharedInfo->totalMemoryUsage >= 0);
	Assert(localInfo->memoryUsed >= 0);

	ResGroupSlotData *slot = &sharedInfo->slots[localInfo->slotId];

	int32 totalUsedMemory = pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&slot->memoryUsage, memoryChunks);
	int32 sharedMemoryUsage = totalUsedMemory - localInfo->memoryQuota;

	if (sharedMemoryUsage > 0)
	{
		pg_atomic_uint32 *memSharedQuotaUsage =
			(pg_atomic_uint32 *)&sharedInfo->memSharedQuotaUsage;

		int32 totalUsedSharedMemory = pg_atomic_add_fetch_u32(memSharedQuotaUsage, sharedMemoryUsage);

		if (CritSectionCount == 0 &&
			totalUsedSharedMemory > localInfo->sharedQuota + overuseChunks)
		{
			pg_atomic_sub_fetch_u32(memSharedQuotaUsage, sharedMemoryUsage);
			pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&slot->memoryUsage, memoryChunks);
			localInfo->memoryUsed -= memoryChunks;
			return false;
		}
	}

	pg_atomic_add_fetch_u32((pg_atomic_uint32 *)&sharedInfo->totalMemoryUsage,
							memoryChunks);

	return true;
}

/*
 * Update the memory usage of resource group
 */
void
ResGroupReleaseMemory(int32 memoryChunks)
{
	ResGroupProcData *localInfo = MyResGroupProcData;
	ResGroup sharedInfo = CurrentResGroupSharedInfo;

	if (!IsResGroupEnabled())
		return;

	Assert(memoryChunks >= 0);

	if (sharedInfo == NULL)
	{
		localInfo->memoryUsed -= memoryChunks;
		return;
	}

	if (localInfo->groupId == InvalidOid || sharedInfo->groupId != localInfo->groupId)
	{
		CurrentResGroupSharedInfo = NULL;
		localInfo->memoryUsed -= memoryChunks;
		return;
	}

	ResGroupSlotData *slot = &sharedInfo->slots[localInfo->slotId];
	int32 sharedMemoryUsage = slot->memoryUsage - localInfo->memoryQuota;
	if (sharedMemoryUsage > 0)
	{
		int32 returnSize = Min(memoryChunks, sharedMemoryUsage);

		pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&sharedInfo->memSharedQuotaUsage,
								returnSize);
	}

	localInfo->memoryUsed -= memoryChunks;
	pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&slot->memoryUsage, memoryChunks);

	pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&sharedInfo->totalMemoryUsage,
							memoryChunks);
}

/*
 * Calculate the new concurrency 'value' of pg_resgroupcapability
 */
int
CalcConcurrencyValue(int groupId, int val, int proposed, int newProposed)
{
	ResGroup	group;
	int			ret;

	if (!IsResGroupEnabled())
		return newProposed;

	LWLockAcquire(ResGroupLock, LW_SHARED);

	group = ResGroupHashFind(groupId);
	if (group == NULL)
	{
		LWLockRelease(ResGroupLock);

		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Cannot find resource group with Oid %d in shared memory", groupId)));
	}

	if (newProposed == RESGROUP_CONCURRENCY_UNLIMITED || group->nRunning <= newProposed)
		ret = newProposed;
	else if (group->nRunning <= proposed)
		ret = proposed;
	else
		ret = val;

	LWLockRelease(ResGroupLock);
	return ret;
}

/*
 * ResGroupCreate -- initialize the elements for a resource group.
 *
 * Notes:
 *	It is expected that the appropriate lightweight lock is held before
 *	calling this - unless we are the startup process.
 */
static bool
ResGroupCreate(Oid groupId)
{
	ResGroup		group;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));
	Assert(OidIsValid(groupId));

	group = ResGroupHashNew(groupId);
	if (group == NULL)
		return false;

	group->groupId = groupId;
	group->nRunning = 0;
	ProcQueueInit(&group->waitProcs);
	group->totalExecuted = 0;
	group->totalQueued = 0;
	group->totalMemoryUsage = 0;
	group->memSharedQuotaUsage = 0;
	memset(&group->totalQueuedTime, 0, sizeof(group->totalQueuedTime));
	group->lockedForDrop = false;
	memset(group->slots, 0, sizeof(group->slots));

	return true;
}

void
ResGroupSetupMemoryController(void)
{
	int segmentCount;
	int totalMemory;
	int memPerSegment;
	ResGroup sharedInfo = CurrentResGroupSharedInfo;

	/* get segment(including master) count on the host */
	if (Gp_role == GP_ROLE_DISPATCH)
		segmentCount = pResGroupControl->segmentsOnMaster;
	else if (Gp_role == GP_ROLE_EXECUTE)
		segmentCount = host_segments;

	Assert(segmentCount > 0);
	Assert(Gp_role != GP_ROLE_UTILITY);
	Assert(sharedInfo != NULL);

	/*TODO: should we calculate swap? can we compute just once? */
	totalMemory = ResGroupOps_GetTotalMemory() * gp_resource_group_memory_limit;
	memPerSegment = totalMemory / segmentCount;

	LOG_RESGROUP_DEBUG(LOG, "Primary segments on this host: %d, memory per segment: %d MB",
						segmentCount, memPerSegment);

	MyResGroupProcData->memoryQuota = memPerSegment
		* MyResGroupProcData->memoryLimit
		* (100 - MyResGroupProcData->sharedQuota)
		/ MyResGroupProcData->concurrency
		/ 10000;
}

static int
getFreeSlot(void)
{
	int i;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));

	for (i = 0; i < RESGROUP_MAX_CONCURRENCY; i++)
	{
		if (CurrentResGroupSharedInfo->slots[i].inUse)
			continue;

		CurrentResGroupSharedInfo->slots[i].inUse = true;
		return i;
	}

	Assert(false && "No free slot available");
	return RESGROUP_INVALID_SLOT_ID;
}

/*
 * Acquire a resource group slot
 *
 * Call this function at the start of the transaction.
 */
static int
ResGroupSlotAcquire(void)
{
	ResGroup	group;
	Oid			groupId;
	int			concurrencyProposed;
	int			slotId;
	bool		retried = false;

	Assert(MyResGroupProcData->groupId == InvalidOid);

	groupId = GetResGroupIdForRole(GetUserId());
	if (groupId == InvalidOid)
		groupId = superuser() ? ADMINRESGROUP_OID : DEFAULTRESGROUP_OID;

	GetConcurrencyForResGroup(groupId, NULL, &concurrencyProposed);

retry:
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	group = ResGroupHashFind(groupId);
	if (group == NULL)
	{
		LWLockRelease(ResGroupLock);

		MyResGroupProcData->groupId = InvalidOid;
		CurrentResGroupSharedInfo = NULL;

		if (retried)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("Resource group %d was concurrently dropped", groupId)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("Cannot find resource group %d in shared memory", groupId)));
	}

	CurrentResGroupSharedInfo = group;
	MyResGroupProcData->groupId = groupId;

	/* wait on the queue if the group is locked for drop */
	if (group->lockedForDrop)
	{
		Assert(group->nRunning == 0);
		ResGroupWait(group, true);

		/* retry if the drop resource group transaction is finished */
		retried = true;
		goto retry;
	}

	/* acquire a slot */
	if (concurrencyProposed == RESGROUP_CONCURRENCY_UNLIMITED || group->nRunning < concurrencyProposed)
	{
		group->nRunning++;
		group->totalExecuted++;
		slotId = getFreeSlot();
		LWLockRelease(ResGroupLock);
		pgstat_report_resgroup(0, group->groupId);
		return slotId;
	}

	/* We have to wait for the slot */
	ResGroupWait(group, false);

	/*
	 * The waking process has granted us the slot.
	 * Update the statistic information of the resource group.
	 */
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
	group->totalExecuted++;
	addTotalQueueDuration(group);
	slotId = getFreeSlot();
	LWLockRelease(ResGroupLock);
	return slotId;
}

/* Update the total queued time of this group */
static void
addTotalQueueDuration(ResGroup group)
{
	Assert(LWLockHeldExclusiveByMe(ResGroupLock));
	if (group == NULL)
		return;

	TimestampTz start = pgstat_fetch_resgroup_queue_timestamp();
	TimestampTz now = GetCurrentTimestamp();
	Datum durationDatum = DirectFunctionCall2(timestamptz_age, TimestampTzGetDatum(now), TimestampTzGetDatum(start));
	Datum sumDatum = DirectFunctionCall2(interval_pl, IntervalPGetDatum(&group->totalQueuedTime), durationDatum);
	memcpy(&group->totalQueuedTime, DatumGetIntervalP(sumDatum), sizeof(Interval));
}

/*
 * Release the resource group slot
 *
 * Call this function at the end of the transaction.
 */
static void
ResGroupSlotRelease(void)
{
	ResGroup	group;
	PROC_QUEUE	*waitQueue;
	PGPROC		*waitProc;
	int			concurrencyProposed;

	group = CurrentResGroupSharedInfo;
	Assert(group != NULL);

	GetConcurrencyForResGroup(group->groupId, NULL, &concurrencyProposed);

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	waitQueue = &(group->waitProcs);
	
	/* If the concurrency is RESGROUP_CONCURRENCY_UNLIMITED, the wait queue should also be empty */
	if ((RESGROUP_CONCURRENCY_UNLIMITED != concurrencyProposed  && group->nRunning > concurrencyProposed) ||
		waitQueue->size == 0)
	{
		AssertImply(waitQueue->size == 0,
					waitQueue->links.next == MAKE_OFFSET(&waitQueue->links) &&
					waitQueue->links.prev == MAKE_OFFSET(&waitQueue->links));
		Assert(group->nRunning > 0);

		group->nRunning--;
		LWLockRelease(ResGroupLock);
		return;
	}

	/* wake up one process in the wait queue */
	waitProc = (PGPROC *) MAKE_PTR(waitQueue->links.next);
	SHMQueueDelete(&(waitProc->links));
	waitQueue->size--;
	waitProc->resGranted = true;
	LWLockRelease(ResGroupLock);

	waitProc->resWaiting = false;
	SetLatch(&waitProc->procLatch);
}

void
AssignResGroupOnMaster(void)
{
	ResGroup sharedInfo; 
	ResGroupSlotData *slot;
	ResGroupProcData *procInfo;
	int concurrency;
	float memoryLimit, sharedQuota, spillRatio;
	int slotId;
	Oid groupId;

	/* Acquire slot */
	procInfo = MyResGroupProcData;
	slotId = ResGroupSlotAcquire();
	groupId = procInfo->groupId;
	Assert(slotId != RESGROUP_INVALID_SLOT_ID);
	Assert(groupId != InvalidOid);
	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(CurrentResGroupSharedInfo != NULL);

	/* Init slot */
	sharedInfo = CurrentResGroupSharedInfo;
	slot = &sharedInfo->slots[slotId];
	slot->xactid = 0xdeadbeaf;
	slot->sessionid = gp_session_id;
	pg_atomic_add_fetch_u32((pg_atomic_uint32*)&slot->nProcs, 1);

	/* Init MyResGroupProcData */
	GetMemoryCapabilitiesForResGroup(groupId, &memoryLimit, &sharedQuota, &spillRatio);
	GetConcurrencyForResGroup(groupId, NULL, &concurrency);
	/* TODO: handle (concurrency == -1) properly */
	if (concurrency == RESGROUP_CONCURRENCY_UNLIMITED)
		concurrency = 20; /* FIXME */

	procInfo->slotId = slotId;
	procInfo->memoryLimit = memoryLimit * 100;
	procInfo->sharedQuota = sharedQuota * 100;
	procInfo->spillRatio = spillRatio * 100;
	procInfo->concurrency = concurrency;
	Assert(pResGroupControl != NULL);
	Assert(pResGroupControl->segmentsOnMaster > 0);
	procInfo->segmentMem = ResGroupOps_GetTotalMemory() * gp_resource_group_memory_limit / pResGroupControl->segmentsOnMaster;
	procInfo->memoryQuota = procInfo->segmentMem
		* procInfo->memoryLimit
		* (100 - procInfo->sharedQuota)
		/ procInfo->concurrency
		/ 10000;

	/* Add proc memory accounting info to slot */
	int32 slotMemoryUsage = pg_atomic_add_fetch_u32((pg_atomic_uint32*)&slot->memoryUsage,
												procInfo->memoryUsed);

	/* Add proc memory accounting info to memSharedQuotaUsage in sharedInfo */
	int32 sharedMemoryUsage = slotMemoryUsage - procInfo->memoryQuota;
	if (sharedMemoryUsage > 0)
		pg_atomic_add_fetch_u32((pg_atomic_uint32*)&sharedInfo->memSharedQuotaUsage, sharedMemoryUsage);

	/* Add proc memory accounting info to totalMemoryUsage in sharedInfo */
	pg_atomic_add_fetch_u32((pg_atomic_uint32*)&sharedInfo->totalMemoryUsage,
							procInfo->memoryUsed);

	/* Add into cgroup */
	ResGroupOps_AssignGroup(sharedInfo->groupId, MyProcPid);
}

void
UnassignResGroupOnMaster(void)
{
	ResGroup sharedInfo; 
	ResGroupSlotData *slot;
	ResGroupProcData *procInfo;

	if (CurrentResGroupSharedInfo == NULL)
		return;

	procInfo = MyResGroupProcData;
	sharedInfo = CurrentResGroupSharedInfo;

	Assert(sharedInfo->groupId != InvalidOid);
	Assert(procInfo->groupId == sharedInfo->groupId);
	Assert(procInfo->slotId != RESGROUP_INVALID_SLOT_ID);

	slot = &sharedInfo->slots[procInfo->slotId];

	/* Sub proc memory accounting info from totalMemoryUsage in sharedInfo */
	pg_atomic_sub_fetch_u32((pg_atomic_uint32*)&CurrentResGroupSharedInfo->totalMemoryUsage,
							MyResGroupProcData->memoryUsed);

	/* Sub proc memory accounting info from memSharedQuotaUsage in sharedInfo */
	int32 sharedMemoryUsage = slot->memoryUsage - procInfo->memoryQuota;
	if (sharedMemoryUsage > 0)
	{
		int32 returnSize = Min(procInfo->memoryUsed, sharedMemoryUsage);

		pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&sharedInfo->memSharedQuotaUsage,
								returnSize);
	}

	/* Sub proc memory accounting info from slot */
	pg_atomic_sub_fetch_u32((pg_atomic_uint32*)&slot->memoryUsage,
							procInfo->memoryUsed);

	/* Cleanup procInfo */
	if (procInfo->memoryUsed > 10)
		LOG_RESGROUP_DEBUG(LOG, "Idle proc memory usage: %d", procInfo->memoryUsed);
	procInfo->groupId = InvalidOid;
	procInfo->slotId = RESGROUP_INVALID_SLOT_ID;

	/* Cleanup slotInfo */
	pg_atomic_sub_fetch_u32((pg_atomic_uint32*)&slot->nProcs, 1);
	slot->inUse = false;

	ResGroupSlotRelease();

	/* Cleanup sharedInfo */
	CurrentResGroupSharedInfo = NULL;
}

void
SwitchResGroupOnSegment(int prevGroupId, int prevSlotId)
{
	ResGroup sharedInfo; 
	ResGroupSlotData *slot;
	ResGroupProcData *procInfo;

	procInfo = MyResGroupProcData;

	AssertImply(procInfo->groupId != InvalidOid,
				procInfo->slotId != RESGROUP_INVALID_SLOT_ID);
	AssertImply(prevGroupId != InvalidOid,
				prevSlotId != RESGROUP_INVALID_SLOT_ID);

	if (procInfo->groupId == InvalidOid)
	{
		Assert(prevGroupId == InvalidOid);
		Assert(prevSlotId == RESGROUP_INVALID_SLOT_ID);
		Assert(CurrentResGroupSharedInfo == NULL);
		return;
	}

	/* previouse resource group is valid and not dropped yet */
	if (CurrentResGroupSharedInfo != NULL && CurrentResGroupSharedInfo->groupId != InvalidOid)
	{
		ResGroup prevSharedInfo; 
		ResGroupSlotData *prevSlot;

		prevSharedInfo = CurrentResGroupSharedInfo;
		Assert(prevSharedInfo->groupId == prevGroupId);
		prevSlot = &prevSharedInfo->slots[prevSlotId];

		pg_atomic_sub_fetch_u32((pg_atomic_uint32*)&prevSlot->nProcs, 1);

		/* Sub proc memory accounting info from totalMemoryUsage in previous sharedInfo */
		pg_atomic_sub_fetch_u32((pg_atomic_uint32*)&prevSharedInfo->totalMemoryUsage,
								procInfo->memoryUsed);

		/* Sub proc memory accounting info from memSharedQuotaUsage in previous sharedInfo */
		int32 sharedMemoryUsage = prevSlot->memoryUsage - procInfo->memoryQuota;
		if (sharedMemoryUsage > 0)
		{
			int32 returnSize = Min(procInfo->memoryUsed, sharedMemoryUsage);

			pg_atomic_sub_fetch_u32((pg_atomic_uint32 *)&prevSharedInfo->memSharedQuotaUsage,
									returnSize);
		}

		/* Sub proc memory accounting info from slot */
		pg_atomic_sub_fetch_u32((pg_atomic_uint32*)&prevSlot->memoryUsage,
								procInfo->memoryUsed);
	}

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
	sharedInfo = ResGroupHashFind(procInfo->groupId);
	Assert(sharedInfo != NULL);
	LWLockRelease(ResGroupLock);
	slot = &sharedInfo->slots[procInfo->slotId];

	/* Init MyResGroupProcData */
	Assert(host_segments > 0);
	Assert(procInfo->concurrency > 0);
	procInfo->segmentMem = ResGroupOps_GetTotalMemory() * gp_resource_group_memory_limit / host_segments;
	procInfo->memoryQuota = procInfo->segmentMem
		* procInfo->memoryLimit
		* (100 - procInfo->sharedQuota)
		/ procInfo->concurrency
		/ 10000;

	/* Init slot */
	slot = &sharedInfo->slots[procInfo->slotId];
	slot->xactid = 0xdeadbeaf;
	slot->sessionid = gp_session_id;
	pg_atomic_add_fetch_u32((pg_atomic_uint32*)&slot->nProcs, 1);

	/* Add proc memory accounting info to slot */
	int32 slotMemoryUsage = pg_atomic_add_fetch_u32((pg_atomic_uint32*)&slot->memoryUsage,
												procInfo->memoryUsed);

	/* Add proc memory accounting info to memSharedQuotaUsage in sharedInfo */
	int32 sharedMemoryUsage = slotMemoryUsage - procInfo->memoryQuota;
	if (sharedMemoryUsage > 0)
		pg_atomic_add_fetch_u32((pg_atomic_uint32*)&sharedInfo->memSharedQuotaUsage, sharedMemoryUsage);

	/* Add proc memory accounting info to totalMemoryUsage in sharedInfo */
	pg_atomic_add_fetch_u32((pg_atomic_uint32*)&sharedInfo->totalMemoryUsage,
							procInfo->memoryUsed);

	/* Add into cgroup */
	ResGroupOps_AssignGroup(sharedInfo->groupId, MyProcPid);

	CurrentResGroupSharedInfo = sharedInfo;
}

/*
 * Wait on the queue of resource group
 */
static void
ResGroupWait(ResGroup group, bool isLocked)
{
	PGPROC *proc = MyProc, *headProc;
	PROC_QUEUE *waitQueue;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));

	proc->resWaiting = true;

	waitQueue = &(group->waitProcs);

	headProc = (PGPROC *) &(waitQueue->links);
	SHMQueueInsertBefore(&(headProc->links), &(proc->links));
	waitQueue->size++;

	if (!isLocked)
		group->totalQueued++;

	LWLockRelease(ResGroupLock);
	pgstat_report_resgroup(GetCurrentTimestamp(), group->groupId);

	/* similar to lockAwaited in ProcSleep for interrupt cleanup */
	localResWaiting = true;

	/*
	 * Make sure we have released all locks before going to sleep, to eliminate
	 * deadlock situations
	 */
	PG_TRY();
	{
		for (;;)
		{
			ResetLatch(&proc->procLatch);

			CHECK_FOR_INTERRUPTS();

			if (!proc->resWaiting)
				break;
			WaitLatch(&proc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1);
		}
	}
	PG_CATCH();
	{
		ResGroupWaitCancel();
		PG_RE_THROW();
	}
	PG_END_TRY();

	localResWaiting = false;

	pgstat_report_waiting(PGBE_WAITING_NONE);
}

/*
 * ResGroupHashNew -- return a new (empty) group object to initialize.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroup
ResGroupHashNew(Oid groupId)
{
	int			i;
	bool		found;
	ResGroupHashEntry *entry;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));

	if (groupId == InvalidOid)
		return NULL;

	for (i = 0; i < pResGroupControl->nGroups; i++)
	{
		if (pResGroupControl->groups[i].groupId == InvalidOid)
			break;
	}
	Assert(i < pResGroupControl->nGroups);

	entry = (ResGroupHashEntry *)
		hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_ENTER_NULL, &found);
	/* caller should test that the group does not exist already */
	Assert(!found);
	entry->index = i;

	return &pResGroupControl->groups[i];
}

/*
 * ResGroupHashFind -- return the group for a given oid.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroup
ResGroupHashFind(Oid groupId)
{
	bool				found;
	ResGroupHashEntry	*entry;

	Assert(LWLockHeldByMe(ResGroupLock));

	entry = (ResGroupHashEntry *)
		hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_FIND, &found);
	if (!found)
		return NULL;

	Assert(entry->index < pResGroupControl->nGroups);
	return &pResGroupControl->groups[entry->index];
}


/*
 * ResGroupHashRemove -- remove the group for a given oid.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static bool
ResGroupHashRemove(Oid groupId)
{
	bool		found;
	ResGroupHashEntry	*entry;
	ResGroup			group;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));

	entry = (ResGroupHashEntry*)hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_FIND, &found);
	if (!found)
		return false;

	group = &pResGroupControl->groups[entry->index];
	group->groupId = InvalidOid;

	hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_REMOVE, &found);

	return true;
}

/* Process exit without waiting for slot or received SIGTERM */
static void
AtProcExit_ResGroup(int code, Datum arg)
{
	ResGroupWaitCancel();
}

/*
 * Handle the interrupt cases when waiting on the queue
 *
 * The proc may wait on the queue for a slot, or wait for the
 * DROP transaction to finish. In the first case, at the same time
 * we get interrupted (SIGINT or SIGTERM), we could have been
 * grantted a slot or not. In the second case, there's no running
 * transaction in the group. If the DROP transaction is finished
 * (commit or abort) at the same time as we get interrupted,
 * MyProc should have been removed from the wait queue, and the
 * ResGroup entry may have been removed if the DROP is committed.
 */
static void
ResGroupWaitCancel()
{
	ResGroup group;
	PROC_QUEUE	*waitQueue;
	PGPROC		*waitProc;

	/* Process exit without waiting for slot */
	group = CurrentResGroupSharedInfo;
	if (group == NULL || !localResWaiting)
		return;

	/* We are sure to be interrupted in the for loop of ResGroupWait now */
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	if (MyProc->links.next != INVALID_OFFSET)
	{
		/* Still waiting on the queue when get interrupted, remove myself from the queue */
		waitQueue = &(group->waitProcs);

		Assert(waitQueue->size > 0);
		Assert(MyProc->resWaiting);

		addTotalQueueDuration(group);

		SHMQueueDelete(&(MyProc->links));
		waitQueue->size--;
	}
	else if (MyProc->links.next == INVALID_OFFSET && MyProc->resGranted)
	{
		/* Woken up by a slot holder */
		group->totalExecuted++;
		addTotalQueueDuration(group);

		waitQueue = &(group->waitProcs);
		if (waitQueue->size == 0)
		{
			/* This is the last transaction on the wait queue, don't have to wake up others */
			Assert(waitQueue->links.next == MAKE_OFFSET(&waitQueue->links) &&
				   waitQueue->links.prev == MAKE_OFFSET(&waitQueue->links));
			Assert(group->nRunning > 0);

			group->nRunning--;
		}
		else
		{
			/* wake up one process on the wait queue */
			waitProc = (PGPROC *) MAKE_PTR(waitQueue->links.next);
			SHMQueueDelete(&(waitProc->links));
			waitQueue->size--;
			waitProc->resGranted = true;
			waitProc->resWaiting = false;
			SetLatch(&waitProc->procLatch);
		}
	}
	else
	{
		/*
		 * The transaction of DROP RESOURCE GROUP is finished,
		 * ResGroupSlotAcquire will do the retry.
		 */
	}

	LWLockRelease(ResGroupLock);
	localResWaiting = false;
	pgstat_report_waiting(PGBE_WAITING_NONE);
	CurrentResGroupSharedInfo = NULL;
}
