/*-------------------------------------------------------------------------
 *
 * resgroupcmds.c
 *	  Commands for manipulating resource group.
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 * IDENTIFICATION
 *    src/backend/commands/resgroupcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "gp-libpq-fe.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resgroup.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/resgroupcmds.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/fmgroids.h"
#include "utils/resgroup.h"
#include "utils/resgroup-ops.h"
#include "utils/resource_manager.h"
#include "utils/syscache.h"

#define RESGROUP_DEFAULT_CONCURRENCY (20)
#define RESGROUP_DEFAULT_MEM_SHARED_QUOTA (20)
#define RESGROUP_DEFAULT_MEM_SPILL_RATIO (20)

#define RESGROUP_MIN_CONCURRENCY	(1)
#define RESGROUP_MAX_CONCURRENCY	(MaxConnections)

#define RESGROUP_MIN_CPU_RATE_LIMIT	(1)
#define RESGROUP_MAX_CPU_RATE_LIMIT	(100)

#define RESGROUP_MIN_MEMORY_LIMIT	(1)
#define RESGROUP_MAX_MEMORY_LIMIT	(100)

#define RESGROUP_MIN_MEMORY_SHARED_QUOTA	(0)
#define RESGROUP_MAX_MEMORY_SHARED_QUOTA	(100)

#define RESGROUP_MIN_MEMORY_SPILL_RATIO		(1)
#define RESGROUP_MAX_MEMORY_SPILL_RATIO		(100)

/*
 * Internal struct to store the group settings.
 */
typedef struct ResourceGroupOptions
{
	int concurrency;
	int cpuRateLimit;
	int memoryLimit;
	int memSharedQuota;
	int memSpillRatio;
} ResourceGroupOptions;

/*
 * The context to pass to callback in ALTER resource group
 */
typedef struct {
	Oid		groupid;
	int		limittype;
	union {
		int		i;
		float	f;
	}	value;
	union {
		int		i;
		float	f;
	}	proposed;
} ResourceGroupAlterCallbackContext;

/*
 * The form of callbacks for resource group
 */
typedef void (*ResourceGroupCallback) (bool isCommit, void *arg);

/*
 * List of add-on callbacks for resource group related operations
 * The list is maintained as circular doubly linked.
 */
typedef struct ResourceGroupCallbackItem
{
	struct ResourceGroupCallbackItem *next;
	struct ResourceGroupCallbackItem *prev;
	ResourceGroupCallback callback;
	void *arg;
} ResourceGroupCallbackItem;

static ResourceGroupCallbackItem ResourceGroup_callbacks_head =
{
	&ResourceGroup_callbacks_head, &ResourceGroup_callbacks_head, NULL, NULL
};

static ResourceGroupCallbackItem *ResourceGroup_callbacks = &ResourceGroup_callbacks_head;

static int str2Int(const char *str, const char *prop);
static int text2Int(const text *text, const char *prop);
static int getResgroupOptionType(const char* defname);
static void parseStmtOptions(CreateResourceGroupStmt *stmt, ResourceGroupOptions *options);
static void validateCapabilities(Relation rel, Oid groupid, ResourceGroupOptions *options, bool newGroup);
static void getResgroupCapabilityEntry(int groupId, int type, char **value, char **proposed);
static void insertResgroupCapabilityEntry(Relation rel, Oid groupid, uint16 type, char *value);
static void updateResgroupCapabilityEntry(Oid groupid, uint16 type, char *value, char *proposed);
static void insertResgroupCapabilities(Oid groupid, ResourceGroupOptions *options);
static void deleteResgroupCapabilities(Oid groupid);
static void createResGroupAbortCallback(bool isCommit, void *arg);
static void dropResGroupAbortCallback(bool isCommit, void *arg);
static void alterResGroupCommitCallback(bool isCommit, void *arg);
static void registerResourceGroupCallback(ResourceGroupCallback callback, void *arg);

/*
 * Register callback functions for resource group related operations.
 *
 * At transaction end, the callback occurs post-commit or post-abort, so the
 * callback functions can only do noncritical cleanup.
 */
static void
registerResourceGroupCallback(ResourceGroupCallback callback, void *arg)
{
	ResourceGroupCallbackItem *item;

	item = (ResourceGroupCallbackItem *)
		MemoryContextAlloc(TopMemoryContext,
						   sizeof(ResourceGroupCallbackItem));
	item->callback = callback;
	item->arg = arg;

	item->prev = ResourceGroup_callbacks->prev;
	item->next = ResourceGroup_callbacks;
	item->prev->next = item;
	item->next->prev = item;
}

/*
 * Call resource group related callback functions at transaction end.
 *
 * On COMMIT, the callback functions are processed as FIFO.
 * On ABORT,  the callback functions are processed as LIFO.
 *
 * Note the callback functions would be removed as being processed.
 */
void
AtEOXact_ResGroup(bool isCommit)
{
	ResourceGroupCallbackItem *current =
		isCommit ? ResourceGroup_callbacks->next : ResourceGroup_callbacks->prev;
	while (current != ResourceGroup_callbacks)
	{
		ResourceGroupCallbackItem *tmp = isCommit? current->next : current->prev;

		ResourceGroupCallback callback = current->callback;
		void *arg =  current->arg;

		current->prev->next = current->next;
		current->next->prev = current->prev;
		pfree(current);
		current = tmp;

		callback(isCommit, arg);
	}
}

/*
 * CREATE RESOURCE GROUP
 */
void
CreateResourceGroup(CreateResourceGroupStmt *stmt)
{
	Relation	pg_resgroup_rel;
	TupleDesc	pg_resgroup_dsc;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple	tuple;
	Oid			groupid;
	Datum		new_record[Natts_pg_resgroup];
	bool		new_record_nulls[Natts_pg_resgroup];
	ResourceGroupOptions options;
	int			nResGroups;

	/* Permission check - only superuser can create groups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create resource groups")));

	/* Subtransaction is not supported for resource group related operations */
	if (IsSubTransaction())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("CREATE RESOURCE GROUP cannot run inside a subtransaction")));

	/*
	 * Check for an illegal name ('none' is used to signify no group in ALTER
	 * ROLE).
	 */
	if (strcmp(stmt->name, "none") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("resource group name \"none\" is reserved")));

	MemSet(&options, 0, sizeof(options));
	parseStmtOptions(stmt, &options);

	/*
	 * Grant ExclusiveLock to serialize concurrent 'CREATE RESOURCE GROUP'
	 */
	pg_resgroup_rel = heap_open(ResGroupRelationId, ExclusiveLock);

	/* Check if max_resource_group limit is reached */
	sscan = systable_beginscan(pg_resgroup_rel, InvalidOid, false,
							   SnapshotNow, 0, NULL);
	nResGroups = 0;
	while (systable_getnext(sscan) != NULL)
		nResGroups++;
	systable_endscan(sscan);

	if (nResGroups >= MaxResourceGroups)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("insufficient resource groups available"),
				 errhint("Increase max_resource_groups")));

	/*
	 * Check the pg_resgroup relation to be certain the group doesn't already
	 * exist.
	 */
	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));

	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, true,
							   SnapshotNow, 1, &scankey);

	if (HeapTupleIsValid(systable_getnext(sscan)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("resource group \"%s\" already exists",
						stmt->name)));

	systable_endscan(sscan);

	/*
	 * Build a tuple to insert
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_resgroup_rsgname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->name));

	new_record[Anum_pg_resgroup_parent - 1] = Int64GetDatum(0);

	pg_resgroup_dsc = RelationGetDescr(pg_resgroup_rel);
	tuple = heap_form_tuple(pg_resgroup_dsc, new_record, new_record_nulls);

	/*
	 * Insert new record in the pg_resgroup table
	 */
	groupid = simple_heap_insert(pg_resgroup_rel, tuple);
	CatalogUpdateIndexes(pg_resgroup_rel, tuple);

	/* process the WITH (...) list items */
	insertResgroupCapabilities(groupid, &options);

	/* Dispatch the statement to segments */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
		MetaTrackAddObject(ResGroupRelationId,
						   groupid,
						   GetUserId(), /* not ownerid */
						   "CREATE", "RESOURCE GROUP");
	}

	heap_close(pg_resgroup_rel, NoLock);

	/* Add this group into shared memory */
	if (IsResGroupEnabled())
	{
		Oid			*callbackArg;

		AllocResGroupEntry(groupid);

		/* Create os dependent part for this resource group */

		ResGroupOps_CreateGroup(groupid);
		ResGroupOps_SetCpuRateLimit(groupid, options.cpuRateLimit);

		/* Argument of callback function should be allocated in heap region */
		callbackArg = (Oid *)MemoryContextAlloc(TopMemoryContext, sizeof(Oid));
		*callbackArg = groupid;
		registerResourceGroupCallback(createResGroupAbortCallback, (void *)callbackArg);
	}
	else if (Gp_role == GP_ROLE_DISPATCH)
		ereport(WARNING,
				(errmsg("resource group is disabled"),
				 errhint("To enable set resource_scheduler=on and gp_resource_manager=group")));
}

/*
 * DROP RESOURCE GROUP
 */
void
DropResourceGroup(DropResourceGroupStmt *stmt)
{
	Relation	 pg_resgroup_rel;
	Relation	 authIdRel;
	HeapTuple	 tuple;
	ScanKeyData	 scankey;
	SysScanDesc	 sscan;
	ScanKeyData	 authid_scankey;
	SysScanDesc	 authid_scan;
	Oid			 groupid;
	Oid			*callbackArg;

	/* Permission check - only superuser can drop resource groups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop resource groups")));

	/* Subtransaction is not supported for resource group related operations */
	if (IsSubTransaction())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("DROP RESOURCE GROUP cannot run inside a subtransaction")));

	/*
	 * Check the pg_resgroup relation to be certain the resource group already
	 * exists.
	 */
	pg_resgroup_rel = heap_open(ResGroupRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));

	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, true,
							   SnapshotNow, 1, &scankey);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("resource group \"%s\" does not exist",
						stmt->name)));

	/*
	 * Remember the Oid, for destroying the in-memory
	 * resource group later.
	 */
	groupid = HeapTupleGetOid(tuple);

	/* cannot DROP default resource groups  */
	if (groupid == DEFAULTRESGROUP_OID || groupid == ADMINRESGROUP_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop default resource group \"%s\"",
						stmt->name)));

	/*
	 * Check to see if any roles are in this resource group.
	 */
	authIdRel = heap_open(AuthIdRelationId, RowExclusiveLock);
	ScanKeyInit(&authid_scankey,
				Anum_pg_authid_rolresgroup,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupid));

	authid_scan = systable_beginscan(authIdRel, AuthIdRolResGroupIndexId, true,
									 SnapshotNow, 1, &authid_scankey);

	if (HeapTupleIsValid(systable_getnext(authid_scan)))
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("resource group \"%s\" is used by at least one role",
						stmt->name)));

	systable_endscan(authid_scan);
	heap_close(authIdRel, RowExclusiveLock);


	/*
	 * Delete the resource group from the catalog.
	 */
	simple_heap_delete(pg_resgroup_rel, &tuple->t_self);
	systable_endscan(sscan);
	heap_close(pg_resgroup_rel, NoLock);

	/* drop the extended attributes for this resource group */
	deleteResgroupCapabilities(groupid);

	/*
	 * Remove any comments on this resource group
	 */
	DeleteSharedComments(groupid, ResGroupRelationId);

	/* metadata tracking */
	MetaTrackDropObject(ResGroupRelationId, groupid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									NIL, /* FIXME */
									NULL);
	}

	if (IsResGroupEnabled())
	{
		ResGroupCheckForDrop(groupid, stmt->name);

		/* Argument of callback function should be allocated in heap region */
		callbackArg = (Oid *)MemoryContextAlloc(TopMemoryContext, sizeof(Oid));
		*callbackArg = groupid;
		registerResourceGroupCallback(dropResGroupAbortCallback, (void *)callbackArg);
	}
}

/*
 * ALTER RESOURCE GROUP
 */
void
AlterResourceGroup(AlterResourceGroupStmt *stmt)
{
	Relation	pg_resgroup_rel;
	Relation	resgroup_capability_rel;
	HeapTuple	tuple;
	ScanKeyData	scankey;
	SysScanDesc	sscan;
	Oid			groupid;
	ResourceGroupAlterCallbackContext *callbackCtx;
	char		valueStr[16];
	char		proposedStr[16];
	int			concurrency;
	int			concurrencyVal;
	int			concurrencyProposed;
	int			newConcurrency;
	int			cpuRateLimitVal;
	int			cpuRateLimitNew;
	DefElem		*defel;
	int			limitType;
	bool		needDispatch = true;

	/* Permission check - only superuser can alter resource groups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to alter resource groups")));

	/* Subtransaction is not supported for resource group related operations */
	if (IsSubTransaction())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("ALTER RESOURCE GROUP cannot run inside a subtransaction")));

	/* Currently we only support to ALTER one limit at one time */
	Assert(list_length(stmt->options) == 1);
	defel = (DefElem *) lfirst(list_head(stmt->options));

	limitType = getResgroupOptionType(defel->defname);

	switch (limitType)
	{
		case RESGROUP_LIMIT_TYPE_CONCURRENCY:
			concurrency = defGetInt64(defel);
			if (concurrency < RESGROUP_MIN_CONCURRENCY)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_LIMIT_VALUE),
						 errmsg("concurrency limit cannot be less than %d",
								RESGROUP_MIN_CONCURRENCY)));
			if (concurrency > RESGROUP_MAX_CONCURRENCY)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_LIMIT_VALUE),
						 errmsg("concurrency limit cannot be greater than 'max_connections'")));
			break;

		case RESGROUP_LIMIT_TYPE_CPU:
			cpuRateLimitNew = defGetNumeric(defel);
			if (cpuRateLimitNew < RESGROUP_MIN_CPU_RATE_LIMIT)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_LIMIT_VALUE),
						 errmsg("cpu rate limit must be greater than %d",
								RESGROUP_MIN_CPU_RATE_LIMIT)));
			if (cpuRateLimitNew > RESGROUP_MAX_CPU_RATE_LIMIT)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_LIMIT_VALUE),
						 errmsg("cpu rate limit must be less than %d",
								RESGROUP_MAX_CPU_RATE_LIMIT)));
			/* overall limit will be verified later after groupid is known */
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported resource group limit type '%s'", defel->defname)));
	}

	/*
	 * Check the pg_resgroup relation to be certain the resource group already
	 * exists.
	 */
	pg_resgroup_rel = heap_open(ResGroupRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));

	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, true,
							   SnapshotNow, 1, &scankey);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("resource group \"%s\" does not exist",
						stmt->name)));

	groupid = HeapTupleGetOid(tuple);
	systable_endscan(sscan);
	heap_close(pg_resgroup_rel, NoLock);

	/* Argument of callback function should be allocated in heap region */
	callbackCtx = (ResourceGroupAlterCallbackContext *)
		MemoryContextAlloc(TopMemoryContext, sizeof(*callbackCtx));
	callbackCtx->groupid = groupid;
	callbackCtx->limittype = limitType;

	switch (limitType)
	{
		case RESGROUP_LIMIT_TYPE_CONCURRENCY:
			GetConcurrencyForResGroup(groupid,
									  &concurrencyVal,
									  &concurrencyProposed);
			newConcurrency = CalcConcurrencyValue(groupid,
												  concurrencyVal,
												  concurrencyProposed,
												  concurrency);


			snprintf(valueStr, sizeof(valueStr), "%d", newConcurrency);
			snprintf(proposedStr, sizeof(proposedStr), "%d", concurrency);
			updateResgroupCapabilityEntry(groupid, limitType, valueStr, proposedStr);

			callbackCtx->value.i = newConcurrency;
			callbackCtx->proposed.i = concurrency;

			needDispatch = true;
			break;

		case RESGROUP_LIMIT_TYPE_CPU:
			cpuRateLimitVal = GetCpuRateLimitForResGroup(groupid);

			if (cpuRateLimitVal < cpuRateLimitNew)
			{
				ResourceGroupOptions options;

				options.concurrency = 0;
				options.cpuRateLimit = cpuRateLimitNew;
				options.memoryLimit = 0;

				/*
				 * In validateCapabilities() we scan all the resource groups
				 * to check whether the total cpu_rate_limit exceed 100 or not.
				 * We need to use ExclusiveLock here to prevent concurrent
				 * increase on different resource group.
				 */
				resgroup_capability_rel = heap_open(ResGroupCapabilityRelationId,
													ExclusiveLock);
				validateCapabilities(resgroup_capability_rel,
									 groupid, &options, false);
				heap_close(resgroup_capability_rel, NoLock);
			}

			snprintf(valueStr, sizeof(valueStr), "%d", cpuRateLimitNew);
			snprintf(proposedStr, sizeof(proposedStr), "%d", cpuRateLimitNew);
			updateResgroupCapabilityEntry(groupid, limitType,
										  valueStr, proposedStr);

			callbackCtx->value.f = cpuRateLimitNew;
			callbackCtx->proposed.f = cpuRateLimitNew;
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported resource group limit type '%s'", defel->defname)));
	}

	if (needDispatch && Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(), /* FIXME */
									NULL);
	}

	/* Bump command counter to make this change visible in the callback function alterResGroupCommitCallback() */
	CommandCounterIncrement();

	if (IsResGroupEnabled())
	{
		registerResourceGroupCallback(alterResGroupCommitCallback, (void *)callbackCtx);
	}
}

/*
 * Get 'concurrency' of one resource group in pg_resgroupcapability.
 */
void
GetConcurrencyForResGroup(int groupId, int *value, int *proposed)
{
	char *valueStr;
	char *proposedStr;

	getResgroupCapabilityEntry(groupId, RESGROUP_LIMIT_TYPE_CONCURRENCY, &valueStr, &proposedStr);

	if (value != NULL)
		*value = pg_atoi(valueStr, sizeof(int32), 0);

	if (proposed != NULL)
		*proposed = pg_atoi(proposedStr, sizeof(int32), 0);
}

/*
 * Get 'cpu_rate_limit' of one resource group in pg_resgroupcapability.
 */
int
GetCpuRateLimitForResGroup(int groupId)
{
	char *valueStr;
	char *proposedStr;

	getResgroupCapabilityEntry(groupId, RESGROUP_LIMIT_TYPE_CPU,
							   &valueStr, &proposedStr);

	return str2Int(valueStr, "cpu_rate_limit");
}

/*
 * Get memory capabilities of one resource group in pg_resgroupcapability.
 * TODO: scan the catalog table only once?
 */
void
GetMemoryCapabilitiesForResGroup(int groupId, int *memoryLimit, int *sharedQuota, int *spillRatio)
{
	char *valueStr;
	char *proposedStr;

	getResgroupCapabilityEntry(groupId, RESGROUP_LIMIT_TYPE_MEMORY,
							   &valueStr, &proposedStr);
	*memoryLimit = str2Int(valueStr, "memory_limit");

	getResgroupCapabilityEntry(groupId, RESGROUP_LIMIT_TYPE_MEMORY_SHARED_QUOTA,
							   &valueStr, &proposedStr);
	*sharedQuota = str2Int(valueStr, "memory_shared_quota");

	getResgroupCapabilityEntry(groupId, RESGROUP_LIMIT_TYPE_MEMORY_SPILL_RATIO,
							   &valueStr, &proposedStr);
	*spillRatio = str2Int(valueStr, "memory_spill_ratio");
}

/*
 * Get resource group id for a role in pg_authid
 */
Oid
GetResGroupIdForRole(Oid roleid)
{
	HeapTuple	tuple;
	ResourceOwner owner = NULL;
	Oid			groupId;
	Relation	rel;
	ScanKeyData	key;
	SysScanDesc	 sscan;

	/*
	 * to cave the code of cache part, we provide a resource owner here if no
	 * existing
	 */
	if (CurrentResourceOwner == NULL)
	{
		owner = ResourceOwnerCreate(NULL, "GetResGroupIdForRole");
		CurrentResourceOwner = owner;
	}

	rel = heap_open(AuthIdRelationId, AccessShareLock);

	ScanKeyInit(&key,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(roleid));

	sscan = systable_beginscan(rel, AuthIdOidIndexId, true, SnapshotNow, 1, &key);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
	{
		systable_endscan(sscan);
		heap_close(rel, AccessShareLock);

		/*
		 * Role should have been dropped by other backends in this case, so this
		 * session cannot execute any command anymore
		 */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("Role with Oid %d was dropped", roleid),
				errhint("Cannot execute commands anymore, please terminate this session.")));
	}

	/* must access tuple before systable_endscan */
	groupId = DatumGetObjectId(heap_getattr(tuple, Anum_pg_authid_rolresgroup, rel->rd_att, NULL));

	systable_endscan(sscan);

	/*
	 * release lock here to guarantee we have no lock held when acquiring
	 * resource group slot
	 */
	heap_close(rel, AccessShareLock);

	if (!OidIsValid(groupId))
		groupId = InvalidOid;

	if (owner)
	{
		CurrentResourceOwner = NULL;
		ResourceOwnerDelete(owner);
	}

	return groupId;
}

/*
 * Get the option type from a name string.
 *
 * @param defname  the name string
 *
 * @return the option type or UNKNOWN if the name is unknown
 */
static int
getResgroupOptionType(const char* defname)
{
	if (strcmp(defname, "cpu_rate_limit") == 0)
		return RESGROUP_LIMIT_TYPE_CPU;
	else if (strcmp(defname, "memory_limit") == 0)
		return RESGROUP_LIMIT_TYPE_MEMORY;
	else if (strcmp(defname, "concurrency") == 0)
		return RESGROUP_LIMIT_TYPE_CONCURRENCY;
	else if (strcmp(defname, "memory_shared_quota") == 0)
		return RESGROUP_LIMIT_TYPE_MEMORY_SHARED_QUOTA;
	else if (strcmp(defname, "memory_spill_ratio") == 0)
		return RESGROUP_LIMIT_TYPE_MEMORY_SPILL_RATIO;
	else
		return RESGROUP_LIMIT_TYPE_UNKNOWN;
}

/*
 * Parse a statement and store the settings in options.
 *
 * @param stmt     the statement
 * @param options  used to store the settings
 */
static void
parseStmtOptions(CreateResourceGroupStmt *stmt, ResourceGroupOptions *options)
{
	ListCell *cell;
	int types = 0;

	foreach(cell, stmt->options)
	{
		DefElem *defel = (DefElem *) lfirst(cell);
		int type = getResgroupOptionType(defel->defname);

		if (type == RESGROUP_LIMIT_TYPE_UNKNOWN)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" not recognized", defel->defname)));

		if (types & (1 << type))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("Find duplicate resoure group resource type: %s",
						   defel->defname)));
		else
			types |= 1 << type;

		switch (type)
		{
			case RESGROUP_LIMIT_TYPE_CONCURRENCY:
				options->concurrency = defGetInt64(defel);
				if (options->concurrency < RESGROUP_MIN_CONCURRENCY ||
					options->concurrency > RESGROUP_MAX_CONCURRENCY)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("concurrency range is [%d, 'max_connections']",
								   RESGROUP_MIN_CONCURRENCY)));
				break;

			case RESGROUP_LIMIT_TYPE_CPU:
				options->cpuRateLimit = defGetNumeric(defel);
				if (options->cpuRateLimit < RESGROUP_MIN_CPU_RATE_LIMIT ||
					options->cpuRateLimit > RESGROUP_MAX_CPU_RATE_LIMIT)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cpu_rate_limit range is [%d, %d]",
								   RESGROUP_MIN_CPU_RATE_LIMIT,
								   RESGROUP_MAX_CPU_RATE_LIMIT)));
				break;

			case RESGROUP_LIMIT_TYPE_MEMORY:
				options->memoryLimit = defGetNumeric(defel);
				if (options->memoryLimit < RESGROUP_MIN_MEMORY_LIMIT ||
					options->memoryLimit > RESGROUP_MAX_MEMORY_LIMIT)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("memory_limit range is [%d, %d]",
								   RESGROUP_MIN_MEMORY_LIMIT,
								   RESGROUP_MAX_MEMORY_LIMIT)));
				break;

			case RESGROUP_LIMIT_TYPE_MEMORY_SHARED_QUOTA:
				options->memSharedQuota = defGetNumeric(defel);
				if (options->memSharedQuota < RESGROUP_MIN_MEMORY_SHARED_QUOTA ||
					options->memSharedQuota > RESGROUP_MAX_MEMORY_SHARED_QUOTA)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("memory_shared_quota range is [%d, %d]",
								   RESGROUP_MIN_MEMORY_SHARED_QUOTA,
								   RESGROUP_MAX_MEMORY_SHARED_QUOTA)));
				break;

			case RESGROUP_LIMIT_TYPE_MEMORY_SPILL_RATIO:
				options->memSpillRatio = defGetNumeric(defel);
				if (options->memSpillRatio < RESGROUP_MIN_MEMORY_SPILL_RATIO ||
					options->memSpillRatio > RESGROUP_MAX_MEMORY_SPILL_RATIO)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("memory_spill_ratio range is [%d, %d]",
								   RESGROUP_MIN_MEMORY_SPILL_RATIO,
								   RESGROUP_MAX_MEMORY_SPILL_RATIO)));
				break;

			default:
				Assert(!"unexpected options");
				break;
		}
	}

	if (options->memoryLimit == 0 || options->cpuRateLimit == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("must specify both memory_limit and cpu_rate_limit")));

	if (!(types & (1 << RESGROUP_LIMIT_TYPE_CONCURRENCY)))
		options->concurrency = RESGROUP_DEFAULT_CONCURRENCY;

	if (!(types & (1 << RESGROUP_LIMIT_TYPE_MEMORY_SHARED_QUOTA)))
		options->memSharedQuota = RESGROUP_DEFAULT_MEM_SHARED_QUOTA;

	if (!(types & (1 << RESGROUP_LIMIT_TYPE_MEMORY_SPILL_RATIO)))
		options->memSpillRatio = RESGROUP_DEFAULT_MEM_SPILL_RATIO;

	if (options->memSpillRatio + options->memSharedQuota > RESGROUP_MAX_MEMORY_LIMIT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("The sum of memory_shared_quota (%d) and memory_spill_ratio (%d) exceeds %d",
						options->memSharedQuota, options->memSpillRatio,
						RESGROUP_MAX_MEMORY_LIMIT)));
}

/*
 * Resource group call back function
 *
 * Remove resource group entry in shared memory when abort transaction which
 * creates resource groups
 */
static void
createResGroupAbortCallback(bool isCommit, void *arg)
{
	Oid groupId;

	if (!isCommit)
	{
		groupId = *(Oid *)arg;

		/*
		 * FreeResGroupEntry would acquire LWLock, since this callback is called
		 * after LWLockReleaseAll in AbortTransaction, it is safe here
		 */
		FreeResGroupEntry(groupId);

		/* remove the os dependent part for this resource group */
		ResGroupOps_DestroyGroup(groupId);
	}

	pfree(arg);
}

/*
 * Resource group call back function
 *
 * Remove resource group entry in shared memory when commit transaction which
 * drops resource groups
 */
static void
dropResGroupAbortCallback(bool isCommit, void *arg)
{
	Oid groupId;

	groupId = *(Oid *)arg;
	ResGroupDropCheckForWakeup(groupId, isCommit);

	if (isCommit)
	{
		/* remove the os dependent part for this resource group */
		ResGroupOps_DestroyGroup(groupId);
	}

	pfree(arg);
}

/*
 * Resource group call back function
 *
 * When ALTER RESOURCE GROUP SET CONCURRENCY commits, some queueing
 * transaction of this resource group may need to be woke up.
 *
 */
static void
alterResGroupCommitCallback(bool isCommit, void *arg)
{
	volatile int savedInterruptHoldoffCount;
	ResourceGroupAlterCallbackContext *ctx =
		(ResourceGroupAlterCallbackContext *) arg;

	if (!isCommit)
	{
		pfree(arg);
		return;
	}

	switch (ctx->limittype)
	{
		case RESGROUP_LIMIT_TYPE_CONCURRENCY:
			/* wake up */
			ResGroupAlterCheckForWakeup(ctx->groupid,
										ctx->value.i,
										ctx->proposed.i);
			break;

		case RESGROUP_LIMIT_TYPE_CPU:
			/*
			 * Apply the cpu rate limit to cgroup.
			 *
			 * This operation can fail in some cases, e.g.:
			 * 1. BEGIN;
			 * 2. CREATE RESOURCE GROUP g1 ...;
			 * 3. ALTER RESOURCE GROUP g1 SET CPU_RATE_LIMIT ...;
			 * 4. DROP RESOURCE GROUP g1;
			 * 5. COMMIT; -- or ABORT;
			 *
			 * So the error needs to be catched here.
			 */
			PG_TRY();
			{
				savedInterruptHoldoffCount = InterruptHoldoffCount;
				ResGroupOps_SetCpuRateLimit(ctx->groupid, ctx->value.f);
			}
			PG_CATCH();
			{
				InterruptHoldoffCount = savedInterruptHoldoffCount;
				elog(LOG, "Fail to set cpu_rate_limit for resource group %d", ctx->groupid);
			}
			PG_END_TRY();
			break;

		default:
			break;
	}

	pfree(arg);
}

/*
 * Catalog access functions
 */

/*
 * Insert all the capabilities to the capability table.
 *
 * We store the capabilities in multiple lines for one group,
 * so we have to insert them one by one. This function will
 * handle the type conversion etc..
 *
 * @param groupid  oid of the resource group
 * @param options  the capabilities
 */
static void
insertResgroupCapabilities(Oid groupid,
						   ResourceGroupOptions *options)
{
	char value[64];
	Relation resgroup_capability_rel = heap_open(ResGroupCapabilityRelationId, RowExclusiveLock);

	validateCapabilities(resgroup_capability_rel, groupid, options, true);

	sprintf(value, "%d", options->concurrency);
	insertResgroupCapabilityEntry(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_CONCURRENCY, value);

	sprintf(value, "%d", options->cpuRateLimit);
	insertResgroupCapabilityEntry(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_CPU, value);

	sprintf(value, "%d", options->memoryLimit);
	insertResgroupCapabilityEntry(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_MEMORY, value);

	sprintf(value, "%d", options->memSharedQuota);
	insertResgroupCapabilityEntry(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_MEMORY_SHARED_QUOTA, value);

	sprintf(value, "%d", options->memSpillRatio);
	insertResgroupCapabilityEntry(resgroup_capability_rel, groupid, RESGROUP_LIMIT_TYPE_MEMORY_SPILL_RATIO, value);

	heap_close(resgroup_capability_rel, NoLock);
}

/*
 * Update an entry in pg_resgroupcapability
 *
 * groupid and type are the update key, value and proposed are the update value.
 */
static void
updateResgroupCapabilityEntry(Oid groupid, uint16 type, char *value, char *proposed)
{
	HeapTuple	oldTuple;
	HeapTuple	newTuple;
	SysScanDesc	sscan;
	ScanKeyData	scankey[2];
	Datum		values[Natts_pg_resgroupcapability];
	bool		isnull[Natts_pg_resgroupcapability];
	bool		repl[Natts_pg_resgroupcapability];

	Relation resgroupCapabilityRel = heap_open(ResGroupCapabilityRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey[0],
				Anum_pg_resgroupcapability_resgroupid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupid));

	ScanKeyInit(&scankey[1],
				Anum_pg_resgroupcapability_reslimittype,
				BTEqualStrategyNumber, F_INT2EQ,
				UInt16GetDatum(type));

	sscan = systable_beginscan(resgroupCapabilityRel, ResGroupCapabilityResgroupidResLimittypeIndexId, true,
							   SnapshotNow, 2, scankey);

	if (!HeapTupleIsValid(oldTuple = systable_getnext(sscan)))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("capability missing for resource group")));

	MemSet(isnull, 0, sizeof(bool) * Natts_pg_resgroupcapability);
	MemSet(repl, 0, sizeof(bool) * Natts_pg_resgroupcapability);

	if (value != NULL)
	{
		values[Anum_pg_resgroupcapability_value - 1] = CStringGetTextDatum(value);
		isnull[Anum_pg_resgroupcapability_value - 1] = false;
		repl[Anum_pg_resgroupcapability_value - 1]  = true;
	}

	if (proposed != NULL)
	{
		values[Anum_pg_resgroupcapability_proposed - 1] = CStringGetTextDatum(proposed);
		isnull[Anum_pg_resgroupcapability_proposed - 1] = false;
		repl[Anum_pg_resgroupcapability_proposed - 1]  = true;
	}

	newTuple = heap_modify_tuple(oldTuple, RelationGetDescr(resgroupCapabilityRel),
								  values, isnull, repl);

	simple_heap_update(resgroupCapabilityRel, &oldTuple->t_self, newTuple);
	CatalogUpdateIndexes(resgroupCapabilityRel, newTuple);

	systable_endscan(sscan);
	heap_close(resgroupCapabilityRel, NoLock);
}

/*
 * Validate the capabilities.
 *
 * The policy is resouces can't be over used, take memory for example,
 * all the allocated memory can not exceed 100.
 *
 * Also detect for duplicate settings for the group.
 *
 * @param rel      the relation
 * @param groupid  oid of the resource group
 * @param options  the options for the resource group
 */
static void
validateCapabilities(Relation rel,
					 Oid groupid,
					 ResourceGroupOptions *options,
					 bool newGroup)
{
	HeapTuple tuple;
	SysScanDesc sscan;
	int totalCpu = options->cpuRateLimit;
	int totalMem = options->memoryLimit;

	sscan = systable_beginscan(rel, InvalidOid, false, SnapshotNow, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		Form_pg_resgroupcapability resgCapability =
						(Form_pg_resgroupcapability)GETSTRUCT(tuple);

		if (resgCapability->resgroupid == groupid)
		{
			if (!newGroup)
				continue;

			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("Find duplicate resoure group id:%d", groupid)));
		}

		if (resgCapability->reslimittype == RESGROUP_LIMIT_TYPE_CPU)
		{
			totalCpu += text2Int(&resgCapability->value, "cpu_rate_limit");
			if (totalCpu > RESGROUP_MAX_CPU_RATE_LIMIT)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("total cpu_rate_limit exceeded the limit of %d",
							   RESGROUP_MAX_CPU_RATE_LIMIT)));
		}
		else if (resgCapability->reslimittype == RESGROUP_LIMIT_TYPE_MEMORY)
		{
			totalMem += text2Int(&resgCapability->value, "memory_limit");
			if (totalMem > RESGROUP_MAX_MEMORY_LIMIT)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("total memory_limit exceeded the limit of %d",
							   RESGROUP_MAX_MEMORY_LIMIT)));
		}
	}

	systable_endscan(sscan);
}

/*
 * Insert one capability to the capability table.
 *
 * 'value' and 'proposed' are both used to describe a resource,
 * in this routine we assume 'proposed' has the same value as 'value'.
 *
 * @param rel      the relation
 * @param groupid  oid of the resource group
 * @param type     the resource limit type
 * @param value    the limit value
 */
static void
insertResgroupCapabilityEntry(Relation rel,
							 Oid groupid,
							 uint16 type,
							 char *value)
{
	Datum new_record[Natts_pg_resgroupcapability];
	bool new_record_nulls[Natts_pg_resgroupcapability];
	HeapTuple tuple;
	TupleDesc tupleDesc = RelationGetDescr(rel);

	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_resgroupcapability_resgroupid - 1] = ObjectIdGetDatum(groupid);
	new_record[Anum_pg_resgroupcapability_reslimittype - 1] = UInt16GetDatum(type);
	new_record[Anum_pg_resgroupcapability_value - 1] = CStringGetTextDatum(value);
	new_record[Anum_pg_resgroupcapability_proposed - 1] = CStringGetTextDatum(value);

	tuple = heap_form_tuple(tupleDesc, new_record, new_record_nulls);
	simple_heap_insert(rel, tuple);
	CatalogUpdateIndexes(rel, tuple);
}

/*
 * Retrive capability value from pg_resgroupcapability
 */
static void
getResgroupCapabilityEntry(int groupId, int type, char **value, char **proposed)
{
	SysScanDesc	sscan;
	ScanKeyData	key[2];
	HeapTuple	tuple;
	bool isNull;
	Datum valueDatum;
	Datum proposedDatum;
	Relation	relResGroupCapability;
	ResourceOwner owner = NULL;

	if (CurrentResourceOwner == NULL)
	{
		owner = ResourceOwnerCreate(NULL, "getResgroupCapabilityEntry");
		CurrentResourceOwner = owner;
	}

	relResGroupCapability = heap_open(ResGroupCapabilityRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_resgroupcapability_resgroupid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupId));
	ScanKeyInit(&key[1],
				Anum_pg_resgroupcapability_reslimittype,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(type));

	sscan = systable_beginscan(relResGroupCapability,
							   ResGroupCapabilityResgroupidResLimittypeIndexId,
							   true,
							   SnapshotNow, 2, key);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
	{
		systable_endscan(sscan);
		heap_close(relResGroupCapability, AccessShareLock);

		if (owner)
		{
			CurrentResourceOwner = NULL;
			ResourceOwnerDelete(owner);
		}

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Cannot find capability for group: %d and type: %d", groupId, type)));
	}

	valueDatum = heap_getattr(tuple, Anum_pg_resgroupcapability_value, relResGroupCapability->rd_att, &isNull);
	*value = DatumGetCString(DirectFunctionCall1(textout, valueDatum));

	proposedDatum = heap_getattr(tuple, Anum_pg_resgroupcapability_proposed, relResGroupCapability->rd_att, &isNull);
	*proposed = DatumGetCString(DirectFunctionCall1(textout, proposedDatum));

	systable_endscan(sscan);

	/*
	 * release lock here to guarantee we have no lock held when acquiring
	 * resource group slot
	 */
	heap_close(relResGroupCapability, AccessShareLock);

	if (owner)
	{
		CurrentResourceOwner = NULL;
		ResourceOwnerDelete(owner);
	}
}

/*
 * Delete capability entries of one resource group.
 */
static void
deleteResgroupCapabilities(Oid groupid)
{
	Relation	 resgroup_capability_rel;
	HeapTuple	 tuple;
	ScanKeyData	 scankey;
	SysScanDesc	 sscan;

	resgroup_capability_rel = heap_open(ResGroupCapabilityRelationId,
										RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_resgroupcapability_resgroupid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupid));

	sscan = systable_beginscan(resgroup_capability_rel,
							   ResGroupCapabilityResgroupidIndexId,
							   true, SnapshotNow, 1, &scankey);

	while ((tuple = systable_getnext(sscan)) != NULL)
		simple_heap_delete(resgroup_capability_rel, &tuple->t_self);

	systable_endscan(sscan);

	heap_close(resgroup_capability_rel, NoLock);
}

/*
 * GetResGroupIdForName -- Return the Oid for a resource group name
 *
 * Notes:
 *	Used by the various admin commands to convert a user supplied group name
 *	to Oid.
 */
Oid
GetResGroupIdForName(char *name, LOCKMODE lockmode)
{
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid			rsgid;

	rel = heap_open(ResGroupRelationId, lockmode);

	/* SELECT oid FROM pg_resgroup WHERE rsgname = :1 */
	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));
	scan = systable_beginscan(rel, ResGroupRsgnameIndexId, true,
							  SnapshotNow, 1, &scankey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
		rsgid = HeapTupleGetOid(tuple);
	else
		rsgid = InvalidOid;

	systable_endscan(scan);
	heap_close(rel, lockmode);

	return rsgid;
}

/*
 * GetResGroupNameForId -- Return the resource group name for an Oid
 */
char *
GetResGroupNameForId(Oid oid, LOCKMODE lockmode)
{
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tuple;
	char		*name = NULL;

	rel = heap_open(ResGroupRelationId, lockmode);

	/* SELECT rsgname FROM pg_resgroup WHERE oid = :1 */
	ScanKeyInit(&scankey,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(oid));
	scan = systable_beginscan(rel, ResGroupOidIndexId, true,
							  SnapshotNow, 1, &scankey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		bool isnull;
		Datum nameDatum = heap_getattr(tuple, Anum_pg_resgroup_rsgname, rel->rd_att, &isnull);
		Assert (!isnull);
		Name resGroupName = DatumGetName(nameDatum);
		name = pstrdup(NameStr(*resGroupName));
	}

	systable_endscan(scan);
	heap_close(rel, lockmode);

	return name;
}

/*
 * Convert a C str to a integer value.
 *
 * @param str   the C str
 * @param prop  the property name
 */
static int
str2Int(const char *str, const char *prop)
{
	char *end = NULL;
	double val = strtod(str, &end);

	/* both the property name and value are already checked
	 * by the syntax parser, but we'll check it again anyway for safe. */
	if (end == NULL || end == str || *end != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("%s requires a numeric value", prop)));

	return floor(val);
}

/*
 * Convert a text to a integer value.
 *
 * @param text  the text
 * @param prop  the property name
 */
static int
text2Int(const text *text, const char *prop)
{
	char *str = DatumGetCString(DirectFunctionCall1(textout,
								 PointerGetDatum(text)));

	return str2Int(str, prop);
}
