/*-------------------------------------------------------------------------
 *
 * cdbgang.c
 *	  Query Executor Factory for gangs of QEs
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>				/* getpid() */
#include <pthread.h>
#include <limits.h>

#include "gp-libpq-fe.h"
#include "miscadmin.h"			/* MyDatabaseId */
#include "storage/proc.h"		/* MyProc */
#include "storage/ipc.h"
#include "utils/memutils.h"

#include "catalog/namespace.h"
#include "commands/variable.h"
#include "nodes/execnodes.h"	/* CdbProcess, Slice, SliceTable */
#include "postmaster/postmaster.h"
#include "tcop/tcopprot.h"
#include "utils/portal.h"
#include "utils/sharedsnapshot.h"
#include "tcop/pquery.h"

#include "cdb/cdbconn.h"		/* SegmentDatabaseDescriptor */
#include "cdb/cdbfts.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbgang.h"		/* me */
#include "cdb/cdbtm.h"			/* discardDtxTransaction() */
#include "cdb/cdbutil.h"		/* CdbComponentDatabaseInfo */
#include "cdb/cdbvars.h"		/* Gp_role, etc. */
#include "storage/bfz.h"
#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"
#include "libpq/libpq-be.h"
#include "libpq/ip.h"

#include "utils/guc_tables.h"

#define LOG_GANG_DEBUG(...) do { \
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG) elog(__VA_ARGS__); \
    } while(false);

#define MAX_CACHED_1_GANGS 1
/*
 *	thread_DoConnect is the thread proc used to perform the connection to one of the qExecs.
 */
static void *thread_DoConnect(void *arg);
static void build_gpqeid_param(char *buf, int bufsz, int segIndex, bool is_writer);

static Gang *createGang(GangType type, int gang_id, int size, int content, char *portal_name);

static void disconnectAndDestroyGang(Gang *gp);
static void disconnectAndDestroyAllReaderGangs(bool destroyAllocated);

static Gang *buildGangDefinition(GangType type, int gang_id, int size, int content, char *portal_name);

static bool isTargetPortal(const char *p1, const char *p2);

static void	addOptions(StringInfo string, bool iswriter, bool i_am_superuser);

static bool cleanupGang(Gang * gp);

extern void resetSessionForPrimaryGangLoss(void);

static const char* gangTypeToString(GangType);
static CdbComponentDatabaseInfo *copyCdbComponentDatabaseInfo(CdbComponentDatabaseInfo *dbInfo);
static CdbComponentDatabaseInfo *findDatabaseInfoBySegIndex(
		CdbComponentDatabases *cdbs, int segIndex);
static void addGangToAvailable(Gang *gp);
static Gang *getAvailableGang(GangType type, int size, int content, char *portalName);

/*
 * Points to the result of getCdbComponentDatabases()
 */
static CdbComponentDatabases *cdb_component_dbs = NULL;

static int	largest_gangsize = 0;

static MemoryContext GangContext = NULL;

/*
 * Parameter structure for the DoConnect threads
 */
typedef struct DoConnectParms
{
	/*
	 * db_count: The number of segdbs that this thread is responsible for
	 * connecting to.
	 * Equals the count of segdbDescPtrArray below.
	 */
	int			db_count;

	/*
	 * segdbDescPtrArray: Array of SegmentDatabaseDescriptor* 's that this thread is
	 * responsible for connecting to. Has size equal to db_count.
	 */
	SegmentDatabaseDescriptor **segdbDescPtrArray;

	/* type of gang. */
	GangType	type;

	bool		i_am_superuser;

	StringInfo  connectOptions;

	/*
	 * The pthread_t thread handle.
	 */
	pthread_t	thread;
}	DoConnectParms;


static DoConnectParms* makeConnectParms(int parmsCount, GangType type);
static void destroyConnectParms(DoConnectParms *doConnectParmsAr, int count);

int
largestGangsize(void)
{
	return largest_gangsize;
}

static bool
segment_failure_due_to_recovery(SegmentDatabaseDescriptor *segdbDesc)
{
	char *fatal=NULL, *message=NULL, *ptr=NULL;
	int fatal_len=0;

	if (segdbDesc == NULL)
		return false;

	message = segdbDesc->error_message.data;

	if (message == NULL)
		return false;

	fatal = _("FATAL");
	if (fatal == NULL)
		return false;

	fatal_len = strlen(fatal);

	/*
	 * it would be nice if we could check errcode for ERRCODE_CANNOT_CONNECT_NOW, instead
	 * we wind up looking for at the strings.
	 *
	 * And because if LC_MESSAGES gets set to something which changes
	 * the strings a lot we have to take extreme care with looking at
	 * the string.
	 */
	ptr = strstr(message, fatal);
	if ((ptr != NULL) && ptr[fatal_len] == ':')
	{
		if (strstr(message, _(POSTMASTER_IN_STARTUP_MSG)))
		{
			return true;
		}
		if (strstr(message, _(POSTMASTER_IN_RECOVERY_MSG)))
		{
			return true;
		}
		/* We could do retries for "sorry, too many clients already" here too */
	}

	return false;
}

/*
 * Every gang created must have a unique identifier, so the QD and Dispatch Agents can agree
 * about what they are talking about.
 *
 * Since there can only be one primary writer gang, and only one mirror writer gang, we
 * use id 1 and 2 for those two (helps in debugging).
 *
 * Reader gang ids start at 3
 */
#define PRIMARY_WRITER_GANG_ID 1
static int	gang_id_counter = 2;

static DoConnectParms* makeConnectParms(int parmsCount, GangType type)
{
	DoConnectParms *doConnectParmsAr = (DoConnectParms*) palloc0(
			parmsCount * sizeof(DoConnectParms));
	DoConnectParms* pParms = NULL;
	StringInfo pOptions = makeStringInfo();
	bool i_am_superuser = superuser_arg(MyProc->roleId);
	int i = 0;

	addOptions(pOptions, type == GANGTYPE_PRIMARY_WRITER, i_am_superuser);

	for (i = 0; i < parmsCount; i++)
	{
		pParms = &doConnectParmsAr[i];
		pParms->segdbDescPtrArray = (SegmentDatabaseDescriptor**) palloc0(
				(gp_connections_per_thread == 0 ?
						getgpsegmentCount() : gp_connections_per_thread)
						* sizeof(SegmentDatabaseDescriptor *));
		MemSet(&pParms->thread, 0, sizeof(pthread_t));
		pParms->db_count = 0;
		pParms->type = type;
		pParms->i_am_superuser = i_am_superuser;
		pParms->connectOptions = pOptions;
	}
	return doConnectParmsAr;
}

static void destroyConnectParms(DoConnectParms *doConnectParmsAr, int count)
{
	if (doConnectParmsAr != NULL)
	{
		int i = 0;
		for (i = 0; i < count; i++)
		{
			DoConnectParms *pParms = &doConnectParmsAr[i];
			StringInfo pOptions = pParms->connectOptions;
			if(pOptions->data != NULL)
			{
				pfree(pOptions->data);
				pOptions->data = NULL;
			}
			pParms->connectOptions = NULL;

			pfree(pParms->segdbDescPtrArray);
			pParms->segdbDescPtrArray = NULL;
		}

		pfree(doConnectParmsAr);
	}
}

static void checkConnectionStatus(Gang* gp,
		int* in_recovery_mode_count, int* successful_connections)
{
	/*
	 * In this loop, we check whether the connections were successful.
	 * If not, we recreate the error message with palloc and show it as
	 * a warning.
	 */
	SegmentDatabaseDescriptor* segdbDesc = NULL;
	CdbComponentDatabaseInfo* cdbInfo = NULL;
	int size = gp->size;
	int i = 0;

	for (i = 0; i < size; i++)
	{
		segdbDesc = &gp->db_descriptors[i];
		cdbInfo = segdbDesc->segment_database_info;
		/*
		 * check connection established or not, if not, we may have to
		 * re-build this gang.
		 */
		if (!segdbDesc->conn)
		{
			if (segdbDesc->whoami)
				elog(LOG, "Failed connection to %s", segdbDesc->whoami);

			/*
			 * Log failed connections.	Complete failures
			 * are taken care of later.
			 */
			ereport(LOG,
					(errcode(segdbDesc->errcode), errmsg("%s", segdbDesc->error_message.data)));
			insist_log(
					segdbDesc->errcode != 0
							|| segdbDesc->error_message.len != 0,
					"connection is null, but no error code or error message, for segDB %d",
					i);
			Assert(segdbDesc->errcode != 0);
			Assert(segdbDesc->error_message.len > 0);
			/* this connect failed -- but why ? */
			if (segment_failure_due_to_recovery(segdbDesc))
			{
				(*in_recovery_mode_count)++;
			}
			segdbDesc->errcode = 0;
			resetPQExpBuffer(&segdbDesc->error_message);
			gp->all_valid_segdbs_connected = false;
		}
		else
		{
			Assert(
					PQstatus(segdbDesc->conn) != CONNECTION_BAD
							&& segdbDesc->errcode == 0
							&& segdbDesc->error_message.len == 0);
			LOG_GANG_DEBUG(LOG,
					"Connected to %s motionListenerPorts=%d with options %s",
					segdbDesc->whoami, segdbDesc->motionListener,
					PQoptions(segdbDesc->conn));
			/*
			 * We should have retrieved the IP from our cache
			 */
			Assert(cdbInfo->hostip != NULL);

			/*
			 * We have a live connection!
			 */
			(*successful_connections)++;
		}
	}
}

/*
 * creates a new gang by logging on a session to each segDB involved
 *
 */
static Gang *
createGang(GangType type, int gang_id, int size, int content, char *portal_name)
{
	Gang	   *newGangDefinition;
	SegmentDatabaseDescriptor *segdbDesc = NULL;
	DoConnectParms *doConnectParmsAr = NULL;
	DoConnectParms *pParms = NULL;
	int parmIndex = 0;
	int			threadCount;
	int			i;
	Portal		portal;
	int			create_gang_retry_counter = 0;
	int			in_recovery_mode_count = 0;
	int			successful_connections = 0;

	/* check arguments */
	Assert(size == 1 || size == getgpsegmentCount());
	Assert(CurrentResourceOwner != NULL);
	Assert(CurrentMemoryContext == GangContext);
	if (type == GANGTYPE_PRIMARY_WRITER)
		Assert(gang_id == PRIMARY_WRITER_GANG_ID);
	Assert(gp_connections_per_thread >= 0);
	if (portal_name != 0)
	{
		/* Using a named portal */
		portal = GetPortalByName(portal_name);
		Assert(portal);
	}

	LOG_GANG_DEBUG(LOG,
			"createGang type = %d, gang_id %d, size %d, content %d, portal_name %s",
			type, gang_id, size, content, portal_name == NULL ? "" : portal_name);

	/* If we're in a retry, we may need to reset our initial state, a bit */
create_gang_retry:
	newGangDefinition = NULL;
	doConnectParmsAr = NULL;
	successful_connections = 0;
	in_recovery_mode_count = 0;
	threadCount = 0;

	/* allocate and initialize gang structure */
	newGangDefinition = buildGangDefinition(type, gang_id, size, content, portal_name);
	Assert(newGangDefinition != NULL);
	Assert(newGangDefinition->size == size);

	/*
	 * Loop through the segment_database_descriptors items inside newGangDefinition.
	 * For each segdb , attempt to get
	 * a connection to the Postgres instance for that database.
	 * If the connection fails, mark it (locally) as unavailable
	 * If no database serving a segindex is available, fail the user's connection request.
	 */

	if (size > largest_gangsize)
		largest_gangsize = size;

	/*
	 * The most threads we could have is segdb_count / gp_connections_per_thread, rounded up.
	 * This is equivalent to 1 + (segdb_count-1) / gp_connections_per_thread.
	 * We allocate enough memory for this many DoConnectParms structures,
	 * even though we may not use them all.
	 */
	if (gp_connections_per_thread == 0)
		threadCount = 1;
	else
		threadCount = 1 + (size - 1) / gp_connections_per_thread;
	Assert(threadCount > 0);

	doConnectParmsAr = makeConnectParms(threadCount, type);
	for (i = 0; i < size; i++)
	{
		segdbDesc = &newGangDefinition->db_descriptors[i];
		parmIndex = i / gp_connections_per_thread;
		pParms = &doConnectParmsAr[parmIndex];
		pParms->segdbDescPtrArray[pParms->db_count++] = segdbDesc;
	}

	newGangDefinition->active = true;

	for (i = 0; i < threadCount; i++)
	{
		LOG_GANG_DEBUG(LOG,
				"createGang creating thread %d of %d for libpq connections",
				i + 1, threadCount);

		int			pthread_err;
		pParms = &doConnectParmsAr[i];

		pthread_err = gp_pthread_create(&pParms->thread, thread_DoConnect, pParms, "createGang");

		if (pthread_err != 0)
		{
			int			j;

			/*
			 * Error during thread create (this should be caused by resource
			 * constraints). If we leave the threads running, they'll
			 * immediately have some problems -- so we need to join them, and
			 * *then* we can issue our FATAL error
			 */

			for (j = 0; j < i; j++)
			{
				DoConnectParms *pParms;

				pParms = &doConnectParmsAr[j];
				pthread_join(pParms->thread, NULL);
			}

			ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("failed to create thread %d of %d", i + 1, threadCount),
							errdetail("pthread_create() failed with err %d", pthread_err)));
		}
	}

	/*
	 * wait for all of the DoConnect threads to complete.
	 */
	for (i = 0; i < threadCount; i++)
	{
		LOG_GANG_DEBUG(LOG, "joining to thread %d of %d for libpq connections",
				i + 1, threadCount);

		DoConnectParms *pParms = &doConnectParmsAr[i];

		if (0 != pthread_join(pParms->thread, NULL))
		{
			elog(FATAL, "could not create segworker group");
		}
	}

	/*
	 * Free the memory allocated for the threadParms array
	 */
	destroyConnectParms(doConnectParmsAr, threadCount);
	doConnectParmsAr = NULL;

	newGangDefinition->active = false;

	/*
	 * For now, set all_valid_segdbs_connected to true.
	 * Reset to false if we get fail to connect to a valid segdb
	 */
	newGangDefinition->all_valid_segdbs_connected = true;

	checkConnectionStatus(newGangDefinition,
			&in_recovery_mode_count, &successful_connections);

	if (size != successful_connections)
	{
		/*
		 * Before we do anything rash, let's take a closer look at the
		 * connection which failed.
		 *
		 * We're going to see if *all* of the connect-failures we've got look
		 * like a simple-retry will help.
		 */
		LOG_GANG_DEBUG(LOG,
				"createGang: %d processes requested; %d successful connections %d in recovery",
				size, successful_connections, in_recovery_mode_count);


		if (successful_connections + in_recovery_mode_count == size
				&& gp_gang_creation_retry_count
				&& create_gang_retry_counter++ < gp_gang_creation_retry_count)
		{
			LOG_GANG_DEBUG(LOG, "createGang: gang creation failed, but retryable.");

			/*
			 * On the first retry, we want to verify that we are
			 * using the most current version of the
			 * configuration.
			 */
			if (create_gang_retry_counter == 0)
				FtsNotifyProber();

			/*
			 * MPP-10751: In case we're destroying a
			 * writer-gang, we don't want to cause a
			 * session-reset (and odd side-effect inside
			 * disconnectAndDestroyGang()) so let's pretend
			 * that this is a reader-gang.
			 */
			if (newGangDefinition->type == GANGTYPE_PRIMARY_WRITER)
			{
				disconnectAndDestroyAllReaderGangs(true);
				newGangDefinition->type = GANGTYPE_PRIMARY_READER;
			}

			disconnectAndDestroyGang(newGangDefinition); /* free up connections */
			newGangDefinition = NULL;

			CHECK_FOR_INTERRUPTS();

			pg_usleep(gp_gang_creation_retry_timer * 1000);

			CHECK_FOR_INTERRUPTS();

			goto create_gang_retry;
		}
		else if(!isFTSEnabled())
		{
			disconnectAndDestroyGang(newGangDefinition);
			newGangDefinition = NULL;
		}
		else
		{
			if (FtsTestSegmentDBIsDown(newGangDefinition->db_descriptors, size))
			{
				disconnectAndDestroyAllGangs();
			}
			else
			{
				disconnectAndDestroyGang(newGangDefinition);
			}

			newGangDefinition = NULL;
			CheckForResetSession();
		}

		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR), errmsg("failed to acquire resources on one or more segments")));
	}

	return newGangDefinition;
}

static const char* gangTypeToString(GangType type)
{
	const char *ret = "";
	switch (type)
	{
	case GANGTYPE_PRIMARY_WRITER:
		ret = "primary writer";
		break;
	case GANGTYPE_PRIMARY_READER:
		ret = "primary reader";
		break;
	case GANGTYPE_ENTRYDB_READER:
		ret = "entry db reader";
		break;
	case GANGTYPE_UNALLOCATED:
		ret = "unallocated";
		break;
	default:
		Assert(false);
	}
	return ret;
}

static CdbComponentDatabaseInfo *copyCdbComponentDatabaseInfo(
		CdbComponentDatabaseInfo *dbInfo)
{
	int i = 0;
	int size = sizeof(CdbComponentDatabaseInfo);
	CdbComponentDatabaseInfo *newInfo = palloc0(size);
	memcpy(newInfo, dbInfo, size);

	if (dbInfo->address)
		newInfo->address = pstrdup(dbInfo->address);

	if (dbInfo->hostname)
		newInfo->hostname = pstrdup(dbInfo->hostname);

	if (dbInfo->hostip)
		newInfo->hostip = pstrdup(dbInfo->hostip);

	for (i = 0; i < COMPONENT_DBS_MAX_ADDRS; i++)
	{
		if (dbInfo->hostaddrs[i] != NULL)
		{
			newInfo->hostaddrs[i] = pstrdup(dbInfo->hostaddrs[i]);
		}
	}

	return newInfo;
}

static CdbComponentDatabaseInfo *findDatabaseInfoBySegIndex(
		CdbComponentDatabases *cdbs, int segIndex)
{
	Assert(cdbs != NULL);
	int i = 0;
	CdbComponentDatabaseInfo *cdbInfo = NULL;
	for (i = 0; i < cdbs->total_segment_dbs; i++)
	{
		cdbInfo = &cdbs->segment_db_info[i];
		if (segIndex != cdbInfo->segindex)
			break;
	}

	return cdbInfo;
}

/*
 *	buildGangDefinition: reads the GP catalog tables and build a CdbComponentDatabases structure.
 *	It then converts this to a Gang structure and initializes
 *	all the non-connection related fields.
 */
static Gang *
buildGangDefinition(GangType type, int gang_id, int size, int content, char *portal_name)
{
	Gang	   *newGangDefinition = NULL;
	CdbComponentDatabaseInfo *cdbinfo = NULL;
	CdbComponentDatabaseInfo *cdbInfoCopy = NULL;
	SegmentDatabaseDescriptor *segdbDesc = NULL;

	int			segCount = 0;
	int			i = 0;

	Assert(CurrentMemoryContext == GangContext);
	Assert(size == 1 || size == getgpsegmentCount());

	LOG_GANG_DEBUG(LOG, "Starting %d qExec processes for %s gang, portal=%s",
			size, gangTypeToString(type),
			portal_name == NULL ? "null" : portal_name);

	/*
	 * MPP-2613
	 * NOTE: global, and we may be leaking some here (but if we free
	 * anyone who has pointers into the free()ed space is going to
	 * freak out)
	 */
	cdb_component_dbs = getComponentDatabases();

	if (cdb_component_dbs == NULL ||
		cdb_component_dbs->total_segments <= 0 ||
		cdb_component_dbs->total_segment_dbs <= 0)
	{
		insist_log(false, "schema not populated while building segworker group");
	}

	/* if mirroring is not configured */
	if (cdb_component_dbs->total_segment_dbs == cdb_component_dbs->total_segments)
	{
		LOG_GANG_DEBUG(LOG, "building Gang: mirroring not configured");
		disableFTS();
	}

	/*
	 * Get the structure representing the configured segment
	 * databases.  This structure is fetched into a permanent memory
	 * context.
	 */
	newGangDefinition = (Gang *) palloc0(sizeof(Gang));
	newGangDefinition->type = type;
	newGangDefinition->size = size;
	newGangDefinition->gang_id = gang_id;
	newGangDefinition->allocated = false;
	newGangDefinition->active = false;
	newGangDefinition->noReuse = false;
	newGangDefinition->dispatcherActive = false;
	newGangDefinition->portal_name = (portal_name ? pstrdup(portal_name) : (char *) NULL);
	newGangDefinition->all_valid_segdbs_connected = false;
	newGangDefinition->db_descriptors =
		(SegmentDatabaseDescriptor *) palloc0(size * sizeof(SegmentDatabaseDescriptor));

	switch (type)
	{
	case GANGTYPE_ENTRYDB_READER:
		cdbinfo = &cdb_component_dbs->entry_db_info[0];
		if (cdbinfo->hostip == NULL)
		{
			/* we know we want to use localhost */
			cdbinfo->hostip = "127.0.0.1";
		}
		cdbInfoCopy = copyCdbComponentDatabaseInfo(cdbinfo);
		segdbDesc = &newGangDefinition->db_descriptors[0];
		cdbconn_initSegmentDescriptor(segdbDesc, cdbInfoCopy);
		break;

	case GANGTYPE_SINGLETON_READER:
		cdbinfo = findDatabaseInfoBySegIndex(cdb_component_dbs, content);
		cdbInfoCopy = copyCdbComponentDatabaseInfo(cdbinfo);
		segdbDesc = &newGangDefinition->db_descriptors[0];
		cdbconn_initSegmentDescriptor(segdbDesc, cdbInfoCopy);
		break;

	case GANGTYPE_PRIMARY_READER:
	case GANGTYPE_PRIMARY_WRITER:
		/*
		 * We loop through the segment_db_info.  Each item has a segindex.
		 * They are sorted by segindex, and there can be > 1 segment_db_info for
		 * a given segindex (currently, there can be 1 or 2)
		 */
		for (i = 0; i < cdb_component_dbs->total_segment_dbs; i++)
		{
			cdbinfo = &cdb_component_dbs->segment_db_info[i];
			if (SEGMENT_IS_ACTIVE_PRIMARY(cdbinfo))
			{
				segdbDesc = &newGangDefinition->db_descriptors[segCount];
				cdbInfoCopy = copyCdbComponentDatabaseInfo(cdbinfo);
				cdbconn_initSegmentDescriptor(segdbDesc, cdbInfoCopy);
				segCount++;
			}
		}

		if (size != segCount)
		{
			FtsReConfigureMPP(false);
			elog(ERROR, "Not all primary segment instances are active and connected");
		}
		break;

	default:
		Assert(false);
	}

	return newGangDefinition;
}



int
gp_pthread_create(pthread_t * thread,
				  void *(*start_routine) (void *),
				  void *arg, const char *caller)
{
	int			pthread_err = 0;
	pthread_attr_t t_atts;

	/*
	 * Call some init function. Before any thread is created, we need to init
	 * some static stuff. The main purpose is to guarantee the non-thread safe
	 * stuff are called in main thread, before any child thread get running.
	 * Note these staic data structure should be read only after init.	Thread
	 * creation is a barrier, so there is no need to get lock before we use
	 * these data structures.
	 *
	 * So far, we know we need to do this for getpwuid_r (See MPP-1971, glibc
	 * getpwuid_r is not thread safe).
	 */
#ifndef WIN32
	get_gp_passwdptr();
#endif

	/*
	 * save ourselves some memory: the defaults for thread stack size are
	 * large (1M+)
	 */
	pthread_err = pthread_attr_init(&t_atts);
	if (pthread_err != 0)
	{
		elog(LOG, "%s: pthread_attr_init failed.  Error %d", caller, pthread_err);
		return pthread_err;
	}

#ifdef pg_on_solaris
	/* Solaris doesn't have PTHREAD_STACK_MIN ? */
	pthread_err = pthread_attr_setstacksize(&t_atts, (256 * 1024));
#else
	pthread_err = pthread_attr_setstacksize(&t_atts, Max(PTHREAD_STACK_MIN, (256 * 1024)));
#endif
	if (pthread_err != 0)
	{
		elog(LOG, "%s: pthread_attr_setstacksize failed.  Error %d", caller, pthread_err);
		pthread_attr_destroy(&t_atts);
		return pthread_err;
	}

	pthread_err = pthread_create(thread, &t_atts, start_routine, arg);

	pthread_attr_destroy(&t_atts);

	return pthread_err;
}


static void
addOneOption(StringInfo string, struct config_generic * guc)
{
	Assert(guc && (guc->flags & GUC_GPDB_ADDOPT));
	switch (guc->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *bguc = (struct config_bool *) guc;

				appendStringInfo(string, " -c %s=%s", guc->name,
								  *(bguc->variable) ? "true" : "false"
					);
				break;
			}
		case PGC_INT:
			{
				struct config_int *iguc = (struct config_int *) guc;

				appendStringInfo(string, " -c %s=%d", guc->name, *iguc->variable);
				break;
			}
		case PGC_REAL:
			{
				struct config_real *rguc = (struct config_real *) guc;

				appendStringInfo(string, " -c %s=%f", guc->name, *rguc->variable);
				break;
			}
		case PGC_STRING:
			{
				struct config_string *sguc = (struct config_string *) guc;
				const char *str = *sguc->variable;
				int			i;

				appendStringInfo(string, " -c %s=", guc->name);
				/*
				 * All whitespace characters must be escaped. See
				 * pg_split_opts() in the backend.
				 */
				for (i = 0; str[i] != '\0'; i++)
				{
					if (isspace((unsigned char) str[i]))
						appendStringInfoChar(string, '\\');

					appendStringInfoChar(string, str[i]);
				}
				break;
			}
		default:
			Insist(false);
	}
}


/*
 * Add GUCs to option string.
 */

static void
addOptions(StringInfo string, bool iswriter, bool i_am_superuser)
{
	struct config_generic **gucs = get_guc_variables();
	int			ngucs = get_num_guc_variables();
	int			i;


	LOG_GANG_DEBUG(LOG, "addOptions: iswriter %d", iswriter);

	/* GUCs need special handling */
	/* TODO: gp_qd_hostname, gp_qd_port and gp_qd_callback_info are not actually used*/
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbComponentDatabaseInfo *qdinfo;

		qdinfo = &cdb_component_dbs->entry_db_info[0];
		if (qdinfo->hostip != NULL)
			appendStringInfo(string, " -c gp_qd_hostname=%s", qdinfo->hostip);
		else
			appendStringInfo(string, " -c gp_qd_hostname=%s", qdinfo->hostname);
		appendStringInfo(string, " -c gp_qd_port=%d", qdinfo->port);
	}
	else
	{
		appendStringInfo(string, " -c gp_qd_hostname=%s", qdHostname);
		appendStringInfo(string, " -c gp_qd_port=%d", qdPostmasterPort);
	}

	appendStringInfo(string, " -c gp_qd_callback_info=port=%d", PostPortNumber);

	/*
	 * Transactions are tricky.
	 * Here is the copy and pasted code, and we know they are working.
	 * The problem, is that QE may ends up with different iso level, but
	 * postgres really does not have read uncommited and repeated read.
	 * (is this true?) and they are mapped.
	 *
	 * Put these two gucs in the generic framework works (pass make installcheck-good)
	 * if we make assign_defaultxactisolevel and assign_XactIsoLevel correct take
	 * string "readcommitted" etc.	(space stripped).  However, I do not
	 * want to change this piece of code unless I know it is broken.
	 */
	if (DefaultXactIsoLevel != XACT_READ_COMMITTED)
	{
		if (DefaultXactIsoLevel == XACT_SERIALIZABLE)
			appendStringInfo(string, " -c default_transaction_isolation=serializable");
	}

	if (XactIsoLevel != XACT_READ_COMMITTED)
	{
		if (XactIsoLevel == XACT_SERIALIZABLE)
			appendStringInfo(string, " -c transaction_isolation=serializable");
	}

	for (i = 0; i < ngucs; ++i)
	{
		struct config_generic *guc = gucs[i];

		if ((guc->flags & GUC_GPDB_ADDOPT) &&
			(guc->context == PGC_USERSET || i_am_superuser))
			addOneOption(string, guc);
	}
}

/*
 *	thread_DoConnect is the thread proc used to perform the connection to one of the qExecs.
 */
static void *
thread_DoConnect(void *arg)
{
	DoConnectParms *pParms;
	SegmentDatabaseDescriptor **segdbDescPtrArray;

	int			db_count;
	int			i;

	SegmentDatabaseDescriptor *segdbDesc;
	CdbComponentDatabaseInfo *cdbinfo;

	gp_set_thread_sigmasks();

	pParms = (DoConnectParms *) arg;
	segdbDescPtrArray = pParms->segdbDescPtrArray;
	db_count = pParms->db_count;

	/*
	 * The pParms contains an array of SegmentDatabaseDescriptors
	 * to connect to.
	 */
	for (i = 0; i < db_count; i++)
	{
		char		gpqeid[100];

		segdbDesc = segdbDescPtrArray[i];

		if (segdbDesc == NULL || segdbDesc->segment_database_info == NULL)
		{
			write_log("thread_DoConnect: bad segment definition during gang creation %d/%d\n", i, db_count);
			continue;
		}

		cdbinfo = segdbDesc->segment_database_info;

		/*
		 * Build the connection string.  Writer-ness needs to be processed
		 * early enough now some locks are taken before command line options
		 * are recognized.
		 */
		build_gpqeid_param(gpqeid, sizeof(gpqeid), cdbinfo->segindex,
						   pParms->type == GANGTYPE_PRIMARY_WRITER);

		if (cdbconn_doConnect(segdbDesc, gpqeid, pParms->connectOptions->data))
		{
			if (segdbDesc->motionListener == -1)
			{
				segdbDesc->errcode = ERRCODE_GP_INTERNAL_ERROR;
				appendPQExpBuffer(&segdbDesc->error_message,
						"Internal error: No motion listener port for %s\n",
						segdbDesc->whoami);
				PQfinish(segdbDesc->conn);
				segdbDesc->conn = NULL;
			}
		}
	}

	return (NULL);
}	/* thread_DoConnect */


/*
 * build_gpqeid_params
 *
 * Called from the qDisp process to create the "gpqeid" parameter string
 * to be passed to a qExec that is being started.  NB: Can be called in a
 * thread, so mustn't use palloc/elog/ereport/etc.
 */
static void
build_gpqeid_param(char *buf, int bufsz, int segIndex, bool is_writer)
{
#ifdef HAVE_INT64_TIMESTAMP
#define TIMESTAMP_FORMAT INT64_FORMAT
#else
#ifndef _WIN32
#define TIMESTAMP_FORMAT "%.14a"
#else
#define TIMESTAMP_FORMAT "%g"
#endif
#endif

	snprintf(buf, bufsz,
			"%d;%d;" TIMESTAMP_FORMAT ";%s",
			gp_session_id,
			segIndex,
			PgStartTime,
			(is_writer ? "true" : "false"));

}	/* build_gpqeid_params */

/*
 * cdbgang_parse_gpqeid_params
 *
 * Called very early in backend initialization, to interpret the "gpqeid"
 * parameter value that a qExec receives from its qDisp.
 *
 * At this point, client authentication has not been done; the backend
 * command line options have not been processed; GUCs have the settings
 * inherited from the postmaster; etc; so don't try to do too much in here.
 */
static bool
gpqeid_next_param(char **cpp, char **npp)
{
	*cpp = *npp;
	if (!*cpp)
		return false;

	*npp = strchr(*npp, ';');
	if (*npp)
	{
		**npp = '\0';
		++*npp;
	}
	return true;
}

void
cdbgang_parse_gpqeid_params(struct Port * port __attribute__((unused)), const char *gpqeid_value)
{
	char	   *gpqeid = pstrdup(gpqeid_value);
	char	   *cp;
	char	   *np = gpqeid;

	/* The presence of an gpqeid string means this backend is a qExec. */
	SetConfigOption("gp_session_role", "execute",
					PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* gp_session_id */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_session_id", cp,
						PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* gp_segment */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_segment", cp,
						PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* PgStartTime */
	if (gpqeid_next_param(&cp, &np))
	{
#ifdef HAVE_INT64_TIMESTAMP
		if (!scanint8(cp, true, &PgStartTime))
			goto bad;
#else
		PgStartTime = strtod(cp, NULL);
#endif
	}

	/* Gp_is_writer */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_is_writer", cp,
						PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* Too few items, or too many? */
	if (!cp || np)
		goto bad;

	if (gp_session_id <= 0 ||
		PgStartTime <= 0)
		goto bad;

	pfree(gpqeid);
	return;

bad:
	elog(FATAL, "Segment dispatched with invalid option: 'gpqeid=%s'", gpqeid_value);
}	/* cdbgang_parse_gpqeid_params */


void
cdbgang_parse_gpqdid_params(struct Port * port __attribute__((unused)), const char *gpqdid_value)
{
	char	   *gpqdid = pstrdup(gpqdid_value);
	char	   *cp;
	char	   *np = gpqdid;

	/*
	 * The presence of an gpqdid string means this is a callback from QE to
	 * QD.
	 */
	SetConfigOption("gp_is_callback", "true",
					PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* gp_session_id */
	if (gpqeid_next_param(&cp, &np))
		SetConfigOption("gp_session_id", cp,
						PGC_POSTMASTER, PGC_S_OVERRIDE);

	/* PgStartTime */
	if (gpqeid_next_param(&cp, &np))
	{
#ifdef HAVE_INT64_TIMESTAMP
		if (!scanint8(cp, true, &PgStartTime))
			goto bad;
#else
		PgStartTime = strtod(cp, NULL);
#endif
	}
	Assert(gp_is_callback);

	/* Too few items, or too many? */
	if (!cp || np)
		goto bad;

	if (gp_session_id <= 0 ||
		PgStartTime <= 0)
		goto bad;

	pfree(gpqdid);
	return;

bad:
	elog(FATAL, "Master callback dispatched with invalid option: 'gpqdid=%s'", gpqdid_value);
}	/* cdbgang_parse_gpqdid_params */

/*
 * This is where we keep track of all the gangs that exist for this session.
 * On a QD, gangs can either be "available" (not currently in use), or "allocated".
 *
 * On a Dispatch Agent, we just store them in the "available" lists, as the DA doesn't
 * keep track of allocations (it assumes the QD will keep track of what is allocated or not).
 *
 */

static List *allocatedReaderGangsN = NIL;
static List *availableReaderGangsN = NIL;
static List *allocatedReaderGangs1 = NIL;
static List *availableReaderGangs1 = NIL;
static Gang *primaryWriterGang = NULL;

List *
getAllReaderGangs()
{
	return list_concat(getAllIdleReaderGangs(), getAllBusyReaderGangs());
}

List *
getAllIdleReaderGangs()
{
	List	   *res = NIL;
	ListCell   *le;

	/*
	 * Do not use list_concat() here, it would destructively modify the lists!
	 */
	foreach(le, availableReaderGangsN)
	{
		res = lappend(res, lfirst(le));
	}
	foreach(le, availableReaderGangs1)
	{
		res = lappend(res, lfirst(le));
	}

	return res;
}

List *
getAllBusyReaderGangs()
{
	List	   *res = NIL;
	ListCell   *le;

	/*
	 * Do not use list_concat() here, it would destructively modify the lists!
	 */
	foreach(le, allocatedReaderGangsN)
	{
		res = lappend(res, lfirst(le));
	}
	foreach(le, allocatedReaderGangs1)
	{
		res = lappend(res, lfirst(le));
	}

	return res;
}

static Gang *getAvailableGang(GangType type, int size, int content, char *portalName)
{
	Gang *retGang = NULL;
	switch (type)
	{
	case GANGTYPE_SINGLETON_READER:
	case GANGTYPE_ENTRYDB_READER:
		if (availableReaderGangs1 != NULL) /* There are gangs already created */
		{
			ListCell *cell = NULL;
			ListCell *prevcell = NULL;

			foreach(cell, availableReaderGangs1)
			{
				Gang *gang = (Gang *) lfirst(cell);
				Assert(gang != NULL);
				Assert(gang->size == size);
				if (gang->db_descriptors[0].segindex == content)
				{
					LOG_GANG_DEBUG(LOG,
							"reusing an available reader 1-gang for seg%d", content);
					retGang = gang;
					availableReaderGangs1 = list_delete_cell(availableReaderGangs1,
							cell, prevcell);
					break;
				}
				prevcell = cell;
			}
		}
		break;

	case GANGTYPE_PRIMARY_READER:
		if (availableReaderGangsN != NULL) /* There are gangs already created */
		{
			LOG_GANG_DEBUG(LOG, "Reusing an available reader N-gang");

			retGang = linitial(availableReaderGangsN);
			Assert(retGang != NULL);
			Assert(retGang->type == type && retGang->size == size);

			availableReaderGangsN = list_delete_first(availableReaderGangsN);
		}
		break;

	default:
		Assert(false);
	}

	/*
	 * make sure no memory is still allocated for previous
	 * portal name that this gang belonged to
	 */
	if (retGang->portal_name)
		pfree(retGang->portal_name);

	/* let the gang know which portal it is being assigned to */
	retGang->portal_name = (portalName ? pstrdup(portalName) : (char *) NULL);

	return retGang;
}

static void addGangToAvailable(Gang *gp)
{
	switch (gp->type)
	{
	case GANGTYPE_SINGLETON_READER:
	case GANGTYPE_ENTRYDB_READER:
		allocatedReaderGangs1 = lappend(allocatedReaderGangs1, gp);
		break;

	case GANGTYPE_PRIMARY_READER:
		allocatedReaderGangsN = lappend(allocatedReaderGangsN, gp);
		break;
	default:
		Assert(false);
	}
}

/*
 * This is the main routine.   It allocates a gang to a query.
 * If we have a previously created but unallocated gang lying around, we use that.
 * otherwise, we create a new gang
 *
 * This is only used on the QD.
 *
 */
Gang *
allocateReaderGang(GangType type, char *portal_name)
{
	/*
	 * First, we look for an unallocated but created gang of the right type
	 * if it exists, we return it.
	 * Else, we create a new gang
	 */

	MemoryContext oldContext;
	Gang *gp = NULL;
	int size = 0;
	int content = 0;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		elog(FATAL, "dispatch process called with role %d", Gp_role);
	}

	insist_log(IsTransactionOrTransactionBlock(),
			"cannot allocate segworker group outside of transaction");

	LOG_GANG_DEBUG(LOG,
			"allocateReaderGang for portal %s: allocatedReaderGangsN %d, availableReaderGangsN %d, allocatedReaderGangs1 %d, availableReaderGangs1 %d",
			(portal_name ? portal_name : "<unnamed>"),
			list_length(allocatedReaderGangsN),
			list_length(availableReaderGangsN),
			list_length(allocatedReaderGangs1),
			list_length(availableReaderGangs1));

	if (GangContext == NULL)
	{
		GangContext = AllocSetContextCreate(TopMemoryContext, "Gang Context",
		ALLOCSET_DEFAULT_MINSIZE,
		ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);
	}
	Assert(GangContext != NULL);

	oldContext = MemoryContextSwitchTo(GangContext);

	switch(type)
	{
	case GANGTYPE_ENTRYDB_READER:
		content = -1;
		size = 1;
		break;

	case GANGTYPE_SINGLETON_READER:
		content = gp_singleton_segindex;
		size = 1;
		break;

	case GANGTYPE_PRIMARY_READER:
		content = 0;
		size = getgpsegmentCount();
		break;

	default:
		Assert(false);
	}

	gp = getAvailableGang(type, size, content, portal_name);

	if(gp == NULL)
	{
		LOG_GANG_DEBUG(LOG, "Creating a new reader size %d gang for %s", size,
				(portal_name ? portal_name : "unnamed portal"));

		gp = createGang(type, gang_id_counter++, size, content, portal_name);
		if (gp == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("segworker group creation failed"),
							errhint("server log may have more detailed error message")));
		}
		gp->allocated = true;
	}

	/* sanity check the gang */
	insist_log(gangOK(gp),
			"could not connect to segment: initialization of segworker group failed");

	addGangToAvailable(gp);

	MemoryContextSwitchTo(oldContext);

	LOG_GANG_DEBUG(LOG,
			" on return: allocatedReaderGangs %d, availableReaderGangsN %d, allocatedReaderGangs1 %d, availableReaderGangs1 %d",
			list_length(allocatedReaderGangsN),
			list_length(availableReaderGangsN),
			list_length(allocatedReaderGangs1),
			list_length(availableReaderGangs1));

	return gp;
}


Gang *
allocateWriterGang()
{
	Gang *writer_gang = NULL;
	MemoryContext oldContext;

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		elog(FATAL, "dispatch process called with role %d", Gp_role);
	}
	/*
	 * First, we look for an unallocated but created gang of the right type
	 * if it exists, we return it.
	 * Else, we create a new gang
	 */
	if (primaryWriterGang == NULL)
	{
		int nsegdb = getgpsegmentCount();

		insist_log(IsTransactionOrTransactionBlock(),
				"cannot allocate segworker group outside of transaction");

		if (GangContext == NULL)
		{
			GangContext = AllocSetContextCreate(TopMemoryContext,
					"Gang Context",
					ALLOCSET_DEFAULT_MINSIZE,
					ALLOCSET_DEFAULT_INITSIZE,
					ALLOCSET_DEFAULT_MAXSIZE);
		}
		Assert(GangContext != NULL);

		oldContext = MemoryContextSwitchTo(GangContext);

		writer_gang = createGang(GANGTYPE_PRIMARY_WRITER, PRIMARY_WRITER_GANG_ID, nsegdb, -1, NULL);
		if (!writer_gang)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("segworker group creation failed"),
							errhint("server log may have more detailed error message")));
		}

		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		LOG_GANG_DEBUG(LOG, "Reusing an existing primary writer gang");
		writer_gang = primaryWriterGang;
	}

	/* sanity check the gang */
	if (!gangOK(writer_gang))
	{
		elog(ERROR, "could not temporarily connect to one or more segments");
	}

	writer_gang->allocated = true;

	primaryWriterGang = writer_gang;

	return writer_gang;
}

/*
 * When we are the dispatch agent, we get told which gang to use by its "gang_id"
 * We need to find the gang in our lists.
 *
 * keeping the gangs in the "available" lists on the Dispatch Agent is a hack,
 * as the dispatch agent doesn't differentiate allocated from available, or 1 gangs
 * from n-gangs.  It assumes the QD keeps track of all that.
 *
 * It might be nice to pass gang type and size to this routine as safety checks.
 *
 */

Gang *
findGangById(int gang_id)
{
	Assert(gang_id >= PRIMARY_WRITER_GANG_ID);

	if (primaryWriterGang && primaryWriterGang->gang_id == gang_id)
		return primaryWriterGang;

	if (gang_id == PRIMARY_WRITER_GANG_ID)
	{
		elog(LOG, "findGangById: primary writer didn't exist when we expected it to");
		return allocateWriterGang();
	}

	/*
	 * Now we iterate through the list of reader gangs
	 * to find the one that matches
	 */
	if (availableReaderGangsN != NIL)
	{
		ListCell   *cur_item;
		ListCell   *prev_item = NULL;

		cur_item = list_head(availableReaderGangsN);

		while (cur_item != NULL)
		{
			Gang	   *gp = (Gang *) lfirst(cur_item);

			if (gp && gp->gang_id == gang_id)
			{
				return gp;
			}

			/* cur_item must be preserved */
			prev_item = cur_item;
			cur_item = lnext(prev_item);

		}
	}
	if (availableReaderGangs1 != NIL)
	{
		ListCell   *cur_item;
		ListCell   *prev_item = NULL;

		cur_item = list_head(availableReaderGangs1);

		while (cur_item != NULL)
		{
			Gang	   *gp = (Gang *) lfirst(cur_item);

			if (gp && gp->gang_id == gang_id)
			{
				return gp;
			}

			/* cur_item must be preserved */
			prev_item = cur_item;
			cur_item = lnext(prev_item);

		}
	}

	/*
	 * 1-gangs can exist on some dispatch agents, and not on others.
	 *
	 * so, we can't tell if not finding the gang is an error or not.
	 *
	 * It would be good if we knew if this was a 1-gang on not.
	 *
	 * The writer gangs are always n-gangs.
	 *
	 */

	if (gang_id <= 2)
	{
		insist_log(false, "could not find segworker group %d", gang_id);
	}
	else
	{
		if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		{
			elog(LOG, "could not find segworker group %d", gang_id);
		}
	}

	return NULL;


}

struct SegmentDatabaseDescriptor *
getSegmentDescriptorFromGang(const Gang *gp, int seg)
{

	int			i;

	if (gp == NULL)
		return NULL;

	for (i = 0; i < gp->size; i++)
	{
		if (gp->db_descriptors[i].segindex == seg)
		{
			return &(gp->db_descriptors[i]);
		}
	}

	return NULL;
}

/**
 * @param directDispatch may be null
 */
List *
getCdbProcessList(Gang *gang, int sliceIndex, DirectDispatchInfo *directDispatch)
{
	int			i;
	List	   *list = NIL;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(gang != NULL);

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "getCdbProcessList slice%d gangtype=%d gangsize=%d",
			 sliceIndex, gang->type, gang->size);

	if (gang != NULL && gang->type != GANGTYPE_UNALLOCATED)
	{
		CdbComponentDatabaseInfo *qeinfo;
		int			listsize = 0;

		for (i = 0; i < gang->size; i++)
		{
			CdbProcess *process;
			SegmentDatabaseDescriptor *segdbDesc;
			bool includeThisProcess = true;

			segdbDesc = &gang->db_descriptors[i];
			if (directDispatch != NULL && directDispatch->isDirectDispatch)
			{
				ListCell *cell;

				includeThisProcess = false;
				foreach(cell, directDispatch->contentIds)
				{
					if (lfirst_int(cell) == segdbDesc->segindex)
					{
						includeThisProcess = true;
						break;
					}
				}
			}

			/*
			 * We want the n-th element of the list to be the segDB that handled content n.
			 * And if no segDb is available (down mirror), we want it to be null.
			 *
			 * But the gang structure element n is not necessarily the guy who handles content n,
			 * so we need to skip some slots.
			 *
			 *
			 * We don't do this for reader 1-gangs
			 *
			 */
			if (gang->size > 1 ||
				gang->type == GANGTYPE_PRIMARY_WRITER)
			{
				while (segdbDesc->segindex > listsize)
				{
					list = lappend(list, NULL);
					listsize++;
				}
			}

			if (!includeThisProcess)
			{
				list = lappend(list, NULL);
				listsize++;
				continue;
			}

			process = (CdbProcess *) makeNode(CdbProcess);
			qeinfo = segdbDesc->segment_database_info;

			if (qeinfo == NULL)
			{
				elog(ERROR, "required segment is unavailable");
			}
			else if (qeinfo->hostip == NULL)
			{
				elog(ERROR, "required segment IP is unavailable");
			}

			process->listenerAddr = pstrdup(qeinfo->hostip);

			process->listenerPort = segdbDesc->motionListener;

			process->pid = segdbDesc->backendPid;
			process->contentid = segdbDesc->segindex;

			if (gp_log_gang >= GPVARS_VERBOSITY_VERBOSE || DEBUG4 >= log_min_messages)
				elog(LOG, "Gang assignment (gang_id %d): slice%d seg%d %s:%d pid=%d",
					 gang->gang_id,
					 sliceIndex,
					 process->contentid,
					 process->listenerAddr,
					 process->listenerPort,
					 process->pid);

			list = lappend(list, process);
			listsize++;
		}

		insist_log( ! (gang->type == GANGTYPE_PRIMARY_WRITER &&
			listsize < getgpsegmentCount()),
			"master segworker group smaller than number of segments");

		if (gang->size > 1 ||
			gang->type == GANGTYPE_PRIMARY_WRITER)
		{
			while (listsize < getgpsegmentCount())
			{
				list = lappend(list, NULL);
				listsize++;
			}
		}
		Assert(listsize == 1 || listsize == getgpsegmentCount());
	}

	return list;
}

/*
 * getCdbProcessForQD:	Manufacture a CdbProcess representing the QD,
 * as if it were a worker from the executor factory.
 *
 * NOTE: Does not support multiple (mirrored) QDs.
 */
List *
getCdbProcessesForQD(int isPrimary)
{
	List	   *list = NIL;

	CdbComponentDatabaseInfo *qdinfo;
	CdbProcess *proc;

	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(cdb_component_dbs != NULL);

	if (!isPrimary)
	{
		elog(FATAL, "getCdbProcessesForQD: unsupported request for master mirror process");
	}

	qdinfo = &(cdb_component_dbs->entry_db_info[0]);

	Assert(qdinfo->segindex == -1);
	Assert(SEGMENT_IS_ACTIVE_PRIMARY(qdinfo));
	Assert(qdinfo->hostip != NULL);

	proc = makeNode(CdbProcess);
	/*
	 * Set QD listener address to NULL. This
	 * will be filled during starting up outgoing
	 * interconnect connection.
	 */
	proc->listenerAddr = NULL;
	proc->listenerPort = Gp_listener_port;

	proc->pid = MyProcPid;
	proc->contentid = -1;

	list = lappend(list, proc);
	return list;
}

/*
 * cleanupGang():
 *
 * A return value of "true" means that the gang was intact (or NULL).
 *
 * A return value of false, means that a problem was detected and the
 * gang has been disconnected (and so should not be put back onto the
 * available list).
 */
bool
cleanupGang(Gang *gp)
{
	int			i;

	if (gp == NULL)
		return true;

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
	{
		elog(LOG, "cleanupGang: cleaning gang id %d type %d size %d, was used for portal: %s", gp->gang_id, gp->type, gp->size, (gp->portal_name ? gp->portal_name : "(unnamed)"));
	}

	/* disassociate this gang with any portal that it may have belonged to */
	if (gp->portal_name)
		pfree(gp->portal_name);

	gp->portal_name = NULL;

	if (gp->active)
	{
		elog((gp_log_gang >= GPVARS_VERBOSITY_DEBUG ? LOG : DEBUG2), "cleanupGang called on a gang that is active");
	}

	/*
	 * if the process is in the middle of blowing up... then we don't do
	 * anything here.  making libpq and other calls can definitely result in
	 * things getting HUNG.
	 */
	if (proc_exit_inprogress)
		return true;

	/*
	 * Loop through the segment_database_descriptors array and, for each
	 * SegmentDatabaseDescriptor:
	 *	   1) discard the query results (if any)
	 */
	for (i = 0; i < gp->size; i++)
	{
		SegmentDatabaseDescriptor *segdbDesc = &(gp->db_descriptors[i]);

		if (segdbDesc == NULL)
			return false;

		/*
		 * Note, we cancel all "still running" queries
		 */
		/* PQstatus() is smart enough to handle NULL */
		if (PQstatus(segdbDesc->conn) == CONNECTION_OK)
		{
			PGresult   *pRes;
			int			retry = 0;

			ExecStatusType stat;

			while (NULL != (pRes = PQgetResult(segdbDesc->conn)))
			{
				stat = PQresultStatus(pRes);

				elog(LOG, "(%s) Leftover result at freeGang time: %s %s",
					 segdbDesc->whoami,
					 PQresStatus(stat),
					 PQerrorMessage(segdbDesc->conn));

				PQclear(pRes);
				if (stat == PGRES_FATAL_ERROR)
					break;
				if (stat == PGRES_BAD_RESPONSE)
					break;
				retry++;
				if (retry > 20)
				{
					elog(LOG, "cleanup called when a segworker is still busy, waiting didn't help, trying to destroy the segworker group");
					disconnectAndDestroyGang(gp);
					gp = NULL;
					elog(FATAL, "cleanup called when a segworker is still busy");
					return false;
				}
			}
		}
		else
		{
			/*
			 * something happened to this gang, disconnect instead of
			 * sending it back onto our lists!
			 */
			elog(LOG, "lost connection with segworker group member");
			disconnectAndDestroyGang(gp);
			return false;
		}

		/* QE is no longer associated with a slice. */
		if (!cdbconn_setSliceIndex(segdbDesc, -1))
		{
			insist_log(false, "could not reset slice index during cleanup");
		}

	}

	gp->allocated = false;

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "cleanupGang done");

	if (gp->noReuse)
	{
		disconnectAndDestroyGang(gp);
		gp = NULL;
		return false;
	}

	return true;
}

static bool NeedResetSession = false;
static bool NeedSessionIdChange = false;
static Oid	OldTempNamespace = InvalidOid;

/*
 * cleanupIdleReaderGangs() and cleanupAllIdleGangs().
 *
 * These two routines are used when a session has been idle for a while (waiting for the
 * client to send us SQL to execute).  The idea is to consume less resources while sitting idle.
 *
 * The expectation is that if the session is logged on, but nobody is sending us work to do,
 * we want to free up whatever resources we can.  Usually it means there is a human being at the
 * other end of the connection, and that person has walked away from their terminal, or just hasn't
 * decided what to do next.  We could be idle for a very long time (many hours).
 *
 * Of course, freeing gangs means that the next time the user does send in an SQL statement,
 * we need to allocate gangs (at least the writer gang) to do anything.  This entails extra work,
 * so we don't want to do this if we don't think the session has gone idle.
 *
 * Only call these routines from an idle session.
 *
 * These routines are called from the sigalarm signal handler (hopefully that is safe to do).
 *
 */

/*
 * Destroy all idle (i.e available) reader gangs.
 * It is always safe to get rid of the reader gangs.
 *
 * call only from an idle session.
 */
void
cleanupIdleReaderGangs(void)
{
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "cleanupIdleReaderGangs beginning");

	disconnectAndDestroyAllReaderGangs(false);

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "cleanupIdleReaderGangs done");

	return;
}

/*
 * Destroy all gangs to free all resources on the segDBs, if it is possible (safe) to do so.
 *
 * Call only from an idle session.
 */
void
cleanupAllIdleGangs(void)
{
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "cleanupAllIdleGangs beginning");

	/*
	 * It's always safe to get rid of the reader gangs.
	 *
	 * Since we are idle, any reader gangs will be available but not allocated.
	 */
	disconnectAndDestroyAllReaderGangs(false);

	/*
	 * If we are in a transaction, we can't release the writer gang, as this will abort the transaction.
	 */
	if (!IsTransactionOrTransactionBlock())
	{
		/*
		 * if we have a TempNameSpace, we can't release the writer gang, as this would drop any temp tables we own.
		 *
		 * This test really should be "do we have any temp tables", not "do we have a temp schema", but I'm still trying to figure
		 * out how to test for that.  In general, we only create a temp schema if there was a create temp table statement, and
		 * most of the time people are too lazy to drop their temp tables prior to session end, so 90% of the time this will be OK.
		 * And when we get it wrong, it just means we don't free the writer gang when we could have.
		 *
		 * Better yet would be to have the QD control the dropping of the temp tables, and not doing it by letting each segment
		 * drop it's part of the temp table independently.
		 */
		if (!TempNamespaceOidIsValid())
		{
			/*
			 * Get rid of ALL gangs... Readers, mirror writer, and primary writer.  After this, we have no
			 * resources being consumed on the segDBs at all.
			 */
			disconnectAndDestroyAllGangs();
			/*
			 * Our session wasn't destroyed due to an fatal error or FTS action, so we don't need to
			 * do anything special.  Specifically, we DON'T want to act like we are now in a new session,
			 * since that would be confusing in the log.
			 */
			NeedResetSession = false;
		}
		else
			if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
				elog(LOG, "TempNameSpaceOid is valid, can't free writer");
	}
	else
		if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
			elog(LOG, "We are in a transaction, can't free writer");

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "cleanupAllIdleGangs done");

	return;
}

static int64
getMaxGangMop(Gang *g)
{
	int64		maxmop = 0;
	int			i;

	for (i = 0; i < g->size; ++i)
	{
		SegmentDatabaseDescriptor *segdbDesc = &(g->db_descriptors[i]);

		if (!segdbDesc)
			continue;
		if (segdbDesc->conn && PQstatus(segdbDesc->conn) != CONNECTION_BAD)
		{
			if (segdbDesc->conn->mop_high_watermark > maxmop)
				maxmop = segdbDesc->conn->mop_high_watermark;
		}
	}

	return maxmop;
}

static List *
cleanupPortalGangList(List *gplist, int cachelimit)
{
	Gang	   *gp;

	if (gplist != NIL)
	{
		int			ngang = list_length(gplist);
		ListCell   *prev = NULL;
		ListCell   *curr = list_head(gplist);

		while (curr)
		{
			bool		destroy = ngang > cachelimit;

			gp = lfirst(curr);

			if (!destroy)
			{
				int64		maxmop = getMaxGangMop(gp);

				if ((maxmop >> 20) > gp_vmem_protect_gang_cache_limit)
					destroy = true;
			}

			if (destroy)
			{
				disconnectAndDestroyGang(gp);
				gplist = list_delete_cell(gplist, curr, prev);
				if (!prev)
					curr = list_head(gplist);
				else
					curr = lnext(prev);
				--ngang;
			}
			else
			{
				prev = curr;
				curr = lnext(prev);
			}
		}
	}

	return gplist;
}

/*
 * Portal drop... Clean up what gangs we hold
 */
void
cleanupPortalGangs(Portal portal)
{
	MemoryContext oldContext;
	const char *portal_name;

	if (portal->name && strcmp(portal->name, "") != 0)
	{
		portal_name = portal->name;
		if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		{
			elog(LOG, "cleanupPortalGangs %s", portal_name);
		}
	}
	else
	{
		portal_name = NULL;
		if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		{
			elog(LOG, "cleanupPortalGangs (unamed portal)");
		}
	}


	if (GangContext)
		oldContext = MemoryContextSwitchTo(GangContext);
	else
		oldContext = MemoryContextSwitchTo(TopMemoryContext);

	availableReaderGangsN = cleanupPortalGangList(availableReaderGangsN, gp_cached_gang_threshold);
	availableReaderGangs1 = cleanupPortalGangList(availableReaderGangs1, MAX_CACHED_1_GANGS);

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
	{
		elog(LOG, "cleanupPortalGangs '%s'. Reader gang inventory: "
			 "allocatedN=%d availableN=%d allocated1=%d available1=%d",
		 	(portal_name ? portal_name : "unnamed portal"),
	  		 list_length(allocatedReaderGangsN), list_length(availableReaderGangsN),
			 list_length(allocatedReaderGangs1), list_length(availableReaderGangs1));
	}

	MemoryContextSwitchTo(oldContext);
}

void
disconnectAndDestroyGang(Gang *gp)
{
	int			i;

	if (gp == NULL)
		return;

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "disconnectAndDestroyGang entered: id = %d", gp->gang_id);

	if (gp->active || gp->allocated)
		elog(gp_log_gang >= GPVARS_VERBOSITY_DEBUG ? LOG : DEBUG2, "Warning: disconnectAndDestroyGang called on an %s gang",
			 gp->active ? "active" : "allocated");

	if (!gangOK(gp))
	{
		elog(LOG, "disconnectAndDestroyGang on bad gang");
		return;
	}

	/*
	 * Loop through the segment_database_descriptors array and, for each
	 * SegmentDatabaseDescriptor:
	 *	   1) discard the query results (if any),
	 *	   2) disconnect the session, and
	 *	   3) discard any connection error message.
	 */
	for (i = 0; i < gp->size; i++)
	{
		SegmentDatabaseDescriptor *segdbDesc = &(gp->db_descriptors[i]);

		if (segdbDesc == NULL)
			continue;

		if (segdbDesc->conn && PQstatus(segdbDesc->conn) != CONNECTION_BAD)
		{

			PGTransactionStatusType status =
			PQtransactionStatus(segdbDesc->conn);

			elog((Debug_print_full_dtm ? LOG : (gp_log_gang >= GPVARS_VERBOSITY_DEBUG ? LOG : DEBUG5)),
				 "disconnectAndDestroyGang: got QEDistributedTransactionId = %u, QECommandId = %u, and QEDirty = %s",
				 segdbDesc->conn->QEWriter_DistributedTransactionId,
				 segdbDesc->conn->QEWriter_CommandId,
				 (segdbDesc->conn->QEWriter_Dirty ? "true" : "false"));

			if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
			{
				const char *ts;

				switch (status)
				{
					case PQTRANS_IDLE:
						ts = "idle";
						break;
					case PQTRANS_ACTIVE:
						ts = "active";
						break;
					case PQTRANS_INTRANS:
						ts = "idle, within transaction";
						break;
					case PQTRANS_INERROR:
						ts = "idle, within failed transaction";
						break;
					case PQTRANS_UNKNOWN:
						ts = "unknown transaction status";
						break;
					default:
						ts = "invalid transaction status";
						break;
				}
				elog(LOG, "Finishing connection with %s; %s",
					 segdbDesc->whoami, ts);
			}

			if (status == PQTRANS_ACTIVE)
			{
				char		errbuf[256];
				PGcancel   *cn = PQgetCancel(segdbDesc->conn);

				if (Debug_cancel_print || gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
					elog(LOG, "Calling PQcancel for %s", segdbDesc->whoami);

				if (PQcancel(cn, errbuf, 256) == 0)
				{
					elog(LOG, "Unable to cancel %s: %s", segdbDesc->whoami, errbuf);
				}
				PQfreeCancel(cn);
			}

			PQfinish(segdbDesc->conn);

			segdbDesc->conn = NULL;
		}

		/* Free memory owned by the segdbDesc. */
		cdbconn_termSegmentDescriptor(segdbDesc);
	}

	/*
	 * when we get rid of the primary writer gang we MUST also get rid of the reader
	 * gangs due to the shared local snapshot code that is shared between
	 * readers and writers.
	 */
	if (gp->type == GANGTYPE_PRIMARY_WRITER)
	{
		disconnectAndDestroyAllReaderGangs(true);

		resetSessionForPrimaryGangLoss();
	}

	if (gp->db_descriptors != NULL)
	{
		pfree(gp->db_descriptors);
		gp->db_descriptors = NULL;
	}

	gp->size = 0;
	if (gp->portal_name != NULL)
		pfree(gp->portal_name);
	pfree(gp);

	/*
	 * this is confusing, gp is local variable, no need to null it. gp = NULL;
	 */
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "disconnectAndDestroyGang done");
}

/*
 * freeGangsForPortal
 *
 * Free all gangs that were allocated for a specific portal
 * (could either be a cursor name or an unnamed portal)
 *
 * Be careful when moving gangs onto the available list, if
 * cleanupGang() tells us that the gang has a problem, the gang has
 * been free()ed and we should discard it -- otherwise it is good as
 * far as we can tell.
 */
void
freeGangsForPortal(char *portal_name)
{
	MemoryContext oldContext;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "freeGangsForPortal '%s'. Reader gang inventory: "
			 "allocatedN=%d availableN=%d allocated1=%d available1=%d",
			 (portal_name ? portal_name : "unnamed portal"),
			 list_length(allocatedReaderGangsN), list_length(availableReaderGangsN),
			 list_length(allocatedReaderGangs1), list_length(availableReaderGangs1));

	/*
	 * the primary and mirror writer gangs "belong" to the unnamed portal --
	 * if we have multiple active portals trying to release, we can't just
	 * release and re-release the writers each time !
	 */
	if (!portal_name)
	{
		if (!cleanupGang(primaryWriterGang))
		{
			primaryWriterGang = NULL;	/* cleanupGang called
										 * disconnectAndDestroyGang() already */
			disconnectAndDestroyAllGangs();

			elog(ERROR, "could not temporarily connect to one or more segments");

			return;
		}
	}

	if (GangContext)
		oldContext = MemoryContextSwitchTo(GangContext);
	else
		oldContext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Now we iterate through the list of allocated reader gangs
	 * and we free all the gangs that belong to the portal that
	 * was specified by our caller.
	 */
	if (allocatedReaderGangsN != NIL)
	{
		ListCell   *cur_item;
		ListCell   *prev_item = NULL;

		cur_item = list_head(allocatedReaderGangsN);

		while (cur_item != NULL)
		{
			Gang	   *gp = (Gang *) lfirst(cur_item);

			if (isTargetPortal(gp->portal_name, portal_name))
			{
				if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
					elog(LOG, "Returning a reader N-gang to the available list");

				/* cur_item must be removed */
				allocatedReaderGangsN = list_delete_cell(allocatedReaderGangsN, cur_item, prev_item);

				/* we only return the gang to the available list if it is good */
				if (cleanupGang(gp))
					availableReaderGangsN = lappend(availableReaderGangsN, gp);

				if (prev_item)
					cur_item = lnext(prev_item);
				else
					cur_item = list_head(allocatedReaderGangsN);
			}
			else
			{
				if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
					elog(LOG, "Skipping the release of a reader N-gang. It is used by another portal");

				/* cur_item must be preserved */
				prev_item = cur_item;
				cur_item = lnext(prev_item);
			}
		}
	}

	if (allocatedReaderGangs1 != NIL)
	{
		ListCell   *cur_item;
		ListCell   *prev_item = NULL;

		cur_item = list_head(allocatedReaderGangs1);

		while (cur_item != NULL)
		{
			Gang	   *gp = (Gang *) lfirst(cur_item);

			if (isTargetPortal(gp->portal_name, portal_name))
			{
				if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
					elog(LOG, "Returning a reader 1-gang to the available list");

				/* cur_item must be removed */
				allocatedReaderGangs1 = list_delete_cell(allocatedReaderGangs1, cur_item, prev_item);

				/* we only return the gang to the available list if it is good */
				if (cleanupGang(gp))
					availableReaderGangs1 = lappend(availableReaderGangs1, gp);

				if (prev_item)
					cur_item = lnext(prev_item);
				else
					cur_item = list_head(allocatedReaderGangs1);
			}
			else
			{
				if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
					elog(LOG, "Skipping the release of a reader 1-gang. It is used by another portal");

				/* cur_item must be preserved */
				prev_item = cur_item;
				cur_item = lnext(prev_item);
			}
		}
	}

	MemoryContextSwitchTo(oldContext);

	if (gp_log_gang >= GPVARS_VERBOSITY_VERBOSE)
	{
		if (allocatedReaderGangsN ||
			availableReaderGangsN ||
			allocatedReaderGangs1 ||
			availableReaderGangs1)
		{
			elog(LOG, "Gangs released for portal '%s'. Reader gang inventory: "
				 "allocatedN=%d availableN=%d allocated1=%d available1=%d",
				 (portal_name ? portal_name : "unnamed portal"),
				 list_length(allocatedReaderGangsN), list_length(availableReaderGangsN),
				 list_length(allocatedReaderGangs1), list_length(availableReaderGangs1));
		}
	}
}

/*
 * disconnectAndDestroyAllReaderGangs
 *
 * Here we destroy all reader gangs regardless of the portal they belong to.
 * TODO: This may need to be done more carefully when multiple cursors are
 * enabled.
 * If the parameter destroyAllocated is true, then destroy allocated as well as
 * available gangs.
 */
static void
disconnectAndDestroyAllReaderGangs(bool destroyAllocated)
{
	Gang	   *gp;

	if (allocatedReaderGangsN != NIL && destroyAllocated)
	{
		gp = linitial(allocatedReaderGangsN);
		while (gp != NULL)
		{
			disconnectAndDestroyGang(gp);
			allocatedReaderGangsN = list_delete_first(allocatedReaderGangsN);
			if (allocatedReaderGangsN != NIL)
				gp = linitial(allocatedReaderGangsN);
			else
				gp = NULL;
		}
	}
	if (availableReaderGangsN != NIL)
	{
		gp = linitial(availableReaderGangsN);
		while (gp != NULL)
		{
			disconnectAndDestroyGang(gp);
			availableReaderGangsN = list_delete_first(availableReaderGangsN);
			if (availableReaderGangsN != NIL)
				gp = linitial(availableReaderGangsN);
			else
				gp = NULL;
		}
	}
	if (allocatedReaderGangs1 != NIL && destroyAllocated)
	{
		gp = linitial(allocatedReaderGangs1);
		while (gp != NULL)
		{
			disconnectAndDestroyGang(gp);
			allocatedReaderGangs1 = list_delete_first(allocatedReaderGangs1);
			if (allocatedReaderGangs1 != NIL)
				gp = linitial(allocatedReaderGangs1);
			else
				gp = NULL;
		}
	}
	if (availableReaderGangs1 != NIL)
	{
		gp = linitial(availableReaderGangs1);
		while (gp != NULL)
		{
			disconnectAndDestroyGang(gp);
			availableReaderGangs1 = list_delete_first(availableReaderGangs1);
			if (availableReaderGangs1 != NIL)
				gp = linitial(availableReaderGangs1);
			else
				gp = NULL;
		}
	}
	if (destroyAllocated)
		allocatedReaderGangsN = NIL;
	availableReaderGangsN = NIL;

	if (destroyAllocated)
		allocatedReaderGangs1 = NIL;
	availableReaderGangs1 = NIL;
}


/*
 * Drop any temporary tables associated with the current session and
 * use a new session id since we have effectively reset the session.
 *
 * Call this procedure outside of a transaction.
 */
void
CheckForResetSession(void)
{
	int			oldSessionId = 0;
	int			newSessionId = 0;
	Oid			dropTempNamespaceOid;

	if (!NeedResetSession)
		return;

	/*
	 * Do the session id change early.
	 */
	if (NeedSessionIdChange)
	{
		/* If we have gangs, we can't change our session ID. */
		Assert(!gangsExist());

		oldSessionId = gp_session_id;
		ProcNewMppSessionId(&newSessionId);

		gp_session_id = newSessionId;
		gp_command_count = 0;

		/* Update the slotid for our singleton reader. */
		if (SharedLocalSnapshotSlot != NULL)
			SharedLocalSnapshotSlot->slotid = gp_session_id;

		elog(LOG, "The previous session was reset because its gang was disconnected (session id = %d). "
			 "The new session id = %d",
			 oldSessionId, newSessionId);

		NeedSessionIdChange = false;
	}

	if (IsTransactionOrTransactionBlock())
	{
		NeedResetSession = false;
		return;
	}


	dropTempNamespaceOid = OldTempNamespace;
	OldTempNamespace = InvalidOid;
	NeedResetSession = false;

	if (dropTempNamespaceOid != InvalidOid)
	{
		PG_TRY();
		{
			DropTempTableNamespaceForResetSession(dropTempNamespaceOid);
		}
		PG_CATCH();
		{
			/*
			 * But first demote the error to something much less
			 * scary.
			 */
			if (!elog_demote(WARNING))
			{
				elog(LOG, "unable to demote error");
				PG_RE_THROW();
			}

			EmitErrorReport();
			FlushErrorState();
		}
		PG_END_TRY();
	}

}

extern void
resetSessionForPrimaryGangLoss(void)
{
	if (ProcCanSetMppSessionId())
	{
		/*
		 * Not too early.
		 */
		NeedResetSession = true;
		NeedSessionIdChange = true;

		/*
		 * Keep this check away from transaction/catalog access, as we are
		 * possibly just after releasing ResourceOwner at the end of Tx.
		 * It's ok to remember uncommitted temporary namespace because
		 * DropTempTableNamespaceForResetSession will simply do nothing
		 * if the namespace is not visible.
		 */
		if (TempNamespaceOidIsValid())
		{
			/*
			 * Here we indicate we don't have a temporary table namespace
			 * anymore so all temporary tables of the previous session will
			 * be inaccessible.  Later, when we can start a new transaction,
			 * we will attempt to actually drop the old session tables to
			 * release the disk space.
			 */
			OldTempNamespace = ResetTempNamespace();

			elog(WARNING,
				 "Any temporary tables for this session have been dropped "
				 "because the gang was disconnected (session id = %d)",
				 gp_session_id);
		}
		else
			OldTempNamespace = InvalidOid;

	}

}

void
disconnectAndDestroyAllGangs(void)
{
	if (Gp_role == GP_ROLE_UTILITY)
		return;

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "disconnectAndDestroyAllGangs");

	/* for now, destroy all readers, regardless of the portal that owns them */
	disconnectAndDestroyAllReaderGangs(true);

	disconnectAndDestroyGang(primaryWriterGang);
	primaryWriterGang = NULL;

	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG)
		elog(LOG, "disconnectAndDestroyAllGangs done");
}

bool
gangOK(Gang *gp)
{
	int			i;

	if (gp == NULL)
		return false;

	if (gp->gang_id < 1 || gp->gang_id > 100000000
			|| gp->type > GANGTYPE_PRIMARY_WRITER
			|| (gp->size != getgpsegmentCount() && gp->size != 1))
		return false;

	/*
	 * Gang is direct-connect (no agents).
	 */

	for (i = 0; i < gp->size; i++)
	{
		SegmentDatabaseDescriptor *segdbDesc = &(gp->db_descriptors[i]);

		if (PQstatus(segdbDesc->conn) == CONNECTION_BAD)
			return false;
	}

	return true;
}


CdbComponentDatabases *
getComponentDatabases(void)
{
	Assert(Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_UTILITY);
	if(cdb_component_dbs == NULL)
	{
		cdb_component_dbs = getCdbComponentDatabases();
		cdb_component_dbs->fts_version = ftsProbeInfo->fts_statusVersion;
	}
	else if(cdb_component_dbs->fts_version != ftsProbeInfo->fts_statusVersion)
	{
		LOG_GANG_DEBUG(LOG, "FTS rescanned, get new component databases info.");
		freeCdbComponentDatabases(cdb_component_dbs);
		cdb_component_dbs = getCdbComponentDatabases();
		cdb_component_dbs->fts_version = ftsProbeInfo->fts_statusVersion;
	}

	return cdb_component_dbs;
}

bool
gangsExist(void)
{
	return (primaryWriterGang != NULL ||
			allocatedReaderGangsN != NIL ||
			availableReaderGangsN != NIL ||
			allocatedReaderGangs1 != NIL ||
			availableReaderGangs1 != NIL);
}

/*
 * the gang is working for portal p1. we are only interested in gangs
 * from portal p2. if p1 and p2 are the same portal return true. false
 * otherwise.
 */
static
bool
isTargetPortal(const char *p1, const char *p2)
{
	/* both are unnamed portals (represented as NULL) */
	if (!p1 && !p2)
		return true;

	/* one is unnamed, the other is named */
	if (!p1 || !p2)
		return false;

	/* both are the same named portal */
	if (strcmp(p1, p2) == 0)
		return true;

	return false;
}

#ifdef USE_ASSERT_CHECKING
/**
 * Assert that slicetable is valid. Must be called after ExecInitMotion, which sets up the slice table
 */
void
AssertSliceTableIsValid(SliceTable *st, struct PlannedStmt *pstmt)
{
	if (!st)
	{
		return;
	}

	Assert(st);
	Assert(pstmt);

	Assert(pstmt->nMotionNodes == st->nMotions);
	Assert(pstmt->nInitPlans == st->nInitPlans);

	ListCell *lc = NULL;
	int i = 0;

	int maxIndex = st->nMotions + st->nInitPlans + 1;

	Assert(maxIndex == list_length(st->slices));

	foreach (lc, st->slices)
	{
		Slice *s = (Slice *) lfirst(lc);

		/* The n-th slice entry has sliceIndex of n */
		Assert(s->sliceIndex == i && "slice index incorrect");

		/* The root index of a slice is either 0 or is a slice corresponding to an init plan */
		Assert((s->rootIndex == 0)
				|| (s->rootIndex > st->nMotions && s->rootIndex < maxIndex));

		/* Parent slice index */
		if (s->sliceIndex == s->rootIndex )
		{
			/* Current slice is a root slice. It will have parent index -1.*/
			Assert(s->parentIndex == -1 && "expecting parent index of -1");
		}
		else
		{
			/* All other slices must have a valid parent index */
			Assert(s->parentIndex >= 0 && s->parentIndex < maxIndex && "slice's parent index out of range");
		}

		/* Current slice's children must consider it the parent */
		ListCell *lc1 = NULL;
		foreach (lc1, s->children)
		{
			int childIndex = lfirst_int(lc1);
			Assert(childIndex >= 0 && childIndex < maxIndex && "invalid child slice");
			Slice *sc = (Slice *) list_nth(st->slices, childIndex);
			Assert(sc->parentIndex == s->sliceIndex && "slice's child does not consider it the parent");
		}

		/* Current slice must be in its parent's children list */
		if (s->parentIndex >= 0)
		{
			Slice *sp = (Slice *) list_nth(st->slices, s->parentIndex);

			bool found = false;
			foreach (lc1, sp->children)
			{
				int childIndex = lfirst_int(lc1);
				Assert(childIndex >= 0 && childIndex < maxIndex && "invalid child slice");
				Slice *sc = (Slice *) list_nth(st->slices, childIndex);

				if (sc->sliceIndex == s->sliceIndex)
				{
					found = true;
					break;
				}
			}

			Assert(found && "slice's parent does not consider it a child");
		}



		i++;
	}
}

#endif
