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
#include "miscadmin.h"			/* MyDatabaseId */
#include "cdb/cdbconn.h"		/* SegmentDatabaseDescriptor */
#include "cdb/cdbfts.h"
#include "cdb/cdbgang_thread.h"		/* me */
#include "cdb/cdbvars.h"		/* Gp_role, etc. */
#include <pthread.h>


#define LOG_GANG_DEBUG(...) do { \
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG) elog(__VA_ARGS__); \
    } while(false);

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
	int db_count;

	/*
	 * segdbDescPtrArray: Array of SegmentDatabaseDescriptor* 's that this thread is
	 * responsible for connecting to. Has size equal to db_count.
	 */
	SegmentDatabaseDescriptor **segdbDescPtrArray;

	/* type of gang. */
	GangType type;

	/* connect options. GUC etc. */
	char *connectOptions;

	/* The pthread_t thread handle. */
	pthread_t thread;
} DoConnectParms;

static DoConnectParms* makeConnectParms(int parmsCount, GangType type);
static void destroyConnectParms(DoConnectParms *doConnectParmsAr, int count);
static void *thread_DoConnect(void *arg);
extern Gang *createGang_thread(GangType type, int gang_id, int size, int content);

/*
 * Creates a new gang by logging on a session to each segDB involved.
 *
 * call this function in GangContext memory context.
 * elog ERROR or return a non-NULL gang.
 */
Gang *
createGang_thread(GangType type, int gang_id, int size, int content)
{
	Gang *newGangDefinition;
	SegmentDatabaseDescriptor *segdbDesc = NULL;
	DoConnectParms *doConnectParmsAr = NULL;
	DoConnectParms *pParms = NULL;
	int parmIndex = 0;
	int threadCount = 0;
	int i = 0;
	int create_gang_retry_counter = 0;
	int in_recovery_mode_count = 0;
	int successful_connections = 0;

	LOG_GANG_DEBUG(LOG, "createGang type = %d, gang_id = %d, size = %d, content = %d",
			type, gang_id, size, content);

	/* check arguments */
	Assert(size == 1 || size == getgpsegmentCount());
	Assert(CurrentResourceOwner != NULL);
	Assert(CurrentMemoryContext == GangContext);
	Assert(gp_connections_per_thread > 0);

create_gang_retry:
	/* If we're in a retry, we may need to reset our initial state, a bit */
	newGangDefinition = NULL;
	doConnectParmsAr = NULL;
	successful_connections = 0;
	in_recovery_mode_count = 0;
	threadCount = 0;

	/* Check the writer gang first. */
	if (type != GANGTYPE_PRIMARY_WRITER && !isPrimaryWriterGangAlive())
	{
		elog(LOG, "primary writer gang is broken");
		goto exit;
	}

	/* allocate and initialize a gang structure */
	newGangDefinition = buildGangDefinition(type, gang_id, size, content);
	Assert(newGangDefinition != NULL);
	Assert(newGangDefinition->size == size);
	Assert(newGangDefinition->perGangContext != NULL);
	MemoryContextSwitchTo(newGangDefinition->perGangContext);

	/*
	 * The most threads we could have is segdb_count / gp_connections_per_thread, rounded up.
	 * This is equivalent to 1 + (segdb_count-1) / gp_connections_per_thread.
	 * We allocate enough memory for this many DoConnectParms structures,
	 * even though we may not use them all.
	 */

	threadCount = 1 + (size - 1) / gp_connections_per_thread;
	Assert(threadCount > 0);

	/* initialize connect parameters */
	doConnectParmsAr = makeConnectParms(threadCount, type);
	for (i = 0; i < size; i++)
	{
		parmIndex = i / gp_connections_per_thread;
		pParms = &doConnectParmsAr[parmIndex];
		segdbDesc = &newGangDefinition->db_descriptors[i];
		pParms->segdbDescPtrArray[pParms->db_count++] = segdbDesc;
	}

	/* start threads and doing the connect */
	for (i = 0; i < threadCount; i++)
	{
		int pthread_err;
		pParms = &doConnectParmsAr[i];

		LOG_GANG_DEBUG(LOG,
				"createGang creating thread %d of %d for libpq connections",
				i + 1, threadCount);

		pthread_err = gp_pthread_create(&pParms->thread, thread_DoConnect, pParms, "createGang");
		if (pthread_err != 0)
		{
			int j;

			/*
			 * Error during thread create (this should be caused by resource
			 * constraints). If we leave the threads running, they'll
			 * immediately have some problems -- so we need to join them, and
			 * *then* we can issue our FATAL error
			 */
			for (j = 0; j < i; j++)
			{
				pthread_join(doConnectParmsAr[j].thread, NULL);
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

		if (0 != pthread_join(doConnectParmsAr[i].thread, NULL))
		{
			elog(FATAL, "could not create segworker group");
		}
	}

	/*
	 * Free the memory allocated for the threadParms array
	 */
	destroyConnectParms(doConnectParmsAr, threadCount);
	doConnectParmsAr = NULL;

	/* find out the successful connections and the failed ones */
	checkConnectionStatus(newGangDefinition, &in_recovery_mode_count,
			&successful_connections);

	LOG_GANG_DEBUG(LOG,"createGang: %d processes requested; %d successful connections %d in recovery",
			size, successful_connections, in_recovery_mode_count);

	MemoryContextSwitchTo(GangContext);

	if (size == successful_connections)
	{
		setLargestGangsize(size);
		return newGangDefinition;
	}

	/* there'er failed connections */

	/*
	 * If this is a reader gang and the writer gang is invalid, destroy all gangs.
	 * This happens when one segment is reset.
	 */
	if (type != GANGTYPE_PRIMARY_WRITER && !isPrimaryWriterGangAlive())
	{
		elog(LOG, "primary writer gang is broken");
		goto exit;
	}

	/* FTS shows some segment DBs are down, destroy all gangs. */
	if (isFTSEnabled() &&
		FtsTestSegmentDBIsDown(newGangDefinition->db_descriptors, size))
	{
		elog(LOG, "FTS detected some segments are down");
		goto exit;
	}

	disconnectAndDestroyGang(newGangDefinition);
	newGangDefinition = NULL;

	/* Writer gang is created before reader gangs. */
	if (type == GANGTYPE_PRIMARY_WRITER)
		Insist(!gangsExist());

	/* We could do some retry here */
	if (successful_connections + in_recovery_mode_count == size &&
		gp_gang_creation_retry_count &&
		create_gang_retry_counter++ < gp_gang_creation_retry_count)
	{
		LOG_GANG_DEBUG(LOG, "createGang: gang creation failed, but retryable.");

		/*
		 * On the first retry, we want to verify that we are
		 * using the most current version of the
		 * configuration.
		 */
		if (create_gang_retry_counter == 0)
			FtsNotifyProber();

		CHECK_FOR_INTERRUPTS();
		pg_usleep(gp_gang_creation_retry_timer * 1000);
		CHECK_FOR_INTERRUPTS();

		goto create_gang_retry;
	}

exit:
	if(newGangDefinition != NULL)
		disconnectAndDestroyGang(newGangDefinition);

	disconnectAndDestroyAllGangs(true);
	CheckForResetSession();
	ereport(ERROR,
			(errcode(ERRCODE_GP_INTERCONNECTION_ERROR), errmsg("failed to acquire resources on one or more segments")));
	return NULL;
}

/*
 *	Thread procedure.
 *	Perform the connect.
 */
static void *
thread_DoConnect(void *arg)
{
	DoConnectParms *pParms = (DoConnectParms *) arg;
	SegmentDatabaseDescriptor **segdbDescPtrArray = pParms->segdbDescPtrArray;
	int db_count = pParms->db_count;

	SegmentDatabaseDescriptor *segdbDesc = NULL;
	int i = 0;

	gp_set_thread_sigmasks();

	/*
	 * The pParms contains an array of SegmentDatabaseDescriptors
	 * to connect to.
	 */
	for (i = 0; i < db_count; i++)
	{
		char gpqeid[100];

		segdbDesc = segdbDescPtrArray[i];

		if (segdbDesc == NULL || segdbDesc->segment_database_info == NULL)
		{
			write_log("thread_DoConnect: bad segment definition during gang creation %d/%d\n", i, db_count);
			continue;
		}

		/*
		 * Build the connection string.  Writer-ness needs to be processed
		 * early enough now some locks are taken before command line options
		 * are recognized.
		 */
		build_gpqeid_param(gpqeid, sizeof(gpqeid), segdbDesc->segindex, pParms->type == GANGTYPE_PRIMARY_WRITER);

		/* check the result in createGang */
		cdbconn_doConnect(segdbDesc, gpqeid, pParms->connectOptions);
	}

	return (NULL);
} /* thread_DoConnect */


/*
 * Initialize a DoConnectParms structure.
 *
 * Including initialize the connect option string.
 */
static DoConnectParms* makeConnectParms(int parmsCount, GangType type)
{
	DoConnectParms *doConnectParmsAr = (DoConnectParms*) palloc0(
			parmsCount * sizeof(DoConnectParms));
	DoConnectParms* pParms = NULL;
	int segdbPerThread = gp_connections_per_thread;
	int i = 0;

	for (i = 0; i < parmsCount; i++)
	{
		pParms = &doConnectParmsAr[i];
		pParms->segdbDescPtrArray = (SegmentDatabaseDescriptor**) palloc0(
				segdbPerThread * sizeof(SegmentDatabaseDescriptor *));
		MemSet(&pParms->thread, 0, sizeof(pthread_t));
		pParms->db_count = 0;
		pParms->type = type;
		pParms->connectOptions = addOptions(type == GANGTYPE_PRIMARY_WRITER);
	}
	return doConnectParmsAr;
}

/*
 * Free all the memory allocated in DoConnectParms.
 */
static void destroyConnectParms(DoConnectParms *doConnectParmsAr, int count)
{
	if (doConnectParmsAr != NULL)
	{
		int i = 0;
		for (i = 0; i < count; i++)
		{
			DoConnectParms *pParms = &doConnectParmsAr[i];

			if (pParms->connectOptions != NULL)
			{
				pfree(pParms->connectOptions);
				pParms->connectOptions = NULL;
			}

			pfree(pParms->segdbDescPtrArray);
			pParms->segdbDescPtrArray = NULL;
		}

		pfree(doConnectParmsAr);
	}
}
