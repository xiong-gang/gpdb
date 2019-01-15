/*-------------------------------------------------------------------------
 *
 * fts.c
 *	  Process under QD postmaster polls the segments on a periodic basis
 *    or at the behest of QEs.
 *
 * Maintains an array in shared memory containing the state of each segment.
 *
 * Portions Copyright (c) 2005-2010, Greenplum Inc.
 * Portions Copyright (c) 2011, EMC Corp.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/fts/fts.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/gp_segment_config.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbfts.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/fts.h"
#include "postmaster/ftsprobe.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/pmsignal.h"			/* PostmasterIsAlive */
#include "storage/sinvaladt.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/catalog.h"

#include "catalog/gp_configuration_history.h"
#include "catalog/gp_segment_config.h"

#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/backendid.h"
#include "storage/bufmgr.h"
#include "executor/spi.h"

#include "tcop/tcopprot.h" /* quickdie() */

bool am_ftsprobe = false;
bool am_ftshandler = false;

extern bool gp_enable_master_autofailover;

#define MAX_ENTRYDB_COUNT 2

/*
 * STRUCTURES
 */

typedef struct MasterInfo
{
	int16 port;
	char *hostname;
	char *address;
	char *datadir;
} MasterInfo;

/*
 * STATIC VARIABLES
 */

static bool skipFtsProbe = false;

static volatile bool shutdown_requested = false;
static volatile bool probe_requested = false;
static volatile sig_atomic_t got_SIGHUP = false;

/*
 * FUNCTION PROTOTYPES
 */

#ifdef EXEC_BACKEND
static pid_t ftsprobe_forkexec(void);
#endif
NON_EXEC_STATIC void ftsMain(int argc, char *argv[]);
static void FtsLoop(void);

static CdbComponentDatabases *readCdbComponentInfoAndUpdateStatus();

/*
 * Main entry point for ftsprobe process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
ftsprobe_start(void)
{
	pid_t		FtsProbePID;

#ifdef EXEC_BACKEND
	switch ((FtsProbePID = ftsprobe_forkexec()))
#else
	switch ((FtsProbePID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork ftsprobe process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			shmFtsControl->ftsPid = getpid();
			ClosePostmasterPorts(false);

			ftsMain(0, NULL);
			break;
#endif
		default:
			return (int)FtsProbePID;
	}

	
	/* shouldn't get here */
	return 0;
}

/*=========================================================================
 * HELPER FUNCTIONS
 */


#ifdef EXEC_BACKEND
/*
 * ftsprobe_forkexec()
 *
 * Format up the arglist for the ftsprobe process, then fork and exec.
 */
static pid_t
ftsprobe_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkftsprobe";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif   /* EXEC_BACKEND */

static void
RequestShutdown(SIGNAL_ARGS)
{
	shutdown_requested = true;
}

/* SIGHUP: set flag to reload config file */
static void
sigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

/* SIGINT: set flag to indicate a FTS scan is requested */
static void
sigIntHandler(SIGNAL_ARGS)
{
	probe_requested = true;
}

/*
 * FtsProbeMain
 */
NON_EXEC_STATIC void
ftsMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;
	char	   *fullpath;

	IsUnderPostmaster = true;
	am_ftsprobe = true;

	/* Stay away from PMChildSlot */
	MyPMChildSlot = -1;

	/* reset MyProcPid */
	MyProcPid = getpid();
	
	/* Lose the postmaster's on-exit routines */
	on_exit_reset();

	/* Identify myself via ps */
	init_ps_display("ftsprobe process", "", "", "");

	SetProcessingMode(InitProcessing);

	/*
	 * reread postgresql.conf if requested
	 */
	pqsignal(SIGHUP, sigHupHandler);
	pqsignal(SIGINT, sigIntHandler);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, quickdie); /* we don't do any ftsprobe specific cleanup, just use the standard. */
	pqsignal(SIGALRM, SIG_IGN);

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	/* We don't listen for async notifies */
	pqsignal(SIGUSR2, RequestShutdown);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * Copied from bgwriter
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "FTS Probe");

	/* Early initialization */
	BaseInit();

	/* See InitPostgres()... */
	InitProcess();
	InitBufferPoolBackend();
	InitXLOGAccess();

	SetProcessingMode(NormalProcessing);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		AbortCurrentTransaction();

		/*
		 * We can now go away.	Note that because we'll call InitProcess, a
		 * callback will be registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	/*
	 * Add my PGPROC struct to the ProcArray.
	 *
	 * Once I have done this, I am visible to other backends!
	 */
	InitProcessPhase2();

	/*
	 * Initialize my entry in the shared-invalidation manager's array of
	 * per-backend data.
	 *
	 * Sets up MyBackendId, a unique backend identifier.
	 */
	MyBackendId = InvalidBackendId;

	SharedInvalBackendInit(false);

	if (MyBackendId > MaxBackends || MyBackendId <= 0)
		elog(FATAL, "bad backend id: %d", MyBackendId);

	/*
	 * bufmgr needs another initialization call too
	 */
	InitBufferPoolBackend();

	/* heap access requires the rel-cache */
	RelationCacheInitialize();
	InitCatalogCache();

	/*
	 * It's now possible to do real access to the system catalogs.
	 *
	 * Load relcache entries for the system catalogs.  This must create at
	 * least the minimum set of "nailed-in" cache entries.
	 */
	RelationCacheInitializePhase2();

	/*
	 * Start a new transaction here before first access to db, and get a
	 * snapshot.  We don't have a use for the snapshot itself, but we're
	 * interested in the secondary effect that it sets RecentGlobalXmin.
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	/*
	 * In order to access the catalog, we need a database, and a
	 * tablespace; our access to the heap is going to be slightly
	 * limited, so we'll just use some defaults.
	 */
	if (!FindMyDatabase(DB_FOR_COMMON_ACCESS, &MyDatabaseId, &MyDatabaseTableSpace))
		ereport(FATAL,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exit", DB_FOR_COMMON_ACCESS)));

	/* Now we can mark our PGPROC entry with the database ID */
	/* (We assume this is an atomic store so no lock is needed) */
	MyProc->databaseId = MyDatabaseId;

	fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace);

	SetDatabasePath(fullpath);

	RelationCacheInitializePhase3();

	/* close the transaction we started above */
	CommitTransactionCommand();

	/* main loop */
	FtsLoop();

	/* One iteration done, go away */
	proc_exit(0);
}

/*
 * Populate cdb_component_dbs object by reading from catalog.  Use
 * probeContext instead of current memory context because current
 * context will be destroyed by CommitTransactionCommand().
 */
static
CdbComponentDatabases *readCdbComponentInfoAndUpdateStatus()
{
	int i;
	CdbComponentDatabases *cdbs = cdbcomponent_getCdbComponents(false);

	for (i=0; i < cdbs->total_segment_dbs; i++)
	{
		CdbComponentDatabaseInfo *segInfo = &cdbs->segment_db_info[i];
		uint8	segStatus = 0;

		if (!SEGMENT_IS_ALIVE(segInfo))
			FTS_STATUS_SET_DOWN(segStatus);

		ftsProbeInfo->fts_status[segInfo->dbid] = segStatus;
	}

	/*
	 * Initialize fts_stausVersion after populating the config details in
	 * shared memory for the first time after FTS startup.
	 */
	if (ftsProbeInfo->fts_statusVersion == 0)
		ftsProbeInfo->fts_statusVersion++;

	return cdbs;
}

void
probeWalRepUpdateConfig(int16 dbid, int16 segindex, char role,
						bool IsSegmentAlive, bool IsInSync)
{
	AssertImply(IsInSync, IsSegmentAlive);

	/*
	 * Insert new tuple into gp_configuration_history catalog.
	 */
	{
		Relation histrel;
		HeapTuple histtuple;
		Datum histvals[Natts_gp_configuration_history];
		bool histnulls[Natts_gp_configuration_history] = { false };
		char desc[SQL_CMD_BUF_SIZE];

		histrel = heap_open(GpConfigHistoryRelationId,
							RowExclusiveLock);

		histvals[Anum_gp_configuration_history_time-1] =
				TimestampTzGetDatum(GetCurrentTimestamp());
		histvals[Anum_gp_configuration_history_dbid-1] =
				Int16GetDatum(dbid);
		snprintf(desc, sizeof(desc),
				 "FTS: update role, status, and mode for dbid %d with contentid %d to %c, %c, and %c",
				 dbid, segindex, role,
				 IsSegmentAlive ? GP_SEGMENT_CONFIGURATION_STATUS_UP :
				 GP_SEGMENT_CONFIGURATION_STATUS_DOWN,
				 IsInSync ? GP_SEGMENT_CONFIGURATION_MODE_INSYNC :
				 GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC
			);
		histvals[Anum_gp_configuration_history_desc-1] =
				CStringGetTextDatum(desc);
		histtuple = heap_form_tuple(RelationGetDescr(histrel), histvals, histnulls);
		simple_heap_insert(histrel, histtuple);
		CatalogUpdateIndexes(histrel, histtuple);

		SIMPLE_FAULT_INJECTOR(FtsUpdateConfig);

		heap_close(histrel, RowExclusiveLock);
	}

	/*
	 * Find and update gp_segment_configuration tuple.
	 */
	{
		Relation configrel;

		HeapTuple configtuple;
		HeapTuple newtuple;

		Datum configvals[Natts_gp_segment_configuration];
		bool confignulls[Natts_gp_segment_configuration] = { false };
		bool repls[Natts_gp_segment_configuration] = { false };

		ScanKeyData scankey;
		SysScanDesc sscan;

		configrel = heap_open(GpSegmentConfigRelationId,
							  RowExclusiveLock);

		ScanKeyInit(&scankey,
					Anum_gp_segment_configuration_dbid,
					BTEqualStrategyNumber, F_INT2EQ,
					Int16GetDatum(dbid));
		sscan = systable_beginscan(configrel, GpSegmentConfigDbidIndexId,
								   true, NULL, 1, &scankey);

		configtuple = systable_getnext(sscan);

		if (!HeapTupleIsValid(configtuple))
		{
			elog(ERROR, "FTS cannot find dbid=%d in %s", dbid,
				 RelationGetRelationName(configrel));
		}

		configvals[Anum_gp_segment_configuration_role-1] = CharGetDatum(role);
		repls[Anum_gp_segment_configuration_role-1] = true;

		configvals[Anum_gp_segment_configuration_status-1] =
			CharGetDatum(IsSegmentAlive ? GP_SEGMENT_CONFIGURATION_STATUS_UP :
										GP_SEGMENT_CONFIGURATION_STATUS_DOWN);
		repls[Anum_gp_segment_configuration_status-1] = true;

		configvals[Anum_gp_segment_configuration_mode-1] =
			CharGetDatum(IsInSync ? GP_SEGMENT_CONFIGURATION_MODE_INSYNC :
						 GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC);
		repls[Anum_gp_segment_configuration_mode-1] = true;

		newtuple = heap_modify_tuple(configtuple, RelationGetDescr(configrel),
									 configvals, confignulls, repls);
		simple_heap_update(configrel, &configtuple->t_self, newtuple);
		CatalogUpdateIndexes(configrel, newtuple);

		systable_endscan(sscan);
		pfree(newtuple);

		heap_close(configrel, RowExclusiveLock);
	}
}

/*
 * We say the standby is alive when one of the wal-sender processes is alive.
 */
static
bool standbyIsInSync()
{
	bool inSync = false;
	int i;

	LWLockAcquire(SyncRepLock, LW_SHARED);
	for (i = 0; i < max_wal_senders; i++)
	{
		if (WalSndCtl->walsnds[i].pid != 0)
		{
			inSync = (WalSndCtl->walsnds[i].state == WALSNDSTATE_STREAMING);
			break;
		}
	}
	LWLockRelease(SyncRepLock);

	return inSync;
}

/*
 * Master should notify standby before start master prober
 */
static
bool notifyStandbyMasterProberDbid(CdbComponentDatabases *cdbs, int dbid)
{
	CdbComponentDatabaseInfo *standby = NULL;
	char		message[50];
	int		i;

	for (i = 0; i < cdbs->total_entry_dbs; i++)
	{
		CdbComponentDatabaseInfo *cdb = &cdbs->entry_db_info[i];
		if (cdb->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
		{
			standby = cdb;
			break;
		}
	}

	if (standby == NULL || !standbyIsInSync())
		return false;

	snprintf(message, sizeof(message), FTS_MSG_NEW_MASTER_PROBER_FMT, dbid);
	return FtsWalRepMessageOneSegment(standby, message);
}

/*
 * Send master and standby information to segment to start master prober
 */
static
bool notifySegmentStartMasterProber(CdbComponentDatabases *cdbs, bool restart)
{
	char message[MASTER_PROBER_MESSAGE_MAXLEN];
	CdbComponentDatabaseInfo *master = NULL;
	CdbComponentDatabaseInfo *standby = NULL;
	int i;

	for (i = 0; i < cdbs->total_entry_dbs; i++)
	{
		CdbComponentDatabaseInfo *cdb = &cdbs->entry_db_info[i];
		if (cdb->preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
			master = cdb;
		else
			standby = cdb;
	}

	if (!standby || !master)
		return false;

	/*
	 * START_MASTER_PROBER;<is_restart>;<dbid>;<preferred_role>;<role>;<hostname>;<address>;
	 * <port>;<dbid>;<preferred_role>;<role>;<hostname>;<address>;<port>
	 */
	snprintf(message, sizeof(message),
			FTS_MSG_START_MASTER_PROBER_FMT,
			restart,
			master->dbid, master->preferred_role, master->role,
			master->hostname, master->address, master->port,
			standby->dbid, standby->preferred_role, standby->role,
			standby->hostname, standby->address, standby->port);
	return FtsWalRepMessageOneSegment(&cdbs->segment_db_info[0], message);
}

/*
 * Start master prober
 */
static
bool startMasterProber(CdbComponentDatabases *cdbs, bool restart)
{
	/* start the master prober on the primary segment of content 0 */
	int masterProberDbid = cdbs->segment_db_info[0].dbid;

	if (shmFtsControl->masterProberDBID != masterProberDbid)
	{
		if (!notifyStandbyMasterProberDbid(cdbs, masterProberDbid))
		{
			elog(LOG, "FTS: failed to notify standby the master prober");
			return false;
		}

		shmFtsControl->masterProberDBID = masterProberDbid;
	}

	if (!notifySegmentStartMasterProber(cdbs, restart))
	{
		elog(LOG, "FTS: failed to start master prober");
		return false;
	}

	elog(LOG, "FTS: start master prober on segment %d, is restart: %d", masterProberDbid, restart);
	return true;
}

static bool
ftsMessageNextParam(char **cpp, char **npp)
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

/*
 * Parse start message and insert into gp_segment_configuration
 */
static void
masterProberConfigInsert(Relation configrel, char **query)
{
	HeapTuple	configtuple;
	Datum		values[Natts_gp_segment_configuration];
	bool			isnull[Natts_gp_segment_configuration] = { false };
	char			role;
	char			preferredRole;
	int16		dbid;
	const char	*hostname;
	const char	*address;
	int32		port;
	char			*cp;
	bool			ret;

	/*
	 * START_MASTER_PROBER;<dbid>;<preferred_role>;<role>;<hostname>;<address>;
	 * <port>;<dbid>;<preferred_role>;<role>;<hostname>;<address>;<port>
	 */
	ret = ftsMessageNextParam(&cp, query);
	Assert(ret);
	dbid = (int16)strtol(cp, NULL, 10);

	ret = ftsMessageNextParam(&cp, query);
	Assert(ret);
	preferredRole = *cp;

	ret = ftsMessageNextParam(&cp, query);
	Assert(ret);
	role = *cp;

	ret = ftsMessageNextParam(&cp, query);
	Assert(ret);
	hostname = cp;

	ret = ftsMessageNextParam(&cp, query);
	Assert(ret);
	address = cp;

	ret = ftsMessageNextParam(&cp, query);
	Assert(ret);
	port = (int32)strtol(cp, NULL, 10);

	values[Anum_gp_segment_configuration_content - 1] = Int16GetDatum(-1);
	values[Anum_gp_segment_configuration_mode - 1] = CharGetDatum('s');
	values[Anum_gp_segment_configuration_status - 1] = CharGetDatum('u');
	values[Anum_gp_segment_configuration_datadir - 1] = CStringGetTextDatum("");

	values[Anum_gp_segment_configuration_preferred_role - 1] = CharGetDatum(preferredRole);
	values[Anum_gp_segment_configuration_dbid - 1] = Int16GetDatum(dbid);
	values[Anum_gp_segment_configuration_role - 1] = CharGetDatum(role);
	values[Anum_gp_segment_configuration_port - 1] = Int32GetDatum(port);
	values[Anum_gp_segment_configuration_hostname - 1] = CStringGetTextDatum(hostname);
	values[Anum_gp_segment_configuration_address - 1] = CStringGetTextDatum(address);

	configtuple = heap_form_tuple(RelationGetDescr(configrel), values, isnull);
	simple_heap_insert(configrel, configtuple);
	CatalogUpdateIndexes(configrel, configtuple);
	return;
}

/*
 * Clear gp_segment_configuration
 */
static
void masterProberConfigClear(Relation configrel)
{
	HeapTuple	tup;
	SysScanDesc scan;

	scan = systable_beginscan(configrel, InvalidOid, false, NULL, 0, NULL);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
		simple_heap_delete(configrel, &tup->t_self);

	systable_endscan(scan);
}

/*
 * Insert master and standby information into gp_segment_configuration
 */
static
void masterProberConfigInit()
{
	Relation configrel;
	ResourceOwner save;

	char	   *message = pstrdup(shmFtsControl->masterProberMessage);
	char	   *cp;

	ftsMessageNextParam(&cp, &message);
	Assert(strncmp(cp, FTS_MSG_START_MASTER_PROBER,
				   strlen(FTS_MSG_START_MASTER_PROBER)) == 0);
	ftsMessageNextParam(&cp, &message);

	save = CurrentResourceOwner;
	StartTransactionCommand();
	GetTransactionSnapshot();

	configrel = heap_open(GpSegmentConfigRelationId, RowExclusiveLock);
	masterProberConfigClear(configrel);
	masterProberConfigInsert(configrel, &message);
	masterProberConfigInsert(configrel, &message);
	heap_close(configrel, RowExclusiveLock);

	CommitTransactionCommand();
	CurrentResourceOwner = save;
}

/*
 * Check if master and standby information changed
 */
static
bool updateMasterInfomationForMasterProber(CdbComponentDatabases *cdbs, MasterInfo *masters)
{
	bool	masterInfoChanged = false;
	int		i;

	Assert(cdbs->total_entry_dbs == MAX_ENTRYDB_COUNT);

	for (i = 0; i < MAX_ENTRYDB_COUNT; i++)
	{
		if (masters[i].hostname == NULL ||
			strcmp(masters[i].hostname, cdbs->entry_db_info[i].hostname) != 0)
		{
			masters[i].hostname = pstrdup(cdbs->entry_db_info[i].hostname);
			masterInfoChanged = true;
		}

		if (masters[i].address == NULL ||
			strcmp(masters[i].address, cdbs->entry_db_info[i].address) != 0)
		{
			masters[i].address = pstrdup(cdbs->entry_db_info[i].address);
			masterInfoChanged = true;
		}

		if (masters[i].datadir == NULL ||
			strcmp(masters[i].datadir, cdbs->entry_db_info[i].datadir) != 0)
		{
			masters[i].datadir = pstrdup(cdbs->entry_db_info[i].datadir);
			masterInfoChanged = true;
		}

		if (masters[i].port !=  cdbs->entry_db_info[i].port)
		{
			masters[i].port = cdbs->entry_db_info[i].port;
			masterInfoChanged = true;
		}
	}

	return masterInfoChanged;
}

static
void FtsLoop()
{
	bool	updated_probe_state;
	MemoryContext probeContext = NULL, oldContext = NULL;
	time_t elapsed,	probe_start_time;
	CdbComponentDatabases *cdbs = NULL;
	bool	standbyInSync;
	bool	masterInfoChanged = true;
	bool	am_masterprober = !IS_QUERY_DISPATCHER();
	MasterInfo	*masterInfos = palloc0(sizeof(MasterInfo) * MAX_ENTRYDB_COUNT);

	probeContext = AllocSetContextCreate(TopMemoryContext,
										 "FtsProbeMemCtxt",
										 ALLOCSET_DEFAULT_INITSIZE,	/* always have some memory */
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	if (am_masterprober)
		masterProberConfigInit();

	while (!shutdown_requested)
	{
		bool		has_mirrors;
		bool		masterProberStarted = false;

		/* no need to live on if postmaster has died */
		if (!PostmasterIsAlive())
			exit(1);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		probe_start_time = time(NULL);

		/* Need a transaction to access the catalogs */
		StartTransactionCommand();
		cdbs = readCdbComponentInfoAndUpdateStatus();

		/* Check here gp_segment_configuration if has mirror's */
		if (am_masterprober)
			has_mirrors = gp_segment_config_has_standby();
		else
			has_mirrors = gp_segment_config_has_mirrors();

		/* close the transaction we started above */
		CommitTransactionCommand();

		/* is master or standby changed? */
		if (gp_enable_master_autofailover &&
			!am_masterprober &&
			cdbs->total_entry_dbs == MAX_ENTRYDB_COUNT)
			masterInfoChanged = updateMasterInfomationForMasterProber(cdbs, masterInfos);

		/* Reset this as we are performing the probe */
		probe_requested = false;
		skipFtsProbe = false;

#ifdef FAULT_INJECTOR
		if (SIMPLE_FAULT_INJECTOR(FtsProbe) == FaultInjectorTypeSkip)
			skipFtsProbe = true;
#endif

		if (skipFtsProbe || !has_mirrors)
		{
			elogif(gp_log_fts >= GPVARS_VERBOSITY_VERBOSE, LOG,
				   "skipping FTS probes due to %s",
				   !has_mirrors ? "no mirrors" : "fts_probe fault");

		}
		else
		{
			elogif(gp_log_fts == GPVARS_VERBOSITY_DEBUG, LOG,
				   "FTS: starting %s scan with %d segments and %d contents",
				   (probe_requested ? "full " : ""),
				   cdbs->total_segment_dbs,
				   cdbs->total_segments);
			/*
			 * We probe in a special context, some of the heap access
			 * stuff palloc()s internally
			 */
			oldContext = MemoryContextSwitchTo(probeContext);

			updated_probe_state = FtsWalRepMessageSegments(cdbs, am_masterprober, &masterProberStarted);

			MemoryContextSwitchTo(oldContext);

			/* free any pallocs we made inside probeSegments() */
			MemoryContextReset(probeContext);

			/* Bump the version if configuration was updated. */
			if (updated_probe_state)
				ftsProbeInfo->fts_statusVersion++;
		}

		standbyInSync = (cdbs->entry_db_info[0].mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC);
		if (!am_masterprober && cdbs->total_entry_dbs == 2 && standbyInSync != standbyIsInSync())
		{
			ResourceOwner save = CurrentResourceOwner;

			standbyInSync = !standbyInSync;
			StartTransactionCommand();
			GetTransactionSnapshot();
			if (standbyInSync)
			{
				probeWalRepUpdateConfig(cdbs->entry_db_info[0].dbid, -1, 'p', true, true);
				probeWalRepUpdateConfig(cdbs->entry_db_info[1].dbid, -1, 'm', true, true);
			}
			else
			{
				probeWalRepUpdateConfig(cdbs->entry_db_info[0].dbid, -1, 'p', true, false);
				probeWalRepUpdateConfig(cdbs->entry_db_info[1].dbid, -1, 'm', false, false);
			}
			CommitTransactionCommand();
			CurrentResourceOwner = save;
		}

		/* Start the master prober if it's not started or the master and standby information has changed */
		if (gp_enable_master_autofailover &&
			!am_masterprober &&
			cdbs->total_entry_dbs == MAX_ENTRYDB_COUNT &&
			(!masterProberStarted || masterInfoChanged))
		{
			/* The primary/mirror segment of content 0 may have changed, get the up-to-date information */
			StartTransactionCommand();
			cdbs = readCdbComponentInfoAndUpdateStatus();
			CommitTransactionCommand();
			startMasterProber(cdbs, masterInfoChanged);
		}

		/* free current components info and free ip addr caches */	
		cdbcomponent_destroyCdbComponents();

		/* Notify any waiting backends about probe cycle completion. */
		ftsProbeInfo->probeTick++;

		/* check if we need to sleep before starting next iteration */
		elapsed = time(NULL) - probe_start_time;
		if (elapsed < gp_fts_probe_interval && !shutdown_requested)
		{
			if (!probe_requested)
				pg_usleep((gp_fts_probe_interval - elapsed) * USECS_PER_SEC);

			CHECK_FOR_INTERRUPTS();
		}
	} /* end server loop */

	return;
}

/*
 * Check if FTS is active
 */
bool
FtsIsActive(void)
{
	return (!skipFtsProbe && !shutdown_requested);
}

/*
 * Start master prober when 'startMasterProber' is set.
 */
bool
shouldStartMasterProber(void)
{
	return shmFtsControl->startMasterProber;
}
