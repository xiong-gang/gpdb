/*-------------------------------------------------------------------------
 *
 * seqserver.c
 *	  Process under QD postmaster used for doling out sequence values to
 * 		QEs.
 *
 *
 * Copyright (c) 2006-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>

#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbselect.h"
#include "commands/sequence.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/deadlockdetector.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"			/* PostmasterIsAlive */
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"
#include "storage/backendid.h"
#include "utils/syscache.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbvars.h"
#include "gp-libpq-fe.h"
#include "libpq/libpq-be.h"
#include "executor/spi.h"

#include "tcop/tcopprot.h"

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#define SHUT_RDWR SD_BOTH
#define SHUT_RD SD_RECEIVE
#define SHUT_WR SD_SEND
#endif


 /*
  * backlog for listen() call:
  */
#define LISTEN_BACKLOG 64
#define getInt16(pNetData)	  ntohs(*((int16 *)(pNetData)))
#define getInt32(pNetData)	  ntohl(*((int32 *)(pNetData)))
#define getOid(pNetData)	

/* each session have one GNode */
typedef struct GNode
{
	/* request lock info */
	int lockType; 
	int database; 
	int relation; 
	int page; 
	int tuple; 
	int transactionId; 

	/* requester info */
	int transaction; 
	int pid; 
	int mppSessionId; 

	/* holder info */
	int holderTransaction;
	int holderPid;
	int holderMppSessionId;

	/* this request happens on segment: */
	int gpSegmentId;

	struct GNode *pHolder;
	/* request lock is hold by session */
	List *lRequester;
}GNode;

typedef struct SNode
{
	int sessionId;
	int inDegree;
	int outDegree;
	List *lIn;
	List *lOut;
}SNode;


List *glSNode;
//typedef struct GEdge
//{
//	GNode *from; 
//	GNode *to;
//}GEdge;



/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */


#ifdef EXEC_BACKEND
static pid_t deadlockdetector_forkexec(void);
#endif
NON_EXEC_STATIC void DeadLockDetectorMain(int argc, char *argv[]);

static void DeadLockDetectorLoop(void);
static void doDeadLockCheck(void);
char * FindSuperuser(bool try_bootstrap);
static char *ddtUser = NULL;
static void dummyprint(char *fmt, ...);
GNode *makeHolder(List **lGNode, int mppSessionId, int gpSegmentId, int pid);
GNode *makeRequester(List **lGNode, char *lockType, 
			   int database, int relation, int page, 
			   int tuple, int transactionId, 
			   int transaction, int pid, int mppSessionId, 
			   int holderTransaction, int holderPid, int holderMppSessionId, 
			   int gpSegmentId, GNode *pHolder);
static bool findCycle(void);

static GNode *searchGNodeInList(List *lGNode, int gpSegmentId, int pid);

static SNode *getSNodeFromList(int mppSessionId);
static void addSessionDependency(List *lSNode, int sessionId, int dependSessionId);

static bool removeNoInDegreeNodeOneLoop(List **lGNode);
static void removeNoInDegreeNode(List **lGNode);
static void cancelSession(int sessionId);
static void cancelDeadlockCycles(void);
/*=========================================================================
 * GLOBAL STATE VARIABLES
 */
/* our timeout value for select() and other socket operations. */

uint8 *inputBuff;

/*=========================================================================
 * VISIBLE FUNCTIONS
 */
//int SeqServerShmemSize(void)
//{
//	/* just our one int4 */
//	return sizeof(SeqServerControlBlock);
//}
//
//void SeqServerShmemInit(void)
//{
//	bool		found;
//
//	/* Create or attach to the SharedSnapshot shared structure */
//	seqServerCtl = (SeqServerControlBlock *) ShmemInitStruct("Sequence Server", SeqServerShmemSize(), &found);
//}

/*
 * Main entry point for seqserver controller process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
deadlockdetector_start(void)
{
	pid_t		DeadLockDetectorPID;

#ifdef EXEC_BACKEND
	switch ((DeadLockDetectorPID = deadlockdetector_forkexec()))
#else
	switch ((DeadLockDetectorPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork dead lock detector process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			DeadLockDetectorMain(0, NULL);
			break;
#endif
		default:
			return (int) DeadLockDetectorPID;
	}

	/* shouldn't get here */
	return 0;
}



/*=========================================================================
 * HELPER FUNCTIONS
 */


#ifdef EXEC_BACKEND
/*
 * deadlockdetector_forkexec()
 *
 * Format up the arglist for the serqserver process, then fork and exec.
 */
static pid_t
deadlockdetector_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--deadlockdetector";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif   /* EXEC_BACKEND */

static bool shutdown_requested=false;

static void
RequestShutdown(SIGNAL_ARGS)
{
	shutdown_requested = true;
}

static char *knownDatabase = "postgres";

/*
 * AutoVacMain
 */
NON_EXEC_STATIC void
DeadLockDetectorMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;
	char	   *fullpath;

	IsUnderPostmaster = true;

	/* Stay away from PMChildSlot */
	MyPMChildSlot = -1;

	/* reset MyProcPid */
	MyProcPid = getpid();
	
	/* Lose the postmaster's on-exit routines */
	on_exit_reset();

	/* Identify myself via ps */
	init_ps_display("deadlockdetector process", "", "", "");

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.	We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 *
	 * Currently, we don't pay attention to postgresql.conf changes that
	 * happen during a single daemon iteration, so we can ignore SIGHUP.
	 */
	pqsignal(SIGHUP, SIG_IGN);

	/*
	 * Presently, SIGINT will lead to autovacuum shutdown, because that's how
	 * we handle ereport(ERROR).  It could be improved however.
	 */
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, quickdie); /* we don't do any seq-server specific cleanup, just use the standard. */
	pqsignal(SIGALRM, handle_sig_alarm);

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	/* We don't listen for async notifies */
	pqsignal(SIGUSR2, RequestShutdown);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

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
	 * The following additional initialization allows us to call the persistent meta-data modules.
	 */

	/*
	 * Create a resource owner to keep track of our resources (currently only
	 * buffer pins).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "DeadLockDetector");

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
	 * In order to access the catalog, we need a database, and a
	 * tablespace; our access to the heap is going to be slightly
	 * limited, so we'll just use some defaults.
	 */
	if (!FindMyDatabase(knownDatabase, &MyDatabaseId, &MyDatabaseTableSpace))
		ereport(FATAL,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exit", knownDatabase)));

	/* Now we can mark our PGPROC entry with the database ID */
	/* (We assume this is an atomic store so no lock is needed) */
	MyProc->databaseId = MyDatabaseId;

	fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace);

	SetDatabasePath(fullpath);

	RelationCacheInitializePhase3();



	ddtUser = FindSuperuser(true);
	MyProcPort = (Port*)malloc(sizeof(Port));
	MyProcPort->user_name = ddtUser;
	MyProcPort->database_name = knownDatabase;


	DeadLockDetectorLoop();

	ResourceOwnerRelease(CurrentResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 false, true);							

	/* One iteration done, go away */
	proc_exit(0);
}

static void
DeadLockDetectorLoop(void)
{
        int sleeptime = 100;   
	for(;;)
	{
		int i = 1;
		CHECK_FOR_INTERRUPTS();

		if (shutdown_requested)
			break;
		
		/* no need to live on if postmaster has died */
		if (!PostmasterIsAlive(true))
			exit(1);
	
		usleep(sleeptime);

		if(i)
		{
			StartTransactionCommand();
			doDeadLockCheck();
			CommitTransactionCommand();
		}
	}

	return;
}

static void
doDeadLockCheck(void)
{
	int		i, j;
	int 	resultCount = 0;
	struct pg_result **results = NULL;
	StringInfoData errbuf;
	StringInfoData strres;
	List *lGNode = NULL;
	int lsegid = -2;

	Assert(glSNode == NULL);

        const char *sql = "select a.*, b.mppsessionid as hold_mppsessionid, b.pid as hold_pid, "
                          " b.transaction as hold_transaction, b.mode as hold_mode from"	
                          " pg_locks a join pg_locks b on"
                          " a.locktype=b.locktype and"
                          " a.granted='f' and b.granted='t' and"
                          " a.gp_segment_id=b.gp_segment_id and"
                          " a.pid <> b.pid and"
                          " ((a.locktype='transactionid' and a.transactionid=b.transactionid) or"
                          "  (a.locktype='tuple' and a.database=b.database and a.relation=b.relation and a.page=b.page and a.tuple=b.tuple)  or"
                          "  (a.locktype='relation' and a.database=b.database and a.relation=b.relation))"
                          " order by b.gp_segment_id;"; 
	initStringInfo(&errbuf);
	initStringInfo(&strres);

	results = cdbdisp_dispatchRMCommand(sql, false, &errbuf, &resultCount);

	if (errbuf.len > 0)
		ereport(ERROR, (errmsg("pg_highest_oid error (gathered %d results from cmd '%s')", resultCount, sql), errdetail("%s", errbuf.data)));
										
	for (i = 0; i < resultCount; i++)
	{
		struct pg_result *res = results[i];
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			elog(ERROR,"dboid: resultStatus not tuples_Ok");
		}
		else
		{
			int nTup = PQntuples(res);
			int i_locktype = PQfnumber(res, "locktype");
			int i_database = PQfnumber(res, "database");
			int i_relation = PQfnumber(res, "relation");
			int i_page = PQfnumber(res, "page");
			int i_tuple = PQfnumber(res, "tuple");
			int i_transactionid = PQfnumber(res, "transactionid");
			int i_transaction = PQfnumber(res, "transaction");
			int i_hold_transaction = PQfnumber(res, "hold_transaction");
			int i_pid = PQfnumber(res, "pid");
			int i_hold_pid = PQfnumber(res, "hold_pid");
			int i_mode = PQfnumber(res, "mode");
			int i_mppsessionid = PQfnumber(res, "mppsessionid");
			int i_hold_mppsessionid = PQfnumber(res, "hold_mppsessionid");
			int i_hold_mode = PQfnumber(res, "hold_mode");
			int i_gp_segment_id = PQfnumber(res, "gp_segment_id");
			for(j = 0; j < nTup; j++)
			{
				char *lockType = PQgetvalue(res, j, i_locktype);
				char *mode = PQgetvalue(res, j, i_mode);
				char *holdMode = PQgetvalue(res, j, i_hold_mode);
				int database = atol(PQgetvalue(res, j, i_database));
				int relation = atol(PQgetvalue(res, j, i_relation));
				int page = atol(PQgetvalue(res, j, i_page));
				int tuple = atol(PQgetvalue(res, j, i_tuple));
				int transactionId = atol(PQgetvalue(res, j, i_transactionid));
				int transaction = atol(PQgetvalue(res, j, i_transaction));
				int holdTransaction = atol(PQgetvalue(res, j, i_hold_transaction));
				int pid = atol(PQgetvalue(res, j, i_pid));
				int holdPid = atol(PQgetvalue(res, j, i_hold_pid));
				int mppSessionId = atol(PQgetvalue(res, j, i_mppsessionid));
				int holdMppSessionId = atol(PQgetvalue(res, j, i_hold_mppsessionid));
				int gpSegmentId = atol(PQgetvalue(res, j, i_gp_segment_id));


                                appendStringInfo(&strres, "nth:%d, locktype:%s, database:%d, relation:%d, page:%d, tuple:%d, transactionid:%d, transaction:%d, pid:%d, session:%d, holdtrans:%d, holdpid:%d, holdesession:%d, segment:%d", j, lockType, database, relation, page, tuple, transactionId, transaction, pid, mppSessionId, holdTransaction, holdPid, holdMppSessionId, gpSegmentId);

				if (lsegid == -2)
				{
					lsegid = gpSegmentId;
				}
				else if (lsegid != gpSegmentId)
				{
					lsegid = gpSegmentId;	
					list_free(lGNode);
					lGNode = NULL;
                                        resetStringInfo(&strres);
				}
			
				GNode *pHolder = makeHolder(&lGNode, holdMppSessionId, gpSegmentId, holdPid);
				Assert(pHolder != NULL);
				GNode *pRequester = makeRequester(&lGNode, lockType, database, relation, page, 
												  tuple, transactionId, transaction, pid, mppSessionId, 
												  holdTransaction, holdPid, holdMppSessionId, gpSegmentId, pHolder);
				Assert(pRequester != NULL);
				dummyprint("test", holdMode, mode, pHolder, pRequester);
			}
		}
	}

	pfree(errbuf.data);

	for (i = 0; i < resultCount; i++)
		PQclear(results[i]);

	free(results);

	findCycle();
}

GNode *makeHolder(List **lGNode, int mppSessionId, int gpSegmentId, int pid)
{
	GNode *gn = NULL;
	gn = searchGNodeInList(*lGNode, gpSegmentId, pid);
	if (NULL == gn) 
	{
		gn = malloc(sizeof(GNode));
		memset(gn, 0, sizeof(GNode));	
		*lGNode = lappend(*lGNode, gn);
	}
	gn->gpSegmentId = gpSegmentId;
	gn->pid = pid;
	gn->mppSessionId = mppSessionId;
	return gn;
}

GNode *makeRequester(List **lGNode, char *lockType, 
			   int database, int relation, int page, 
			   int tuple, int transactionId, 
			   int transaction, int pid, int mppSessionId, 
			   int holderTransaction, int holderPid, int holderMppSessionId, 
			   int gpSegmentId, GNode *pHolder)
{
	const char *LockTypeTransactionId = "transactionid";
	#define LOCK_TYPE_TRANSACTIONID 0
	#define LOCK_TYPE_OTHER 1

	Assert(pHolder != NULL);
	
	int iLockType = -1;
	GNode *gn = NULL;
	
	if (!strncmp(lockType, LockTypeTransactionId, strlen(LockTypeTransactionId)))
	{
		iLockType = LOCK_TYPE_TRANSACTIONID;		
	}
	else
	{
		iLockType = LOCK_TYPE_OTHER;		
	}

	gn = searchGNodeInList(*lGNode, gpSegmentId, pid);
	if (NULL == gn) 
	{
		gn = malloc(sizeof(GNode));
		memset(gn, 0, sizeof(GNode));
		*lGNode = lappend(*lGNode, gn);
	}

	gn->lockType = iLockType;		
	gn->transactionId = transactionId;
	gn->database = database;
	gn->relation = relation;
	gn->page = page;
	gn->tuple = tuple;

	gn->transaction = transaction;
	gn->pid = pid;
	gn->mppSessionId = mppSessionId;

	gn->holderTransaction = holderTransaction;
	gn->holderPid = holderPid;
	gn->holderMppSessionId = holderMppSessionId;

	gn->gpSegmentId = gpSegmentId;

	if (iLockType == LOCK_TYPE_TRANSACTIONID)
	{
		gn->pHolder = pHolder;
		addSessionDependency(glSNode, gn->mppSessionId, pHolder->mppSessionId);

		ListCell *cell = NULL;
		foreach (cell, gn->lRequester)
		{
			GNode *gnr = (GNode *)lfirst(cell);
			Assert(gnr->pHolder == NULL || gnr->pHolder == pHolder);
			gnr->pHolder = pHolder;
			addSessionDependency(glSNode, gnr->mppSessionId, pHolder->mppSessionId);
		}

		list_free(gn->lRequester);
		gn->lRequester = NULL;
	}
	else
	{
		if (pHolder->pHolder != NULL) 		
		{
			gn->pHolder = pHolder->pHolder;		
			addSessionDependency(glSNode, gn->mppSessionId, pHolder->pHolder->mppSessionId);

			ListCell *cell = NULL;
			foreach (cell, gn->lRequester)
			{
				GNode *gnr = (GNode *)lfirst(cell);
				Assert(gnr->pHolder == NULL || gnr->pHolder == pHolder->pHolder);
				gnr->pHolder = pHolder->pHolder;
				addSessionDependency(glSNode, gnr->mppSessionId, pHolder->pHolder->mppSessionId);
			}
		}
		else
		{
			pHolder->lRequester = lappend(pHolder->lRequester, gn);

			ListCell *cell = NULL;
			foreach (cell, gn->lRequester)
			{
				GNode *gnr = (GNode *)lfirst(cell);
				pHolder->lRequester = lappend(pHolder->lRequester, gnr);
			}
		}
	}

	return gn;
}

GNode *searchGNodeInList(List *lGNode, int gpSegmentId, int pid)
{
	ListCell *cell = NULL;
	foreach(cell, lGNode)
	{
		GNode *gn = (GNode *)lfirst(cell);
		if (gn->gpSegmentId == gpSegmentId && gn->pid == pid) 
			return gn;
	}
	return NULL;
}

void addSessionDependency(List *lSNode, int sessionId, int dependSessionId)
{
	SNode *from = getSNodeFromList(sessionId);	
	SNode *to = getSNodeFromList(dependSessionId);	
	from->lOut = lappend(from->lOut, to);
	from->outDegree++;

	to->lIn = lappend(to->lIn, from);
	to->inDegree++;
}

SNode *getSNodeFromList(int mppSessionId)
{
	ListCell *cell = NULL;
	SNode *sn = NULL;
	foreach(cell, glSNode)
	{
		sn = (SNode *)lfirst(cell);
		if (sn->sessionId == mppSessionId)
			return sn;
	}
	
	sn = (SNode *)malloc(sizeof(SNode));
	memset(sn, 0, sizeof(SNode));	
	sn->sessionId = mppSessionId;

	glSNode = lappend(glSNode, sn);	

	return sn;
}

bool findCycle(void)
{
	if (NULL == glSNode)
	{
		return false;
	}

	removeNoInDegreeNode(&glSNode);
	
	/* find the sessions need to be cancelled */	
	cancelDeadlockCycles();
	return true;
}


void removeNoInDegreeNode(List **glSNode)
{
	while(removeNoInDegreeNodeOneLoop(glSNode));
	return;
}

bool removeNoInDegreeNodeOneLoop(List **glSNode)
{
	ListCell *cell = list_head(*glSNode);
	ListCell *prev = NULL;
	bool found = false;
	while(NULL != cell)
	{
		SNode *sn = (SNode *)lfirst(cell);
		if (sn->inDegree == 0 || sn->outDegree == 0)
		{
			if (sn->inDegree == 0)
			{
				List *out = sn->lOut; 
				if (out != NULL)
				{
					ListCell *cell = NULL;
					foreach(cell, out)
					{
						SNode *outNode = (SNode *)lfirst(cell);
						outNode->lIn = list_delete(outNode->lIn, sn);
						outNode->inDegree--;
					}
				}
			}
			else
			{
				List *in = sn->lIn; 
				if (in != NULL)
				{
					ListCell *cell = NULL;
					foreach(cell, in)
					{
						SNode *inNode = (SNode *)lfirst(cell);
						inNode->lOut = list_delete(inNode->lOut, sn);
						inNode->outDegree--;
					}
				}
			}

			ListCell *tmp = lnext(cell);
			*glSNode = list_delete_cell(*glSNode, cell, prev);
			cell = tmp;
			found = true;
		}
		else
		{
			prev = cell;
			cell = lnext(cell);
		}
	}

	return found;
}

static void cancelDeadlockCycles(void)
{
	ListCell *cell = NULL;
	SNode *cancelSN = NULL;
	int maxInDegree = 0;

	if (glSNode == NULL)
		return;

	foreach(cell, glSNode)
	{
		SNode *sn = (SNode *)lfirst(cell);	
		if (sn->inDegree > maxInDegree)
		{
			maxInDegree = sn->inDegree;			
			cancelSN = sn;
		}
	}

	Assert(cancelSN != NULL);
	cancelSession(cancelSN->sessionId);
	list_free(glSNode);
	glSNode = NULL;

	return;
}

static void cancelSession(int sessionId)
{
	StringInfoData buffer;
	bool connected = false;
	int ret = 0;
	
	initStringInfo(&buffer);
	
	const char *sql = "select procpid from pg_stat_activity where sess_id=%d and current_query not like '%<IDLE>%'"; 
	appendStringInfo(&buffer, sql, sessionId);
	
	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR, (errcode(ERRCODE_CDB_INTERNAL_ERROR),
					errmsg("Unable to connect to execute internal query.")));
		}
		connected = true;

		/* Do the query. */
		ret = SPI_execute(buffer.data, false, 0);
		//Assert(SPI_processed == 1);
		if (ret > 0 && SPI_processed == 1 && SPI_tuptable != NULL)
		{
			TupleDesc tupdesc = SPI_tuptable->tupdesc;
			SPITupleTable *tuptable = SPI_tuptable;
			HeapTuple 	tuple = tuptable->vals[0];
			char *valProcPid = SPI_getvalue(tuple, tupdesc, 1);
			int procPid= pg_atoi(valProcPid, sizeof(int32), 0);
			kill(procPid, 2);
		}
	
		connected = false;
		SPI_finish();
	}
	/* Clean up in case of error. */
	PG_CATCH();
	{
		if (connected)
			SPI_finish();

		/* Carry on with error handling. */
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void dummyprint(char *fmt, ...)
{

}

char *
FindSuperuser(bool try_bootstrap)
{
	char *suser = NULL;
	Relation auth_rel;
	HeapTuple	auth_tup;
	cqContext  *pcqCtx;
	cqContext	cqc;
	bool	isNull;

	auth_rel = heap_open(AuthIdRelationId, AccessShareLock);

	if (try_bootstrap)
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), auth_rel),
				cql("SELECT * FROM pg_authid "
					" WHERE rolsuper = :1 "
					" AND rolcanlogin = :2 "
					" AND oid = :3 ",
					BoolGetDatum(true),
					BoolGetDatum(true),
					ObjectIdGetDatum(BOOTSTRAP_SUPERUSERID)));
	}
	else
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), auth_rel),
				cql("SELECT * FROM pg_authid "
					" WHERE rolsuper = :1 "
					" AND rolcanlogin = :2 ",
					BoolGetDatum(true),
					BoolGetDatum(true)));
	}

	while (HeapTupleIsValid(auth_tup = caql_getnext(pcqCtx)))
	{
		Datum	attrName;
		Oid		userOid;
		Datum	validuntil;

		validuntil = heap_getattr(auth_tup, Anum_pg_authid_rolvaliduntil,
								  RelationGetDescr(auth_rel), &isNull);
		/* we actually want it to be NULL, that means always valid */
		if (!isNull)
			continue;

		attrName = heap_getattr(auth_tup, Anum_pg_authid_rolname,
								RelationGetDescr(auth_rel), &isNull);
		Assert(!isNull);
		suser = pstrdup(DatumGetCString(attrName));

		userOid = HeapTupleGetOid(auth_tup);
		SetSessionUserId(userOid, true);

		break;
	}

	caql_endscan(pcqCtx);
	heap_close(auth_rel, AccessShareLock);
	return suser;
}


