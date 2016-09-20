/* ----------
 * pgstat.c
 *
 *	All the statistics collector stuff hacked up in one big, ugly file.
 *
 *	TODO:	- Separate collector, postmaster and backend stuff
 *			  into different files.
 *
 *			- Add some automatic call for pgstat vacuuming.
 *
 *			- Add a pgstat config column to pg_database, so this
 *			  entire thing can be enabled/disabled on a per db basis.
 *
 *	Copyright (c) 2001-2009, PostgreSQL Global Development Group
 *
 *	$PostgreSQL: pgsql/src/backend/postmaster/pgstat.c,v 1.169.2.2 2009/10/02 22:50:03 tgl Exp $
 * ----------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "xmanager.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "libpq/pqsignal.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "executor/instrument.h"
#include "pg_trace.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/backendid.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "cdb/cdbvars.h"
#include "gp-libpq-fe.h"
#include "gp-libpq-int.h"
#include "cdb/cdbgang.h"
#include "nodes/pg_list.h"

// for mkdir
#include "sys/stat.h"
#include "utils/guc_tables.h"

static volatile bool need_exit = false;
static volatile bool got_SIGHUP = false;

NON_EXEC_STATIC void XmanagerMain(int argc, char *argv[]);
static void xmanager_exit(SIGNAL_ARGS);
static void xmanager_handler(SIGNAL_ARGS);

#define XM_SELECT_TIMEOUT 5

typedef struct XMQE
{
	int port;
	int pid;
	int sessionId;
	bool isWriter;
	bool finished;
	PGconn *conn;
	PQExpBufferData *buffer;
} XMQE;

typedef struct XMConnection
{
	char type;
	int sock;
	int len;
	StringInfoData buf;
	pthread_t thread;
	int qeCount;
	List *qes;
}XMConnection;


List *g_qeList = NULL;


/*
 * pgstat_start() -
 *
 *	Called from postmaster at startup or after an existing collector
 *	died.  Attempt to fire up a fresh statistics collector.
 *
 *	Returns PID of child process, or 0 if fail.
 *
 *	Note: if fail, we will be called again from the postmaster main loop.
 */
int xmanager_start(void)
{
	pid_t xmPid;

	/*
	 * Okay, fork off the collector.
	 */
	switch ((xmPid = fork_process()))
	{
	case -1:
		ereport(LOG, (errmsg("could not fork statistics collector: %m")));
		return 0;

#ifndef EXEC_BACKEND
	case 0:
		/* in postmaster child ... */
		/* Close the postmaster's sockets */
		ClosePostmasterPorts(false);

		/* Lose the postmaster's on-exit routines */
		on_exit_reset();

		/* Drop our connection to postmaster's shared memory, as well */
		PGSharedMemoryDetach();

		XmanagerMain(0, NULL);
		break;
#endif

	default:
		return (int) xmPid;
	}

	/* shouldn't get here */
	return 0;
}

static void addOneOption(StringInfo string, struct config_generic * guc)
{
	Assert(guc && (guc->flags & GUC_GPDB_ADDOPT));
	switch (guc->vartype)
	{
	case PGC_BOOL:
	{
		struct config_bool *bguc = (struct config_bool *) guc;

		appendStringInfo(string, " -c %s=%s", guc->name, *(bguc->variable) ? "true" : "false");
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
		int i;

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

static char*
getOptions(void)
{
	struct config_generic **gucs = get_guc_variables();
	int ngucs = get_num_guc_variables();
	StringInfoData string;
	int i;

	initStringInfo(&string);

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
			appendStringInfo(&string, " -c default_transaction_isolation=serializable");
	}

	if (XactIsoLevel != XACT_READ_COMMITTED)
	{
		if (XactIsoLevel == XACT_SERIALIZABLE)
			appendStringInfo(&string, " -c transaction_isolation=serializable");
	}

	for (i = 0; i < ngucs; ++i)
	{
		struct config_generic *guc = gucs[i];

		//TODO: GUC_GPDB_ADDOPT
		if ((guc->flags & GUC_GPDB_ADDOPT) && (guc->context == PGC_USERSET || guc->context == PGC_SUSET ))
			addOneOption(&string, guc);
	}

	return string.data;
}

static void resetQE(XMQE *qe, bool isWriter, int sessionId, int gangId)
{
	StringInfoData buf;
	char *options = getOptions();
	int optionLen = strlen(options);

	pq_beginmessage(&buf, 'R');
	pq_sendbyte(&buf, (char)isWriter);
	pq_sendint(&buf, gangId, 4);
	pq_sendint(&buf, sessionId, 4);
	pq_sendint(&buf, optionLen, 4);
	pq_sendtext(&buf, options, optionLen);

	char type = (char)buf.cursor;
	send(qe->conn->sock, &type, 1, 0);

	uint32		n32;
	n32 = htonl((uint32) (buf.len + 4));
	send(qe->conn->sock, (char *) &n32, 4, 0);

	send(qe->conn->sock, buf.data, buf.len, 0);
	pfree(buf.data);
	return;
}

static void closeConnection(XMConnection *conn)
{
	close(conn->sock);
	conn->sock = 0;
	conn->len = 0;

	ListCell *cell;
	foreach(cell, conn->qes)
	{
		XMQE *qe = (XMQE*)lfirst(cell);
		g_qeList = lappend(g_qeList, qe);
	}

	conn->qeCount = 0;
	conn->qes = NULL;
	conn->type = 0;
	if (conn->buf.data != NULL)
	{
		pfree(conn->buf.data);
		conn->buf.data = NULL;
	}
}


static PGconn*
connectPostMaster(const char *gpqeid, const char *hostip, const char *database, const char *user,
					   const char *options, int *retPort, int *retPid)
{
#define MAX_KEYWORDS 10
#define MAX_INT_STRING_LEN 20
	const char *keywords[MAX_KEYWORDS];
	const char *values[MAX_KEYWORDS];
	char portstr[MAX_INT_STRING_LEN];
	char timeoutstr[MAX_INT_STRING_LEN];
	char hoststr[100];
	char databasestr[100];
	char userstr[100];
	int nkeywords = 0;

	keywords[nkeywords] = "gpqeid";
	values[nkeywords] = gpqeid;
	nkeywords++;

	/*
	 * Build the connection string
	 */
	if (options)
	{
		keywords[nkeywords] = "options";
		values[nkeywords] = options;
		nkeywords++;
	}

	/*
	 * For entry DB connection, we make sure both "hostaddr" and "host" are empty string.
	 * Or else, it will fall back to environment variables and won't use domain socket
	 * in function connectDBStart.
	 *
	 * For other QE connections, we set "hostaddr". "host" is not used.
	 */
	snprintf(hoststr, sizeof(hoststr), "%s", hostip);
	keywords[nkeywords] = "hostaddr";
	values[nkeywords] = hoststr;
	nkeywords++;

	snprintf(databasestr, sizeof(databasestr), "%s", database);
	keywords[nkeywords] = "dbname";
	values[nkeywords] = databasestr;
	nkeywords++;

	snprintf(userstr, sizeof(userstr), "%s", user);
	keywords[nkeywords] = "user";
	values[nkeywords] = userstr;
	nkeywords++;

	keywords[nkeywords] = "host";
	values[nkeywords] = "";
	nkeywords++;

	snprintf(portstr, sizeof(portstr), "%u", PostPortNumber);
	keywords[nkeywords] = "port";
	values[nkeywords] = portstr;
	nkeywords++;

	snprintf(timeoutstr, sizeof(timeoutstr), "%d", 100);
	keywords[nkeywords] = "connect_timeout";
	values[nkeywords] = timeoutstr;
	nkeywords++;

	keywords[nkeywords] = NULL;
	values[nkeywords] = NULL;

	Assert(nkeywords < MAX_KEYWORDS);

	/*
	 * Call libpq to connect
	 */
	PGconn *conn = PQconnectdbParams(keywords, values, false);

	/*
	 * Check for connection failure.
	 */
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		PQfinish(conn);
		elog(ERROR, "failed to connect database");
	}
	/*
	 * Successfully connected.
	 */
	else
	{
		*retPort = PQgetQEdetail(conn);
		*retPid = PQbackendPID(conn);
		if (*retPid == -1 || *retPort == 0)
		{
			PQfinish(conn);
			elog(ERROR, "failed to get QE information");
		}
	}
	return conn;
}

static void
buildqeid(char *buf, int bufsz, int segIndex, int sessionId, bool is_writer, int gangId)
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

	snprintf(buf, bufsz, "%d;%d;" TIMESTAMP_FORMAT ";%s;%d",
			 sessionId, segIndex, GetCurrentTimestamp(),
			 (is_writer ? "true" : "false"), gangId);
}

static XMQE*
getAvailableQE(XMConnection *conn, int isWriter)
{
	ListCell *cell;
	XMQE *info = NULL;
	foreach(cell, conn->qes)
	{
		info = lfirst(cell);
		if (info->isWriter == isWriter)
		{
			conn->qes = list_delete_ptr(conn->qes, info);
			return info;
		}
	}

	if (g_qeList != NULL)
	{
		info = (XMQE*)linitial(g_qeList);
		g_qeList = list_delete_ptr(g_qeList, info);
	}

	return info;
}

static XMQE*
requestQEs(XMConnection *conn, int writerCount, int readerCount,
		int segIndex, int sessionId, int gangIdStart, char *hostip, char *database, char*user, int *retCount)
{
	char gpqeid[100];
	int count = writerCount + readerCount;
	int i = 0;
	XMQE *qeInfos = palloc(count * sizeof(XMQE));

	for (i = 0; i < count; i++)
	{
		int port;
		int pid;
		bool isWriter;

		if (i == 0 && (writerCount-- != 0))
			isWriter = true;
		else
			isWriter = false;

		XMQE *qe = getAvailableQE(conn, isWriter);

		if (qe != NULL)
		{
			qeInfos[i].pid = qe->pid;
			qeInfos[i].port = qe->port;
			qeInfos[i].conn = qe->conn;
			qeInfos[i].buffer = qe->buffer;
			qeInfos[i].sessionId = sessionId;
			qeInfos[i].isWriter = isWriter;
			qeInfos[i].finished = false;

			resetQE(qe, isWriter, sessionId, gangIdStart++);
			continue;
		}

		buildqeid(gpqeid, sizeof(gpqeid), segIndex, sessionId, isWriter, gangIdStart++);
		PGconn *pgconn = connectPostMaster(gpqeid, hostip, database, user, NULL, &port, &pid);

		qeInfos[i].pid = pid;
		qeInfos[i].port = port;
		qeInfos[i].conn = pgconn;
		qeInfos[i].sessionId = sessionId;
		qeInfos[i].isWriter = isWriter;
		qeInfos[i].buffer = createPQExpBuffer();
		qeInfos[i].finished = false;
	}

	for (i = 0; i < count; i++)
		conn->qes = lappend(conn->qes, &qeInfos[i]);

	conn->qeCount = list_length(conn->qes);
	*retCount = count;
	return qeInfos;
}


static XMQE*
handleRequestQEs(XMConnection *conn, int *retCount)
{
	char *ptr = conn->buf.data + 5;
	char hostname[100];
	char database[100];
	char user[100];
	int segIndex;
	int sessionId;
	int gangIdStart;
	int writerCount;
	int readerCount;
	int tmp;


	tmp = *(int*)ptr;
	writerCount = ntohl(tmp);
	ptr +=4;

	tmp = *(int*)ptr;
	readerCount = ntohl(tmp);
	ptr +=4;

	tmp = *(int*)ptr;
	segIndex = ntohl(tmp);
	ptr +=4;

	tmp = *(int*)ptr;
	sessionId = ntohl(tmp);
	ptr +=4;

	tmp = *(int*)ptr;
	gangIdStart = ntohl(tmp);
	ptr +=4;

	memcpy(hostname, ptr, 100);
	ptr += 100;

	memcpy(database, ptr, 100);
	ptr += 100;

	memcpy(user, ptr, 100);

	return requestQEs(conn, writerCount, readerCount, segIndex, sessionId, gangIdStart, hostname, database, user, retCount);
}

static void forwardPlan(XMConnection *conn)
{
	char *ptr = conn->buf.data + 5;
	int totalLen = conn->len + 1;

	ptr += sizeof(int);
	int sessionId;
	memcpy(&sessionId, ptr, sizeof(int));
	sessionId = ntohl(sessionId);

	ListCell *cell;
	foreach(cell, conn->qes)
	{
		XMQE *info = lfirst(cell);
		info->finished = false;
		Assert(info->sessionId == sessionId);
		PQsendGpQuery_shared(info->conn, conn->buf.data, totalLen);
	}
}

static void forwardDtxCommand(XMConnection *conn)
{
	int totalLen = conn->len + 1;

	ListCell *cell;
	foreach(cell, conn->qes)
	{
		XMQE *info = lfirst(cell);
		info->finished = false;

		if (!info->isWriter)
			continue;

		PQsendGpQuery_shared(info->conn, conn->buf.data, totalLen);
	}
}


static bool
processResults(XMQE *info)
{
	char *msg;

	/*
	 * Receive input from QE.
	 */
	if (PQconsumeInput(info->conn) == 0)
	{
		msg = PQerrorMessage(info->conn);
		return true;
	}

	/*
	 * If we have received one or more complete messages, process them.
	 */
	while (!PQisBusy(info->conn))
	{
		/* loop to call PQgetResult; won't block */
		PGresult *pRes;
		ExecStatusType resultStatus;
		int	resultIndex;

		/*
		 * PQisBusy() does some error handling, which can
		 * cause the connection to die -- we can't just continue on as
		 * if the connection is happy without checking first.
		 *
		 * For example, cdbdisp_numPGresult() will return a completely
		 * bogus value!
		 */
		if (PQstatus(info->conn) == CONNECTION_BAD)
		{
			return true;
		}

		/*
		 * Get one message.
		 */
		ELOG_DISPATCHER_DEBUG("PQgetResult");
		pRes = PQgetResult(info->conn);

		/*
		 * Command is complete when PGgetResult() returns NULL. It is critical
		 * that for any connection that had an asynchronous command sent thru
		 * it, we call PQgetResult until it returns NULL. Otherwise, the next
		 * time a command is sent to that connection, it will return an error
		 * that there's a command pending.
		 */
		if (!pRes)
		{
			/* this is normal end of command */
			return true;
		}

		/*
		 * Attach the PGresult object to the CdbDispatchResult object.
		 */
		resultIndex = info->buffer->len / sizeof(PGresult *);
		appendBinaryPQExpBuffer(info->buffer, (char*)&pRes, sizeof(pRes));

		/*
		 * Did a command complete successfully?
		 */
		resultStatus = PQresultStatus(pRes);
		if (resultStatus == PGRES_COMMAND_OK ||
			resultStatus == PGRES_TUPLES_OK ||
			resultStatus == PGRES_COPY_IN ||
			resultStatus == PGRES_COPY_OUT ||
			resultStatus == PGRES_EMPTY_QUERY)
		{
			if (resultStatus == PGRES_COPY_IN ||
				resultStatus == PGRES_COPY_OUT)
				return true;
		}
		/*
		 * Note QE error. Cancel the whole statement if requested.
		 */
		else
		{
//			/* QE reported an error */
//			char	   *sqlstate = PQresultErrorField(pRes, PG_DIAG_SQLSTATE);
//			int			errcode = 0;
//
//			msg = PQresultErrorMessage(pRes);

		}
	}

	return false; /* we must keep on monitoring this socket */
}


//todo
static void handleQEResults(XMConnection *conn)
{
	fd_set readfds;
	int nfd;

	for(;;)
	{
		int maxfd = 0;
		FD_ZERO(&readfds);
		ListCell *cell;
		foreach(cell, conn->qes)
		{
			XMQE *qe = (XMQE*)lfirst(cell);
			int sock = qe->conn->sock;

			if (qe->finished)
				continue;

			if (sock != 0)
				FD_SET(sock, &readfds);

			if (sock > maxfd)
				maxfd = sock;
		}

		if (maxfd == 0)
			break;

		nfd = select(maxfd + 1, &readfds, NULL, NULL, NULL);

		if (nfd < 0 && errno != EINTR)
		{
			elog(LOG, "select error");
			continue;
		}

		foreach(cell, conn->qes)
		{
			XMQE *qe = (XMQE*)lfirst(cell);
			if (FD_ISSET(qe->conn->sock, &readfds))
			{
				if (processResults(qe))
				{
					//todo: handle error
					if (qe->isWriter)
						send(conn->sock, qe->conn->inBuffer, qe->conn->inEnd, 0);
					qe->finished = true;
				}
			}
		}
	}
}

static bool readDtxResult(XMQE *info)
{
	while (!PQisBusy(info->conn))
	{
		/* loop to call PQgetResult; won't block */
		PGresult *pRes;

		/*
		 * PQisBusy() does some error handling, which can
		 * cause the connection to die -- we can't just continue on as
		 * if the connection is happy without checking first.
		 *
		 * For example, cdbdisp_numPGresult() will return a completely
		 * bogus value!
		 */
		if (PQstatus(info->conn) == CONNECTION_BAD)
			return false;

		pRes = PQgetResult(info->conn);

		if (!pRes)
			return true;
	}

	return false;
}

static XMQE *
getWriterQE(XMConnection *conn)
{
	ListCell *cell;
	foreach(cell, conn->qes)
	{
		XMQE *qe = (XMQE*)lfirst(cell);
		if (qe->isWriter)
			return qe;
	}
	return NULL;
}

static void
handleDtxResult(XMConnection *conn)
{
	XMQE *qe = getWriterQE(conn);
	Assert(qe != NULL);

	fd_set readfds;
	int nfd;
	int sock = qe->conn->sock;

	for(;;)
	{
		FD_ZERO(&readfds);
		FD_SET(sock, &readfds);

		nfd = select(sock + 1, &readfds, NULL, NULL, NULL);
		if (nfd < 0)
		{
			if(errno == EINTR)
				continue;

			elog(ERROR, "select error");
		}

		if (!FD_ISSET(sock, &readfds))
			continue;

		if (PQconsumeInput(qe->conn) == 0)
			continue;

		if (!readDtxResult(qe))
			elog(WARNING, "dtx command failed");
		else
			break;
	}

	qe->finished = true;
	int bytesSent = send(conn->sock, qe->conn->inBuffer, qe->conn->inEnd, 0);
	elog(LOG, "send DTX result. %d of %d bytes sent", bytesSent, qe->conn->inEnd);
}


static void
sendResponse(XMConnection *conn, XMQE *qes, int count)
{
	char buf[100];
	int i;
	char *p = buf;
	for (i = 0; i < count; i++)
	{
		int tmp = htonl(qes[i].pid);
		memcpy(p, &tmp, sizeof(tmp));
		p += sizeof(int);
		tmp = htonl(qes[i].port);
		memcpy(p, &tmp, sizeof(tmp));
		p += sizeof(int);
	}

	send(conn->sock, buf, p-buf, 0);
}



static void *
handleIncomingConnection(void *arg)
{
	XMConnection *pConn = (XMConnection *) arg;

	gp_set_thread_sigmasks();

	for(;;)
	{
		int n = read(pConn->sock, pConn->buf.data, 1);
		if (n != 1)
			break;

		pConn->type = *pConn->buf.data;

		switch (pConn->type)
		{
				case 'a':
				{
					read(pConn->sock, pConn->buf.data+1, 4);
					memcpy(&pConn->len, pConn->buf.data+1, 4);
					pConn->len = ntohl(pConn->len);
					read(pConn->sock, pConn->buf.data + 5, pConn->len - 4);

					int count = 0;
					XMQE *qes = handleRequestQEs(pConn, &count);
					sendResponse(pConn, qes, count);
					break;
				}

				case 'M':
					read(pConn->sock, pConn->buf.data+1, 4);
					memcpy(&pConn->len, pConn->buf.data+1, 4);
					pConn->len = ntohl(pConn->len);
					read(pConn->sock, pConn->buf.data + 5, pConn->len - 4);

					forwardPlan(pConn);
					handleQEResults(pConn);
					break;

				case 'T':
					read(pConn->sock, pConn->buf.data+1, 4);
					memcpy(&pConn->len, pConn->buf.data+1, 4);
					pConn->len = ntohl(pConn->len);
					read(pConn->sock, pConn->buf.data + 5, pConn->len - 4);

					forwardDtxCommand(pConn);
					handleDtxResult(pConn);
					break;

				default:
					elog(WARNING, "received unexpected request");
					closeConnection(pConn);
					return (NULL);
		}
	}


	closeConnection(pConn);

	return (NULL);
}


/* ----------
 * PgstatCollectorMain() -
 *
 *	Start up the statistics collector process.	This is the body of the
 *	postmaster child process.
 *
 *	The argc/argv parameters are valid only in EXEC_BACKEND case.
 * ----------
 */
NON_EXEC_STATIC void XmanagerMain(int argc, char *argv[])
{
	int xmSock = -1;
	int opt = 1;
	int i;
	XMConnection aliveConn[1000];
	int maxConns = 1000;
	struct sockaddr_in address;
	int addrlen;
	int serverPort = PostPortNumber + 55;

	IsUnderPostmaster = true; /* we are a postmaster subprocess now */
	MyProcPid = getpid(); /* reset MyProcPid */
	MyStartTime = time(NULL); /* record Start Time for logging */

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(pgstat probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/*
	 * Ignore all signals usually bound to some action in the postmaster,
	 * except SIGQUIT.
	 */
	pqsignal(SIGHUP, xmanager_handler);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SIG_IGN);
	pqsignal(SIGQUIT, xmanager_exit);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);
	PG_SETMASK(&UnBlockSig);

	/*
	 * Identify myself via ps
	 */
	init_ps_display("qx manager process", "", "", "");

	if ((xmSock = socket(AF_INET, SOCK_STREAM, 0)) == 0)
	{
		elog(WARNING, "Failed to create socket");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(xmSock, SOL_SOCKET, SO_REUSEADDR, (char *) &opt, sizeof(opt))
			< 0)
	{
		elog(WARNING, "Failed to setsockopt");
		exit(EXIT_FAILURE);
	}

	if (fcntl(xmSock, F_SETFL, O_NONBLOCK) == -1)
	{
		elog(WARNING, "fcntl(F_SETFL, O_NONBLOCK) failed");
		exit(EXIT_FAILURE);
	}

	address.sin_family = AF_INET;
	address.sin_addr.s_addr = INADDR_ANY;
	address.sin_port = htons(serverPort);
	addrlen = sizeof(address);

	if (bind(xmSock, (struct sockaddr *) &address, sizeof(address)) < 0)
	{
		elog(WARNING, "Failed to bind");
		exit(EXIT_FAILURE);
	}

	if (listen(xmSock, 10) < 0)
	{
		elog(WARNING, "Failed to listen");
		exit(EXIT_FAILURE);
	}

	for (i = 0; i < maxConns; i++)
	{
		aliveConn[i].sock = 0;
		aliveConn[i].len = 0;
	}

	/*
	 * Loop to process messages until we get SIGQUIT or detect ungraceful
	 * death of our parent postmaster.
	 *
	 * For performance reasons, we don't want to do a PostmasterIsAlive() test
	 * after every message; instead, do it only when select()/poll() is
	 * interrupted by timeout.	In essence, we'll stay alive as long as
	 * backends keep sending us stuff often, even if the postmaster is gone.
	 */
	for (;;)
	{
		/*
		 * Quit if we get SIGQUIT from the postmaster.
		 */
		if (need_exit)
			break;

		/*
		 * Reload configuration if we got SIGHUP from the postmaster.
		 */
		if (got_SIGHUP)
		{
			ProcessConfigFile(PGC_SIGHUP);
			got_SIGHUP = false;
		}

		fd_set readfds;
		int maxfd;
		int nfd;
		int newSock;

		FD_ZERO(&readfds);
		FD_SET(xmSock, &readfds);
		maxfd = xmSock;

		for (i = 0; i < maxConns; i++)
		{
			int sock = aliveConn[i].sock;

			if (sock != 0)
				FD_SET(sock, &readfds);

			if (sock > maxfd)
				maxfd = sock;
		}

		nfd = select(maxfd + 1, &readfds, NULL, NULL, NULL);

		if (nfd < 0 && errno != EINTR)
		{
			elog(LOG, "select error");
			continue;
		}

		if (FD_ISSET(xmSock, &readfds))
		{
			if ((newSock = accept(xmSock, (struct sockaddr *) &address,
					(socklen_t*) &addrlen)) < 0)
			{
				if (errno == EINTR || errno == EAGAIN)
					continue;

				elog(WARNING, "Failed to accept");
				exit(EXIT_FAILURE);
			}

			elog(DEBUG1, "New connection, socket fd is %d, ip is : %s, port : %d \n" ,
					newSock, inet_ntoa(address.sin_addr), ntohs(address.sin_port));

			for (i = 0; i < maxConns; i++)
			{
				if (aliveConn[i].sock == 0)
				{
					aliveConn[i].sock = newSock;
					aliveConn[i].qes = NULL;
					aliveConn[i].len = 0;
					initStringInfo(&aliveConn[i].buf);
					break;
				}
			}

			int pthread_err = gp_pthread_create(&aliveConn[i].thread, handleIncomingConnection, (void*)&aliveConn[i], "xmConnection");
			if (pthread_err != 0)
			{
				closeConnection(&aliveConn[i]);
				elog(ERROR, "Failed to create thread");
			}
		}

		if (!PostmasterIsAlive(true))
			break;
	} /* end of message-processing loop */

	exit(0);
}

/* SIGQUIT signal handler for collector process */
static void xmanager_exit(SIGNAL_ARGS)
{
	need_exit = true;
}

/* SIGHUP handler for collector process */
static void xmanager_handler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

