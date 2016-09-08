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

static volatile bool need_exit = false;
static volatile bool got_SIGHUP = false;

NON_EXEC_STATIC void XmanagerMain(int argc, char *argv[]);
static void xmanager_exit(SIGNAL_ARGS);
static void xmanager_handler(SIGNAL_ARGS);

#define XM_SELECT_TIMEOUT 5
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


typedef struct XMConnection
{
	int sock;
	char type;
	int len;
	int received;
	StringInfoData buf;
}XMConnection;

static int
readFromSocket(int sock, char *buf, int len, bool block)
{
	char *p = buf;
	char *q = p + len;
	while (p < q)
	{
		int n = recv(sock, p, q - p, 0);
		if (n == -1)
		{
			if (errno == EINTR)
				continue;

			if (errno == EAGAIN || errno == EINPROGRESS)
			{
				if (block)
					continue;
				else
					break;
			}

			elog(WARNING, "Error when receive from %d, errno:%d", sock, errno);
			return -1;
		}
		else if (n == 0)
			return -1;
		else
			p += n;
	}

	return p - buf;
}

static int
peekHeader(int sock)
{
	char buf[5];
	int ret = recv(sock, &buf, 5, MSG_PEEK | MSG_DONTWAIT);

	if (ret == 0)
		return -1;
	else if (ret == -1)
	{
		if (errno == EAGAIN || errno == EINPROGRESS)
			return 0;
		else if (errno == EINTR)
			return 0;
		else
			return -1;
	}
	else
		return ret;
}

static void closeConnection(XMConnection *conn)
{
	close(conn->sock);
	conn->sock = 0;
	conn->len = 0;
	conn->received = 0;

	if (conn->buf.data != NULL)
	{
		pfree(conn->buf.data);
		conn->buf.data = NULL;
	}
}

typedef struct XMQE
{
	int port;
	int pid;
	int sessionId;
	bool isWriter;
	PGconn *conn;
} XMQE;

List *g_qeList = NULL;

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
findQEBySessionId(int sessionId, bool findWriter)
{
	ListCell *cell;
	foreach(cell, g_qeList)
	{
		XMQE *info = lfirst(cell);
		if (info->sessionId == sessionId)
		{
			if (findWriter && !info->isWriter)
				continue;

			return info;
		}
	}

	return NULL;
}

static XMQE*
requestQEs(int count, int segIndex, int sessionId, int gangIdStart, char *hostip, char *database, char*user)
{
	char gpqeid[100];
	bool isWriter = false;

	XMQE *info = findQEBySessionId(sessionId, false);
	if (info == NULL)
		isWriter = true;

	int i = 0;
	XMQE *qeInfos = palloc(count * sizeof(XMQE));
	for (i = 0; i < count; i++)
	{
		int port;
		int pid;
		buildqeid(gpqeid, sizeof(gpqeid), segIndex, sessionId, isWriter, gangIdStart++);
		PGconn *conn = connectPostMaster(gpqeid, hostip, database, user, NULL, &port, &pid);

		qeInfos[i].pid = pid;
		qeInfos[i].port = port;
		qeInfos[i].conn = conn;
		qeInfos[i].sessionId = sessionId;
		qeInfos[i].isWriter = isWriter;
		g_qeList = lappend(g_qeList, &qeInfos[i]);
	}

	return qeInfos;
}


static XMQE*
handleRequestQEs(XMConnection *conn)
{
	char *ptr = conn->buf.data + 5;
	char hostname[100];
	char database[100];
	char user[100];
	int qeCount;
	int segIndex;
	int sessionId;
	int gangIdStart;
	int tmp;

	tmp = *(int*)ptr;
	qeCount = ntohl(tmp);
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

	return requestQEs(qeCount, segIndex, sessionId, gangIdStart, hostname, database, user);
}

static void forwardPlan(XMConnection *conn)
{
	char *ptr = conn->buf.data + 5;
	int totalLen = conn->len + 5;

	ptr += sizeof(int);
	int sessionId;
	memcpy(&sessionId, ptr, sizeof(int));
	sessionId = ntohl(sessionId);

	XMQE *qe = findQEBySessionId(sessionId, true);

	PQsendGpQuery_shared(qe->conn, conn->buf.data, totalLen);
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
		aliveConn[i].received = 0;
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

			if (fcntl(newSock, F_SETFL, O_NONBLOCK) == -1)
			{
				elog(WARNING, "fcntl(F_SETFL, O_NONBLOCK) failed");
				exit(EXIT_FAILURE);
			}

			elog(DEBUG1, "New connection, socket fd is %d, ip is : %s, port : %d \n" ,
					newSock, inet_ntoa(address.sin_addr), ntohs(address.sin_port));

			for (i = 0; i < maxConns; i++)
			{
				if (aliveConn[i].sock == 0)
				{
					aliveConn[i].sock = newSock;
					initStringInfo(&aliveConn[i].buf);
					break;
				}
			}
		}

		for (i = 0; i < maxConns; i++)
		{
			XMConnection *conn = &aliveConn[i];
			int sock = conn->sock;
			if (FD_ISSET(sock, &readfds))
			{
				if (conn->len == 0)
				{
					int ret = peekHeader(sock);
					if (ret == -1)
					{
						closeConnection(conn);
						continue;
					}
					if (ret != 5)
						continue;

					char *buf = conn->buf.data;
					ret = readFromSocket(sock, buf, 5, true);
					if (ret == -1)
					{
						closeConnection(conn);
						continue;
					}
					else
					{
						conn->type = buf[0];

						int len;
						memcpy(&len, &buf[1], 4);
						conn->len = ntohl(len) - 4;
					}
				}

				conn->received += readFromSocket(sock, conn->buf.data + 5 + conn->received, conn->len - conn->received, false);
				if (conn->received == conn->len)
				{
					switch (conn->type)
					{
						case 'a':
						{
							XMQE *qes = handleRequestQEs(conn);
							sendResponse(conn, qes, 1);
							break;
						}

						case 'M':
							forwardPlan(conn);
							break;

						default:
							elog(WARNING, "received unexpected request");
							closeConnection(conn);
							break;
					}

					conn->len = 0;
					conn->received = 0;
				}
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

