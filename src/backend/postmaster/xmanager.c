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
#include "libpq/ip.h"
#include "libpq/libpq.h"
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
int
xmanager_start(void)
{
	pid_t		xmPid;

	/*
	 * Okay, fork off the collector.
	 */
	switch ((xmPid = fork_process()))
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork statistics collector: %m")));
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

/* ----------
 * PgstatCollectorMain() -
 *
 *	Start up the statistics collector process.	This is the body of the
 *	postmaster child process.
 *
 *	The argc/argv parameters are valid only in EXEC_BACKEND case.
 * ----------
 */
NON_EXEC_STATIC void
XmanagerMain(int argc, char *argv[])
{
	int xmSock = -1;
    int opt = 1;
    int i;
    int aliveSocks[1000];
    int maxConns = 1000;
    struct sockaddr_in address;
    int addrlen;
	int serverPort = 5555;

	IsUnderPostmaster = true;	/* we are a postmaster subprocess now */
	MyProcPid = getpid();		/* reset MyProcPid */
	MyStartTime = time(NULL);	/* record Start Time for logging */

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

    if( (xmSock = socket(AF_INET , SOCK_STREAM , 0)) == 0)
    {
        elog(WARNING, "Failed to create socket");
        exit(EXIT_FAILURE);
    }

    if( setsockopt(xmSock, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 )
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

    if (bind(xmSock, (struct sockaddr *)&address, sizeof(address))<0)
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
        aliveSocks[i] = 0;
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

        for ( i = 0 ; i < maxConns ; i++)
        {
            int sock = aliveSocks[i];

            if(sock != 0)
                FD_SET(sock , &readfds);

            if(sock > maxfd)
                maxfd = sock;
        }

        nfd = select(maxfd + 1 , &readfds , NULL , NULL , NULL);

        if (nfd < 0 && errno != EINTR)
        {
            elog(LOG, "select error");
            continue;
        }

        if (FD_ISSET(maxfd, &readfds))
        {
            if ((newSock = accept(xmSock, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0)
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
                if( aliveSocks[i] == 0 )
                {
                	aliveSocks[i] = newSock;
                    break;
                }
            }
        }

        for (i = 0; i < maxConns; i++)
        {
            int sock = aliveSocks[i];
            int nread;
            char buffer[1024];

            if (FD_ISSET(sock , &readfds))
            {
                if ((nread = read(sock, buffer, sizeof(buffer))) == 0)
                {
                    getpeername(sock, (struct sockaddr*)&address , (socklen_t*)&addrlen);
                    elog(DEBUG1, "Host disconnected , ip %s , port %d \n" , inet_ntoa(address.sin_addr) , ntohs(address.sin_port));

                    close(sock);
                    aliveSocks[i] = 0;
                }
            }
        }

		if (!PostmasterIsAlive(true))
			break;
	}							/* end of message-processing loop */

	exit(0);
}


/* SIGQUIT signal handler for collector process */
static void
xmanager_exit(SIGNAL_ARGS)
{
	need_exit = true;
}

/* SIGHUP handler for collector process */
static void
xmanager_handler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

