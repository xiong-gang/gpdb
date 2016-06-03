/*-------------------------------------------------------------------------
 *
 * cdbgang.c

 *	  Query Executor Factory for gangs of QEs
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include <poll.h>

#include "postgres.h"
#include "miscadmin.h"			/* MyDatabaseId */
#include "cdb/cdbconn.h"		/* SegmentDatabaseDescriptor */
#include "cdb/cdbfts.h"
#include "cdb/cdbgang_non_thread.h"		/* me */
#include "cdb/cdbvars.h"		/* Gp_role, etc. */


#define LOG_GANG_DEBUG(...) do { \
	if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG) elog(__VA_ARGS__); \
    } while(false);


extern Gang *createGang_non_thread(GangType type, int gang_id, int size, int content);

static int getTimeout(const struct timeval* startTS)
{
	struct timeval now;
	int timeout;
	int64 diff_us;

	gettimeofday(&now, NULL);

	if (gp_segment_connect_timeout > 0)
	{
		diff_us = (now.tv_sec - startTS->tv_sec) * 1000000;
		diff_us += (int) now.tv_usec - (int) startTS->tv_usec;
		if (diff_us > (int64) gp_segment_connect_timeout * 1000000)
			timeout = 0;
		else
			timeout = gp_segment_connect_timeout * 1000 - diff_us / 1000;
	}
	else
		timeout = -1;

	return timeout;
}

/*
 * Creates a new gang by logging on a session to each segDB involved.
 *
 * call this function in GangContext memory context.
 * elog ERROR or return a non-NULL gang.
 */
Gang *
createGang_non_thread(GangType type, int gang_id, int size, int content)
{
	Gang *newGangDefinition;
	SegmentDatabaseDescriptor *segdbDesc = NULL;
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

create_gang_retry:
	/* If we're in a retry, we may need to reset our initial state, a bit */
	newGangDefinition = NULL;
	successful_connections = 0;
	in_recovery_mode_count = 0;

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

	PG_TRY();
	{
		struct pollfd *fds;
		int *indexes;
		struct timeval startTS;

		for (i = 0; i < size; i++)
		{
			char gpqeid[100];
			char *options;

			/*
			 * Create the connection requests.	If we find a segment without a
			 * valid segdb we error out.  Also, if this segdb is invalid, we must
			 * fail the connection.
			 */
			segdbDesc = &newGangDefinition->db_descriptors[i];

			/*
			 * Build the connection string.  Writer-ness needs to be processed
			 * early enough now some locks are taken before command line options
			 * are recognized.
			 */
			build_gpqeid_param(gpqeid, sizeof(gpqeid), segdbDesc->segindex, type == GANGTYPE_PRIMARY_WRITER);
			options = addOptions(type == GANGTYPE_PRIMARY_WRITER);

			cdbconn_doConnect(segdbDesc, gpqeid, options, false);
			if (cdbconn_isBadConnection(segdbDesc))
			{
				/*
				 * Log the details to the server log, but give a more
				 * generic error to the client. XXX: The user would
				 * probably prefer to see a bit more details too.
				 */
				ereport(LOG, (errcode(ERRCODE_GP_INTERNAL_ERROR),
							  errmsg("Master unable to connect to %s with options %s: %s",
								segdbDesc->whoami,
								options,
								PQerrorMessage(segdbDesc->conn))));

				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("failed to acquire resources on one or more segments")));
			}
			segdbDesc->poll_status = PGRES_POLLING_WRITING;
		}

		/*
		 * Ok, we've now launched all the connection attempts. Start the
		 * timeout clock (= get the start timestamp), and poll until they're
		 * all completed or we reach timeout.
		 */
		gettimeofday(&startTS, NULL);
		fds = (struct pollfd *) palloc0(sizeof(struct pollfd) * size);
		indexes = (int *) palloc(sizeof(int) * size);

		for(;;)
		{
			int			nfds = 0;
			int			nready;
			int			timeout;

			for (i = 0; i < size; i++)
			{
				segdbDesc = &newGangDefinition->db_descriptors[i];

				if (segdbDesc->conn &&
					(segdbDesc->poll_status == PGRES_POLLING_READING ||
					 segdbDesc->poll_status == PGRES_POLLING_WRITING))
				{
					indexes[nfds] = i;
					fds[nfds].fd = PQsocket(segdbDesc->conn);
					fds[nfds].events = 0;
					if (segdbDesc->poll_status == PGRES_POLLING_READING)
						fds[nfds].events |= POLLIN;
					if (segdbDesc->poll_status == PGRES_POLLING_WRITING)
						fds[nfds].events |= POLLOUT;
					nfds++;
				}
			}

			if (nfds == 0)
				break;

			timeout = getTimeout(&startTS);

			/* Wait until something happens */
			nready = poll(fds, nfds, timeout);

			if (nready < 0)
			{
				int	sock_errno = SOCK_ERRNO;
				if (sock_errno == EINTR)
					continue;

				ereport(LOG, (errcode_for_socket_access(),
							  errmsg("poll() failed while connecting to segments")));
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("failed to acquire resources on one or more segments")));
			}
			else if (nready == 0)
			{
				if (timeout != 0)
					continue;

				elog(LOG, "poll() timeout while connecting to segments");
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("failed to acquire resources on one or more segments")));
			}
			else
			{
				for (i = 0; i < nfds; i++)
				{
					Assert(fds[i].events != 0);

					segdbDesc = &newGangDefinition->db_descriptors[indexes[i]];

					segdbDesc->poll_status = PQconnectPoll(segdbDesc->conn);

					if (segdbDesc->poll_status == PGRES_POLLING_OK)
						cdbconn_doConnectComplete(segdbDesc);
					else if (segdbDesc->poll_status == PGRES_POLLING_FAILED)
					{
						elog(LOG, "Failed to connect to %s", segdbDesc->whoami);
						ereport(ERROR,
								(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								 errmsg("failed to acquire resources on one or more segments")));
					}
				}
			}
		}

		MemoryContextSwitchTo(GangContext);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(GangContext);
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

		/* Writer gang is created before reader gangs. */
		if (type == GANGTYPE_PRIMARY_WRITER)
			Insist(!gangsExist());

		/* find out the successful connections and the failed ones */
		checkConnectionStatus(newGangDefinition, &in_recovery_mode_count,
				&successful_connections);
		LOG_GANG_DEBUG(LOG,"createGang: %d processes requested; %d successful connections %d in recovery",
				size, successful_connections, in_recovery_mode_count);

		/*
		 * Retry when:
		 * 1) It's a writer gang.
		 * 2) It's the first reader gang.
		 * 3) All failed segment are in recovery mode.
		 */
		if(gp_gang_creation_retry_count &&
		   create_gang_retry_counter++ < gp_gang_creation_retry_count &&
		   (type == GANGTYPE_PRIMARY_WRITER ||
		    readerGangsExist() ||
		    successful_connections + in_recovery_mode_count == size))
		{
			disconnectAndDestroyGang(newGangDefinition);
			newGangDefinition = NULL;

			LOG_GANG_DEBUG(LOG, "createGang: gang creation failed, but retryable.");

			CHECK_FOR_INTERRUPTS();
			pg_usleep(gp_gang_creation_retry_timer * 1000);
			CHECK_FOR_INTERRUPTS();

			goto create_gang_retry;
		}
	}
	PG_END_TRY();

	setLargestGangsize(size);
	return newGangDefinition;

exit:
	disconnectAndDestroyGang(newGangDefinition);
	disconnectAndDestroyAllGangs(true);
	CheckForResetSession();
	ereport(ERROR,
			(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
			errmsg("failed to acquire resources on one or more segments")));
	return NULL;
}
