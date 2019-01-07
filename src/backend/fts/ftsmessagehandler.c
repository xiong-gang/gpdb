/*-------------------------------------------------------------------------
 *
 * ftsmessagehandler.c
 *	  Implementation of handling of FTS messages
 *
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/fts/ftsmessagehandler.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"

#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbfts.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "postmaster/fts.h"
#include "postmaster/postmaster.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "replication/gp_replication.h"
#include "storage/fd.h"
#include "storage/pmsignal.h"


#define FTS_PROBE_FILE_NAME "fts_probe_file.bak"
#define FTS_PROBE_MAGIC_STRING "FtS PrObEr MaGiC StRiNg, pRoBiNg cHeCk......."

/*
 * Check if we can smoothly read and write to data directory.
 *
 * O_DIRECT flag requires buffer to be OS/FS block aligned.
 * Best to have it IO Block alligned henece using BLCKSZ
 */
static bool
checkIODataDirectory(void)
{
	int fd;
	int size = BLCKSZ + BLCKSZ;
	int magic_len = strlen(FTS_PROBE_MAGIC_STRING) + 1;
	char *data = palloc0(size);

	/*
	 * Buffer needs to be alligned to BLOCK_SIZE for reads and writes if using O_DIRECT
	 */
	char* dataAligned = (char *) TYPEALIGN(BLCKSZ, data);

	errno = 0;
	bool failure = false;

	fd = BasicOpenFile(FTS_PROBE_FILE_NAME, O_RDWR | PG_O_DIRECT | O_EXCL,
                                           S_IRUSR | S_IWUSR);
	do
	{
		if (fd < 0)
		{
			if (errno == ENOENT)
			{
				elog(LOG, "FTS: \"%s\" file doesn't exist, creating it once.", FTS_PROBE_FILE_NAME);
				fd = BasicOpenFile(FTS_PROBE_FILE_NAME, O_RDWR | O_CREAT | O_EXCL,
                                           S_IRUSR | S_IWUSR);
				if (fd < 0)
				{
					failure = true;
					ereport(LOG, (errcode_for_file_access(),
							errmsg("FTS: could not create file \"%s\": %m",
								FTS_PROBE_FILE_NAME)));
				}
				else
				{
					strncpy(dataAligned, FTS_PROBE_MAGIC_STRING, magic_len);
					if (write(fd, dataAligned, BLCKSZ) != BLCKSZ)
					{
						ereport(LOG, (errcode_for_file_access(),
									  errmsg("FTS: could not write file \"%s\" : %m",
											 FTS_PROBE_FILE_NAME)));
						failure = true;
					}
				}
			}
			else
			{
				/*
				 * Some other error
				 */
				failure = true;
				ereport(LOG, (errcode_for_file_access(),
						errmsg("FTS: could not open file \"%s\": %m",
							FTS_PROBE_FILE_NAME)));
			}
			break;
		}

		int len = read(fd, dataAligned, BLCKSZ);
		if (len != BLCKSZ)
		{
			ereport(LOG, (errcode_for_file_access(),
					errmsg("FTS: could not read file \"%s\" "
						"(actual bytes read %d, required: %d): %m",
						FTS_PROBE_FILE_NAME, len, BLCKSZ)));
			failure = true;
			break;
		}

		if (strncmp(dataAligned, FTS_PROBE_MAGIC_STRING, magic_len) != 0)
		{
			ereport(LOG, (errmsg("FTS: Read corrupted data from \"%s\" file", FTS_PROBE_FILE_NAME)));
			failure = true;
			break;
		}

		if (lseek(fd, (off_t) 0, SEEK_SET) < 0)
		{
			ereport(LOG, (errcode_for_file_access(),
					errmsg("FTS: could not seek in file \"%s\" to offset zero: %m",
					FTS_PROBE_FILE_NAME)));
			failure = true;
			break;
		}

		/*
		 * Read worked, lets overwrite what we read, to check if can write also
		 */
		if (write(fd, dataAligned, BLCKSZ) != BLCKSZ)
		{
			ereport(LOG, (errcode_for_file_access(),
					errmsg("FTS: could not write file \"%s\" : %m",
					FTS_PROBE_FILE_NAME)));
			failure = true;
			break;
		}
	} while (0);

	if (fd > 0)
	{
		close(fd);

		/*
		 * We are more concerned with IOs hanging than failures.
		 * Cleanup the file as detected the problem and reporting the same.
		 * This is done to cover for cases like:
		 * 1] FTS detects corruption/read failure on the file, reports to Master
		 * 2] Triggers failover to mirror
		 * 3] But if the file stays around, when it transitions back to Primary
		 *    would again detect this corrupted file and again trigger failover.
		 * To avoid such scenarios remove the file.
		 */
		if (failure)
		{
			if (unlink(FTS_PROBE_FILE_NAME) < 0)
				ereport(LOG,
				(errcode_for_file_access(),
				errmsg("could not unlink file \"%s\": %m", FTS_PROBE_FILE_NAME)));
		}
	}

	if (failure)
		ereport(ERROR,
				(errmsg("disk IO check during FTS probe failed")));

	pfree(data);
	return failure;
}

static void
SendFtsResponse(FtsResponse *response, const char *messagetype)
{
	StringInfoData buf;

	initStringInfo(&buf);

	BeginCommand(messagetype, DestRemote);

	pq_beginmessage(&buf, 'T');
	pq_sendint(&buf, Natts_fts_message_response, 2); /* # of columns */

	pq_sendstring(&buf, "is_mirror_up");
	pq_sendint(&buf, 0, 4);		/* table oid */
	pq_sendint(&buf, Anum_fts_message_response_is_mirror_up, 2);		/* attnum */
	pq_sendint(&buf, BOOLOID, 4);		/* type oid */
	pq_sendint(&buf, 1, 2);	/* typlen */
	pq_sendint(&buf, -1, 4);		/* typmod */
	pq_sendint(&buf, 0, 2);		/* format code */

	pq_sendstring(&buf, "is_in_sync");
	pq_sendint(&buf, 0, 4);		/* table oid */
	pq_sendint(&buf, Anum_fts_message_response_is_in_sync, 2);		/* attnum */
	pq_sendint(&buf, BOOLOID, 4);		/* type oid */
	pq_sendint(&buf, 1, 2);	/* typlen */
	pq_sendint(&buf, -1, 4);		/* typmod */
	pq_sendint(&buf, 0, 2);		/* format code */

	pq_sendstring(&buf, "is_syncrep_enabled");
	pq_sendint(&buf, 0, 4);		/* table oid */
	pq_sendint(&buf, Anum_fts_message_response_is_syncrep_enabled, 2);		/* attnum */
	pq_sendint(&buf, BOOLOID, 4);		/* type oid */
	pq_sendint(&buf, 1, 2);	/* typlen */
	pq_sendint(&buf, -1, 4);		/* typmod */
	pq_sendint(&buf, 0, 2);		/* format code */

	pq_sendstring(&buf, "is_role_mirror");
	pq_sendint(&buf, 0, 4);		/* table oid */
	pq_sendint(&buf, Anum_fts_message_response_is_role_mirror, 2);		/* attnum */
	pq_sendint(&buf, BOOLOID, 4);		/* type oid */
	pq_sendint(&buf, 1, 2);	/* typlen */
	pq_sendint(&buf, -1, 4);		/* typmod */
	pq_sendint(&buf, 0, 2);		/* format code */

	pq_sendstring(&buf, "request_retry");
	pq_sendint(&buf, 0, 4);		/* table oid */
	pq_sendint(&buf, Anum_fts_message_response_request_retry, 2);		/* attnum */
	pq_sendint(&buf, BOOLOID, 4);		/* type oid */
	pq_sendint(&buf, 1, 2);	/* typlen */
	pq_sendint(&buf, -1, 4);		/* typmod */
	pq_sendint(&buf, 0, 2);		/* format code */

	pq_sendstring(&buf, "master_prober_started");
	pq_sendint(&buf, 0, 4);		/* table oid */
	pq_sendint(&buf, Anum_fts_message_response_master_prober_started, 2);		/* attnum */
	pq_sendint(&buf, BOOLOID, 4);		/* type oid */
	pq_sendint(&buf, 1, 2);	/* typlen */
	pq_sendint(&buf, -1, 4);		/* typmod */
	pq_sendint(&buf, 0, 2);		/* format code */

	pq_endmessage(&buf);

	/* Send a DataRow message */
	pq_beginmessage(&buf, 'D');
	pq_sendint(&buf, Natts_fts_message_response, 2);		/* # of columns */

	pq_sendint(&buf, 1, 4); /* col1 len */
	pq_sendint(&buf, response->IsMirrorUp, 1);

	pq_sendint(&buf, 1, 4); /* col2 len */
	pq_sendint(&buf, response->IsInSync, 1);

	pq_sendint(&buf, 1, 4); /* col3 len */
	pq_sendint(&buf, response->IsSyncRepEnabled, 1);

	pq_sendint(&buf, 1, 4); /* col4 len */
	pq_sendint(&buf, response->IsRoleMirror, 1);

	pq_sendint(&buf, 1, 4); /* col5 len */
	pq_sendint(&buf, response->RequestRetry, 1);

	pq_sendint(&buf, 1, 4); /* col6 len */
	pq_sendint(&buf, response->MasterProberStarted, 1);

	pq_endmessage(&buf);
	EndCommand(messagetype, DestRemote);
	pq_flush();
}

static void
HandleFtsWalRepProbe(void)
{
	FtsResponse response = {
		false, /* IsMirrorUp */
		false, /* IsInSync */
		false, /* IsSyncRepEnabled */
		false, /* IsRoleMirror */
		false, /* RequestRetry */
		(!IS_QUERY_DISPATCHER() && shmFtsControl->ftsPid != 0), /* MasterProberStarted */
	};

	if (am_mirror)
	{
		response.IsRoleMirror = true;
		elog(LOG, "received probe message while acting as mirror");
	}
	else
	{
		GetMirrorStatus(&response);

		/*
		 * We check response.IsSyncRepEnabled even though syncrep is again checked
		 * later in the set function to avoid acquiring the SyncRepLock again.
		 */
		if (response.IsMirrorUp && !response.IsSyncRepEnabled)
		{
			SetSyncStandbysDefined();
			/* Syncrep is enabled now, so respond accordingly. */
			response.IsSyncRepEnabled = true;
		}

		/*
		 * Precautionary action: Unlink PROMOTE_SIGNAL_FILE if incase it
		 * exists. This is to avoid driving to any incorrect conclusions when
		 * this segment starts acting as mirror or gets copied over using
		 * pg_basebackup.
		 */
		// GPDB_93_MERGE_FIXME: should we check for FALLBACK_PROMOTE_SIGNAL_FILE, too?
		if (CheckPromoteSignal())
		{
			unlink(PROMOTE_SIGNAL_FILE);
			elog(LOG, "found and hence deleted '%s' file", PROMOTE_SIGNAL_FILE);
		}
	}

	/*
	 * Perform basic sanity check for disk IO on segment. Without this check
	 * in many situations FTS didn't detect the problem and hence didn't
	 * trigger failover to mirror. It caused extended data unavailable
	 * situations. Hence performing some read-write as part of FTS probe
	 * helped detect and trigger failover.
	 */
	checkIODataDirectory();
	SendFtsResponse(&response, FTS_MSG_PROBE);
}

static void
HandleFtsWalRepSyncRepOff(void)
{
	FtsResponse response = {
		false, /* IsMirrorUp */
		false, /* IsInSync */
		false, /* IsSyncRepEnabled */
		false, /* IsRoleMirror */
		false, /* RequestRetry */
		false, /* MasterProberStarted */
	};

	ereport(LOG,
			(errmsg("turning off synchronous wal replication due to FTS request")));
	UnsetSyncStandbysDefined();

	GetMirrorStatus(&response);
	SendFtsResponse(&response, FTS_MSG_SYNCREP_OFF);
}

static void
HandleFtsWalRepPromote(const char *query)
{
	FtsResponse response = {
		false, /* IsMirrorUp */
		false, /* IsInSync */
		false, /* IsSyncRepEnabled */
		am_mirror,  /* IsRoleMirror */
		false, /* RequestRetry */
		false, /* MasterProberStarted */
	};
	int dbid;

	ereport(LOG,
			(errmsg("promoting mirror to primary due to FTS request")));

	sscanf(query, FTS_MSG_PROMOTE_FMT, &dbid);

	if (IS_QUERY_DISPATCHER() &&
		shmFtsControl->masterProberDBID != 0 &&
		dbid != shmFtsControl->masterProberDBID)
	{
		elog(LOG, "ignoring promote request from deprecated master prober %d,"
			 " current master prober %d", dbid, shmFtsControl->masterProberDBID);
	}
	else
	{
		/*
		 * FTS sends promote message to a mirror.  The mirror may be undergoing
		 * promotion.  Promote messages should therefore be handled in an
		 * idempotent way.
		 */
		DBState state = GetCurrentDBState();
		if (state == DB_IN_STANDBY_MODE)
		{
			/*
			 * Reset sync_standby_names on promotion. This is to avoid commits
			 * hanging/waiting for replication till next FTS probe. Next FTS probe
			 * will detect this node to be not in sync and reset the same which
			 * can take a min. Since we know on mirror promotion its marked as not
			 * in sync in gp_segment_configuration, best to right away clean the
			 * sync_standby_names.
			 */
			UnsetSyncStandbysDefined();
			SignalPromote();
		}
		else
		{
			elog(LOG, "ignoring promote request, walreceiver not running,"
				 " DBState = %d", state);
		}
	}

	SendFtsResponse(&response, FTS_MSG_PROMOTE);
}

static void
HandleFtsWalRepNewMasterProber(const char *query)
{
	FtsResponse response = {
		false, /* IsMirrorUp */
		false, /* IsInSync */
		false, /* IsSyncRepEnabled */
		false, /* IsRoleMirror */
		false, /* RequestRetry */
		false, /* MasterProberStarted */
	};

	sscanf(query, FTS_MSG_NEW_MASTER_PROBER_FMT, &shmFtsControl->masterProberDBID);
	elog(LOG, "master prober process is on segment %d", shmFtsControl->masterProberDBID);

	SendFtsResponse(&response, FTS_MSG_NEW_MASTER_PROBER);
}

static void
HandleFtsWalRepStartMasterProber(const char *query)
{
	int restart;
	int ftsPid;
	FtsResponse response = {
		false, /* IsMirrorUp */
		false, /* IsInSync */
		false, /* IsSyncRepEnabled */
		false, /* IsRoleMirror */
		false, /* RequestRetry */
		false, /* MasterProberStarted */
	};

	sscanf(query, FTS_MSG_START_MASTER_PROBER_SCAN_FMT, &restart);
	ftsPid = shmFtsControl->ftsPid;

	/* terminate the exsisting master prober process */
	if (restart == 1 && ftsPid != 0)
	{
		kill(ftsPid, SIGTERM);
		elog(LOG, "terminating master prober process: %d", ftsPid);
		for(;;)
		{
			if (kill(ftsPid, 0) == -1 &&
				errno == ESRCH)
				break;
			pg_usleep(100000L); /* 100ms */
		}
		elog(LOG, "master prober process terminated");
		ftsPid = 0;
	}

	strncpy(shmFtsControl->masterProberMessage,
			query,
			sizeof(shmFtsControl->masterProberMessage));

	elog(LOG, "start master prober process");
	/* start master prober */
	shmFtsControl->startMasterProber = true;
	SendPostmasterSignal(PMSIGNAL_START_MASTER_PROBER);
	SendFtsResponse(&response, FTS_MSG_START_MASTER_PROBER);
}

void
HandleFtsMessage(const char* query_string)
{
	SIMPLE_FAULT_INJECTOR(FtsHandleMessage);

	if (strncmp(query_string, FTS_MSG_PROBE,
				strlen(FTS_MSG_PROBE)) == 0)
		HandleFtsWalRepProbe();
	else if (strncmp(query_string, FTS_MSG_SYNCREP_OFF,
					 strlen(FTS_MSG_SYNCREP_OFF)) == 0)
		HandleFtsWalRepSyncRepOff();
	else if (strncmp(query_string, FTS_MSG_PROMOTE,
					 strlen(FTS_MSG_PROMOTE)) == 0)
		HandleFtsWalRepPromote(query_string);
	else if (strncmp(query_string, FTS_MSG_NEW_MASTER_PROBER,
					 strlen(FTS_MSG_NEW_MASTER_PROBER)) == 0)
		HandleFtsWalRepNewMasterProber(query_string);
	else if (strncmp(query_string, FTS_MSG_START_MASTER_PROBER,
					 strlen(FTS_MSG_START_MASTER_PROBER)) == 0)
		HandleFtsWalRepStartMasterProber(query_string);
	else
		ereport(ERROR,
				(errmsg("received unknown FTS query: %s", query_string)));
}
