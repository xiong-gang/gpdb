#include "postgres.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#include "gp-libpq-fe.h"               /* prerequisite for libpq-int.h */
#include "gp-libpq-int.h"              /* PQExpBufferData */
#include "cdb/cdbpartition.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbdisp_utils.h"
#include "cdb/cdbdispatchresult.h"

int getMaxThreads()
{
	int maxThreads = 0;
	if (gp_connections_per_thread == 0)
		maxThreads = 1;	/* one, not zero, because we need to allocate one param block */
	else
		maxThreads = 1 + (largestGangsize() - 1) / gp_connections_per_thread;
	return maxThreads;
}

void cdbdisp_fillParms(DispatchCommandParms *pParms, DispatchType *mppDispatchCommandType,
						int sliceId, int maxfds, void *commandTypeParms)
{
	pParms->localSlice = sliceId;
	pParms->cmdID = gp_command_count;
	pParms->db_count = 0;
	pParms->sessUserId = GetSessionUserId();
	pParms->outerUserId = GetOuterUserId();
	pParms->currUserId = GetUserId();
	pParms->sessUserId_is_super = superuser_arg(GetSessionUserId());
	pParms->outerUserId_is_super = superuser_arg(GetOuterUserId());

	pParms->nfds = maxfds;
	pParms->fds = (struct pollfd *) palloc0(sizeof(struct pollfd) * maxfds);

	pParms->dispatchResultPtrArray =
		(CdbDispatchResult **) palloc0((gp_connections_per_thread == 0 ? largestGangsize() : gp_connections_per_thread)*
									   sizeof(CdbDispatchResult *));
	MemSet(&pParms->thread, 0, sizeof(pthread_t));

	pParms->mppDispatchCommandType = mppDispatchCommandType;
	(*mppDispatchCommandType->init)(pParms, (void*)commandTypeParms);
}

void cdbdisp_freeParms(DispatchCommandParms *pParms, bool isFirst)
{
	if (pParms->dispatchResultPtrArray)
	{
		pfree(pParms->dispatchResultPtrArray);
		pParms->dispatchResultPtrArray = NULL;
	}

	if (pParms->fds != NULL)
	{
		pfree(pParms->fds);
		pParms->fds = NULL;
	}

	(*pParms->mppDispatchCommandType->destroy)(pParms, isFirst);
}

void makeDispatcherState(CdbDispatcherState	*ds, int nResults, int nSlices, bool cancelOnError)
{
	int maxThreads = getMaxThreads();
	/* the maximum number of command parameter blocks we'll possibly need is
	 * one for each slice on the primary gang. Max sure that we
	 * have enough -- once we've created the command block we're stuck with it
	 * for the duration of this statement (including CDB-DTM ).
	 * 1 * maxthreads * slices for each primary
	 * X 2 for good measure ? */
	int paramCount = maxThreads * 4 * Max(nSlices, 5);

	ds->primaryResults = cdbdisp_makeDispatchResults(nResults, nSlices, cancelOnError);
	//todo: memory context
	ds->dispatchThreads = cdbdisp_makeDispatchThreads(paramCount);
	elog(DEBUG4, "dispatcher: allocating command array with maxslices %d paramCount %d", nSlices, paramCount);
}

HTAB *
process_aotupcounts(PartitionNode *parts, HTAB *ht,
					void *aotupcounts, int naotupcounts)
{
	PQaoRelTupCount *ao = (PQaoRelTupCount *)aotupcounts;

	if (Debug_appendonly_print_insert)
		ereport(LOG,(errmsg("found %d AO tuple counts to process",
							naotupcounts)));

	if (naotupcounts)
	{
		int j;

		for (j = 0; j < naotupcounts; j++)
		{
			if (OidIsValid(ao->aorelid))
			{
				bool found;
				PQaoRelTupCount *entry;

				if (!ht)
				{
					HASHCTL ctl;
					/* reasonable assumption? */
					long num_buckets =
						list_length(all_partition_relids(parts));
					num_buckets /= num_partition_levels(parts);

					ctl.keysize = sizeof(Oid);
					ctl.entrysize = sizeof(*entry);
					ht = hash_create("AO hash map",
									 num_buckets,
									 &ctl,
									 HASH_ELEM);
				}

				entry = hash_search(ht,
									&(ao->aorelid),
									HASH_ENTER,
									&found);

				if (found)
					entry->tupcount += ao->tupcount;
				else
					entry->tupcount = ao->tupcount;

				if (Debug_appendonly_print_insert)
					ereport(LOG,(errmsg("processed AO tuple counts for partitioned "
										"relation %d. found total " INT64_FORMAT
										"tuples", ao->aorelid, entry->tupcount)));
			}
			ao++;
		}
	}


	return ht;
}

/*
 * sum tuple counts that were added into a partitioned AO table
 */
HTAB *
cdbdisp_sumAoPartTupCount(PartitionNode *parts,
						  CdbDispatchResults *results)
{
	int i;
	HTAB *ht = NULL;

	if (!parts)
		return NULL;


	for (i = 0; i < results->resultCount; ++i)
	{
		CdbDispatchResult  *dispatchResult = &results->resultArray[i];
		int nres = cdbdisp_numPGresult(dispatchResult);
		int ires;
		for (ires = 0; ires < nres; ++ires)
		{						   /* for each PGresult */
			PGresult *pgresult = cdbdisp_getPGresult(dispatchResult, ires);

			ht = process_aotupcounts(parts, ht, (void *)pgresult->aotupcounts,
									 pgresult->naotupcounts);
		}
	}

	return ht;
}
