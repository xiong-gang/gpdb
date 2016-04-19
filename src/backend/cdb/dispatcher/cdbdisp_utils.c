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
#include "utils/memutils.h"

static CdbDispatchCmdThreads *
cdbdisp_makeDispatchThreads(int paramCount);
static CdbDispatchResults *
cdbdisp_makeDispatchResults(int     resultCapacity,
                            int     sliceCapacity,
                            bool    cancelOnError);

int getMaxThreads()
{
	int maxThreads = 0;
	if (gp_connections_per_thread == 0)
		maxThreads = 1;	/* one, not zero, because we need to allocate one param block */
	else
		maxThreads = 1 + (largestGangsize() - 1) / gp_connections_per_thread;
	return maxThreads;
}

/*
 * cdbdisp_makeDispatchThreads:
 * Allocates memory for a CdbDispatchCmdThreads struct that holds
 * the thread count and array of dispatch command parameters (which
 * is being allocated here as well).
 */
static CdbDispatchCmdThreads *
cdbdisp_makeDispatchThreads(int paramCount)
{
	CdbDispatchCmdThreads *dThreads = palloc0(sizeof(*dThreads));
	int i = 0;
	Oid sessUserId = GetSessionUserId();
	Oid outerUserId = GetOuterUserId();
	Oid currUserId = GetUserId();
	bool sessUserIdIsSuper = superuser_arg(GetSessionUserId());
	bool outerUserIdIsSuper = superuser_arg(GetOuterUserId());
	int maxfds = 0;

	dThreads->dispatchCommandParmsAr =
		(DispatchCommandParms *)palloc0(paramCount * sizeof(DispatchCommandParms));
	dThreads->dispatchCommandParmsArSize = paramCount;
    dThreads->threadCount = 0;

	if (gp_connections_per_thread == 0)
		maxfds = largestGangsize();
	else
		maxfds = gp_connections_per_thread;

    for (i = 0; i < paramCount; i++)
    {
    	DispatchCommandParms *pParms = &dThreads->dispatchCommandParmsAr[i];

    	/* important */
    	pParms->localSlice = -1;

    	pParms->cmdID = gp_command_count;
    	pParms->sessUserId = sessUserId;
    	pParms->outerUserId = outerUserId;
    	pParms->currUserId = currUserId;
    	pParms->sessUserId_is_super = sessUserIdIsSuper;
    	pParms->outerUserId_is_super = outerUserIdIsSuper;

    	pParms->dispatchResultPtrArray =
    		(CdbDispatchResult **) palloc0((gp_connections_per_thread == 0 ? largestGangsize() : gp_connections_per_thread)*
    									   sizeof(CdbDispatchResult *));
    	pParms->nfds = maxfds;
    	pParms->fds = (struct pollfd *) palloc0(sizeof(struct pollfd) * maxfds);

    	MemSet(&pParms->thread, 0, sizeof(pthread_t));
    }

    return dThreads;
}                               /* cdbdisp_makeDispatchThreads */

/*
 * cdbdisp_makeDispatchResults:
 * Allocates a CdbDispatchResults object in the current memory context.
 * The caller is responsible for calling DestroyCdbDispatchResults on the returned
 * pointer when done using it.
 */
static CdbDispatchResults *
cdbdisp_makeDispatchResults(int     resultCapacity,
                            int     sliceCapacity,
                            bool    cancelOnError)
{
    CdbDispatchResults *results = palloc0(sizeof(*results));
    int     nbytes = resultCapacity * sizeof(results->resultArray[0]);

    results->resultArray = palloc0(nbytes);
    results->resultCapacity = resultCapacity;
    results->resultCount = 0;
    results->iFirstError = -1;
    results->errcode = 0;
    results->cancelOnError = cancelOnError;

    results->sliceMap = NULL;
    results->sliceCapacity = sliceCapacity;
    if (sliceCapacity > 0)
    {
        nbytes = sliceCapacity * sizeof(results->sliceMap[0]);
        results->sliceMap = palloc0(nbytes);
    }

    return results;
}                               /* cdbdisp_makeDispatchResults */


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
	MemoryContext oldContext;
	ds->dispatchStateContext = AllocSetContextCreate(TopMemoryContext,
													"Dispatch Context",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);
	oldContext = MemoryContextSwitchTo(ds->dispatchStateContext);
	ds->primaryResults = cdbdisp_makeDispatchResults(nResults, nSlices, cancelOnError);
	ds->dispatchThreads = cdbdisp_makeDispatchThreads(paramCount);
	MemoryContextSwitchTo(oldContext);
	elog(DEBUG4, "dispatcher: allocating command array with maxslices %d paramCount %d", nSlices, paramCount);
}

void destroyDispatcherState(CdbDispatcherState	*ds)
{
	CdbDispatchResults * results = ds->primaryResults;
    if (results->resultArray != NULL)
    {
        int i;
        for (i = 0; i < results->resultCount; i++)
        {
            cdbdisp_termResult(&results->resultArray[i]);
        }
    }

	MemoryContextDelete(ds->dispatchStateContext);
	ds->dispatchStateContext = NULL;
	ds->dispatchThreads = NULL;
	ds->primaryResults = NULL;
}

void commandParmsSetSlice(MemoryContext *cxt, DispatchCommandParms *parm, int sliceId)
{
	/* DTX command and RM command don't need slice id */
	if (sliceId < 0)
		return;

	/* set once for each parm */
	if(parm->localSlice >= 0)
		return;

	char *query = parm->query_text;
	int len = parm->query_text_len;
	char *newQuery = MemoryContextAlloc(cxt, len);
	int tmp = htonl(sliceId);

	memcpy(newQuery, query, len);
	memcpy(newQuery + 1 + sizeof(int), &tmp, sizeof(tmp));
	parm->query_text = newQuery;
	parm->localSlice = sliceId;
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
