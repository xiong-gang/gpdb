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
#include "cdb/cdbdispatchresult.h"
#include "utils/memutils.h"

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
