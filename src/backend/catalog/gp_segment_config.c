/*
 * gp_segment_config.c
 *
 * Copyright (c) 2017-Present Pivotal Software, Inc.
 *
 */

#include "postgres.h"

#include "catalog/gp_segment_config.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "utils/fmgroids.h"
#include "utils/tqual.h"

/*
 * Tell the caller whether any segment mirrors exist.
 */
bool
gp_segment_config_has_mirrors()
{
	bool mirrors_exist;
	Relation rel;
	ScanKeyData scankey[1];
	SysScanDesc scan;
	HeapTuple tuple;

	rel = heap_open(GpSegmentConfigRelationId, AccessShareLock);

	/*
	 * SELECT dbid FROM gp_segment_configuration
	 * WHERE content != :1 AND role = :2
	 */
	ScanKeyInit(&scankey[0],
				Anum_gp_segment_configuration_role,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum('m'));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 1, scankey);

	tuple = systable_getnext(scan);
	mirrors_exist = HeapTupleIsValid(tuple);

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return mirrors_exist;
}
