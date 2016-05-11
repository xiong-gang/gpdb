#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "../cdbdispatchresult.c"

#define UNITTEST_NUM_SEGS 2

struct CdbDispatchResults *
_init_cdbdisp_makeResult()
{
	struct CdbDispatchResults *results = 
		(struct CdbDispatchResults *) palloc0(sizeof(*results));

	results->resultArray = palloc0(UNITTEST_NUM_SEGS * sizeof(results->resultArray[0]));
	results->resultCapacity = UNITTEST_NUM_SEGS;

	return results;
}


void
test__cdbdisp_makeResult__oom(void **state)
{
	CdbDispatchResult *result = NULL;

	struct CdbDispatchResults *results = _init_cdbdisp_makeResult();
	struct SegmentDatabaseDescriptor *segdbDesc =
		(struct SegmentDatabaseDescriptor *) palloc0(sizeof(struct SegmentDatabaseDescriptor));

	/*
	 * OOM in cdbconn_setSliceIndex
	 */
	will_return(cdbconn_setSliceIndex, false);
	expect_any(cdbconn_setSliceIndex, segdbDesc);
	expect_any(cdbconn_setSliceIndex, sliceIndex);
	result = cdbdisp_makeResult(results, segdbDesc, 0);
	assert_true(result == NULL);
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

    const UnitTest tests[] =
    {
        unit_test(test__cdbdisp_makeResult__oom)
    };

	Gp_role = GP_ROLE_DISPATCH;
	MemoryContextInit();

    return run_tests(tests);
}
