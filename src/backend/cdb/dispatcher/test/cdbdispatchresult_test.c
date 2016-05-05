#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include "../cdbdispatchresult.c"

static bool wrap_createPQExpBufferSwitch = false;

extern PQExpBuffer __wrap_createPQExpBuffer(void);
extern bool __wrap_cdbconn_setSliceIndex(SegmentDatabaseDescriptor *segdbDesc, int sliceIndex);

PQExpBuffer __wrap_createPQExpBuffer(void)
{
	if (wrap_createPQExpBufferSwitch)
	{
		return __real_createPQExpBuffer();
	}
	return mock_ptr_type(PQExpBuffer);
}

bool
__wrap_cdbconn_setSliceIndex(SegmentDatabaseDescriptor *segdbDesc, int sliceIndex)
{
	return mock_type(bool);
}

static void
test_make_cdbdisp_result_oom(void **state)
{
	(void) state;
	
	will_return(__wrap_createPQExpBuffer, NULL);

	int sliceIndex = 3;

	struct CdbDispatchResults *fakeResults =
		(struct CdbDispatchResults *) malloc(sizeof(struct CdbDispatchResults));
	int nbytes = 10 * sizeof(fakeResults->resultArray[0]);
    fakeResults->resultArray = palloc0(nbytes);
    fakeResults->resultCapacity = 10;
    fakeResults->resultCount = 0;
    fakeResults->iFirstError = -1;
    fakeResults->errcode = 0;
    fakeResults->cancelOnError = true;
    fakeResults->sliceMap = NULL;
    fakeResults->sliceCapacity = 10;
    nbytes = 10 * sizeof(fakeResults->sliceMap[0]);
    fakeResults->sliceMap = palloc0(nbytes);

	struct SegmentDatabaseDescriptor *fakeSegdbDesc =
		(struct SegmentDatabaseDescriptor *) malloc(sizeof(struct SegmentDatabaseDescriptor));
	MemSet(fakeSegdbDesc, 0, sizeof(*fakeSegdbDesc));
    fakeSegdbDesc->segment_database_info = NULL;
    fakeSegdbDesc->segindex = 1;
    fakeSegdbDesc->conn = NULL;
    fakeSegdbDesc->motionListener = 0;
    fakeSegdbDesc->whoami = NULL;
    fakeSegdbDesc->myAgent = NULL;
    fakeSegdbDesc->errcode = 0;
    initPQExpBuffer(&fakeSegdbDesc->error_message);

	CdbDispatchResult *result = cdbdisp_makeResult(fakeResults, fakeSegdbDesc, sliceIndex);
	assert_ptr_equal(result, NULL);

	will_return(__wrap_cdbconn_setSliceIndex, false);
	wrap_createPQExpBufferSwitch = true;
	result = cdbdisp_makeResult(fakeResults, fakeSegdbDesc, sliceIndex);
	assert_ptr_equal(result, NULL);
}

int
main(void)
{
    const struct CMUnitTest tests[] =
    {
        cmocka_unit_test(test_make_cdbdisp_result_oom)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
