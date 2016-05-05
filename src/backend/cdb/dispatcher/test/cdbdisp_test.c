#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include "../cdbdisp.c"

extern int __wrap_largestGangsize(void);

int
__wrap_largestGangsize(void)
{
	return mock_type(int);
}

static void
test_one_thread_per_gang(void **state)
{
    int rv;

    (void) state; /* unused */

	gp_connections_per_thread = 0;

    //will_return(__wrap_largestGangsize, 128);

    rv = getMaxThreads();

    assert_int_equal(rv, 1);
}

static void
test_multi_threads_per_gang(void **state)
{
    int rv;

    (void) state; /* unused */

	gp_connections_per_thread = 5;
    will_return(__wrap_largestGangsize, 128);

    rv = getMaxThreads();

    assert_int_equal(rv, 26);
}

int
main(void)
{
    const struct CMUnitTest tests[] =
	{
        cmocka_unit_test(test_one_thread_per_gang),
        cmocka_unit_test(test_multi_threads_per_gang)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
