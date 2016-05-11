#include "postgres.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbconn.h"

extern Gang *allocateWriterGang();

int main(int argc, char *argv[])
{
	if (argc != 2)
	{
		printf("must supply a param for gang size\n");
		return 1;
	}

	main_tid = pthread_self();

	int i = 0;
	int gangSize = atoi(argv[1]);
	struct CdbDispatcherState ds = {0};


	Gang *mockGang = allocateWriterGang();
	if (mockGang == NULL)
	{
		printf("create gang failed\n");
		return -1;
	}
	cdbdisp_makeDispatcherState(&ds, gangSize, 0, false);

	CdbDispatchCmdThreads *dThreads = ds.dispatchThreads;
	struct DispatchCommandParms *pParms;
	char queryText[100];
	memset(queryText, 'a', 100);
	queryText[0] = 'M';

	int tmp = htonl(20);
	memcpy(queryText+1, &tmp, sizeof(tmp));


	for (i = 0; i < dThreads->dispatchCommandParmsArSize; i++)
	{
		pParms = &dThreads->dispatchCommandParmsAr[i];
		pParms->query_text = queryText;
		pParms->query_text_len = 21;
	}
	ds.primaryResults->writer_gang = mockGang;


	PG_TRY();
	{
		/* Launch the command.	Don't cancel on error. */
		cdbdisp_dispatchToGang(&ds, mockGang, 0, DEFAULT_DISP_DIRECT);

		/* Wait for all QEs to finish.	Don't cancel. */
		cdbdisp_finishCommand(&ds, NULL, NULL);
	}
	PG_CATCH();
	{
		/* Something happend, clean up after ourselves */
		CdbCheckDispatchResult((struct CdbDispatcherState *)&ds,
							   DISPATCH_WAIT_CANCEL);

		cdbdisp_destroyDispatcherState((struct CdbDispatcherState *)&ds);
		PG_RE_THROW();
		/* not reached */
	}
	PG_END_TRY();

	printf("end\n");
}
