/*-------------------------------------------------------------------------
 *
 * cdbdisp.h
 * routines for dispatching commands from the dispatcher process
 * to the qExec processes.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDISP_UTILS_H
#define CDBDISP_UTILS_H

#include "cdb/cdbdisp.h"
#include "miscadmin.h"

void cdbdisp_fillParms(DispatchCommandParms *pParms, DispatchType *mppDispatchCommandType,
						int sliceId, void *commandTypeParms);

#endif   /* CDBDISP_UTILS_H */
