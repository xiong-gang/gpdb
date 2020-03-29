/*-------------------------------------------------------------------------
 *
 * distributedlog.h
 *     A GP parallel log to the Postgres clog that records the full distributed
 * xid information for each local transaction id.
 *
 * It is used to determine if the committed xid for a transaction we want to
 * determine the visibility of is for a distributed transaction or a
 * local transaction.
 *
 * By default, entries in the SLRU (Simple LRU) module used to implement this
 * log will be set to zero.  A non-zero entry indicates a committed distributed
 * transaction.
 *
 * We must persist this log and the DTM does reuse the DistributedTransactionId
 * between restarts, so we will be storing the upper half of the whole
 * distributed transaction identifier -- the timestamp -- also so we can
 * be sure which distributed transaction we are looking at.
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/distributedlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DISTRIBUTEDLOG_H
#define DISTRIBUTEDLOG_H

#include "access/xlog.h"

extern void DistributedLog_SetCommittedTree(TransactionId xid, int nxids, TransactionId *xids,
								DistributedTransactionTimeStamp	distribTimeStamp,
								DistributedTransactionId distribXid,
								bool isRedo);
extern bool DistributedLog_CommittedCheck(
							  TransactionId localXid,
							  DistributedTransactionTimeStamp *dtxStartTime,
							  DistributedTransactionId *distribXid);
extern bool DistributedLog_ScanForPrevCommitted(
									TransactionId *indexXid,
									DistributedTransactionTimeStamp *distribTimeStamp,
									DistributedTransactionId *distribXid);
extern TransactionId DistributedLog_AdvanceOldestXmin(TransactionId oldestInProgressLocalXid,
								 DistributedTransactionTimeStamp distribTimeStamp,
								 DistributedTransactionId oldestDistribXid);
extern TransactionId DistributedLog_GetOldestXmin(TransactionId oldestLocalXmin);

extern Size DistributedLog_ShmemSize(void);
extern void DistributedLog_ShmemInit(void);
extern void DistributedLog_CheckPoint(void);
extern void DistributedLog_InitOldestXmin(void);

extern const char *DistributedLog_identify(uint8 info);
extern void DistributedLog_GetDistributedXid(
				TransactionId 						localXid,
				DistributedTransactionTimeStamp		*distribTimeStamp,
				DistributedTransactionId 			*distribXid);

#endif							/* DISTRIBUTEDLOG_H */
