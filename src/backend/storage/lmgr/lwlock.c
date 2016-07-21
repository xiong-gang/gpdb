/*-------------------------------------------------------------------------
 *
 * lwlock.c
 *	  Lightweight lock manager
 *
 * Lightweight locks are intended primarily to provide mutual exclusion of
 * access to shared-memory data structures.  Therefore, they offer both
 * exclusive and shared lock modes (to support read/write and read-only
 * access to a shared object).	There are few other frammishes.  User-level
 * locking should be done with the full lock manager --- which depends on
 * LWLocks to protect its shared state.
 *
 *
 * NOTES:
 *
 * This used to be a pretty straight forward reader-writer lock
 * implementation, in which the internal state was protected by a
 * spinlock. Unfortunately the overhead of taking the spinlock proved to be
 * too high for workloads/locks that were taken in shared mode very
 * frequently. Often we were spinning in the (obviously exclusive) spinlock,
 * while trying to acquire a shared lock that was actually free.
 *
 * Thus a new implementation was devised that provides wait-free shared lock
 * acquisition for locks that aren't exclusively locked.
 *
 * The basic idea is to have a single atomic variable 'lockcount' instead of
 * the formerly separate shared and exclusive counters and to use atomic
 * operations to acquire the lock. That's fairly easy to do for plain
 * rw-spinlocks, but a lot harder for something like LWLocks that want to wait
 * in the OS.
 *
 * For lock acquisition we use an atomic compare-and-exchange on the lockcount
 * variable. For exclusive lock we swap in a sentinel value
 * (LW_VAL_EXCLUSIVE), for shared locks we count the number of holders.
 *
 * To release the lock we use an atomic decrement to release the lock. If the
 * new value is zero (we get that atomically), we know we can/have to release
 * waiters.
 *
 * Obviously it is important that the sentinel value for exclusive locks
 * doesn't conflict with the maximum number of possible share lockers -
 * luckily MAX_BACKENDS makes that easily possible.
 *
 *
 * The attentive reader might have noticed that naively doing the above has a
 * glaring race condition: We try to lock using the atomic operations and
 * notice that we have to wait. Unfortunately by the time we have finished
 * queuing, the former locker very well might have already finished it's
 * work. That's problematic because we're now stuck waiting inside the OS.

 * To mitigate those races we use a two phased attempt at locking:
 *   Phase 1: Try to do it atomically, if we succeed, nice
 *   Phase 2: Add ourselves to the waitqueue of the lock
 *   Phase 3: Try to grab the lock again, if we succeed, remove ourselves from
 *            the queue
 *   Phase 4: Sleep till wake-up, goto Phase 1
 *
 * This protects us against the problem from above as nobody can release too
 *    quick, before we're queued, since after Phase 2 we're already queued.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/storage/lmgr/lwlock.c,v 1.53 2009/01/01 17:23:48 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/multixact.h"
#include "access/distributedlog.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "storage/barrier.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "utils/sharedsnapshot.h"
#include "pg_trace.h"

#include "lib/dllist.h"
#include "utils/guc.h"
#include "utils/gp_atomic.h"

/* We use the ShmemLock spinlock to protect LWLockAssign */
extern slock_t *ShmemLock;

uint64 hitcount = 0;

typedef struct LWLock
{
	slock_t		mutex;			/* Protects LWLock and queue of PGPROCs */
	int			exclusivePid;	/* PID of the exclusive holder. */
	Dllist		waiters;
	pg_atomic_uint32 state;         /* state of exlusive/nonexclusive lockers */
} LWLock;

/*
 * All the LWLock structs are allocated as an array in shared memory.
 * (LWLockIds are indexes into the array.)	We force the array stride to
 * be a power of 2, which saves a few cycles in indexing, but more
 * importantly also ensures that individual LWLocks don't cross cache line
 * boundaries.	This reduces cache contention problems, especially on AMD
 * Opterons.  (Of course, we have to also ensure that the array start
 * address is suitably aligned.)
 *
 * LWLock is between 16 and 32 bytes on all known platforms, so these two
 * cases are sufficient.
 */
#define LWLOCK_PADDED_SIZE	(sizeof(LWLock) <= 16 ? 16 : 32)

typedef union LWLockPadded
{
	LWLock		lock;
	char		pad[LWLOCK_PADDED_SIZE];
} LWLockPadded;

#define dllistIsEmpty(dl) ((!(dl)->dll_head) && (!(dl)->dll_tail))
/*
 * This points to the array of LWLocks in shared memory.  Backends inherit
 * the pointer by fork from the postmaster (except in the EXEC_BACKEND case,
 * where we have special measures to pass it down).
 */
NON_EXEC_STATIC LWLockPadded *LWLockArray = NULL;

#define LW_FLAG_HAS_WAITERS                    ((uint32) 1 << 30)
#define LW_FLAG_RELEASE_OK                     ((uint32) 1 << 29)

#define LW_VAL_EXCLUSIVE                       ((uint32) 1 << 24)
#define LW_VAL_SHARED                          1

#define LW_LOCK_MASK                           ((uint32) ((1 << 25)-1))
/* Must be greater than MAX_BACKENDS - which is 2^23-1, so we're fine. */
#define LW_SHARED_MASK                         ((uint32)(1 << 23))

/*
 * We use this structure to keep track of locked LWLocks for release
 * during error recovery.  The maximum size could be determined at runtime
 * if necessary, but it seems unlikely that more than a few locks could
 * ever be held simultaneously.
 */
#define MAX_SIMUL_LWLOCKS	100
#define MAX_FRAME_DEPTH  	64

/* LW lock object id current PGPPROC is sleeping on (valid when PGPROC->lwWaiting = true) */
static LWLockId lwWaitingLockId = NullLock;

static int	num_held_lwlocks = 0;
static LWLockId held_lwlocks[MAX_SIMUL_LWLOCKS];
static bool held_lwlocks_exclusive[MAX_SIMUL_LWLOCKS];

#undef USE_TEST_UTILS_X86

#ifdef USE_TEST_UTILS_X86
static void *held_lwlocks_addresses[MAX_SIMUL_LWLOCKS][MAX_FRAME_DEPTH];
static int32 held_lwlocks_depth[MAX_SIMUL_LWLOCKS];
#endif /* USE_TEST_UTILS_X86 */

static int	lock_addin_request = 0;
static bool lock_addin_request_allowed = true;

#ifdef LWLOCK_STATS
static int	counts_for_pid = 0;
static int *sh_acquire_counts;
static int *ex_acquire_counts;
static int *block_counts;
#endif

static bool
LWLockAttemptLock(LWLock* lock, LWLockMode mode);

static void
LWLockQueueSelf(LWLock *lock, LWLockMode mode);

static void
LWLockDequeueSelf(LWLock *lock);

static void
LWLockWakeup(LWLock *lock);

#ifdef LOCK_DEBUG
bool		Trace_lwlocks = false;

inline static void
PRINT_LWDEBUG(const char *where, LWLockId lockid, const volatile LWLock *lock)
{
	if (Trace_lwlocks)
		elog(LOG, "%s(%d): excl %d excl pid %d shared %d head %p rOK %d",
			 where, (int) lockid,
			 !!(state & LW_VAL_EXCLUSIVE), lock->exclusivePid, state & LW_SHARED_MASK, lock->waiters->dll_head,
			 !!(state & LW_FLAG_RELEASE_OK));
}

inline static void
LOG_LWDEBUG(const char *where, LWLockId lockid, const char *msg)
{
	if (Trace_lwlocks)
		elog(LOG, "%s(%d): %s", where, (int) lockid, msg);
}
#else							/* not LOCK_DEBUG */
#define PRINT_LWDEBUG(a,b,c)
#define LOG_LWDEBUG(a,b,c)
#endif   /* LOCK_DEBUG */

#ifdef LWLOCK_STATS

static void
print_lwlock_stats(int code, Datum arg)
{
	int			i;
	int		   *LWLockCounter = (int *) ((char *) LWLockArray - 2 * sizeof(int));
	int			numLocks = LWLockCounter[1];

	/* Grab an LWLock to keep different backends from mixing reports */
	LWLockAcquire(0, LW_EXCLUSIVE);

	for (i = 0; i < numLocks; i++)
	{
		if (sh_acquire_counts[i] || ex_acquire_counts[i] || block_counts[i])
			fprintf(stderr, "PID %d lwlock %d: shacq %u exacq %u blk %u\n",
					MyProcPid, i, sh_acquire_counts[i], ex_acquire_counts[i],
					block_counts[i]);
	}

	LWLockRelease(0);
}
#endif   /* LWLOCK_STATS */


/*
 * Compute number of LWLocks to allocate.
 */
int
NumLWLocks(void)
{
	int			numLocks;

	/*
	 * Possibly this logic should be spread out among the affected modules,
	 * the same way that shmem space estimation is done.  But for now, there
	 * are few enough users of LWLocks that we can get away with just keeping
	 * the knowledge here.
	 */

	/* Predefined LWLocks */
	numLocks = (int) NumFixedLWLocks;

	/* bufmgr.c needs two for each shared buffer */
	numLocks += 2 * NBuffers;

	/* clog.c needs one per CLOG buffer */
	numLocks += NUM_CLOG_BUFFERS;

	/* subtrans.c needs one per SubTrans buffer */
	numLocks += NUM_SUBTRANS_BUFFERS;
    
    /* cdbtm.c needs one lock */
    numLocks++;
    
    /* cdbfts.c needs one lock */
    numLocks++;

	/* multixact.c needs two SLRU areas */
	numLocks += NUM_MXACTOFFSET_BUFFERS + NUM_MXACTMEMBER_BUFFERS;

	/* cdbdistributedlog.c needs one per DistributedLog buffer */
	numLocks += NUM_DISTRIBUTEDLOG_BUFFERS;

	/* sharedsnapshot.c needs one per shared snapshot slot */
	numLocks += NUM_SHARED_SNAPSHOT_SLOTS;
    
	/*
	 * Add any requested by loadable modules; for backwards-compatibility
	 * reasons, allocate at least NUM_USER_DEFINED_LWLOCKS of them even if
	 * there are no explicit requests.
	 */
	lock_addin_request_allowed = false;
	numLocks += Max(lock_addin_request, NUM_USER_DEFINED_LWLOCKS);

	return numLocks;
}


/*
 * RequestAddinLWLocks
 *		Request that extra LWLocks be allocated for use by
 *		a loadable module.
 *
 * This is only useful if called from the _PG_init hook of a library that
 * is loaded into the postmaster via shared_preload_libraries.	Once
 * shared memory has been allocated, calls will be ignored.  (We could
 * raise an error, but it seems better to make it a no-op, so that
 * libraries containing such calls can be reloaded if needed.)
 */
void
RequestAddinLWLocks(int n)
{
	if (IsUnderPostmaster || !lock_addin_request_allowed)
		return;					/* too late */
	lock_addin_request += n;
}


/*
 * Compute shmem space needed for LWLocks.
 */
Size
LWLockShmemSize(void)
{
	Size		size;
	int			numLocks = NumLWLocks();

	/* Space for the LWLock array. */
	size = mul_size(numLocks, sizeof(LWLockPadded));

	/* Space for dynamic allocation counter, plus room for alignment. */
	size = add_size(size, 2 * sizeof(int) + LWLOCK_PADDED_SIZE);

	return size;
}


/*
 * Allocate shmem space for LWLocks and initialize the locks.
 */
void
CreateLWLocks(void)
{
	int			numLocks = NumLWLocks();
	Size		spaceLocks = LWLockShmemSize();
	LWLockPadded *lock;
	int		   *LWLockCounter;
	char	   *ptr;
	int			id;

	Assert(LW_VAL_EXCLUSIVE > (uint32) MAX_MAX_BACKENDS);

	/* Allocate space */
	ptr = (char *) ShmemAlloc(spaceLocks);

	/* Leave room for dynamic allocation counter */
	ptr += 2 * sizeof(int);

	/* Ensure desired alignment of LWLock array */
	ptr += LWLOCK_PADDED_SIZE - ((unsigned long) ptr) % LWLOCK_PADDED_SIZE;

	LWLockArray = (LWLockPadded *) ptr;

	/*
	 * Initialize all LWLocks to "unlocked" state
	 */
	for (id = 0, lock = LWLockArray; id < numLocks; id++, lock++)
	{
		SpinLockInit(&lock->lock.mutex);
		pg_atomic_init_u32(&lock->lock.state, LW_FLAG_RELEASE_OK);
		DLInitList(&lock->lock.waiters);
	}

	/*
	 * Initialize the dynamic-allocation counter, which is stored just before
	 * the first LWLock.
	 */
	LWLockCounter = (int *) ((char *) LWLockArray - 2 * sizeof(int));
	LWLockCounter[0] = (int) NumFixedLWLocks;
	LWLockCounter[1] = numLocks;
}


/*
 * LWLockAssign - assign a dynamically-allocated LWLock number
 *
 * We interlock this using the same spinlock that is used to protect
 * ShmemAlloc().  Interlocking is not really necessary during postmaster
 * startup, but it is needed if any user-defined code tries to allocate
 * LWLocks after startup.
 */
LWLockId
LWLockAssign(void)
{
	LWLockId	result;

	/* use volatile pointer to prevent code rearrangement */
	volatile int *LWLockCounter;

	LWLockCounter = (int *) ((char *) LWLockArray - 2 * sizeof(int));
	SpinLockAcquire(ShmemLock);
	if (LWLockCounter[0] >= LWLockCounter[1])
	{
		SpinLockRelease(ShmemLock);
		elog(ERROR, "no more LWLockIds available");
	}
	result = (LWLockId) (LWLockCounter[0]++);
	SpinLockRelease(ShmemLock);
	return result;
}

#ifdef LOCK_DEBUG

static void
LWLockTryLockWaiting(
		PGPROC	   *proc, 
		LWLockId lockid, 
		LWLockMode mode)
{
	volatile LWLock *lock = &(LWLockArray[lockid].lock);
	int 			milliseconds = 0;
	int				exclusivePid;
	
	while(true)
	{
		pg_usleep(5000L);
		if (PGSemaphoreTryLock(&proc->sem))
		{
			if (milliseconds >= 750)
				elog(LOG, "Done waiting on lockid %d", lockid);
			return;
		}

		milliseconds += 5;
		if (milliseconds == 750)
		{
			int l;
			int count = 0;
			char buffer[200];

			SpinLockAcquire(&lock->mutex);
			
			if (lock->exclusive > 0)
				exclusivePid = lock->exclusivePid;
			else
				exclusivePid = 0;
			
			SpinLockRelease(&lock->mutex);

			memcpy(buffer, "none", 5);
			
			for (l = 0; l < num_held_lwlocks; l++)
			{
				if (l == 0)
					count += sprintf(&buffer[count],"(");
				else
					count += sprintf(&buffer[count],", ");
				
				count += sprintf(&buffer[count],
							    "lockid %d",
							    held_lwlocks[l]);
			}
			if (num_held_lwlocks > 0)
				count += sprintf(&buffer[count],")");
				
			elog(LOG, "Waited .75 seconds on lockid %d with no success. Exclusive pid %d. Already held: %s", 
				 lockid, exclusivePid, buffer);

		}
	}
}

#endif

// Turn this on if we find a deadlock or missing unlock issue...
// #define LWLOCK_TRACE_MIRROREDLOCK

/*
 * LWLockAcquire - acquire a lightweight lock in the specified mode
 *
 * If the lock is not available, sleep until it is.
 *
 * Side effect: cancel/die interrupts are held off until lock release.
 */
void
LWLockAcquire(LWLockId lockid, LWLockMode mode)
{
	LWLock *lock = &(LWLockArray[lockid].lock);
	PGPROC	   *proc = MyProc;
	int			extraWaits = 0;

	PRINT_LWDEBUG("LWLockAcquire", lockid, lock);

#ifdef LWLOCK_STATS
	/* Set up local count state first time through in a given process */
	if (counts_for_pid != MyProcPid)
	{
		int		   *LWLockCounter = (int *) ((char *) LWLockArray - 2 * sizeof(int));
		int			numLocks = LWLockCounter[1];

		sh_acquire_counts = calloc(numLocks, sizeof(int));
		ex_acquire_counts = calloc(numLocks, sizeof(int));
		block_counts = calloc(numLocks, sizeof(int));

		if(!sh_acquire_counts || !ex_acquire_counts || !block_counts)
			ereport(ERROR, errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("LWLockAcquire failed: out of memory"));

		counts_for_pid = MyProcPid;
		on_shmem_exit(print_lwlock_stats, 0);
	}
	/* Count lock acquisition attempts */
	if (mode == LW_EXCLUSIVE)
		ex_acquire_counts[lockid]++;
	else
		sh_acquire_counts[lockid]++;
#endif   /* LWLOCK_STATS */

	/*
	 * We can't wait if we haven't got a PGPROC.  This should only occur
	 * during bootstrap or shared memory initialization.  Put an Assert here
	 * to catch unsafe coding practices.
	 */
	Assert(!(proc == NULL && IsUnderPostmaster));

	/* Ensure we will have room to remember the lock */
	if (num_held_lwlocks >= MAX_SIMUL_LWLOCKS)
		elog(ERROR, "too many LWLocks taken");

	/*
	 * Lock out cancel/die interrupts until we exit the code section protected
	 * by the LWLock.  This ensures that interrupts will not interfere with
	 * manipulations of data structures in shared memory.
	 */
	HOLD_INTERRUPTS();

	/*
	 * Loop here to try to acquire lock after each time we are signaled by
	 * LWLockRelease.
	 *
	 * NOTE: it might seem better to have LWLockRelease actually grant us the
	 * lock, rather than retrying and possibly having to go back to sleep. But
	 * in practice that is no good because it means a process swap for every
	 * lock acquisition when two or more processes are contending for the same
	 * lock.  Since LWLocks are normally used to protect not-very-long
	 * sections of computation, a process needs to be able to acquire and
	 * release the same lock many times during a single CPU time slice, even
	 * in the presence of contention.  The efficiency of being able to do that
	 * outweighs the inefficiency of sometimes wasting a process dispatch
	 * cycle because the lock is not free when a released waiter finally gets
	 * to run.	See pgsql-hackers archives for 29-Dec-01.
	 */
	for (;;)
	{
		bool		mustwait;
		int			c;

        /*
         * Try to grab the lock the first time, we're not in the waitqueue
         * yet/anymore.
         */
        mustwait = LWLockAttemptLock(lock, mode);

		if (!mustwait)
		{
			LOG_LWDEBUG("LWLockAcquire", lockid, "immediately acquired lock!");
			break;				/* got the lock */
		}

        /*
        * Ok, at this point we couldn't grab the lock on the first try. We
        * cannot simply queue ourselves to the end of the list and wait to be
        * woken up because by now the lock could long have been released.
        * Instead add us to the queue and try to grab the lock again. If we
        * succeed we need to revert the queuing and be happy, otherwise we
        * recheck the lock. If we still couldn't grab it, we know that the
        * other lock will see our queue entries when releasing since they
        * existed before we checked for the lock.
        */
	
		/* add to queue */	
		LWLockQueueSelf(lock, mode);
        lwWaitingLockId = lockid;

       /* we're now guaranteed to be woken up if necessary */
        mustwait = LWLockAttemptLock(lock, mode);

		if (!mustwait)
		{
			LOG_LWDEBUG("LWLockAcquire", lockid, "acquired, undoing queue!");
			LWLockDequeueSelf(lock);
        	lwWaitingLockId = NullLock;
			break;
		}

		/*
		 * Wait until awakened.
		 *
		 * Since we share the process wait semaphore with the regular lock
		 * manager and ProcWaitForSignal, and we may need to acquire an LWLock
		 * while one of those is pending, it is possible that we get awakened
		 * for a reason other than being signaled by LWLockRelease. If so,
		 * loop back and wait again.  Once we've gotten the LWLock,
		 * re-increment the sema by the number of additional signals received,
		 * so that the lock manager or signal manager will see the received
		 * signal when it next waits.
		 */
		LOG_LWDEBUG("LWLockAcquire", lockid, "waiting");

#ifdef LWLOCK_TRACE_MIRROREDLOCK
	if (lockid == MirroredLock)
		elog(LOG, "LWLockAcquire: waiting for MirroredLock (PID %u)", MyProcPid);
#endif

#ifdef LWLOCK_STATS
		block_counts[lockid]++;
#endif

		for (c = 0; c < num_held_lwlocks; c++)
		{
			if (held_lwlocks[c] == lockid)
				elog(PANIC, "Waiting on lock already held!");
		}

		TRACE_POSTGRESQL_LWLOCK_WAIT_START(lockid, mode);

		for (;;)
		{
			/* "false" means cannot accept cancel/die interrupt here. */
#ifndef LOCK_DEBUG
			PGSemaphoreLock(&proc->sem, false);
#else
			LWLockTryLockWaiting(proc, lockid, mode);
#endif
			if (!proc->lwWaiting)
				break;
			extraWaits++;
		}

		TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(lockid, mode);

        /* Retrying, allow LWLockRelease to release waiters again. */
        pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);


		LOG_LWDEBUG("LWLockAcquire", lockid, "awakened");

#ifdef LWLOCK_TRACE_MIRROREDLOCK
		if (lockid == MirroredLock)
			elog(LOG, "LWLockAcquire: awakened for MirroredLock (PID %u)", MyProcPid);
#endif
	}

	TRACE_POSTGRESQL_LWLOCK_ACQUIRE(lockid, mode);

#ifdef LWLOCK_TRACE_MIRROREDLOCK
	if (lockid == MirroredLock)
		elog(LOG, "LWLockAcquire: MirroredLock by PID %u in held_lwlocks[%d] %s", 
			 MyProcPid, 
			 num_held_lwlocks,
			 (mode == LW_EXCLUSIVE ? "Exclusive" : "Shared"));
#endif

	/* Add lock to list of locks held by this backend */
	held_lwlocks_exclusive[num_held_lwlocks] = (mode == LW_EXCLUSIVE);
	held_lwlocks[num_held_lwlocks++] = lockid;

	/*
	 * Fix the process wait semaphore's count for any absorbed wakeups.
	 */
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(&proc->sem);
}

/*
 * LWLockConditionalAcquire - acquire a lightweight lock in the specified mode
 *
 * If the lock is not available, return FALSE with no side-effects.
 *
 * If successful, cancel/die interrupts are held off until lock release.
 */
bool
LWLockConditionalAcquire(LWLockId lockid, LWLockMode mode)
{
	LWLock *lock = &(LWLockArray[lockid].lock);
	bool		mustwait;

	PRINT_LWDEBUG("LWLockConditionalAcquire", lockid, lock);

	/* Ensure we will have room to remember the lock */
	if (num_held_lwlocks >= MAX_SIMUL_LWLOCKS)
		elog(ERROR, "too many LWLocks taken");

	/*
	 * Lock out cancel/die interrupts until we exit the code section protected
	 * by the LWLock.  This ensures that interrupts will not interfere with
	 * manipulations of data structures in shared memory.
	 */
	HOLD_INTERRUPTS();


    /* Check for the lock */
    mustwait = LWLockAttemptLock(lock, mode);

	if (mustwait)
	{
		/* Failed to get lock, so release interrupt holdoff */
		RESUME_INTERRUPTS();
		LOG_LWDEBUG("LWLockConditionalAcquire", lockid, "failed");
		TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL(lockid, mode);
	}
	else
	{
#ifdef LWLOCK_TRACE_MIRROREDLOCK
		if (lockid == MirroredLock)
			elog(LOG, "LWLockConditionalAcquire: MirroredLock by PID %u in held_lwlocks[%d] %s", 
				 MyProcPid, 
				 num_held_lwlocks,
				 (mode == LW_EXCLUSIVE ? "Exclusive" : "Shared"));
#endif

		/* Add lock to list of locks held by this backend */
		held_lwlocks_exclusive[num_held_lwlocks] = (mode == LW_EXCLUSIVE);
		held_lwlocks[num_held_lwlocks++] = lockid;
		TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE(lockid, mode);
	}

	return !mustwait;
}

/*
 * LWLockRelease - release a previously acquired lock
 */
void
LWLockRelease(LWLockId lockid)
{
	LWLock *lock = &(LWLockArray[lockid].lock);
	int			i;
	bool		saveExclusive;
	bool		check_waiters;
	uint32		oldstate;
	PRINT_LWDEBUG("LWLockRelease", lockid, lock);

	/*
	 * Remove lock from list of locks held.  Usually, but not always, it will
	 * be the latest-acquired lock; so search array backwards.
	 */
	for (i = num_held_lwlocks; --i >= 0;)
	{
		if (lockid == held_lwlocks[i])
			break;
	}
	if (i < 0)
		elog(ERROR, "lock %d is not held", (int) lockid);

	saveExclusive = held_lwlocks_exclusive[i];
	if (InterruptHoldoffCount <= 0)
		elog(PANIC, "upon entering lock release, the interrupt holdoff count is bad (%d) for release of lock %d (%s)", 
			 InterruptHoldoffCount,
			 (int)lockid,
			 (saveExclusive ? "Exclusive" : "Shared"));

#ifdef LWLOCK_TRACE_MIRROREDLOCK
	if (lockid == MirroredLock)
		elog(LOG, 
			 "LWLockRelease: release for MirroredLock by PID %u in held_lwlocks[%d] %s", 
			 MyProcPid, 
			 i,
			 (held_lwlocks_exclusive[i] ? "Exclusive" : "Shared"));
#endif
	
	num_held_lwlocks--;
	for (; i < num_held_lwlocks; i++)
	{
		held_lwlocks_exclusive[i] = held_lwlocks_exclusive[i + 1];
		held_lwlocks[i] = held_lwlocks[i + 1];
	}

	// Clear out old last entry.
	held_lwlocks_exclusive[num_held_lwlocks] = false;
	held_lwlocks[num_held_lwlocks] = 0;

	if (saveExclusive)
		oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_EXCLUSIVE);
	else
		oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_SHARED);

	 /* nobody else can have that kind of lock */
	 Assert(!(oldstate & LW_VAL_EXCLUSIVE));

    /*
     * We're still waiting for backends to get scheduled, don't wake them up
     * again.
     */
    if ((oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)) ==
            (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK) &&
            (oldstate & LW_LOCK_MASK) == 0)
            check_waiters = true;
    else
            check_waiters = false;

	/*
     * As waking up waiters requires the spinlock to be acquired, only do so
     * if necessary.
     */
	if (check_waiters)
	{
		 LWLockWakeup(lock);
	}

	TRACE_POSTGRESQL_LWLOCK_RELEASE(lockid);

	/*
	 * Now okay to allow cancel/die interrupts.
	 */
	if (InterruptHoldoffCount <= 0)
		elog(PANIC, "upon exiting lock release, the interrupt holdoff count is bad (%d) for release of lock %d (%s)", 
			 InterruptHoldoffCount,
			 (int)lockid,
			 (saveExclusive ? "Exclusive" : "Shared"));
	RESUME_INTERRUPTS();
}

/*
 * LWLockWaitCancel - cancel currently waiting on LW lock
 *
 * Used to clean up before immediate exit in certain very special situations
 * like shutdown request to Filerep Resync Manger or Workers. Although this is
 * not the best practice it is necessary to avoid any starvation situations
 * during filerep transition situations (Resync Mode -> Changetracking mode)
 *
 * Note:- This function should not be used for normal situations. It is strictly
 * written for very special situations. If you need to use this, you may want
 * to re-think your design.
 */
void
LWLockWaitCancel(void)
{
	LWLock *lwWaitingLock = NULL;

	/* We better have a PGPROC structure */
	Assert(MyProc != NULL);

	/* If we're not waiting on any LWLock then nothing doing here */
	if (!MyProc->lwWaiting)
		return;

	lwWaitingLock = &(LWLockArray[lwWaitingLockId].lock);

	/* Protect from other modifiers */
	SpinLockAcquire(&lwWaitingLock->mutex);
	DLRemove(&MyProc->lwWaitLink);
	/* Done with modification */
	SpinLockRelease(&lwWaitingLock->mutex);

	return;
}

/*
 * LWLockReleaseAll - release all currently-held locks
 *
 * Used to clean up after ereport(ERROR). An important difference between this
 * function and retail LWLockRelease calls is that InterruptHoldoffCount is
 * unchanged by this operation.  This is necessary since InterruptHoldoffCount
 * has been set to an appropriate level earlier in error recovery. We could
 * decrement it below zero if we allow it to drop for each released lock!
 */
void
LWLockReleaseAll(void)
{
	while (num_held_lwlocks > 0)
	{
		HOLD_INTERRUPTS();		/* match the upcoming RESUME_INTERRUPTS */

		LWLockRelease(held_lwlocks[num_held_lwlocks - 1]);
	}
}


/*
 * LWLockHeldByMe - test whether my process currently holds a lock
 *
 * This is meant as debug support only.  We do not distinguish whether the
 * lock is held shared or exclusive.
 */
bool
LWLockHeldByMe(LWLockId lockid)
{
	int			i;

	for (i = 0; i < num_held_lwlocks; i++)
	{
		if (held_lwlocks[i] == lockid)
			return true;
	}
	return false;
}

/*
 * LWLockHeldByMe - test whether my process currently holds an exclusive lock
 *
 * This is meant as debug support only.  We do not distinguish whether the
 * lock is held shared or exclusive.
 */
bool
LWLockHeldExclusiveByMe(LWLockId lockid)
{
	int			i;

	for (i = 0; i < num_held_lwlocks; i++)
	{
		if (held_lwlocks[i] == lockid &&
			held_lwlocks_exclusive[i])
			return true;
	}
	return false;
}

/*
 * Internal function that tries to atomically acquire the lwlock in the passed
 * in mode.
 *
 * This function will not block waiting for a lock to become free - that's the
 * callers job.
 *
 * Returns true if the lock isn't free and we need to wait.
 */
static bool
LWLockAttemptLock(LWLock* lock, LWLockMode mode)
{
       AssertArg(mode == LW_EXCLUSIVE || mode == LW_SHARED);

       /* loop until we've determined whether we could acquire the lock or not */
       while (true)
       {
               uint32 old_state;
               uint32 expected_state;
               uint32 desired_state;
               bool lock_free;

               old_state = pg_atomic_read_u32(&lock->state);
               expected_state = old_state;
               desired_state = expected_state;

               if (mode == LW_EXCLUSIVE)
               {
                       lock_free = (expected_state & LW_LOCK_MASK) == 0;
                       if (lock_free)
                               desired_state += LW_VAL_EXCLUSIVE;
               }
               else
               {
                       lock_free = (expected_state & LW_VAL_EXCLUSIVE) == 0;
                       if (lock_free)
                               desired_state += LW_VAL_SHARED;
               }

               /*
                * Attempt to swap in the state we are expecting. If we didn't see
                * lock to be free, that's just the old value. If we saw it as free,
                * we'll attempt to mark it acquired. The reason that we always swap
                * in the value is that this doubles as a memory barrier. We could try
                * to be smarter and only swap in values if we saw the lock as free,
                * but benchmark haven't shown it as beneficial so far.
                *
                * Retry if the value changed since we last looked at it.
                */
               if (pg_atomic_compare_exchange_u32(&lock->state, &expected_state, desired_state))

               {
                       if (lock_free)
                       {
                               /* Great! Got the lock. */
#ifdef LOCK_DEBUG
                               if (mode == LW_EXCLUSIVE)
                                       lock->owner = MyProc;
#endif
                               return false;
                       }
                       else
                               return true; /* someobdy else has the lock */
               }
       }
}

/*
 * Add ourselves to the end of the queue.
 *
 * NB: Mode can be LW_WAIT_UNTIL_FREE here!
 */
static void
LWLockQueueSelf(LWLock *lock, LWLockMode mode)
{
       /*
        * If we don't have a PGPROC structure, there's no way to wait. This
        * should never occur, since MyProc should only be null during shared
        * memory initialization.
        */
       if (MyProc == NULL)
               elog(PANIC, "cannot wait without a PGPROC structure");

       if (MyProc->lwWaiting)
               elog(PANIC, "queueing for lock while waiting on another one");

       SpinLockAcquire(&lock->mutex);

       /* setting the flag is protected by the spinlock */
       pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_HAS_WAITERS);
       MyProc->lwWaiting = true;
       MyProc->lwExclusive = (mode == LW_EXCLUSIVE);

	   DLAddHead(&lock->waiters, &MyProc->lwWaitLink);

       /* Can release the mutex now */
       SpinLockRelease(&lock->mutex);
}

/*
 * Remove ourselves from the waitlist.
 *
 * This is used if we queued ourselves because we thought we needed to sleep
 * but, after further checking, we discovered that we don't actually need to
 * do so. Returns false if somebody else already has woken us up, otherwise
 * returns true.
 */
static void
LWLockDequeueSelf(LWLock *lock)
{
       bool    found = false;
	   Dlelem * elt;

       SpinLockAcquire(&lock->mutex);

       /*
        * Can't just remove ourselves from the list, but we need to iterate over
        * all entries as somebody else could have unqueued us.
        */
	   	for (elt = DLGetHead(&lock->waiters); elt; elt = DLGetSucc(elt))
		{
			PGPROC * proc = (PGPROC *) DLE_VAL(elt);	
			if (proc == MyProc)
			{
				found = true;
				DLRemove(&proc->lwWaitLink);
				break;
			}
		}
				
       if (dllistIsEmpty(&lock->waiters) &&
               (pg_atomic_read_u32(&lock->state) & LW_FLAG_HAS_WAITERS) != 0)
       {
               pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_HAS_WAITERS);
       }

       SpinLockRelease(&lock->mutex);

       /* clear waiting state again, nice for debugging */
       if (found)
               MyProc->lwWaiting = false;
       else
       {
               int             extraWaits = 0;

               /*
                * Somebody else dequeued us and has or will wake us up. Deal with the
                * superflous absorption of a wakeup.
                */

               /*
                * Reset releaseOk if somebody woke us before we removed ourselves -
                * they'll have set it to false.
                */
               pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

               /*
                * Now wait for the scheduled wakeup, otherwise our ->lwWaiting would
                * get reset at some inconvenient point later. Most of the time this
                * will immediately return.
                */
               for (;;)
               {
                       /* "false" means cannot accept cancel/die interrupt here. */
                       PGSemaphoreLock(&MyProc->sem, false);
                       if (!MyProc->lwWaiting)
                               break;
                       extraWaits++;
               }

               /*
                * Fix the process wait semaphore's count for any absorbed wakeups.
                */
               while (extraWaits-- > 0)
                       PGSemaphoreUnlock(&MyProc->sem);
       }
}

/*
 * Wakeup all the lockers that currently have a chance to acquire the lock.
 */
static void
LWLockWakeup(LWLock *lock)
{
       bool            new_release_ok = true;
       bool            wokeup_somebody = false;
	   Dlelem	*elt;
	   Dlelem	*nextElt;
	   PGPROC	*waiter = NULL;

       Dllist wakeup;
	   DLInitList(&wakeup);

       /* Acquire mutex.  Time spent holding mutex should be short! */
       SpinLockAcquire(&lock->mutex);
	   	for (elt = DLGetHead(&lock->waiters); elt; elt = DLGetSucc(elt))
		{
			waiter = (PGPROC *) DLE_VAL(elt);	
	
               if (wokeup_somebody && waiter->lwExclusive == true)
			{

                       continue;
			}
                DLRemove(&waiter->lwWaitLink);
				DLAddTail(&wakeup, &waiter->lwWaitLink);
                       /*
                        * Prevent additional wakeups until retryer gets to run. Backends
                        * that are just waiting for the lock to become free don't retry
                        * automatically.
                        */
                new_release_ok = false;
                       /*
                        * Don't wakeup (further) exclusive locks.
                        */
                wokeup_somebody = true;
               /*
                * Once we've woken up an exclusive lock, there's no point in waking
                * up anybody else.
                */
               if(waiter->lwExclusive == true)
                       break;
       }

       Assert(dllistIsEmpty(&wakeup) || pg_atomic_read_u32(&lock->state) & LW_FLAG_HAS_WAITERS);

       /* Unset both flags at once if required */
       if (!new_release_ok && dllistIsEmpty(&wakeup)) 
               pg_atomic_fetch_and_u32(&lock->state, ~(LW_FLAG_RELEASE_OK | LW_FLAG_HAS_WAITERS));
       else if (!new_release_ok)
               pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_RELEASE_OK);
       else if (dllistIsEmpty(&wakeup))
               pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_HAS_WAITERS);
       else if (new_release_ok)
               pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

       /* We are done updating the shared state of the lock queue. */
       SpinLockRelease(&lock->mutex);

       /* Awaken any waiters I removed from the queue. */
	   	for (elt = DLGetHead(&wakeup); elt; elt = nextElt)
		{
			   nextElt = DLGetSucc(elt);
               DLRemove(&waiter->lwWaitLink);

			   waiter = (PGPROC *) DLE_VAL(elt);	
               /*
                * Guarantee that lwWaiting being unset only becomes visible once the
                * unlink from the link has completed. Otherwise the target backend
                * could be woken up for other reason and enqueue for a new lock - if
                * that happens before the list unlink happens, the list would end up
                * being corrupted.
                *
                * The barrier pairs with the SpinLockAcquire() when enqueing for
                * another lock.
                */
               pg_write_barrier();
               waiter->lwWaiting = false;
               PGSemaphoreUnlock(&waiter->sem);
       }
}

