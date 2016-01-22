/*-------------------------------------------------------------------------
 *
 * deadlockdetector.c
 *
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEADLOCKDETECTOR_H 
#define DEADLOCKDETECTOR_H

#ifndef _WIN32
#include <stdint.h>             /* uint32_t (C99) */
#else
typedef unsigned int uint32_t;
#endif

extern int deadlockdetector_start(void);


#endif   
