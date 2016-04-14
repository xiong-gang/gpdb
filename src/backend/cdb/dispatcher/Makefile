#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for motion (CDB2)
#
#-------------------------------------------------------------------------



subdir = src/backend/cdb/dispatcher
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)

OBJS	= cdbdisp.o cdbdispatchresult.o cdbdisp_dtx.o cdbdisp_query.o
include $(top_srcdir)/src/backend/common.mk

dispatcher: cdbdisp.o cdbdispatchresult.o cdbdisp_dtx.o cdbdisp_query.o
	gcc -o $@ $^ $(CFLAGS) 

guc.c: $(top_srcdir)/src/backend/utils/misc/guc.c
	rm -f $@ && $(LN_S) $< .
