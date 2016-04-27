#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for motion (CDB2)
#
#-------------------------------------------------------------------------



subdir = src/backend/cdb/dispatcher
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe -I$(top_builddir)/src/port -I$(top_builddir)//src/backend/utils/misc $(CPPFLAGS)

OBJS	= cdbdisp.o cdbdispatchresult.o cdbdisp_dtx.o cdbdisp_query.o cdbdisp_utils.o
include $(top_srcdir)/src/backend/common.mk

dispatcher: cdbdisp.o cdbdispatchresult.o cdbdisp_test.o \
	    fe-connect.o fe-auth.o fe-exec.o fe-misc.o fe-protocol3.o pqexpbuffer.o ip.o md5.o wchar.o \
	    stringinfo.o thread.o pgstrcasecmp.o noblock.o strlcpy.o inet_net_ntop.o
	gcc -o $@ $^ $(CFLAGS) 

fe-connect.c: $(top_srcdir)/src/backend/gp_libpq_fe/fe-connect.c
	rm -f $@ && $(LN_S) $< .
fe-auth.c: $(top_srcdir)/src/backend/gp_libpq_fe/fe-auth.c
	rm -f $@ && $(LN_S) $< .
fe-exec.c: $(top_srcdir)/src/backend/gp_libpq_fe/fe-exec.c
	rm -f $@ && $(LN_S) $< .
fe-misc.c: $(top_srcdir)/src/backend/gp_libpq_fe/fe-misc.c
	rm -f $@ && $(LN_S) $< .
fe-protocol3.c: $(top_srcdir)/src/backend/gp_libpq_fe/fe-protocol3.c
	rm -f $@ && $(LN_S) $< .
pqexpbuffer.c: $(top_srcdir)/src/backend/gp_libpq_fe/pqexpbuffer.c
	rm -f $@ && $(LN_S) $< .
ip.c: $(top_srcdir)/src/backend/libpq/ip.c
	rm -f $@ && $(LN_S) $< .
md5.c: $(top_srcdir)/src/backend/libpq/md5.c
	rm -f $@ && $(LN_S) $< .
wchar.c: $(top_srcdir)/src/backend/utils/mb/wchar.c
	rm -f $@ && $(LN_S) $< .
stringinfo.c: $(top_srcdir)/src/backend/lib/stringinfo.c
	rm -f $@ && $(LN_S) $< .
thread.c: $(top_srcdir)/src/port/thread.c
	rm -f $@ && $(LN_S) $< .
pgstrcasecmp.c: $(top_srcdir)/src/port/pgstrcasecmp.c
	rm -f $@ && $(LN_S) $< .
noblock.c: $(top_srcdir)/src/port/noblock.c
	rm -f $@ && $(LN_S) $< .
strlcpy.c: $(top_srcdir)/src/port/strlcpy.c
	rm -f $@ && $(LN_S) $< .
inet_net_ntop.c: $(top_srcdir)/src/port/inet_net_ntop.c
	rm -f $@ && $(LN_S) $< .

clean:
	rm -f *.o && find . -type l -delete
