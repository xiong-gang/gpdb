#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/commands
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/commands/Makefile,v 1.39 2008/12/19 16:25:17 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/commands
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)

OBJS = aggregatecmds.o alter.o analyze.o analyzeutils.o async.o cluster.o comment.o  \
	conversioncmds.o copy.o \
	dbcommands.o define.o explain.o extprotocolcmds.o filespace.o functioncmds.o \
	indexcmds.o lockcmds.o operatorcmds.o opclasscmds.o \
	portalcmds.o prepare.o proclang.o queue.o \
	schemacmds.o sequence.o tablecmds.o tablespace.o trigger.o \
	typecmds.o user.o vacuum.o vacuumlazy.o variable.o view.o

include $(top_srcdir)/src/backend/common.mk
