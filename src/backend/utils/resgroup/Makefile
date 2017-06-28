#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/resgroup
#
# IDENTIFICATION
#    src/backend/utils/resgroup/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/resgroup
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)

OBJS = resgroup.o

ifeq ($(PORTNAME),linux)
OBJS += resgroup-ops-linux.o
else
OBJS += resgroup-ops-dummy.o
endif

include $(top_srcdir)/src/backend/common.mk

MODULES	= gp_resgroup_helper
NO_PGXS = 1
include $(top_srcdir)/src/makefiles/pgxs.mk
