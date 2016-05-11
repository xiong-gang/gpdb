subdir=src/backend/cdb/dispatcher
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=cdbdisp

include $(top_builddir)/src/backend/mock.mk

cdbdisp.t: \
	$(MOCK_DIR)/backend/cdb/cdbrelsize_mock.o \
	$(MOCK_DIR)/backend/cdb/cdbsrlz_mock.o \
	$(MOCK_DIR)/backend/utils/error/assert_mock.o \
	$(MOCK_DIR)/backend/executor/execUtils_mock.o

dispatcher: \
	dispatcher.o \
	$(MOCK_DIR)/backend/cdb/cdbgang_mock.o \
	$(OBJFILES) $(CMOCKERY_OBJS) $(MOCK_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) \
		$(call BACKEND_OBJS, $(top_srcdir)/$(subdir)/$*.o $(patsubst $(MOCK_DIR)/%_mock.o,$(top_builddir)/src/%.o, $^)) \
		$(filter-out %/objfiles.txt, $^) $(MOCK_LIBS) -o $@

clean:
	rm -f dispatcher.o dispatcher
