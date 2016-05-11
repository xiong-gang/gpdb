subdir=src/backend/cdb/dispatcher
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=cdbdisp_query \
		cdbdispatchresult_mock_pqexpbuffer \
		cdbdispatchresult_mock_cdbconn

include $(top_builddir)/src/backend/mock.mk

cdbdisp_query.t: \
	$(MOCK_DIR)/backend/cdb/cdbrelsize_mock.o \
	$(MOCK_DIR)/backend/cdb/cdbsrlz_mock.o \
	$(MOCK_DIR)/backend/utils/error/assert_mock.o \
	$(MOCK_DIR)/backend/executor/execUtils_mock.o

cdbdispatchresult_mock_pqexpbuffer.t: \
	$(MOCK_DIR)/backend/gp_libpq_fe/pqexpbuffer_mock.o

cdbdispatchresult_mock_cdbconn.t: \
	$(MOCK_DIR)/backend/cdb/cdbconn_mock.o

dispatcher: \
	dispatcher.o \
	$(MOCK_DIR)/backend/cdb/cdbgang_mock.o \
	$(OBJFILES) $(CMOCKERY_OBJS) $(MOCK_OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) \
		$(call BACKEND_OBJS, $(patsubst $(MOCK_DIR)/%_mock.o,$(top_builddir)/src/%.o, $^)) \
		$(filter-out %/objfiles.txt, $^) $(MOCK_LIBS) -o $@

clean:
	rm -f dispatcher.o dispatcher
