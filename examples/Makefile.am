AM_CPPFLAGS = -I$(top_srcdir)/src
LDADD   = $(top_builddir)/src/libokmongo.la

if ENABLE_COVERAGE
AM_CPPFLAGS += --coverage
AM_LDFLAGS = --coverage
endif

noinst_PROGRAMS = bson_test mongo_test string_matcher_test

bson_test_SOURCES = bson_test.cc
mongo_test_SOURCES = mongo_test.cc
string_matcher_test_SOURCES = string_matcher_test.cc

if RUN_CLANG_ANALYZE
plists = $(SOURCES:%.cc=%.plist)
MOSTLYCLEANFILES = $(plists)

.cc.plist:
	$(AM_V_CXX)$(CXXCOMPILE) -O3 --analyze $< -o $@

all-local: $(plists)

endif

if HAS_CLANG_FORMAT
format-src-local:
	for src in $(SOURCES) $(HEADERS); do $(CLANG_FORMAT) -i @srcdir@/$$src; done
endif

