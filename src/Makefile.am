lib_LTLIBRARIES = libokmongo.la
libokmongo_la_SOURCES = bson.cc mongo.cc bson_dumper.cc
libokmongo_la_LDFLAGS = -version-info $(LIBVERSION)

pkginclude_HEADERS = bson.h mongo.h string_matcher.h bson_dumper.h

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
