MODULE_big = pg_streampack
OBJS = $(MODULE_big).o

REGRESS_OPTS = --temp-config $(MODULE_big).conf --temp-instance=./tmp_check
REGRESS = test_logical_compression

PG_CPPFLAGS += -Wall -Werror -Wno-unused-parameter -Wno-uninitialized -Wno-implicit-fallthrough -Iinclude -I$(libpq_srcdir)
SHLIB_LINK += $(libpq)

ifeq ($(enable_coverage),yes)
	PG_CPPFLAGS += -fprofile-arcs -ftest-coverage -O0
	SHLIB_LINK += -lgcov --coverage
endif
EXTRA_CLEAN += *.gcno *.gcda
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/$(MODULE_big)
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
