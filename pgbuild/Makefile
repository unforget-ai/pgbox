# pgbox build — PostgreSQL 17.4 + pgvector 0.8.0
#
# Compiles PostgreSQL and pgvector from source into pginstall/
# which gets packaged into the Python wheel.
#
# Usage:
#   make            — build everything
#   make clean      — remove build artifacts
#   make clean-all  — remove everything including downloads
#
# Prerequisites:
#   macOS: xcode-select --install
#   Linux: apt install build-essential libreadline-dev zlib1g-dev

PG_VERSION     := 17.4
PGVECTOR_VERSION := 0.8.0

PG_TARBALL     := postgresql-$(PG_VERSION).tar.bz2
PG_URL         := https://ftp.postgresql.org/pub/source/v$(PG_VERSION)/$(PG_TARBALL)
PG_SRC         := postgresql-$(PG_VERSION)

VEC_TARBALL    := pgvector-$(PGVECTOR_VERSION).tar.gz
VEC_URL        := https://github.com/pgvector/pgvector/archive/refs/tags/v$(PGVECTOR_VERSION).tar.gz
VEC_SRC        := pgvector-$(PGVECTOR_VERSION)

INSTALL_PREFIX := $(CURDIR)/../src/pgbox/pginstall
PG_CONFIG      := $(INSTALL_PREFIX)/bin/pg_config

NPROC := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

.PHONY: all clean clean-all

VECTOR_LIB := $(shell [ "$$(uname)" = "Darwin" ] && echo "vector.dylib" || echo "vector.so")

all: $(INSTALL_PREFIX)/bin/postgres $(INSTALL_PREFIX)/lib/postgresql/$(VECTOR_LIB)

# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

$(PG_TARBALL):
	curl -L -o $@ $(PG_URL)

$(PG_SRC)/configure: $(PG_TARBALL)
	tar xjf $<
	touch $@

$(PG_SRC)/config.status: $(PG_SRC)/configure
	cd $(PG_SRC) && MACOSX_DEPLOYMENT_TARGET=$(shell sw_vers -productVersion 2>/dev/null || echo "15.0") \
		./configure \
		--prefix=$(INSTALL_PREFIX) \
		--without-readline \
		--without-icu
	touch $@

DEPLOY_TARGET := $(shell sw_vers -productVersion 2>/dev/null || echo "15.0")

$(INSTALL_PREFIX)/bin/postgres: $(PG_SRC)/config.status
	unset MAKELEVEL && unset MAKEFLAGS && unset MFLAGS && \
		cd $(PG_SRC) && MACOSX_DEPLOYMENT_TARGET=$(DEPLOY_TARGET) $(MAKE) -j$(NPROC) && $(MAKE) install
	@echo "PostgreSQL $(PG_VERSION) installed to $(INSTALL_PREFIX)"

# ---------------------------------------------------------------------------
# pgvector
# ---------------------------------------------------------------------------

$(VEC_TARBALL):
	curl -L -o $@ $(VEC_URL)

$(VEC_SRC)/Makefile: $(VEC_TARBALL)
	tar xzf $<
	touch $@

$(INSTALL_PREFIX)/lib/postgresql/$(VECTOR_LIB): $(VEC_SRC)/Makefile $(INSTALL_PREFIX)/bin/postgres
	unset MAKELEVEL && unset MAKEFLAGS && unset MFLAGS && \
		cd $(VEC_SRC) && $(MAKE) PG_CONFIG=$(PG_CONFIG) -j$(NPROC) && \
		$(MAKE) install PG_CONFIG=$(PG_CONFIG)
	@echo "pgvector $(PGVECTOR_VERSION) installed"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean:
	rm -rf $(PG_SRC) $(VEC_SRC)
	rm -rf $(INSTALL_PREFIX)

clean-all: clean
	rm -f $(PG_TARBALL) $(VEC_TARBALL)
