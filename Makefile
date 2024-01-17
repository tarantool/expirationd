# This way everything works as expected ever for
# `make -C /path/to/project` or
# `make -f /path/to/project/Makefile`.
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
LUACOV_REPORT := $(PROJECT_DIR)/luacov.report.out
LUACOV_STATS := $(PROJECT_DIR)/luacov.stats.out

SHELL := $(shell which bash) # Required for brace expansion used in a clean target.
SEED ?= $(shell /bin/bash -c "echo $$RANDOM")

all: test

# The template (ldoc.tpl) is written using tarantool specific
# functions like string.split(), string.endswith(), so we run
# ldoc using tarantool.
apidoc:
	ldoc -c $(PROJECT_DIR)/doc/ldoc/config.ld \
             -d $(PROJECT_DIR)/doc/apidoc/ expirationd/

check: luacheck

luacheck:
	luacheck --config .luacheckrc --codes .

.PHONY: test
test:
	luatest -v --coverage --shuffle all:${SEED}

$(LUACOV_STATS): test

coverage: $(LUACOV_STATS)
	sed -i -e 's@'"$$(realpath .)"'/@@' $(LUACOV_STATS)
	cd $(PROJECT_DIR) && luacov expirationd/*.lua
	grep -A999 '^Summary' $(LUACOV_REPORT)

coveralls: $(LUACOV_STATS)
	echo "Send code coverage data to the coveralls.io service"
	luacov-coveralls --include ^expirationd --verbose --repo-token ${GITHUB_TOKEN}

deps:
	tt rocks install luatest 0.5.7
	tt rocks install luacheck 0.26.0
	tt rocks install luacov 0.13.0-1
	tt rocks install ldoc --server=https://tarantool.github.io/LDoc/
	tt rocks install luacov-coveralls 0.2.3-1 --server=http://luarocks.org
	tt rocks make

deps-full: deps
	tt rocks install cartridge 2.7.4
	tt rocks install metrics 0.13.0
