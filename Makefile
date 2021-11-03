# This way everything works as expected ever for
# `make -C /path/to/project` or
# `make -f /path/to/project/Makefile`.
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
LUACOV_REPORT := $(PROJECT_DIR)/luacov.report.out
LUACOV_STATS := $(PROJECT_DIR)/luacov.stats.out

SHELL := $(shell which bash) # Required for brace expansion used in a clean target.

CLEANUP_FILES  = tarantool.log
CLEANUP_FILES += *.index
CLEANUP_FILES += *.run
CLEANUP_FILES += *.snap
CLEANUP_FILES += *.vylog
CLEANUP_FILES += *.xlog
CLEANUP_FILES += 51{2,3,4,5,6,7,8,9}  #  Directories that vinyl creates.

all: test

# The template (ldoc.tpl) is written using tarantool specific
# functions like string.split(), string.endswith(), so we run
# ldoc using tarantool.
apidoc:
	ldoc -c $(PROJECT_DIR)/doc/ldoc/config.ld \
             -d $(PROJECT_DIR)/doc/apidoc/ expirationd.lua

check: luacheck

luacheck:
	luacheck --config .luacheckrc --codes .

.PHONY: test
test:
	luatest -v --coverage
	rm -rf ${CLEANUP_FILES}
	INDEX_TYPE='TREE' SPACE_TYPE='vinyl' ./test.lua
	rm -rf ${CLEANUP_FILES}
	INDEX_TYPE='HASH' ./test.lua
	rm -rf ${CLEANUP_FILES}
	INDEX_TYPE='TREE' ./test.lua
	rm -rf ${CLEANUP_FILES}

$(LUACOV_STATS): test

coverage: $(LUACOV_STATS)
	sed -i -e 's@'"$$(realpath .)"'/@@' $(LUACOV_STATS)
	cd $(PROJECT_DIR) && luacov expirationd.lua
	grep -A999 '^Summary' $(LUACOV_REPORT)

coveralls: $(LUACOV_STATS)
	echo "Send code coverage data to the coveralls.io service"
	luacov-coveralls --include ^expirationd --verbose --repo-token ${GITHUB_TOKEN}

deps:
	tarantoolctl rocks install luatest 0.5.6
	tarantoolctl rocks install luacheck 0.26.0
	tarantoolctl rocks install luacov 0.13.0-1
	tarantoolctl rocks install ldoc --server=https://tarantool.github.io/LDoc/
	tarantoolctl rocks install luacov-coveralls 0.2.3-1 --server=http://luarocks.org
	tarantoolctl rocks make

clean:
	rm -rf ${CLEANUP_FILES}
