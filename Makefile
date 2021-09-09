# This way everything works as expected ever for
# `make -C /path/to/project` or
# `make -f /path/to/project/Makefile`.
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))

CLEANUP_FILES  = tarantool.log
CLEANUP_FILES += *.xlog*
CLEANUP_FILES += *.snap
CLEANUP_FILES += 51{2,3,4,5,6,7}  #  Directories that vinyl creates.

all: test

# The template (ldoc.tpl) is written using tarantool specific
# functions like string.split(), string.endswith(), so we run
# ldoc using tarantool.
apidoc:
	.rocks/bin/ldoc -c $(PROJECT_DIR)/doc/ldoc/config.ld \
             -d $(PROJECT_DIR)/doc/apidoc/ expirationd.lua

check: luacheck

luacheck:
	.rocks/bin/luacheck --config .luacheckrc --codes .

.PHONY: test
test:
	.rocks/bin/luatest -v
	rm -rf ${CLEANUP_FILES}
	INDEX_TYPE='TREE' SPACE_TYPE='vinyl' ./test.lua
	rm -rf ${CLEANUP_FILES}
	INDEX_TYPE='HASH' ./test.lua
	rm -rf ${CLEANUP_FILES}
	INDEX_TYPE='TREE' ./test.lua
	rm -rf ${CLEANUP_FILES}

clean:
	rm -rf ${CLEANUP_FILES}
