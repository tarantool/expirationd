CLEANUP_FILES  = tarantool.log
CLEANUP_FILES += *.xlog*
CLEANUP_FILES += *.snap
CLEANUP_FILES += 51{2,3,4,5,6,7}  #  Directories that vinyl creates.

all: test

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
