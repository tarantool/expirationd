all: test

check: luacheck

luacheck:
	.rocks/bin/luacheck --config .luacheckrc --codes .

.PHONY: test
test:
	.rocks/bin/luatest -v
	rm -rf *.xlog* *.snap 51{2,3,4,5,6,7}
	INDEX_TYPE='TREE' SPACE_TYPE='vinyl' ./test.lua
	rm -rf *.xlog* *.snap 51{2,3,4,5,6,7}
	INDEX_TYPE='HASH' ./test.lua
	rm -rf *.xlog* *.snap 51{2,3,4,5,6,7}
	INDEX_TYPE='TREE' ./test.lua
	rm -rf *.xlog* *.snap 51{2,3,4,5,6,7}
