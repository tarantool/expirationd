all:
	@echo "Only tests are available: make test"

test:
	rm -rf *.xlog* *.snap 51{2,3,4,5,6,7}
	INDEX_TYPE='TREE' SPACE_TYPE='vinyl' ./test.lua
	rm -rf *.xlog* *.snap 51{2,3,4,5,6,7}
	INDEX_TYPE='HASH' ./test.lua
	rm -rf *.xlog* *.snap 51{2,3,4,5,6,7}
	INDEX_TYPE='TREE' ./test.lua
	rm -rf *.xlog* *.snap 51{2,3,4,5,6,7}
