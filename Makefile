all:
	@echo "Only tests are available: make test"

test:
	INDEX_TYPE='HASH' prove -v ./test.lua
	rm -rf *.xlog* *.snap
	INDEX_TYPE='TREE' prove -v ./test.lua
	rm -rf *.xlog* *.snap
