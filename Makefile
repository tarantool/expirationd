all:
	@echo "Only tests are available [common]"

clean:
	rm *.xlog
	rm *.snap
common:
	tarantool test.lua TREE
	make clean
	tarantool test.lua HASH
	make clean
