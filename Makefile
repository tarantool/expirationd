all: hash tree


help:
	@echo "Only tests are available [hash, tree]"

clean:
	rm -f *.xlog *.snap

tree:
	tarantool test.lua TREE 2> tarantool.log
	make clean
hash:
	tarantool test.lua HASH 2>> tarantool.log
	make clean
