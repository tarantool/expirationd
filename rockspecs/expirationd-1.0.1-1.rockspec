package = "expirationd"
version = "1.0.1-1"
source = {
    url = "git://github.com/tarantool/expirationd.git",
    tag = "1.0.1",
}
description = {
    summary = "Expiration daemon for Tarantool",
    homepage = "https://github.com/tarantool/expirationd",
    license = "BSD2",
    maintainer = "Eugine Blikh <bigbes@tarantool.org>"
}
dependencies = {
    "lua >= 5.1" -- actually tarantool > 1.6
}
build = {
    type = "builtin",
    modules = {
        ["expirationd"] = "expirationd.lua",
    }
}
-- vim: syntax=lua
