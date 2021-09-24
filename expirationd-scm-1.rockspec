package = "expirationd"
version = "scm-1"
source = {
    url = "git://github.com/tarantool/expirationd.git",
    branch = "master",
}
description = {
    summary = "Expiration daemon for Tarantool",
    homepage = "https://github.com/tarantool/expirationd",
    license = "BSD2",
    maintainer = "Sergey Bronnikov <sergeyb@tarantool.org>"
}
dependencies = {
    "lua >= 5.1", -- actually tarantool > 1.6
    "checks == 3.1.0-1",
}
build = {
    type = "builtin",
    modules = {
        ["expirationd"] = "expirationd.lua",
    }
}
-- vim: syntax=lua
