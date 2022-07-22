package = "expirationd"
version = "scm-1"
source = {
    url = "git+https://github.com/tarantool/expirationd.git",
    branch = "master",
}
description = {
    summary = "Expiration daemon for Tarantool",
    homepage = "https://github.com/tarantool/expirationd",
    license = "BSD2",
    maintainer = "Oleg Jukovec <oleg.jukovec@tarantool.org>"
}
dependencies = {
    "lua >= 5.1", -- actually tarantool > 1.6
    "checks >= 2.1",
}
build = {
    type = "builtin",
    modules = {
        ["expirationd"] = "expirationd.lua",
        ["cartridge.roles.expirationd"] = "cartridge/roles/expirationd.lua",
    }
}
-- vim: syntax=lua
