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
        ["cartridge.roles.expirationd"] = "cartridge/roles/expirationd.lua",

        ["expirationd.roles.config"] = "expirationd/roles/config.lua",
        ["expirationd.strategy.lifetime_all"] = "expirationd/strategy/lifetime_all.lua",
        ["expirationd"] = "expirationd/init.lua",
        ["expirationd.version"] = "expirationd/version.lua",

        ["roles.expirationd"] = "roles/expirationd.lua"
    }
}
-- vim: syntax=lua
