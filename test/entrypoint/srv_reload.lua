#!/usr/bin/env tarantool

require("strict").on()
_G.is_initialized = function() return false end

local log = require("log")
local errors = require("errors")
local cartridge = require("cartridge")
errors.set_deprecation_handler(function(err)
    log.error("%s", err)
    os.exit(1)
end)

local roles_reload_allowed = nil
if not os.getenv("TARANTOOL_FORBID_HOTRELOAD") then
    roles_reload_allowed = true
end

package.preload["mymodule"] = function()
    return {
        role_name = "myrole",
        validate_config = function()
            return true
        end,
        init = function()
            local alpha = box.schema.create_space("alpha")
            alpha:create_index("pri")
            local beta = box.schema.create_space("beta")
            beta:create_index("pri")
        end,
        apply_config = function() end,
        stop = function() end,
    }
end

local ok, err = errors.pcall("CartridgeCfgError", cartridge.cfg, {
    roles = {
        "cartridge.roles.expirationd",
        "mymodule",
    },
    roles_reload_allowed = roles_reload_allowed,
})
if not ok then
    log.error("%s", err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
