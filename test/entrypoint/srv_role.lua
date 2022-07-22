#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local fiber = require('fiber')
local cartridge = require('cartridge')
local hotreload = require('cartridge.hotreload')

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
        init = function()
            local customers_space = box.schema.space.create('customers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                },
                if_not_exists = true,
                engine = 'memtx',
            })

            customers_space:create_index('id', {
                parts = { {field = 'id'} },
                unique = true,
                type = 'TREE',
                if_not_exists = true,
            })
        end,
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'customers-storage',
        'cartridge.roles.vshard-router',
        'cartridge.roles.vshard-storage',
        'cartridge.roles.expirationd'
    },
    roles_reload_allowed = true,
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
_G.always_true_test = function() return true end
_G.is_expired_test_continue = function(_, tuple)
    if rawget(_G, "is_expired_test_first_tuple") == nil then
        rawset(_G, "is_expired_test_first_tuple", tuple)
    end

    local cnt = rawget(_G, "is_expired_test_wait_cnt") or 0
    cnt = cnt + 1
    rawset(_G, "is_expired_test_wait_cnt", cnt)
    if cnt == 5 then
        fiber.sleep(60)
    end
    return true
end

hotreload.whitelist_globals({"always_true_test"})
hotreload.whitelist_globals({"is_expired_test_continue"})
