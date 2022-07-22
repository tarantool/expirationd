local fio = require('fio')
local t = require('luatest')
local helpers = require('test.helper')
local g = t.group('expirationd_intergration_role')
local is_cartridge_helpers, cartridge_helpers = pcall(require, 'cartridge.test-helpers')

g.before_all(function(cg)
    if is_cartridge_helpers then
        local entrypoint_path = fio.pathjoin(helpers.project_root,
                                             'test',
                                             'entrypoint',
                                             'srv_role.lua')
        cg.cluster = cartridge_helpers.Cluster:new({
            datadir = fio.tempdir(),
            server_command = entrypoint_path,
            use_vshard = true,
            replicasets = {
                {
                    uuid = helpers.uuid('a'),
                    alias = 'router',
                    roles = { 'vshard-router' },
                    servers = {
                        { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
                    },
                },
                {
                    uuid = helpers.uuid('b'),
                    alias = 's-1',
                    roles = { 'vshard-storage', 'customers-storage', 'expirationd' },
                    servers = {
                        { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                        { instance_uuid = helpers.uuid('b', 2), alias = 's1-slave' },
                    }
                }
            },
        })
        cg.cluster:start()
    end
end)

g.after_all(function(cg)
    if is_cartridge_helpers then
        cg.cluster:stop()
        fio.rmtree(cg.cluster.datadir)
    end
end)

g.before_each(function(cg)
    t.skip_if(not is_cartridge_helpers, "cartridge is not installed")
    cg.cluster.main_server:upload_config({})
end)

g.after_each(function(cg)
    cg.cluster:server('s1-master').net_box:eval([[
        box.space.customers:truncate()
    ]])
end)

function g.test_expirationd_service_calls(cg)
    local result, err = cg.cluster:server('s1-master').net_box:eval([[
        local expirationd = require('expirationd')
        local cartridge = require('cartridge')
        local service = cartridge.service_get("expirationd")

        for k, v in pairs(expirationd) do
            if service[k] == nil then
                return false
            end
        end
        return true
    ]])
    t.assert_equals({result, err}, {true, nil})
end

-- init/stop/validate_config/apply_config well tested in test/unit/role_test.lua
-- here we just ensure that it works as expected
function g.test_start_task_from_config(cg)
    t.assert_equals(3, cg.cluster:server('s1-master').net_box:eval([[
        box.space.customers:insert({1})
        box.space.customers:insert({2})
        box.space.customers:insert({3})
        return #box.space.customers:select({}, {limit = 10})
    ]]))
    cg.cluster.main_server:upload_config({
        expirationd = {
            test_task = {
                space_id = "customers",
                is_expired = "always_true_test",
                is_master_only = true,
            }
        },
    })
    t.assert_equals(cg.cluster:server('s1-master').net_box:eval([[
        local cartridge = require("cartridge")
        return #cartridge.service_get("expirationd").tasks()
    ]]), 1)
    helpers.retrying({}, function()
        t.assert_equals(cg.cluster:server('s1-master').net_box:eval([[
            return #box.space.customers:select({}, {limit = 10})
        ]]), 0)
    end)

    -- is_master == false
    t.assert_equals(cg.cluster:server("s1-slave").net_box:eval([[
        local cartridge = require("cartridge")
        return #cartridge.service_get("expirationd").tasks()
    ]]), 0)
    helpers.retrying({}, function()
        t.assert_equals(cg.cluster:server('s1-slave').net_box:eval([[
            return #box.space.customers:select({}, {limit = 10})
        ]]), 0)
    end)
end

function g.test_continue_after_hotreload(cg)
    t.assert_equals(10, cg.cluster:server('s1-master').net_box:eval([[
        for i = 1,10 do
            box.space.customers:insert({i})
        end
        return #box.space.customers:select({}, {limit = 20})
    ]]))
    cg.cluster.main_server:upload_config({
        expirationd = {
            test_task = {
                space_id = "customers",
                is_expired = "is_expired_test_continue",
                is_master_only = true,
                options = {
                    process_expired_tuple = "always_true_test",
                },
            }
        },
    })

    t.assert_equals(cg.cluster:server('s1-master').net_box:eval([[
        local cartridge = require("cartridge")
        return #cartridge.service_get("expirationd").tasks()
    ]]), 1)
    helpers.retrying({}, function()
        t.assert_equals(cg.cluster:server('s1-master').net_box:eval([[
            return _G.is_expired_test_first_tuple
        ]]), {1})
    end)

    cg.cluster:server('s1-master').net_box:eval([[
        return require('cartridge.roles').reload()
    ]])

    t.assert_equals(cg.cluster:server('s1-master').net_box:eval([[
        local cartridge = require("cartridge")
        return #cartridge.service_get("expirationd").tasks()
    ]]), 1)
    t.assert_equals(cg.cluster:server('s1-master').net_box:eval([[
        return #box.space.customers:select({}, {limit = 20})
    ]]), 10)
    helpers.retrying({}, function()
        t.assert_equals(cg.cluster:server('s1-master').net_box:eval([[
            return _G.is_expired_test_first_tuple
        ]]), {5})
    end)
end
