local fio = require('fio')
local t = require('luatest')
local helpers = require('test.helper')
local g = t.group('expirationd_master_replica')
local is_cartridge_helpers, cartridge_helpers = pcall(require, 'cartridge.test-helpers')

g.before_all(function(cg)
    if is_cartridge_helpers then
        local entrypoint_path = fio.pathjoin(helpers.project_root,
                                             'test',
                                             'entrypoint',
                                             'srv_base.lua')
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
                    roles = { 'vshard-storage', 'customers-storage' },
                    servers = {
                        { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                        { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
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

g.before_each(function()
    t.skip_if(not is_cartridge_helpers, "cartridge is not installed")
end)

g.after_each(function(cg)
    cg.cluster:server('s1-master').net_box:eval([[
        box.space.customers:truncate()
    ]])
end)

local tuples_cnt = 3
local function insert_tuples(cg)
    cg.cluster:server('s1-master').net_box:eval([[
        box.space.customers:insert({1})
        box.space.customers:insert({2})
        box.space.customers:insert({3})
    ]])
end

local expirationd_eval = string.format([[
    local deleted = 0
    local task = require('expirationd').start("clean_all", 'customers',
                                              function()
                                                  deleted = deleted + 1
                                                  return true
                                              end,
                                              {full_scan_delay = 0})
    local retry = 100
    for _ = 1, 100 do
        if deleted == %d then
            break
        end
        require("fiber").yield()
    end
    task:kill()
    return #box.space.customers:select({}, {limit = 10})
]], tuples_cnt)

function g.test_expirationd_on_master_processing(cg)
    insert_tuples(cg)
    local ret = cg.cluster:server('s1-master').net_box:eval([[
        return #box.space.customers:select({}, {limit = 10})
    ]])
    t.assert_equals(ret, tuples_cnt)

    ret = cg.cluster:server('s1-master').net_box:eval(expirationd_eval)
    t.assert_equals(ret, 0)
end

function g.test_expirationd_on_replica_no_processing(cg)
    insert_tuples(cg)
    local ret = cg.cluster:server('s1-master').net_box:eval([[
        return #box.space.customers:select({}, {limit = 10})
    ]])
    t.assert_equals(ret, tuples_cnt)

    -- wait tuples on the replica
    helpers.retrying({}, function()
        local ret = cg.cluster:server('s1-replica').net_box:eval([[
            return #box.space.customers:select({}, {limit = 10})
        ]])
        t.assert_equals(ret, tuples_cnt)
    end)

    ret = cg.cluster:server('s1-replica').net_box:eval(expirationd_eval)
    t.assert_equals(ret, tuples_cnt)
end
