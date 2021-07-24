local utils = require("cartridge.utils")
local expirationd = require("expirationd")
local fio = require("fio")
local t = require("luatest")
local g = t.group("cartridge_role")

local helpers = require("test.helper")

local function reload_myrole(fn)
    -- For the sake of string.dump() function must have no upvalues.
    -- https://www.lua.org/manual/5.1/manual.html#pdf-string.dump
    utils.assert_upvalues(fn, {})

    local ok, err = g.srv.net_box:eval([[
        package.preload["mymodule"] = loadstring(...)
        return require("cartridge.roles").reload()
    ]], {string.dump(fn)})

    t.assert_equals({ok, err}, {true, nil})
end

g.before_all(function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint("srv_reload"),
        replicasets = {{
                           alias = "A",
                           roles = {"myrole"},
                           servers = 1,
                       }},
    })
    g.srv = g.cluster:server("A-1")
    g.cluster:start()
end)

g.after_all(function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end)

function g.test_enabled_service()
    local role_name = g.srv.net_box:eval([[
        local cartridge = require("cartridge")
        local expirationd = cartridge.service_get("expirationd")
        return expirationd.role_name
    ]])
    t.assert_equals(role_name, "expirationd")
end

function g.test_with_start_key_state()
    reload_myrole(function()
        return {
            role_name = "myrole",
            init = function()
                -- start tasks
                local expirationd = require("expirationd")
                local helpers = require("test.helper")
                local count_of_tuples = 1024 * 100

                for i = 1,count_of_tuples do
                    box.space.alpha:insert({i, tostring(i)})
                    box.space.beta:insert({i, tostring(i)})
                end
                expirationd.start("alpha", box.space.alpha.id, helpers.is_expired_debug,{
                    force = true,
                    iterator_type = box.index.LE
                })
                expirationd.start("beta", box.space.beta.id, helpers.is_expired_debug,{
                    force = true,
                    iterator_type = box.index.LE
                })
                return true
            end,
            stop = function() end
        }
    end)

    reload_myrole(function()
        return {
            role_name = "myrole",
            init = function()
                local expirationd = require("expirationd")
                local helpers = require("test.helper")
                local t = require("luatest")

                local count_of_tuples = 1024 * 100
                local counters = {}
                -- we use _G._cluster_vars_values cause cartridge.vars
                -- does not allow us to go over the entire table,
                -- maybe it is worth making a ticket in cartridge
                for task_name, start_tuple in pairs(_G._cluster_vars_values.expirationd) do
                    _G._cluster_vars_values.expirationd[task_name] = nil
                    local count = box.space[task_name]:count()
                    counters[task_name] = count

                    -- save count to use in future asserts
                    local first_tuple = box.space[task_name]:select(nil, {limit = 1, iterator = box.index.LE})[1]

                    -- get start key from previous expiration daemon task
                    local start_key = start_tuple[1]
                    -- check some number of tuples removed
                    t.assert(count > 0)
                    t.assert(count < count_of_tuples)
                    -- and check that the smallest tuple
                    -- is the last tuple that was not removed by the last expiration
                    t.assert_equals(first_tuple[1], start_key)

                    -- start a new task from tuple the previous task ended
                    expirationd.start(task_name, box.space[task_name].id, helpers.is_expired_debug,  {
                        force = true,
                        iterator_type = box.index.LE,
                        start_key = start_key
                    })
                end
                rawset(_G, "counters", counters)
            end,
            stop = function() end
        }
    end)

    -- check the task really works and it's deleting some data
    for _, name in pairs(expirationd.tasks()) do
        local old_count = g.srv.net_box:eval("return counters[...]", name)
        local count = g.srv.net_box:eval("return box.space[...]:count()", name)
        t.assert(old_count > count)
    end
end
