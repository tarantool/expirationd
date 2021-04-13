local utils = require("cartridge.utils")
local fio = require("fio")
local t = require("luatest")
local g = t.group("hot_reload")

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

g.after_each(function()
    g.srv.net_box:eval("box.space.origin:truncate()")
end)

local function get_fiber_names()
    local fiber_info = g.srv.net_box:eval("return require('fiber').info()")
    local fiber_names = {}
    for _, f in pairs(fiber_info) do
        fiber_names[f.name] = true
    end
    return fiber_names
end

g.before_all(function()
    local tempdir = fio.tempdir()
    g.cluster = helpers.Cluster:new({
        datadir = tempdir,
        server_command = helpers.entrypoint("srv_reload"),
        cookie = "secret-cluster-cookie",
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

function g.test_graceful_shutdown()
    -- In this test, we will check how the expirationd task
    -- will behave during cartridge reload, if no additional logic is written.
    -- It is expected that the task will be completed gracefully
    -- and will not delete any more data. All fibers in the task will be removed.
    -- And we will be able to launch new expirationd tasks with role reload
    -- and they will also work correctly (deleting data)

    reload_myrole(function()
        return {
            role_name = "myrole",
            init = function()
                local expirationd = require("expirationd")
                local helpers = require("test.helper")

                box.space.origin:insert({1})
                expirationd.start("first_task", box.space.origin.id, helpers.is_expired_debug,  {
                    force = true
                })
                return true, nil
            end,
            stop = function() end
        }
    end)
    -- check that the task is running
    t.assert_equals(
            g.srv.net_box:eval([[
                local expirationd = require("expirationd")
                return expirationd.tasks()
            ]]),
            {"first_task"}
    )
    -- fibers of task
    local fiber_names = get_fiber_names()
    t.assert(fiber_names['guardian of "first_task"'])
    t.assert(fiber_names['worker of "first_task"'])
    -- we see that the tuple is expired
    t.assert_equals(
            g.srv.net_box:eval("return box.space.origin:select()"),
            {}
    )
    -- expirationd task checked the tuple
    t.assert_equals(
            g.srv.net_box:eval([[
                local helpers = require("test.helper")
                return helpers.iteration_result
            ]]),
            {{1}}
    )
    reload_myrole(function()
        return {
            role_name = "myrole",
            init = function()
                local expirationd = require("expirationd")
                local helpers = require("test.helper")

                -- after reload we will see that tuple {2} has not expired
                box.space.origin:insert({2})

                expirationd.start("second_task", box.space.origin.id, helpers.is_expired_debug, {
                    force = true,
                    process_expired_tuple = function()  end -- don't delete tuple
                })
                return true, nil
            end,
            stop = function() end
        }
    end)

    -- check that the first_task is gone and second_task is running
    t.assert_equals(
            g.srv.net_box:eval([[
                local expirationd = require("expirationd")
                return expirationd.tasks()
            ]]),
            {"second_task"}
    )
    fiber_names = get_fiber_names()
    -- no fibers of first_task
    t.assert_not(fiber_names['guardian of "first_task"'])
    t.assert_not(fiber_names['worker of "first_task"'])
    -- fibers of second_task
    t.assert(fiber_names['guardian of "second_task"'])
    t.assert(fiber_names['worker of "second_task"'])
    -- the tuple has not expired
    t.assert_equals(
            g.srv.net_box:eval("return box.space.origin:select()"),
            {{ 2 }}
    )
    -- but expirationd checked this tuple
    t.assert_equals(
            g.srv.net_box:eval([[
                local helpers = require("test.helper")
                return helpers.iteration_result
            ]]),
            {{ 2 }}
    )
    -- fix deletion
    g.srv.net_box:eval([[
        local expirationd = require("expirationd")
        local task = expirationd.get_task("second_task")
        task.process_expired_tuple = function(space_id, args, tuple)
            box.space[space_id]:delete{tuple[1]}
        end
    ]])

    -- the tuple expired
    helpers.retrying({}, function()
        t.assert_equals(
            g.srv.net_box:eval("return box.space.origin:select()"),
            {}
        )
    end)
end

function g.test_atomic()
    local count_of_tuples = 1024 * 100
    reload_myrole(function()
        return {
            role_name = "myrole",
            init = function()
                local expirationd = require("expirationd")
                local helpers = require("test.helper")
                local count_of_tuples = 1024 * 100

                for i = 1,count_of_tuples do
                    box.space.origin:insert({i, tostring(i)})
                end
                expirationd.start("first_task", box.space.origin.id, helpers.is_expired_debug,  {
                    force = true,
                    atomic_iteration = true
                })
                return true, nil
            end,
            stop = function() end
        }
    end)
    -- check that the task is running
    t.assert_equals(
            g.srv.net_box:eval([[
                local expirationd = require("expirationd")
                return expirationd.tasks()
            ]]),
            {"first_task"}
    )
    reload_myrole(function()
        return {
            role_name = "myrole",
            init = function() end,
            stop = function() end
        }
    end)

    local count = g.srv.net_box:eval("return box.space.origin:count()")

    t.assert(count > 0)
    t.assert(count < count_of_tuples)
    t.assert(count % 1024 == 0)
end

function g.test_with_start_key_state()
    reload_myrole(function()
        return {
            role_name = "myrole",
            init = function()
                local expirationd = require("expirationd")
                local helpers = require("test.helper")
                local count_of_tuples = 1024 * 100

                for i = 1,count_of_tuples do
                    box.space.origin:insert({i, tostring(i)})
                end
                expirationd.start("first_task", box.space.origin.id, helpers.is_expired_debug,  {
                    force = true,
                    iterator_type = box.index.LE
                })
                return true, nil
            end,
            stop = function()
                local cartridge_vars = require("cartridge.vars")
                local expirationd = require("expirationd")
                local helpers = require("test.helper")
                local t = require("luatest")

                local task = expirationd.get_task("first_task")
                task:kill()
                -- save the state
                local func, state, var = task:iterate_with()
                local _, start_tuple = func(state, var)

                -- check that the last deleted tuple is previous to our start
                t.assert_equals(helpers.iteration_result[#helpers.iteration_result][1], start_tuple[1] + 1)

                cartridge_vars.new('expirationd').start_key = start_tuple[1]
            end
        }
    end)
    -- check that the task is running
    t.assert_equals(
            g.srv.net_box:eval([[
                local expirationd = require("expirationd")
                return expirationd.tasks()
            ]]),
            {"first_task"}
    )

    reload_myrole(function()
        return {
            role_name = "myrole",
            init = function()
                local cartridge_vars = require('cartridge.vars')
                local expirationd = require("expirationd")
                local helpers = require("test.helper")
                local t = require("luatest")

                local count_of_tuples = 1024 * 100
                local count = box.space.origin:count()
                -- save count to use in future asserts
                rawset(_G, "count", count)
                local first_tuple = box.space.origin:select(nil, {limit = 1, iterator = box.index.LE})[1]

                local start_key = cartridge_vars.new('expirationd').start_key
                -- check some number of tuples removed
                t.assert(count > 0)
                t.assert(count < count_of_tuples)
                -- and check that the smallest tuple
                -- is the last tuple that was not removed by the last expiration
                t.assert_equals(first_tuple[1], start_key)

                -- start a new task from tuple the previous task ended
                expirationd.start("first_task", box.space.origin.id, helpers.is_expired_debug,  {
                    force = true,
                    iterator_type = box.index.LE,
                    start_key = start_key
                })
            end,
            stop = function() end
        }
    end)

    local old_count, count = g.srv.net_box:eval("return count, box.space.origin:count()")
    t.assert(old_count > count)
    local start_key, first_deleted_tuple  = g.srv.net_box:eval([[
        local cartridge_vars = require("cartridge.vars")
        local helpers = require("test.helper")
        return cartridge_vars.new('expirationd').start_key, helpers.iteration_result[1]
    ]])
    -- check that we started deleting from the right place
    t.assert_equals(start_key, first_deleted_tuple[1])
end
