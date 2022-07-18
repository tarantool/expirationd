local expirationd = require("expirationd")
local fiber = require("fiber")
local t = require("luatest")
local helpers = require("test.helper")
local g = t.group('expirationd_continue')

local task_name = "walk_all"
g.before_each(function()
    g.space = helpers.create_space_with_tree_index('memtx')
    for _, task in ipairs(expirationd.tasks()) do
         if task == task_name then
             expirationd.task(task_name):kill()
         end
    end
end)

g.after_each(function()
    g.space:drop()
    if box.space.tmp ~= nil then
        box.space.tmp:drop()
    end
end)

local tuples_wait_event = {{1, "1"}, {2, "2"}, {3, "3"}, {4, "4"}, {5, "5"}}
local tuples_all = {{1, "1"}, {2, "2"}, {3, "3"}, {4, "4"}, {5, "5"},
                    {6, "6"}, {7, "7"}, {8, "8"}, {9, "9"}, {10, "10"}}
local tuples_repeat = {{1, "1"}, {2, "2"}, {3, "3"}, {4, "4"}, {5, "5"},
                       {1, "1"}, {2, "2"}, {3, "3"}, {4, "4"}, {5, "5"},
                       {6, "6"}, {7, "7"}, {8, "8"}, {9, "9"}, {10, "10"}}

local function insert_tuples(space)
    for i = 1,10 do
        space:insert({i, tostring(i)})
    end
end

local function start_walk_task(space, sleep)
    local cnt = 0
    local is_expired = function(args, tuple)
        cnt = cnt + 1
        if cnt == 6 then
            if sleep then
                fiber.sleep(60)
            else
                error("test error in iteration")
            end
        end
        return helpers.is_expired_debug(args, tuple)
    end
    local task = expirationd.start(task_name, space.id, is_expired,
                                   {process_expired_tuple = function() return true end})
    return task
end

function g.test_task_continue_after_error()
    insert_tuples(g.space)

    helpers.iteration_result = {}
    local task = start_walk_task(g.space, false)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_all)
    end)

    task:kill()
end

function g.test_task_continue_after_stop_start()
    insert_tuples(g.space)

    helpers.iteration_result = {}
    local task = start_walk_task(g.space, true)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    task:stop()
    task:start()

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_all)
    end)

    task:kill()
end

function g.test_task_continue_after_stop_recreate()
    insert_tuples(g.space)

    helpers.iteration_result = {}
    local task = start_walk_task(g.space, true)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    task:stop()
    local task = expirationd.start(task_name, g.space.id, helpers.is_expired_debug,
                                   {process_expired_tuple = function() return true end})


    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_all)
    end)

    task:kill()
end

function g.test_task_not_continue_after_kill_start()
    insert_tuples(g.space)

    helpers.iteration_result = {}
    local task = start_walk_task(g.space, true)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    task:kill()

    local task = expirationd.start(task_name, g.space.id, helpers.is_expired_debug,
                                   {process_expired_tuple = function() return true end})
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_repeat)
    end)

    task:kill()
end

function g.test_task_not_continue_after_restart()
    insert_tuples(g.space)

    helpers.iteration_result = {}
    local task = start_walk_task(g.space, true)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    task:restart()

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_repeat)
    end)

    task:kill()
end

function g.test_task_not_continue_after_index_changed()
    local space = box.schema.create_space("tmp")
    local index = space:create_index("primary", {type = "TREE", parts = {{field = 1}}})
    insert_tuples(space)

    helpers.iteration_result = {}
    local task = start_walk_task(space, false)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    index:alter({parts = {{field = 2}}})

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result,
                        {{1, "1"}, {2, "2"}, {3, "3"}, {4, "4"}, {5, "5"},
                         {1, "1"}, {10, "10"}, {2, "2"}, {3, "3"}, {4, "4"},
                         {5, "5"}, {6, "6"}, {7, "7"}, {8, "8"}, {9, "9"}})
    end)

    task:kill()
    space:drop()
end

function g.test_task_not_continue_after_stop_recreate_other_space()
    insert_tuples(g.space)
    local space = box.schema.create_space("tmp")
    space:create_index("primary", {type = "TREE", parts = {{field = 1}}})
    insert_tuples(space)

    helpers.iteration_result = {}
    local task = start_walk_task(g.space, true)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    task:stop()
    local task = expirationd.start(task_name, space.id, helpers.is_expired_debug,
                                   {process_expired_tuple = function() return true end})

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_repeat)
    end)

    task:kill()
    space:drop()
end

function g.test_task_not_continue_after_stop_recreate_other_index()
    insert_tuples(g.space)

    helpers.iteration_result = {}
    local task = start_walk_task(g.space, true)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    task:stop()
    local task = expirationd.start(task_name, g.space.id, helpers.is_expired_debug,
                                   {process_expired_tuple = function() return true end,
                                    index = "index_for_first_name" })


    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result,
                        {{1, "1"}, {2, "2"}, {3, "3"}, {4, "4"}, {5, "5"},
                         {1, "1"}, {10, "10"}, {2, "2"}, {3, "3"}, {4, "4"},
                         {5, "5"}, {6, "6"}, {7, "7"}, {8, "8"}, {9, "9"}})
    end)

    task:kill()
end

function g.test_task_not_continue_after_stop_recreate_other_iterator()
    insert_tuples(g.space)

    helpers.iteration_result = {}
    local task = start_walk_task(g.space, true)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_wait_event)
    end)

    task:stop()
    local task = expirationd.start(task_name, g.space.id, helpers.is_expired_debug,
                                   {process_expired_tuple = function() return true end,
                                    iterator_type = box.index.GE})


    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples_repeat)
    end)

    task:kill()
end
