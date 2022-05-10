local expirationd = require("expirationd")
local fiber = require("fiber")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('update_and_kill', {
    {index_type = 'TREE', engine = 'vinyl'},
    {index_type = 'TREE', engine = 'memtx'},
    {index_type = 'HASH', engine = 'memtx'},
})

g.before_each({index_type = 'TREE'}, function(cg)
    t.skip_if(cg.params.engine == 'vinyl' and not helpers.vinyl_is_supported(),
            'Blocked by https://github.com/tarantool/tarantool/issues/6448')
    g.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.before_each({index_type = 'HASH'}, function(cg)
    g.space = helpers.create_space_with_hash_index(cg.params.engine)
end)

g.before_each({index_type = 'BITSET'}, function(cg)
    g.space = helpers.create_space_with_bitset_index(cg.params.engine)
end)

g.after_each(function(g)
    g.space:drop()
end)

g.before_test('test_expirationd_update', function(cg)
    local space = cg.space

    local total_tasks = 4
    for i = 1, total_tasks do
        expirationd.start("test" .. i, space.id, helpers.is_expired_true)
    end

    cg.total_tasks = total_tasks
end)

g.after_test('test_expirationd_update', function(cg)
    local total_tasks = cg.total_tasks
    for i = 1, total_tasks do
        expirationd.kill("test" .. i)
    end
end)

function g.test_expirationd_update(cg)
    local space = cg.space

    local old_expd = expirationd

    local chan = fiber.channel(1)
    fiber.create(function()
        expirationd.update()
        chan:put(1)
    end)
    local _, err = pcall(function() expirationd.start() end)
    t.assert_str_contains(err, "Wait until update is done")
    chan:get()

    expirationd = require("expirationd")
    t.assert_not_equals(
            tostring(old_expd):match("0x.*"),
            tostring(expirationd):match("0x.*"))

    local total = 10
    for i = 1, total do
        space:insert({i, tostring(i)})
    end

    t.assert_equals(space:count{}, total)
    helpers.retrying({}, function()
        t.assert_equals(space:count{}, 0)
    end)
end

g.before_test('test_zombie_task_kill', function(cg)
    local space = cg.space
    local task_name = "test"

    local one_hour = 3600
    local task = expirationd.start(task_name, space.id, helpers.is_expired_true,
            {
                full_scan_delay = one_hour,
            })

    local first_task_fiber
    helpers.retrying({}, function()
        first_task_fiber = task.worker_fiber
        t.assert_equals(first_task_fiber:status(), "suspended")
    end)
    local total = 10
    for i = 1, total do
        space:insert({i, tostring(i)})
    end

    cg.total = total
    cg.task = task
    cg.task_name = task_name
    cg.first_task_fiber = first_task_fiber
end)

g.after_test('test_zombie_task_kill', function(cg)
    expirationd.kill(cg.task_name)
end)

function g.test_zombie_task_kill(cg)
    local space = cg.space
    local total = cg.total
    local task_name = cg.task_name
    local first_task_fiber = cg.first_task_fiber

    t.assert_equals(space:count{}, total)

    -- Run again and check - it must kill first task.
    local task = expirationd.start(task_name, space.id, helpers.is_expired_true)

    t.assert_equals(task.restarts, 1)
    -- Check is first fiber killed.
    t.assert_equals(first_task_fiber:status(), "dead")

    helpers.retrying({}, function()
        t.assert_equals(space:count{}, 0)
    end)
end
