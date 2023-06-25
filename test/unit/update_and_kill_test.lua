local fiber = require("fiber")
local expirationd = require("expirationd")
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
    cg.first_space = helpers.create_space_with_tree_index(cg.params.engine)
    local second_space = helpers.create_space('second_space', cg.params.engine)
    second_space:create_index('primary')
    cg.second_space = second_space
end)

g.before_each({index_type = 'HASH'}, function(cg)
    cg.first_space = helpers.create_space_with_hash_index(cg.params.engine)
    local second_space = helpers.create_space('second_space', cg.params.engine)
    second_space:create_index('primary', {type = 'HASH'})
    cg.second_space = second_space
end)

g.after_each(function(cg)
    for _, task_name in pairs(expirationd.tasks()) do
        expirationd.kill(task_name)
    end
    cg.first_space:drop()
    cg.second_space:drop()
end)

g.after_test('test_expirationd_update', function()
    -- Back old link in require. It's necessary to avoid problem of double update call.
    -- The problem that we can't use old link properly after double update.
    -- Old link wouldn't see new tasks which were started by new link. That could happen without this line.
    -- Expirationd module changes only one previous link that stores in package.loaded.
    package.loaded["expirationd"] = expirationd
end)

function g.test_expirationd_update(cg)
    local first_space = cg.first_space
    local second_space = cg.second_space

    local first_expd_link = require("expirationd")

    -- Start tasks by first expirationd link.
    local first_expd_tasks_cnt = 4
    local first_expd_task_name_prefix = "first_"
    for i = 1, first_expd_tasks_cnt do
        first_expd_link.start(first_expd_task_name_prefix .. i, first_space.id, helpers.is_expired_true)
    end

    -- Check updating in progress message.
    local chan = fiber.channel(1)
    fiber.create(function()
        first_expd_link.update()
        chan:put(1)
    end)
    local _, err = pcall(function() first_expd_link.start() end)
    t.assert_str_contains(err, "Wait until update is done")
    chan:get()

    -- Check that links are not equals.
    local second_expd_link = require("expirationd")
    t.assert_not_equals(
        tostring(first_expd_link):match("0x.*"),
        tostring(second_expd_link):match("0x.*"))

    -- Start tasks by second expirationd link.
    local second_expd_tasks_cnt = 4
    local second_expd_task_name_prefix = "second_"
    for i = 1, second_expd_tasks_cnt do
        second_expd_link.start(second_expd_task_name_prefix .. i, second_space.id, helpers.is_expired_true)
    end

    -- Check that we have all tasks be shared between both tasks.
    t.assert_equals(first_expd_link.tasks(), second_expd_link.tasks())

    -- And tasks work correctly.
    for _, space in pairs({first_space, second_space}) do
        local total = 10
        for i = 1, total do
            space:insert({i, tostring(i)})
        end

        t.assert_equals(space:count(), total)
        helpers.retrying({}, function()
            t.assert_equals(space:count(), 0)
        end)
    end
end

function g.test_zombie_task_kill(cg)
    local space = cg.first_space
    local task_name = 'test'

    local one_hour = 3600
    local task = expirationd.start(task_name, space.id, helpers.is_expired_true,
        {
            full_scan_delay = one_hour,
        }
    )

    local first_task_fiber
    helpers.retrying({}, function()
        first_task_fiber = task.worker_fiber
        t.assert_equals(first_task_fiber:status(), "suspended")
    end)
    local total = 10
    for i = 1, total do
        space:insert({ i, tostring(i) })
    end
    t.assert_equals(space:count(), total)

    -- Run again and check - it must kill first task.
    task = expirationd.start(task_name, space.id, helpers.is_expired_true)

    t.assert_equals(task.restarts, 1)
    -- Check is first fiber killed.
    t.assert_equals(first_task_fiber:status(), "dead")

    helpers.retrying({}, function()
        t.assert_equals(space:count(), 0)
    end)
end
