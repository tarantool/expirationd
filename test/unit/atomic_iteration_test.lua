local fiber = require("fiber")
local expirationd = require("expirationd")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('atomic_iteration', {
    {index_type = 'TREE', engine = 'vinyl'},
    {index_type = 'TREE', engine = 'memtx'},
    {index_type = 'HASH', engine = 'memtx'},
})

g.before_each({index_type = 'TREE'}, function(cg)
    g.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.before_each({index_type = 'HASH'}, function(cg)
    g.space = helpers.create_space_with_hash_index(cg.params.engine)
end)

g.after_each(function(g)
    g.space:drop()
end)

function g.test_passing(cg)
    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true)
    t.assert_equals(task.atomic_iteration, false)
    task:kill()

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {atomic_iteration = true})
    t.assert_equals(task.atomic_iteration, true)
    task:kill()

    -- errors
    t.assert_error_msg_content_equals("bad argument options.atomic_iteration to nil (?boolean expected, got number)",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true,
            {atomic_iteration = 1})
end

function g.test_memtx(cg)
    t.skip_if(cg.params.engine ~= 'memtx', 'Unsupported engine')

    helpers.iteration_result = {}

    local space = cg.space
    local transactions = {}
    local function f(iterator)
        local transaction = {}
        -- old / new_tuple is not passed for vinyl
        for _, old_tuple in iterator() do
            table.insert(transaction, old_tuple:totable())
        end
        table.insert(transactions, transaction)
    end

    local true_box_begin = box.begin

    -- mock box.begin
    box.begin = function ()
        true_box_begin()
        box.on_commit(f)
    end

    -- tuples expired in one atomic_iteration
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})


    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {atomic_iteration = true})
    -- wait for tuples expired
    helpers.retrying({}, function()
        if space.index[0].type == "HASH" then
            t.assert_equals(helpers.iteration_result, {{3, "1"}, {2, "2"}, {1, "3"}})
        else
            t.assert_equals(helpers.iteration_result, {{1, "3"}, {2, "2"}, {3, "1"}})
        end
    end)
    task:kill()
    helpers.iteration_result = {}

    -- check out three row transaction
    if space.index[0].type == "HASH" then
        t.assert_equals(transactions, {
            { {3, "1"}, {2, "2"}, {1, "3"} }
        })
    else
        t.assert_equals(transactions, {
            { {1, "3"}, {2, "2"}, {3, "1"} }
        })
    end
    transactions = {}

    -- tuples expired in two atomic_iteration
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {atomic_iteration = true, tuples_per_iteration = 2})
    -- wait for tuples expired
    -- 2 seconds because suspend will be yield in task
    helpers.retrying({}, function()
        if space.index[0].type == "HASH" then
            t.assert_equals(helpers.iteration_result, {{3, "1"}, {2, "2"}, {1, "3"}})
        else
            t.assert_equals(helpers.iteration_result, {{1, "3"}, {2, "2"}, {3, "1"}})
        end
    end)
    task:kill()
    helpers.iteration_result = {}

    if space.index[0].type == "HASH" then
        t.assert_equals(transactions, {
            { {3, "1"}, {2, "2"} }, -- check two row transaction
            { {1, "3"} }            -- check single row transactions
        })
    else
        t.assert_equals(transactions, {
            { {1, "3"}, {2, "2"} }, -- check two row transaction
            { {3, "1"} }            -- check single row transactions
        })
    end

    transactions = {}

    -- unmock
    box.begin = true_box_begin
end

-- it's not check tarantool or vinyl as engine
-- just check expirationd task continue work after conflicts
function g.test_mvcc_vinyl_tx_conflict(cg)
    t.skip_if(cg.params.engine ~= 'vinyl', 'Unsupported engine')
    local tuples_cnt = 10

    for i = 1,tuples_cnt do
        cg.space:insert({i, tostring(i), nil, nil, 0})
    end

    local updaters = {}
    for i = 1,tuples_cnt do
        local updater = fiber.new(function()
            fiber.name(string.format("updater of %d", i), { truncate = true })
            cg.space:update({i}, { {"+", 5, 1} })
        end)
        updater:set_joinable(true)
        table.insert(updaters, updater)
    end

    local is_expired = function(args, tuple)
        -- The idea is to switch explicity to an updater fiber in the middle of
        -- an expirationd's transaction:
        -- Delete from expirationd + update from an updater == conflict at the
        -- expirationd's transaction.
        fiber.yield()
        return helpers.is_expired_debug(args, tuple)
    end

    helpers.iteration_result = {}
    local task = expirationd.start("clean_all", cg.space.id, is_expired,
                                   {atomic_iteration = true})
    -- ensure that expirationd task does not delete a tuple yet
    t.assert_equals(helpers.iteration_result, {})

    for _, updater in pairs(updaters) do
        updater:join()
    end

    helpers.retrying({}, function()
        t.assert_equals(cg.space:select(), {})
    end)
    t.assert_gt(box.stat.vinyl().tx.conflict, 0)
    t.assert_gt(#helpers.iteration_result, tuples_cnt)
    t.assert_equals(box.stat.vinyl().tx.conflict, box.stat.vinyl().tx.rollback)
    t.assert_equals(box.stat.vinyl().tx.transactions, 0)
    task:kill()
end

-- Create a task that use atomic_iteration and check that task is gone after
-- kill.
function g.test_kill_task(cg)
    local task_name = 'clean_all'

    for i = 1, 100 do
        cg.space:insert({i, tostring(i)})
    end

    local task = expirationd.start(task_name, cg.space.id, helpers.is_expired_debug, {
        atomic_iteration = true,
        tuples_per_iteration = 10,
    })
    task:kill()

    -- There are two methods to know about task state:
    -- expirationd.task(task_name) and expirationd.stats() that returns
    -- statistics for each task. expirationd.task(task_name) raise error if
    -- task is not found. So we use stats() here and check that there are no
    -- stats for task with our task name.
    local stats = expirationd.stats()
    t.assert_equals(stats[task_name], nil)
end
