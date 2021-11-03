local fiber = require("fiber")
local expirationd = require("expirationd")
local t = require("luatest")
local g = t.group("atomic_iteration")

local helpers = require("test.helper")

g.before_each(function()
    g.tree = helpers.create_space_with_tree_index("memtx")
    g.hash = helpers.create_space_with_hash_index("memtx")
    g.bitset = helpers.create_space_with_bitset_index("memtx")
    g.vinyl = helpers.create_space_with_vinyl("vinyl")
end)

g.after_each(function()
    g.tree:drop()
    g.hash:drop()
    g.bitset:drop()
    g.vinyl:drop()
end)

function g.test_passing()
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true)
    t.assert_equals(task.atomic_iteration, false)
    task:kill()

    task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {atomic_iteration = true})
    t.assert_equals(task.atomic_iteration, true)
    task:kill()

    -- errors
    t.assert_error_msg_content_equals("bad argument options.atomic_iteration to nil (?boolean expected, got number)",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            {atomic_iteration = 1})
end

local function test_memtx(space)
    helpers.iteration_result = {}

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

function g.test_memtx_tree_index()
    test_memtx(g.tree)
end

function g.test_memtx_hash_index()
    test_memtx(g.hash)
end

-- it's not check tarantool or vinyl as engine
-- just check expirationd task continue work after conflicts
function g.test_mvcc_vinyl_tx_conflict()
    for i = 1,10 do
        g.vinyl:insert({i, tostring(i), nil, nil, 0})
    end

    local updaters = {}
    for i = 1,10 do
        local updater = fiber.create(function()
            fiber.name(string.format("updater of %d", i), { truncate = true })
            while true do
                g.vinyl:update({i}, { {"+", 5, 1} })
                fiber.yield()
            end
        end)
        table.insert(updaters, updater)
    end

    local task = expirationd.start("clean_all", g.vinyl.id, helpers.is_expired_debug,
            {atomic_iteration = true})

    -- wait for tuples expired
    fiber.sleep(3)

    for i = 1,10 do
        updaters[i]:cancel()
    end

    helpers.retrying({}, function()
        t.assert_equals(g.vinyl:select(), {})
    end)
    t.assert(box.stat.vinyl().tx.conflict > 0)
    t.assert_equals(box.stat.vinyl().tx.conflict, box.stat.vinyl().tx.rollback)
    t.assert_equals(box.stat.vinyl().tx.transactions, 0)
    task:kill()
end

function g.test_kill_task()
    for i = 1,1024*10 do
        g.tree:insert({i, tostring(i)})
    end

    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_debug,
            {atomic_iteration = true})

    task:kill()
    t.assert(g.tree:count() > 0)
    t.assert(g.tree:count() % 1024 == 0)

    -- return to default value
    box.cfg{vinyl_memory = 134217728}
end
