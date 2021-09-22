local expirationd = require("expirationd")
local fiber = require("fiber")
local t = require("luatest")
local g = t.group("stats")

local helpers = require("test.helper")

g.before_each(function()
    g.tree = helpers.create_space_with_tree_index("memtx")
    g.hash = helpers.create_space_with_hash_index("memtx")
    g.bitset = helpers.create_space_with_bitset_index("memtx")
    g.vinyl = helpers.create_space_with_tree_index("vinyl")
end)

g.after_each(function()
    g.tree:drop()
    g.hash:drop()
    g.bitset:drop()
    g.vinyl:drop()
end)

function g.test_stats_basic()
    local task = expirationd.start("stats_basic", g.tree.id, helpers.is_expired_true)
    local stats = expirationd.stats("stats_basic")
    t.assert_equals(stats, {
        checked_count = 0,
        expired_count = 0,
        restarts = 1,
        working_time = 0,
    })
    task:kill()
end

function g.test_stats_expired_count()
    helpers.iteration_result = {}
    g.hash:insert({1, "a"})
    g.hash:insert({2, "b"})
    g.hash:insert({3, "c"})

    local task = expirationd.start("stats_expired_count", g.hash.id, helpers.is_expired_debug)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "c"},
            {2, "b"},
            {1, "a"}
        })
    end)
    local stats = expirationd.stats("stats_expired_count")
    t.assert_equals(stats, {
        checked_count = 3,
        expired_count = 3,
        restarts = 1,
        working_time = 0,
    })
    task:kill()
end

function g.test_stats_restarts()
    local task = expirationd.start("stats_restarts", g.tree.id, helpers.is_expired_true)
    task:restart()
    task:restart()
    local stats = expirationd.stats("stats_restarts")
    t.assert_equals(stats, {
        checked_count = 0,
        expired_count = 0,
        restarts = 3,
        working_time = 0,
    })
    task:kill()
end

function g.test_stats_working_time()
    local task = expirationd.start("stats_working_time", g.tree.id, helpers.is_expired_true)
    local running_time = 1
    local threshold = 0.3

    local start_time = fiber.clock()
    fiber.sleep(running_time)
    local duration = fiber.clock() - start_time

    local stats = expirationd.stats("stats_working_time")
    t.assert_almost_equals(stats.working_time, duration, threshold)
    stats.working_time = nil
    t.assert_equals(stats, {
        checked_count = 0,
        expired_count = 0,
        restarts = 1,
    })
    task:kill()
end
