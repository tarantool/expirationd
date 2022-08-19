local expirationd = require("expirationd")
local fiber = require("fiber")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('expirationd_stats', {
    {index_type = 'TREE', engine = 'vinyl'},
    {index_type = 'TREE', engine = 'memtx'},
    {index_type = 'HASH', engine = 'memtx'},
})

g.before_each({index_type = 'TREE'}, function(cg)
    t.skip_if(cg.params.engine == 'vinyl' and not helpers.vinyl_is_supported(),
        'Blocked by https://github.com/tarantool/tarantool/issues/6448 on ' ..
		'this Tarantool version')
    g.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.before_each({index_type = 'HASH'}, function(cg)
    g.space = helpers.create_space_with_hash_index(cg.params.engine)
end)

g.after_each(function(g)
    g.space:drop()
end)

function g.test_stats_basic(cg)
    local task = expirationd.start("stats_basic", cg.space.id, helpers.is_expired_true)
    local stats = expirationd.stats("stats_basic")
    t.assert_equals(stats, {
        checked_count = 0,
        expired_count = 0,
        restarts = 1,
        working_time = 0,
    })
    task:kill()
end

function g.test_stats_expired_count(cg)
    helpers.iteration_result = {}
    cg.space:insert({1, "a"})
    cg.space:insert({2, "b"})
    cg.space:insert({3, "c"})

    local iteration_result
    if cg.params.index_type == 'TREE' then
        iteration_result = {
            {1, "a"},
            {2, "b"},
            {3, "c"},
        }
    elseif cg.params.index_type == 'HASH' then
        iteration_result = {
            {3, "c"},
            {2, "b"},
            {1, "a"},
        }
    else
        error('Expected result is undefined.')
    end

    expirationd.start("stats_expired_count", cg.space.id, helpers.is_expired_debug)
    helpers.retrying({}, function()
        t.assert_covers(helpers.iteration_result, iteration_result)
    end)

    helpers.iteration_result = {}
    cg.space:insert({1, "a"})
    cg.space:insert({2, "b"})
    cg.space:insert({3, "c"})

    local task = expirationd.start("stats_expired_count", cg.space.id, helpers.is_expired_debug)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, iteration_result)
    end)
    local stats = expirationd.stats("stats_expired_count")
    t.assert_items_equals(stats, {
        checked_count = 3,
        expired_count = 3,
        restarts = 1,
        working_time = 0,
    })
    task:kill()
end

function g.test_stats_restarts(cg)
    local task = expirationd.start("stats_restarts", cg.space.id, helpers.is_expired_true)
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

function g.test_stats_working_time(cg)
    local task = expirationd.start("stats_working_time", cg.space.id, helpers.is_expired_true)
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
