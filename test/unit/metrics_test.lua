local expirationd = require("expirationd")
local t = require("luatest")
local helpers = require("test.helper")
local g = t.group('expirationd_metrics')

g.before_all(function()
    g.default_cfg = { metrics = expirationd.cfg.metrics }
end)

g.before_each(function()
    t.skip_if(not helpers.is_metrics_supported(),
              "metrics >= 0.11.0 is not installed")
    g.space = helpers.create_space_with_tree_index('memtx')
    -- kill live tasks (it can still live after failed tests)
    for _, t in ipairs(expirationd.tasks()) do
        expirationd.kill(t)
    end
    -- disable and clean metrics by default
    expirationd.cfg({metrics = false})
    require('metrics').clear()
end)

local task = nil
g.after_each(function(g)
    expirationd.cfg(g.default_cfg)
    g.space:drop()
    if task ~= nil then
        task:kill()
        task = nil
    end
end)

local function get_metrics()
    local metrics = require('metrics')
    metrics.invoke_callbacks()
    return metrics.collect()
end

local function assert_metrics_equals(t, value, expected)
    local copy = table.deepcopy(value)
    for _, v in ipairs(copy) do
         v['timestamp'] = nil
    end
    t.assert_items_equals(copy, expected)
end

local function assert_metrics_restarts_equals(t, value, expected)
    local copy = {}
    for _, v in ipairs(value) do
        if v['metric_name'] == "expirationd_restarts" then
            table.insert(copy, v)
        end
    end
    assert_metrics_equals(t, copy, expected)
end

function g.test_metrics_disabled(cg)
    expirationd.cfg({metrics = false})
    task = expirationd.start("stats_basic", cg.space.id, helpers.is_expired_true)

    local metrics = get_metrics()

    assert_metrics_equals(t, metrics, {})
    task:kill()
    task = nil
end

local metrics_basic = {
    {
        label_pairs = {name = "stats_basic"},
        metric_name = "expirationd_expired_count",
        value = 0,
    },
    {
        label_pairs = {name = "stats_basic"},
        metric_name = "expirationd_checked_count",
        value = 0,
    },
    {
        label_pairs = {name = "stats_basic"},
        metric_name = "expirationd_working_time",
        value = 0,
    },
    {
        label_pairs = {name = "stats_basic"},
        metric_name = "expirationd_restarts",
        value = 1,
    },
}

function g.test_metrics_basic(cg)
    expirationd.cfg({metrics = true})
    task = expirationd.start("stats_basic", cg.space.id, helpers.is_expired_true)

    local metrics = get_metrics()
    assert_metrics_equals(t, metrics, metrics_basic)
    task:kill()
    task = nil
end

function g.test_metrics_no_values_after_kill(cg)
    expirationd.cfg({metrics = true})
    task = expirationd.start("stats_basic", cg.space.id, helpers.is_expired_true)

    local metrics = get_metrics()
    assert_metrics_equals(t, metrics, metrics_basic)

    task:kill()
    task = nil

    local metrics = get_metrics()
    assert_metrics_equals(t, metrics, {})
end

function g.test_metrics_multiple_tasks_and_kill(cg)
    expirationd.cfg({metrics = true})
    local task1 = expirationd.start("stats_basic1", cg.space.id, helpers.is_expired_true)
    local task2 = expirationd.start("stats_basic2", cg.space.id, helpers.is_expired_true)
    task2:restart()
    local task3 = expirationd.start("stats_basic3", cg.space.id, helpers.is_expired_true)
    task3:restart()
    task3:restart()

    local before_kill_metrics = get_metrics()

    task1:kill()
    local after1_kill_metrics = get_metrics()

    task3:kill()
    local after13_kill_metrics = get_metrics()

    task2:kill()
    local after123_kill_metrics = get_metrics()

    assert_metrics_restarts_equals(t, before_kill_metrics, {
        {
            label_pairs = {name = "stats_basic1"},
            metric_name = "expirationd_restarts",
            value = 1,
        },
        {
            label_pairs = {name = "stats_basic2"},
            metric_name = "expirationd_restarts",
            value = 2,
        },
        {
            label_pairs = {name = "stats_basic3"},
            metric_name = "expirationd_restarts",
            value = 3,
        },
    })
    assert_metrics_restarts_equals(t, after1_kill_metrics, {
        {
            label_pairs = {name = "stats_basic2"},
            metric_name = "expirationd_restarts",
            value = 2,
        },
        {
            label_pairs = {name = "stats_basic3"},
            metric_name = "expirationd_restarts",
            value = 3,
        },
    })
    assert_metrics_restarts_equals(t, after13_kill_metrics, {
        {
            label_pairs = {name = "stats_basic2"},
            metric_name = "expirationd_restarts",
            value = 2,
        },
    })
    assert_metrics_restarts_equals(t, after123_kill_metrics, {})
end

function g.test_metrics_no_values_after_disable(cg)
    expirationd.cfg({metrics = true})
    task = expirationd.start("stats_basic", cg.space.id, helpers.is_expired_true)

    local metrics = get_metrics()
    assert_metrics_equals(t, metrics, metrics_basic)

    expirationd.cfg({metrics = false})
    local metrics = get_metrics()

    assert_metrics_equals(t, metrics, {})
    task:kill()
    task = nil
end

function g.test_metrics_new_values_after_restart(cg)
    expirationd.cfg({metrics = true})
    task = expirationd.start("stats_basic", cg.space.id, helpers.is_expired_true)
    task:restart()

    local metrics = get_metrics()
    assert_metrics_equals(t, metrics, {
        {
            label_pairs = {name = "stats_basic"},
            metric_name = "expirationd_expired_count",
            value = 0,
        },
        {
            label_pairs = {name = "stats_basic"},
            metric_name = "expirationd_checked_count",
            value = 0,
        },
        {
            label_pairs = {name = "stats_basic"},
            metric_name = "expirationd_working_time",
            value = 0,
        },
        {
            label_pairs = {name = "stats_basic"},
            metric_name = "expirationd_restarts",
            value = 2,
        }
    })

    task:kill()
    local metrics = get_metrics()
    assert_metrics_equals(t, metrics, {})

    task = expirationd.start("stats_basic", cg.space.id, helpers.is_expired_true)
    local metrics = get_metrics()
    assert_metrics_equals(t, metrics, metrics_basic)

    task:kill()
    task = nil
end

function g.test_metrics_expired_count(cg)
    local iteration_result = {
        {1, "a"},
        {2, "b"},
        {3, "c"},
    }

    helpers.iteration_result = {}
    cg.space:insert({1, "a"})
    cg.space:insert({2, "b"})
    cg.space:insert({3, "c"})

    expirationd.cfg({metrics = true})
    task = expirationd.start("stats_expired_count", cg.space.id, helpers.is_expired_debug)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, iteration_result)
    end)

    local metrics = get_metrics()
    assert_metrics_equals(t, metrics, {
        {
            label_pairs = {name = "stats_expired_count"},
            metric_name = "expirationd_expired_count",
            value = 3,
        },
        {
            label_pairs = {name = "stats_expired_count"},
            metric_name = "expirationd_checked_count",
            value = 3,
        },
        {
            label_pairs = {name = "stats_expired_count"},
            metric_name = "expirationd_working_time",
            value = 0,
        },
        {
            label_pairs = {name = "stats_expired_count"},
            metric_name = "expirationd_restarts",
            value = 1,
        },
    })

    task:kill()
    task = nil
end
