local expirationd = require("expirationd")
local t = require("luatest")
local g = t.group("space_manipulation")

local helpers = require("test.helper")

g.before_all(function()
    helpers.init_spaces(g)
end)

g.after_each(function()
    helpers.truncate_spaces(g)
end)

function g.test_rename()
    helpers.iteration_result = {}

    g.tree:insert({1, "3"})
    g.tree:insert({2, "2"})
    g.tree:insert({3, "1"})

    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_debug)
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(g.tree:select(), {})
        t.assert_equals(helpers.iteration_result, {
            {1, "3"},
            {2, "2"},
            {3, "1"}
        })
    end)

    g.tree:rename("renamed_tree")

    g.tree:insert({1, "3"})
    g.tree:insert({2, "2"})
    g.tree:insert({3, "1"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(g.tree:select(), {})
        t.assert_equals(helpers.iteration_result, {
            {1, "3"},
            {2, "2"},
            {3, "1"},
            {1, "3"},
            {2, "2"},
            {3, "1"}
        })
    end)
    task:kill()
    -- return to default name
    g.tree:rename("tree")
end

function g.test_drop()
    helpers.iteration_result = {}
    local drop_space = box.schema.space.create("drop_space")
    drop_space:create_index("pri")
    for i = 1, 1024 do
        drop_space:insert({i, tostring(i)})
    end
    -- check default primary index
    local task = expirationd.start("clean_all", drop_space.id, helpers.is_expired_debug)
    -- wait for first three tuples expired
    helpers.retrying({}, function()
        t.assert_items_include(helpers.iteration_result, {
            {1, "1"},
            {2, "2"},
            {3, "3"}
        })
    end)

    drop_space:drop()
    -- task is raising error
    -- ER_NO_SUCH_SPACE: Space 'SomeNumber' does not exist
    -- because space was dropped
    helpers.retrying({}, function()
        t.assert(task.restarts > 3)
    end)
end

function g.test_truncate()
    helpers.iteration_result = {}
    local truncate_space = box.schema.space.create("truncate_space")
    truncate_space:create_index("pri")
    for i = 1, 1024 * 100 do
        truncate_space:insert({i, tostring(i)})
    end
    -- check default primary index
    local task = expirationd.start("clean_all", truncate_space.id, helpers.is_expired_debug, {
        tuples_per_iteration = 1024 * 100
    })
    -- wait for first three tuples expired
    helpers.retrying({}, function()
        t.assert_items_include(helpers.iteration_result, {
            {1, "1"},
            {2, "2"},
            {3, "3"}
        })
    end)

    truncate_space:truncate()
    -- wait to check there is no error in worker fiber
    require("fiber").sleep(3)
    t.assert(task.restarts == 1)
    t.assert(#helpers.iteration_result < 1024 * 100)
end
