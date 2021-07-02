local expirationd = require("expirationd")
local t = require("luatest")
local g = t.group("start_key")

local helpers = require("test.helper")

g.before_all(function()
    helpers.init_spaces(g)
end)

g.after_each(function()
    helpers.truncate_spaces(g)
end)

function g.test_passing()
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true)
    -- default start element is nil, iterate all elements
    t.assert_equals(task.start_key(), nil)
    task:kill()

    task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {start_key = box.NULL})
    -- default start element is nil, iterate all elements
    t.assert_equals(task.start_key(), box.NULL)
    task:kill()

    task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {start_key = 1})
    t.assert_equals(task.start_key(), 1)
    task:kill()

    task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            { index = "multipart_index", start_key = {1, 1}})
    t.assert_equals(task.start_key(), {1, 1})
    task:kill()

    -- errors
    t.assert_error_msg_content_equals(
            "Supplied key type of part 0 does not match index part type: expected number",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            { start_key = "" })
    t.assert_error_msg_content_equals(
            "Supplied key type of part 0 does not match index part type: expected number",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            { index = "multipart_index", start_key = "" })
    t.assert_error_msg_content_equals(
            "Supplied key type of part 0 does not match index part type: expected number",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            { index = "multipart_index", start_key = {"", ""} })
    t.assert_error_msg_content_equals(
            "Supplied key type of part 1 does not match index part type: expected number",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            { index = "multipart_index", start_key = {1, ""} })
end

function g.test_tree_index()
    for _, space in pairs({g.tree, g.vinyl}) do
        -- without start key
        helpers.iteration_result = {}
        space:insert({1, "3"})
        space:insert({2, "2"})
        space:insert({3, "1"})

        local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug)
        -- wait for tuples expired
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {
                {1, "3"},
                {2, "2"},
                {3, "1"}
            })
        end)
        task:kill()

        -- box.NULL
        helpers.iteration_result = {}
        space:insert({1, "3"})
        space:insert({2, "2"})
        space:insert({3, "1"})

        task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
                        {start_key = box.NULL})
        -- wait for tuples expired
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {
                {1, "3"},
                {2, "2"},
                {3, "1"}
            })
        end)
        task:kill()

        -- with start key
        helpers.iteration_result = {}
        space:insert({1, "3"})
        space:insert({2, "2"})
        space:insert({3, "1"})

        task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
                {start_key = 2})
        -- wait for tuples expired
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {
                {2, "2"},
                {3, "1"}
            })
        end)
        task:kill()
    end
end

function g.test_hash_index()
    -- without start key
    helpers.iteration_result = {}
    g.hash:insert({1, "3"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "1"})

    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug)
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "1"},
            {2, "2"},
            {1, "3"}
        })
    end)
    task:kill()

    -- box.NULL
    helpers.iteration_result = {}
    g.hash:insert({1, "3"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "1"})

    task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
            {start_key = box.NULL})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "1"},
            {2, "2"},
            {1, "3"}
        })
    end)
    task:kill()

    -- with start key
    helpers.iteration_result = {}
    g.hash:insert({1, "3"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "1"})

    task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
            {start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "1"},
            {2, "2"},
            {1, "3"}
        })
    end)
    task:kill()
end