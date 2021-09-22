local expirationd = require("expirationd")
local t = require("luatest")
local g = t.group("process_while")

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

function g.test_passing()
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true)
    -- default process_while always return false, iterations never stopped by this function
    t.assert_equals(task.process_while(), true)
    task:kill()

    local function process_while()
        return false
    end

    task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {process_while = process_while})
    t.assert_equals(task.process_while(), false)
    task:kill()

    -- errors
    t.assert_error_msg_contains("bad argument options.process_while to nil (?function expected, got string)",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            { process_while = "" })
end

local function process_while(task)
    if task.checked_tuples_count >= 1 then return false end
    return true
end

local function test_tree_index(space)
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})
    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {process_while = process_while})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{1, "3"}})
    end)
    task:kill()
end

function g.test_tree_index_vinyl()
    test_tree_index(g.tree)
end

function g.test_tree_index()
    test_tree_index(g.tree)
end

function g.test_hash_index()
    helpers.iteration_result = {}
    g.hash:insert({1, "3"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "1"})

    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
            {process_while = process_while})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{3, "1"}})
    end)
    task:kill()
end
