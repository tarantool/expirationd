local expirationd = require("expirationd")
local t = require("luatest")
local g = t.group("iterate_with")

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
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            { iterate_with = helpers.iterate_with_func })
    -- default process_while always return false, iterations never stopped by this function
    t.assert_equals(task.iterate_with, helpers.iterate_with_func)
    task:kill()

    -- errors
    t.assert_error_msg_contains("bad argument options.iterate_with to nil (?function expected, got string)",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            { iterate_with = "" })
end
