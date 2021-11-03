local expirationd = require("expirationd")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('iterate_with', {
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
    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            { iterate_with = helpers.iterate_with_func })
    -- default process_while always return false, iterations never stopped by this function
    t.assert_equals(task.iterate_with, helpers.iterate_with_func)
    task:kill()

    -- errors
    t.assert_error_msg_contains("bad argument options.iterate_with to nil (?function expected, got string)",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true,
            { iterate_with = "" })
end
