local expirationd = require("expirationd")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('process_while', {
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
    -- default process_while always return false, iterations never stopped by this function
    t.assert_equals(task.process_while(), true)
    task:kill()

    local function process_while()
        return false
    end

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {process_while = process_while})
    t.assert_equals(task.process_while(), false)
    task:kill()

    -- errors
    t.assert_error_msg_contains("bad argument options.process_while to nil (?function expected, got string)",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true,
            { process_while = "" })
end

local function process_while(task)
    if task.checked_tuples_count >= 1 then return false end
    return true
end

function g.test_tree_index(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local space = cg.space
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

function g.test_hash_index(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    helpers.iteration_result = {}
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {process_while = process_while})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{3, "1"}})
    end)
    task:kill()
end
