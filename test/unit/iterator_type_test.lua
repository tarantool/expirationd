local expirationd = require("expirationd")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('iterator_type', {
    {index_type = 'TREE', engine = 'vinyl'},
    {index_type = 'TREE', engine = 'memtx'},
    {index_type = 'HASH', engine = 'memtx'},
    {index_type = 'BITSET', engine = 'memtx'},
})

g.before_each({index_type = 'TREE'}, function(cg)
    t.skip_if(cg.params.engine == 'vinyl' and helpers.vinyl_is_broken(),
        'Blocked by https://github.com/tarantool/tarantool/issues/6448')
    g.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.before_each({index_type = 'HASH'}, function(cg)
    g.space = helpers.create_space_with_hash_index(cg.params.engine)
end)

g.before_each({index_type = 'BITSET'}, function(cg)
    g.space = helpers.create_space_with_bitset_index(cg.params.engine)
end)

g.after_each(function(g)
    g.space:drop()
end)

function g.test_passing_errors_tree_index(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    t.assert_error_msg_content_equals(
            "Unknown iterator type 'ERROR'",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true, {iterator_type = "ERROR"})
end

function g.test_passing_errors_hash_index(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    t.assert_error_msg_contains(
            "Index 'primary' (HASH) of space 'hash' (memtx) does not support requested iterator type",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true, {start_key = 1, iterator_type = 1})

    t.assert_error_msg_content_equals(
            "Index 'primary' (HASH) of space 'hash' (memtx) does not support requested iterator type",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true, {start_key = 1, iterator_type = "GE"})
end

function g.test_passing_errors_bitset_index(cg)
    t.skip_if(cg.params.index_type ~= 'BITSET', 'Unsupported index type')

    t.assert_error_msg_content_equals(
            "Not supported index type, expected TREE or HASH",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true, {index = "index_for_first_name"})
end

function g.test_passing_all(cg)
    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true)
    -- default iterator_type for tree index is "ALL"
    t.assert_equals(task.iterator_type, "ALL")
    task:kill()

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "ALL"})
    t.assert_equals(task.iterator_type, "ALL")
    task:kill()
end

function g.test_passing_all_hash_index(cg)
    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true)
    -- default iterator_type for hash index is "GE"
    t.assert_equals(task.iterator_type, "ALL")
    task:kill()
end

function g.test_passing_eq_tree_index(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "EQ"})
    t.assert_equals(task.iterator_type, "EQ")
    task:kill()
end

function g.test_passing_eq_hash_index(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {start_key = 1, iterator_type = "EQ"})
    t.assert_equals(task.iterator_type, "EQ")
    task:kill()
end

function g.test_passing_gt_tree_index(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "GT"})
    t.assert_equals(task.iterator_type, "GT")
    task:kill()
end

function g.test_passing_gt_hash_index(cg)
    -- FIXME: t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "GT"})
    t.assert_equals(task.iterator_type, "GT")
    task:kill()
end

function g.test_passing_req(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "REQ"})
    t.assert_equals(task.iterator_type, "REQ")
    task:kill()
end

function g.test_passing_ge(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "GE"})
    t.assert_equals(task.iterator_type, "GE")
    task:kill()
end

function g.test_passing_lt(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "LT"})
    t.assert_equals(task.iterator_type, "LT")
    task:kill()
end

function g.test_passing_le(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "LE"})
    t.assert_equals(task.iterator_type, "LE")
    task:kill()
end

function g.test_tree_index_all(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local space = cg.space
    -- without start key
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "ALL"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {1, "3"},
            {2, "2"},
            {3, "1"}
        })
    end)
    task:kill()

    -- with start_key
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "ALL", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"},
            {3, "1"}
        })
    end)
    task:kill()
end

function g.test_hash_index_all(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    -- without start key
    helpers.iteration_result = {}
    cg.space:insert({3, "1"})
    cg.space:insert({2, "2"})
    cg.space:insert({1, "3"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "ALL"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {1, "3"},
            {2, "2"},
            {3, "1"}
        })
    end)
    task:kill()

    -- with start_key
    -- Implicit behavior hash index ALL with start key
    helpers.iteration_result = {}
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "ALL", start_key = 2})
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

function g.test_tree_index_eq(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local space = cg.space
    -- without start key
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "EQ"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {1, "3"},
            {2, "2"},
            {3, "1"}
        })
    end)
    task:kill()

    -- with start_key
    helpers.iteration_result = {}
    space:insert({1, "1", nil, nil, 1})
    space:insert({2, "2", nil, nil, 2})
    space:insert({3, "3", nil, nil, 2})
    space:insert({4, "4", nil, nil, 4})

    task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "EQ", index = "non_unique_index", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2", nil, nil, 2},
            {3, "3", nil, nil, 2}
        })
    end)
    task:kill()
end

function g.test_hash_index_eq(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    -- iterator_type EQ with partial key (nil or {} is a partial key)
    t.assert_error_msg_content_equals(
            "HASH index  does not support selects via a partial key " ..
            "(expected 1 parts, got 0). Please Consider changing index type to TREE.",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true,
            {iterator_type = "EQ"})

    -- with start key
    -- HASH doesn't support non unique indexes
    helpers.iteration_result = {}
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "EQ", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"}
        })
    end)
    task:kill()
end

function g.test_tree_index_gt(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local space = cg.space
    -- without start key
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "GT"})
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
            {iterator_type = "GT", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "1"}
        })
    end)
    task:kill()
end

function g.test_hash_index_gt(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    -- without start key
    helpers.iteration_result = {}
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "GT"})
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
    -- will compare by hash
    helpers.iteration_result = {}
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "GT", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {1, "3"},
            {3, "1"}
        })
    end)
    task:kill()
end

function g.test_tree_index_req(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local space = cg.space
    -- without start key
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "REQ"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "1"},
            {2, "2"},
            {1, "3"}
        })
    end)
    task:kill()

    -- with start_key
    helpers.iteration_result = {}
    space:insert({1, "1", nil, nil, 1})
    space:insert({2, "2", nil, nil, 2})
    space:insert({3, "3", nil, nil, 2})
    space:insert({4, "4", nil, nil, 4})

    task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "REQ", index = "non_unique_index", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "3", nil, nil, 2},
            {2, "2", nil, nil, 2}
        })
    end)
    task:kill()
end

function g.test_tree_index_ge(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local space = cg.space
    -- without start key
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "GE"})
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
            {iterator_type = "GE", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"},
            {3, "1"}
        })
    end)
    task:kill()
end

function g.test_tree_index_lt(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local space = cg.space
    -- without start key
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "LT"})
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
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "LT", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {1, "3"}
        })
    end)
    task:kill()
end

function g.test_tree_index_le(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')
    t.skip_if(cg.params.engine ~= 'vinyl', 'Unsupported engine')

    local space = cg.space
    -- without start key
    helpers.iteration_result = {}
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {iterator_type = "LE"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"},
            {3, "1"}
        })
    end)
    task:kill()
end

function g.test_tree_index_lt(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    -- without start key
    helpers.iteration_result = {}
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "LT"})
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
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "LE", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"},
            {1, "3"}
        })
    end)
    task:kill()
end

function g.test_tree_index_le(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    -- without start key
    helpers.iteration_result = {}
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "LE"})
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
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {iterator_type = "LE", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"},
            {1, "3"}
        })
    end)
    task:kill()
end
