local expirationd = require("expirationd")
local t = require("luatest")
local g = t.group("iterator_type")

local helpers = require("test.helper")

g.before_each(function()
    g.tree = helpers.create_space_with_tree_index()
    g.hash = helpers.create_space_with_hash_index()
    g.bitset = helpers.create_space_with_bitset_index()
    g.vinyl = helpers.create_space_with_vinyl()
end)

g.after_each(function()
    g.tree:drop()
    g.hash:drop()
    g.bitset:drop()
    g.vinyl:drop()
end)

function g.test_passing_errors_tree_index()
    t.assert_error_msg_content_equals(
            "Unknown iterator type 'ERROR'",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true, {iterator_type = "ERROR"})
end

function g.test_passing_errors_hash_index()
    t.assert_error_msg_contains(
            "Index 'primary' (HASH) of space 'hash' (memtx) does not support requested iterator type",
            expirationd.start, "clean_all", g.hash.id, helpers.is_expired_true, {start_key = 1, iterator_type = 1})

    t.assert_error_msg_content_equals(
            "Index 'primary' (HASH) of space 'hash' (memtx) does not support requested iterator type",
            expirationd.start, "clean_all", g.hash.id, helpers.is_expired_true, {start_key = 1, iterator_type = "GE"})
end

function g.test_passing_errors_bitset_index()
    t.assert_error_msg_content_equals(
            "Not supported index type, expected TREE or HASH",
            expirationd.start, "clean_all", g.bitset.id, helpers.is_expired_true, {index = "index_for_first_name"})
end

function g.test_passing_all_tree_index()
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true)
    -- default iterator_type for tree index is "ALL"
    t.assert_equals(task.iterator_type, "ALL")
    task:kill()

    task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {iterator_type = "ALL"})
    t.assert_equals(task.iterator_type, "ALL")
    task:kill()
end

function g.test_passing_all_hash_index()
    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_true)
    -- default iterator_type for hash index is "GE"
    t.assert_equals(task.iterator_type, "ALL")
    task:kill()

    task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_true,
            {iterator_type = "ALL"})
    t.assert_equals(task.iterator_type, "ALL")
    task:kill()
end

function g.test_passing_eq_tree_index()
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {iterator_type = "EQ"})
    t.assert_equals(task.iterator_type, "EQ")
    task:kill()
end

function g.test_passing_eq_hash_index()
    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_true,
            {start_key = 1, iterator_type = "EQ"})
    t.assert_equals(task.iterator_type, "EQ")
    task:kill()
end

function g.test_passing_gt_tree_index()
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {iterator_type = "GT"})
    t.assert_equals(task.iterator_type, "GT")
    task:kill()
end

function g.test_passing_gt_hash_index()
    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_true,
            {iterator_type = "GT"})
    t.assert_equals(task.iterator_type, "GT")
    task:kill()
end

function g.test_passing_req()
    -- ========================== --
    -- tree index
    -- ========================== --
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {iterator_type = "REQ"})
    t.assert_equals(task.iterator_type, "REQ")
    task:kill()
end

function g.test_passing_ge()
    -- ========================== --
    -- tree index
    -- ========================== --
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {iterator_type = "GE"})
    t.assert_equals(task.iterator_type, "GE")
    task:kill()
end

function g.test_passing_lt()
    -- ========================== --
    -- tree index
    -- ========================== --
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {iterator_type = "LT"})
    t.assert_equals(task.iterator_type, "LT")
    task:kill()
end

function g.test_passing_le()
    -- ========================== --
    -- tree index
    -- ========================== --
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {iterator_type = "LE"})
    t.assert_equals(task.iterator_type, "LE")
    task:kill()
end

local function test_tree_index_all(space)
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

function g.test_tree_index_all_vinyl()
    test_tree_index_all(g.vinyl)
end

function g.test_tree_index_all()
    test_tree_index_all(g.tree)
end

function g.test_hash_index_all()
    -- without start key
    helpers.iteration_result = {}
    g.hash:insert({3, "1"})
    g.hash:insert({2, "2"})
    g.hash:insert({1, "3"})

    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
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
    g.hash:insert({1, "3"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "1"})

    task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
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

local function test_tree_index_eq(space)
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

function g.test_tree_index_eq_vinyl()
    test_tree_index_eq(g.vinyl)
end

function g.test_tree_index_eq()
    test_tree_index_eq(g.tree)
end

function g.test_hash_index_eq()
    -- iterator_type EQ with partial key (nil or {} is a partial key)
    t.assert_error_msg_content_equals(
            "HASH index  does not support selects via a partial key " ..
            "(expected 1 parts, got 0). Please Consider changing index type to TREE.",
            expirationd.start, "clean_all", g.hash.id, helpers.is_expired_true,
            {iterator_type = "EQ"})

    -- with start key
    -- HASH doesn't support non unique indexes
    helpers.iteration_result = {}
    g.hash:insert({1, "3"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "1"})

    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
            {iterator_type = "EQ", start_key = 2})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"}
        })
    end)
    task:kill()
end

local function test_tree_index_gt(space)
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

function g.test_tree_index_gt_vinyl()
    test_tree_index_gt(g.vinyl)
end

function g.test_tree_index_gt()
    test_tree_index_gt(g.tree)
end

function g.test_hash_index_gt()
    -- without start key
    helpers.iteration_result = {}
    g.hash:insert({1, "3"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "1"})

    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
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
    g.hash:insert({1, "3"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "1"})

    task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
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

local function test_tree_index_req(space)
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

function g.test_tree_index_req_vinyl()
    test_tree_index_req(g.vinyl)
end

function g.test_tree_index_req()
    test_tree_index_req(g.tree)
end

local function test_tree_index_ge(space)
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

function g.test_tree_index_ge()
    test_tree_index_ge(g.vinyl)
end

function g.test_tree_index_ge()
    test_tree_index_ge(g.tree)
end

local function test_tree_index_lt(space)
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

function g.test_tree_index_lt_vinyl()
    test_tree_index_lt(g.vinyl)
end

function g.test_tree_index_lt()
    test_tree_index_lt(g.tree)
end

local function test_tree_index_le(space)
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

function g.test_tree_index_le_vinyl()
    test_tree_index_le(g.vinyl)
end

function g.test_tree_index_le()
    test_tree_index_le(g.tree)
end
