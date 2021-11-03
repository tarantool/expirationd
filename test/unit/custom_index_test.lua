local expirationd = require("expirationd")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('custom_index', {
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

g.before_each({index_type = 'BITSET'}, function(cg)
    g.space = helpers.create_space_with_bitset_index(cg.params.engine)
end)

g.after_each(function(g)
    g.space:drop()
end)

function g.test_passing(cg)
    t.skip_if(cg.params.index_type == 'BITSET', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true)
    -- if we don't specify index, program should use primary index
    t.assert_equals(task.index, cg.space.index[0])
    task:kill()

    -- index by name
    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {index = "index_for_first_name"})
    t.assert_equals(task.index, cg.space.index[1])
    task:kill()

    -- index by id
    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {index = 1})
    t.assert_equals(task.index, cg.space.index[1])
    task:kill()
end

function g.test_tree_index_errors(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    -- errors
    t.assert_error_msg_content_equals("Index with name not_exists_index does not exist",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true,
            {index = "not_exists_index"})
    t.assert_error_msg_content_equals("Index with id 10 does not exist",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true,
            {index = 10})
    t.assert_error_msg_contains("bad argument options.index to nil (?number|string expected, got table)",
            expirationd.start, "clean_all", cg.space.id, helpers.is_expired_true,
            {index = { 10 }})
end

function g.test_tree_index(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    helpers.iteration_result = {}

    local space = cg.space
    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    -- check default primary index
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
    helpers.iteration_result = {}

    space:insert({1, "3"})
    space:insert({2, "2"})
    space:insert({3, "1"})

    -- check custom index
    task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {index = "index_for_first_name"})
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

function g.test_tree_index_multipart(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    helpers.iteration_result = {}

    local space = cg.space
    space:insert({1, "1", 2, 1})
    space:insert({2, "2", 2, 2})
    space:insert({3, "3", 1, 3})

    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {index = "multipart_index"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "3", 1, 3},
            {1, "1", 2, 1},
            {2, "2", 2, 2}
        })
    end)
    task:kill()
end

function g.test_tree_index_non_unique(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    helpers.iteration_result = {}

    cg.space:insert({1, "3", nil, nil, 1})
    cg.space:insert({2, "2", nil, nil, 2})
    cg.space:insert({3, "1", nil, nil, 1})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {index = "non_unique_index"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
                    {1, "3", nil, nil, 1},
                    {3, "1", nil, nil, 1},
                    {2, "2", nil, nil, 2}
                })
    end)
    task:kill()
end

function g.test_tree_index_json_path(cg)
    t.skip_if(_TARANTOOL < "2", 'Unsupported Tarantool version')
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    helpers.iteration_result = {}

    local space = cg.space
    space:insert({1, "1", nil, nil, nil, { age  = 3 }})
    space:insert({2, "2", nil, nil, nil, { age  = 1 }})
    space:insert({3, "3", nil, nil, nil, { age  = 2 }})
    space:insert({4, "4", nil, nil, nil, { days = 3 }})
    space:insert({5, "5", nil, nil, nil, { days = 1 }})
    space:insert({6, "6", nil, nil, nil, { days = 2 }})


    local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
            {index = "json_path_index"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {4, "4", nil, nil, nil, { days = 3 }},
            {5, "5", nil, nil, nil, { days = 1 }},
            {6, "6", nil, nil, nil, { days = 2 }},
            {2, "2", nil, nil, nil, { age = 1 }},
            {3, "3", nil, nil, nil, { age = 2 }},
            {1, "1", nil, nil, nil, { age = 3 }}
        })
    end)
    task:kill()
end

function g.test_tree_index_multikey(cg)
    t.skip_if(_TARANTOOL < "2", "Unsupported Tarantool version")
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    helpers.iteration_result = {}

    cg.space:insert({1, "1", nil, nil, nil, nil, {data = {{name = "A"},
                                                       {name = "B"}},
                                               extra_field = 1}})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {index = "multikey_index"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        -- met only once, since we delete and cannot walk a second time on name = "B"
        t.assert_equals(helpers.iteration_result, {
            {1, "1", nil, nil, nil, nil, {data = {{name = "A"},
                                                  {name = "B"}},
                                          extra_field = 1}}
        })
    end)
    task:kill()
end

function g.test_memtx_tree_functional_index(cg)
    t.skip_if(_TARANTOOL < "2", "Unsupported Tarantool version")
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')
    t.skip_if(cg.params.engine == 'vinyl', 'Unsupported engine')

    helpers.iteration_result = {}

    cg.space:insert({1, "1", nil, nil, nil, nil, nil, "12"})
    cg.space:insert({2, "2", nil, nil, nil, nil, nil, "21"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {index = "functional_index"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        -- sort by second character to eighth field
        t.assert_equals(helpers.iteration_result, {
            {2, "2", nil, nil, nil, nil, nil, "21"},
            {1, "1", nil, nil, nil, nil, nil, "12"}
        })
    end)
    task:kill()
end

function g.test_hash_index(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    helpers.iteration_result = {}
    cg.space:insert({1, "a"})
    cg.space:insert({2, "b"})
    cg.space:insert({3, "c"})

    -- check default primary index
    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug)
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "c"},
            {2, "b"},
            {1, "a"}
        })
    end)
    task:kill()

    helpers.iteration_result = {}
    cg.space:insert({1, "a"})
    cg.space:insert({2, "b"})
    cg.space:insert({3, "c"})

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {index = "index_for_first_name"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {1, "a"},
            {3, "c"},
            {2, "b"}
        })
    end)
    helpers.iteration_result = {}
    task:kill()
end

function g.test_hash_index_multipart(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    helpers.iteration_result = {}

    cg.space:insert({1, "1"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "3"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
            {index = "multipart_index"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"},
            {1, "1"},
            {3, "3"}
        })
    end)
    task:kill()
end
