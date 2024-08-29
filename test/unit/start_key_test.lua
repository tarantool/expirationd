local expirationd = require("expirationd")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('start_key', {
    {index_type = 'TREE', engine = 'vinyl'},
    {index_type = 'TREE', engine = 'memtx'},
    {index_type = 'HASH', engine = 'memtx'},
})

g.before_each({index_type = 'TREE'}, function(cg)
    t.skip_if(cg.params.engine == 'vinyl' and not helpers.vinyl_is_supported(),
        'Blocked by https://github.com/tarantool/tarantool/issues/6448 on ' ..
		'this Tarantool version')
    g.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.before_each({index_type = 'HASH'}, function(cg)
    g.space = helpers.create_space_with_hash_index(cg.params.engine)
end)

g.after_each(function(g)
    g.space:drop()
end)

function g.test_passing(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true)
    -- default start element is nil, iterate all elements
    t.assert_equals(task.start_key(), nil)
    task:kill()

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {start_key = box.NULL})
    -- default start element is nil, iterate all elements
    t.assert_equals(task.start_key(), box.NULL)
    task:kill()

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            {start_key = 1})
    t.assert_equals(task.start_key(), 1)
    task:kill()

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true,
            { index = "multipart_index", start_key = {1, 1}})
    t.assert_equals(task.start_key(), {1, 1})
    task:kill()

    -- errors
    task = expirationd.start(
            "clean_all",
            cg.space.id,
            helpers.is_expired_true,
            {
                start_key = ""
            }
    )

    t.helpers.retrying({}, function()
        t.assert_equals(task.alert, "Expirationd warning, task \"clean_all\": Supplied key type of part " ..
                "0 does not match index part type: expected number")
    end)

    task = expirationd.start(
            "clean_all",
            cg.space.id,
            helpers.is_expired_true,
            {
                index = "multipart_index",
                start_key = ""
            }
    )

    t.helpers.retrying({}, function()
        t.assert_equals(task.alert, "Expirationd warning, task \"clean_all\": Supplied key type of part " ..
                "0 does not match index part type: expected number")
    end)

    task = expirationd.start(
            "clean_all",
            cg.space.id,
            helpers.is_expired_true,
            {
                index = "multipart_index",
                start_key = {"", ""}
            }
    )

    t.helpers.retrying({}, function()
        t.assert_equals(task.alert, "Expirationd warning, task \"clean_all\": Supplied key type of part " ..
                "0 does not match index part type: expected number")
    end)

    task = expirationd.start(
            "clean_all",
            cg.space.id,
            helpers.is_expired_true,
            {
                index = "multipart_index",
                start_key = {1, ""}
            }
    )

    t.helpers.retrying({}, function()
        t.assert_equals(task.alert, "Expirationd warning, task \"clean_all\": Supplied key type of part " ..
                "1 does not match index part type: expected number")
    end)
end

function g.test_tree_index(cg)
    t.skip_if(cg.params.index_type ~= 'TREE', 'Unsupported index type')

    local space = cg.space
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

function g.test_hash_index(cg)
    t.skip_if(cg.params.index_type ~= 'HASH', 'Unsupported index type')

    -- without start key
    helpers.iteration_result = {}
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug)
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
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
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
    cg.space:insert({1, "3"})
    cg.space:insert({2, "2"})
    cg.space:insert({3, "1"})

    task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_debug,
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
