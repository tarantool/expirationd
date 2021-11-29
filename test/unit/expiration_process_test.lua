local expirationd = require('expirationd')
local fiber = require('fiber')
local t = require('luatest')

local helpers = require('test.helper')

local g = t.group('expiration_process', {
    {index_type = 'TREE', engine = 'vinyl'},
    {index_type = 'TREE', engine = 'memtx'},
    {index_type = 'HASH', engine = 'memtx'},
})

g.before_each({index_type = 'TREE'}, function(cg)
    t.skip_if(cg.params.engine == 'vinyl' and not helpers.vinyl_is_supported(),
        'Blocked by https://github.com/tarantool/tarantool/issues/6448')
    g.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.before_each({index_type = 'HASH'}, function(cg)
    g.space = helpers.create_space_with_hash_index(cg.params.engine)
end)

g.before_each({index_type = 'BITSET'}, function(cg)
    g.space = helpers.create_space_with_bitset_index(cg.params.engine)
end)

g.before_each(function(cg)
    local space_archive = helpers.create_space('archived_tree', cg.params.engine)
    space_archive:create_index('primary')
    g.space_archive = space_archive

    cg.task_name = 'test'
end)

g.after_each(function(g)
    g.space:drop()
    g.space_archive:drop()
    expirationd.kill(g.task_name)
end)

-- Check tuple's expiration by timestamp.
local function check_tuple_expire_by_timestamp(args, tuple)
    local tuple_expire_time = tuple[args.field_no]

    local current_time = fiber.time()
    return current_time >= tuple_expire_time
end

-- Put expired tuple in archive.
local function put_tuple_to_archive(space_id, args, tuple)
    -- Delete expired tuple.
    box.space[space_id]:delete({tuple.id})
    local id, first_name = tuple.id, tuple.first_name
    if args.archive_space_id ~= nil and id ~= nil and first_name ~= nil then
        box.space[args.archive_space_id]:insert({id, first_name, fiber.time()})
    end
end

-- Checking that we can use custom is_tuple_expired, process_expired_tuple,
-- these basic functions are included in expiration_process.
-- We also test the timestamp expiration check.
g.before_test('test_archive_by_timestamp', function(cg)
    local space = cg.space
    local space_archive = cg.space_archive
    local task_name = cg.task_name

    local total = 10
    local time = fiber.time()
    for i = 1, total do
        space:insert({i, tostring(i), time + i})
    end

    cg.task = expirationd.start(task_name, space.id, check_tuple_expire_by_timestamp,
            {
                process_expired_tuple = put_tuple_to_archive,
                args = {
                    field_no = 3,
                    archive_space_id = space_archive.id
                },
            })
end)

function g.test_archive_by_timestamp(cg)
    local space_archive = cg.space_archive
    local task = cg.task

    local start_time = fiber.time()

    -- Wait for at least 4 tuples archived, but not more than 6.
    -- Once a second, one tuple should be expired by timestamp,
    -- given that the retry timeout is 5 seconds, we should get the desired result.
    helpers.retrying({timeout = 5}, function()
        t.assert_ge(space_archive:count(), 4)
        t.assert_lt(space_archive:count(), 7)
        t.assert_ge(task.expired_tuples_count, 4)
        t.assert_lt(task.expired_tuples_count, 7)
    end)

    -- Check the validity of the task parameters.
    t.assert_almost_equals(task.start_time, start_time, 0.1)
    t.assert_equals(task.name, 'test')
    t.assert_equals(task.restarts, 1)

    helpers.retrying({}, function()
        t.assert_ge(space_archive:count(), 7)
        t.assert_ge(task.expired_tuples_count, 7)
    end)
end
