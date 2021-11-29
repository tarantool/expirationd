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

function g.test_broken_is_tuple_expired(cg)
    local space = cg.space
    local space_archive = cg.space_archive
    local task_name = cg.task_name

    local full_scan_counter = 0
    local task = expirationd.start(
            task_name,
            space.id,
            helpers.error_function,
            {
                process_expired_tuple = put_tuple_to_archive,
                args = {
                    field_no = 3,
                    archive_space_id = space_archive.id,
                },
                on_full_scan_complete = function() full_scan_counter = full_scan_counter + 1 end
            }
    )
    t.assert_equals(task.restarts, 1)

    -- Check that task is alive and running.
    helpers.retrying({}, function()
        t.assert_ge(full_scan_counter, 3)
    end)
end

function g.test_broken_process_expired_tuple(cg)
    local space = cg.space
    local space_archive = cg.space_archive
    local task_name = cg.task_name

    local full_scan_counter = 0
    local task = expirationd.start(
            task_name,
            space.id,
            check_tuple_expire_by_timestamp,
            {
                process_expired_tuple = helpers.error_function,
                args = {
                    field_no = 3,
                    archive_space_id = space_archive.id,
                },
                on_full_scan_complete = function() full_scan_counter = full_scan_counter + 1 end
            }
    )
    t.assert_equals(task.restarts, 1)

    -- Check that task is alive and running.
    helpers.retrying({}, function()
        t.assert_ge(full_scan_counter, 3)
    end)
end

g.before_test('test_check_tuples_not_expired_by_timestamp', function(cg)
    local space = cg.space
    local space_archive = cg.space_archive
    local task_name = cg.task_name

    local total = 5
    for i = 1, total do
        local time = fiber.time()
        space:insert({i, tostring(i), time + 2})
    end

    cg.full_scan_counter = 0
    cg.task = expirationd.start(task_name, space.id, check_tuple_expire_by_timestamp,
            {
                process_expired_tuple = put_tuple_to_archive,
                args = {
                    field_no = 3,
                    archive_space_id = space_archive.id
                },
                on_full_scan_complete = function()
                    cg.full_scan_counter = cg.full_scan_counter + 1
                end
            })
    cg.total = total
end)

function g.test_check_tuples_not_expired_by_timestamp(cg)
    local space_archive = cg.space_archive
    local task = cg.task
    local total = cg.total

    -- Tuples are not expired after run.
    -- Сheck that after the expiration starts,
    -- no tuples will be archived since the timestamp has an advantage of 2 seconds.
    helpers.retrying({}, function()
        t.assert(cg.full_scan_counter > 0)
        t.assert_equals(task.expired_tuples_count, 0)
        t.assert_equals(space_archive:count(), 0)
    end)

    -- Wait and check: all tuples must be expired.
    helpers.retrying({}, function()
        t.assert_equals(task.expired_tuples_count, total)
        t.assert_equals(space_archive:count(), total)
    end)
end

g.before_test('test_default_tuple_drop_function', function(cg)
    local space = cg.space
    local task_name = cg.task_name

    local total = 10
    for i = 1, total do
        local time = fiber.time()
        space:insert({i, tostring(i), time})
    end
    t.assert_equals(space:count{}, total)

    cg.task = expirationd.start(task_name, space.id, check_tuple_expire_by_timestamp,
            {
                args = {
                    field_no = 3,
                },
            })
    cg.total = total
end)

function g.test_default_tuple_drop_function(cg)
    local space = cg.space
    local space_archive = cg.space_archive
    local task = cg.task
    local total = cg.total

    -- All tuples are expired with default function.
    helpers.retrying({}, function()
        t.assert_equals(task.expired_tuples_count, total)
        t.assert_equals(space_archive:count(), 0)
        t.assert_equals(space:count{}, 0)
    end)
end

g.before_test('test_tuples_per_iteration', function(cg)
    local space = cg.space
    local space_archive = cg.space_archive
    local task_name = cg.task_name

    local total = 10
    local time = fiber.time()
    for i = 1, total do
        space:insert({i, tostring(i), time})
    end
    t.assert_equals(space:count{}, total)

    cg.task = expirationd.start(task_name, space.id, check_tuple_expire_by_timestamp,
            {
                process_expired_tuple = put_tuple_to_archive,
                args = {
                    field_no = 3,
                    archive_space_id = space_archive.id
                },
                iteration_delay = 1,
                vinyl_assumed_space_len = 5, -- iteration_delay will be 1 sec
                tuples_per_iteration = 5,
            })
    cg.total = total
end)

function g.test_tuples_per_iteration(cg)
    local task = cg.task
    local total = cg.total
    
    -- Test first expire part.
    local worker_fiber = task.worker_fiber
    helpers.retrying({}, function()
        t.assert_equals(task.expired_tuples_count, total / 2)
        t.assert_equals(worker_fiber:status(), 'suspended')
    end)

    -- Test second expire part.
    helpers.retrying({}, function()
        t.assert_equals(task.expired_tuples_count, total)
    end)
end
