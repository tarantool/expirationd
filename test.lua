#!/usr/bin/env tarantool

local expirationd = require('expirationd')
local fiber = require('fiber')
local log = require('log')
local tap = require('tap')
-- local strict = require('strict')

local test = tap.test("expirationd")

-- strict.on()

-- ========================================================================= --
-- local support functions
-- ========================================================================= --

-- get field
local function get_field(tuple, field_no)
    if tuple == nil then
        return nil
    end

    if #tuple < field_no then
        return nil
    end

    return tuple[field_no]
end

-- ========================================================================= --
-- Expiration handlers examples
-- ========================================================================= --

-- check tuple's expiration by timestamp (stored in last field)
local function check_tuple_expire_by_timestamp(args, tuple)
    local tuple_expire_time = get_field(tuple, args.field_no)
    if type(tuple_expire_time) ~= 'number' then
        return true
    end

    local current_time = fiber.time()
    return current_time >= tuple_expire_time
end

-- put expired tuple in archive
local function put_tuple_to_archive(space_id, args, tuple)
    -- delete expired tuple
    box.space[space_id]:delete{tuple[1]}
    local email = get_field(tuple, 2)
    if args.archive_space_id ~= nil and email ~= nil then
        box.space[args.archive_space_id]:replace{email, fiber.time()}
    end
end

local function check_tuple_expire_by_timestamp_error(args, tuple)
   nonexistentfunction()
end

local function put_tuple_to_archive_error(space_id, args, tuple)
   nonexistentfunction()
end
-- ========================================================================= --
-- Expiration module test functions
-- ========================================================================= --
-- Warning: for these test functions to work, you need
-- a space with a numeric primary key defined on field[0]

-- generate email string
local function get_email(uid)
    local email = "test_" .. uid .. "@tarantool.org"
    return email
end
-- insert entry to space
local function add_entry(space_id, uid, email, expiration_time)
    box.space[space_id]:replace{uid, email, expiration_time}
end

-- put test tuples
local function put_test_tuples(space_id, total)
    local time = math.floor(fiber.time())
    for i = 0, total do
        add_entry(space_id, i, get_email(i), time + i)
    end

    -- tuple w/o expiration date
    local uid = total + 1
    add_entry(space_id, uid, get_email(uid), "")

    -- tuple w/ invalid expiration date
    uid = total + 2
    add_entry(space_id, uid, get_email(uid), "some string in exp field")
end

-- print test tuples
local function print_test_tuples(space_id)
    for state, tuple in box.space[space_id].index[0]:pairs(nil, {iterator = box.index.ALL}) do
        log.info(tostring(tuple))
    end
end

local function prefix_space_id(space_id)
   if type(space_id) == 'number' then
      return '#'..space_id
   end
   return string.format('%q', space_id)
end

-- Configure box, create spaces and indexes
local function init_box()
    box.cfg{ logger = 'tarantool.log' }
    local index_type = arg[1] or os.getenv('INDEX_TYPE') or 'TREE'
    log.info('Running tests for %s index', index_type)

    local a = box.schema.create_space('origin', {if_not_exists = true})
    a:create_index('first', {type = index_type, parts = {1, 'NUM'}, if_not_exists = true})
    a:truncate()

    local b = box.schema.create_space('cemetery', {if_not_exists = true})
    b:create_index('first', {type = index_type, parts = {1, 'STR'}, if_not_exists = true})
    b:truncate()

    local c = box.schema.create_space('exp_test', {if_not_exists = true})
    c:create_index('first', {type = index_type, parts = {1, 'NUM'}, if_not_exists = true})
    c:truncate()

    local d = box.schema.create_space('drop_test', {if_not_exists = true})
    d:create_index('first', {type = index_type, parts = {1, 'NUM'}, if_not_exists = true})
    d:truncate()

    local e = box.schema.create_space('restart_test', {if_not_exists = true})
    e:create_index('first', {type = index_type, parts = {1, 'NUM'}, if_not_exists = true})
    e:truncate()

    local f = box.schema.create_space('complex_test', {if_not_exists = true})
    f:create_index('first', {type = index_type, parts = {2, 'NUM', 1, 'NUM'},
            if_not_exists = true})
    f:truncate()
end

local space_id = 'origin'
local archive_space_id = 'cemetery'

init_box()

-- ========================================================================= --
-- TAP TESTS:
-- 1. simple archive test.
-- 2. errors test,
-- 3. not expire test,
-- 4. kill zombie test
-- 5. default drop function test
-- ========================================================================= --
test:plan(8)

test:test('simple expires test',  function(test)
    test:plan(4)
    -- put test tuples
    log.info("-------------- put ------------")
    log.info("put to space " .. prefix_space_id(space_id))
    put_test_tuples(space_id, 10)

    -- print before
    log.info("------------- print -----------")
    log.info("before print space " .. prefix_space_id(space_id), "\n")
    print_test_tuples(space_id)
    log.info("before print archive space " .. prefix_space_id(archive_space_id), "\n")
    print_test_tuples(archive_space_id)

    log.info("-------------- run ------------")
    expirationd.run_task(
        "test",
        space_id,
        check_tuple_expire_by_timestamp,
        put_tuple_to_archive,
        {
            field_no = 3,
            archive_space_id = archive_space_id
        }
    )

    -- wait expiration
    local start_time = fiber.time()
    log.info("------------- wait ------------")
    log.info("before time = " .. os.date('%X', start_time))
    require('fiber').sleep(5)
    local end_time = fiber.time()
    log.info("after time = " .. os.date('%X', end_time))

    -- print after
    log.info("------------- print -----------")
    log.info("After print space " .. prefix_space_id(space_id), "\n")
    print_test_tuples(space_id)
    log.info("after print archive space " .. prefix_space_id(archive_space_id), "\n")
    print_test_tuples(archive_space_id)

    expirationd.show_task_list(true)

    local task = expirationd.get_task("test")
    test:is(task.start_time, start_time)
    test:is(task.name, "test")
    test:is(task.restarts, 1)
    test:is(task.expired_tuples_count, 7, 'Test task executed and moved to archive')
    expirationd.kill_task("test")
end)

test:test("execution error test", function (test)
    test:plan(2)
    expirationd.run_task(
        "test",
        space_id,
        check_tuple_expire_by_timestamp_error,
        put_tuple_to_archive,
        {
            field_no = 3,
            archive_space_id = archive_space_id
        }
    )
    test:is(expirationd.get_task("test").restarts, 1)

    expirationd.run_task("test",
         space_id,
         check_tuple_expire_by_timestamp,
         put_tuple_to_archive_error,
         {
             field_no = 3,
             archive_space_id = archive_space_id
         }
    )
    local task = expirationd.get_task("test")
    test:is(task.restarts, 1, 'Error task executed')
    expirationd.kill_task("test")
end)

test:test("not expired task",  function(test)
    test:plan(2)
    box.space[space_id]:truncate()
    local tuples_count = 5
    local time = fiber.time()
    for i = 1, tuples_count do
        add_entry(space_id, i, get_email(i), time + 2)
    end

    expirationd.run_task(
        "test",
         space_id,
         check_tuple_expire_by_timestamp,
         put_tuple_to_archive,
         {
             field_no = 3,
             archive_space_id = archive_space_id
         }
    )
    local task = expirationd.get_task("test")
    -- after run tuples is not expired
    test:is(task.expired_tuples_count, 0)
    -- wait 3 seconds and check: all tuples must be expired
    require('fiber').sleep(3)
    test:is(task.expired_tuples_count, tuples_count)
    expirationd.kill_task("test")
end)

test:test("zombie task kill", function(test)
    test:plan(4)
    local tuples_count = 10
    local time = math.floor(fiber.time())
    for i = 0, tuples_count do
        add_entry(space_id, i, get_email(i), time + i * 5)
    end
    -- first run
    expirationd.run_task(
        "test",
         space_id,
         check_tuple_expire_by_timestamp,
         put_tuple_to_archive,
         {
             field_no = 3,
             archive_space_id = archive_space_id
         }
    )
    local fiber_obj = expirationd.get_task("test").guardian_fiber
    test:is(fiber_obj:status(), 'suspended')
    -- run again and check - it must kill first task
    expirationd.run_task(
        "test",
         space_id,
         check_tuple_expire_by_timestamp,
         put_tuple_to_archive,
         {
             field_no = 3,
             archive_space_id = archive_space_id
         }
    )
    local task = expirationd.get_task("test")
    test:is(task.restarts, 1)
    -- check is first fiber killed
    test:is(task.guardian_fiber:status(), "suspended")
    test:is(fiber_obj:status(), 'dead', "Zombie task was killed and restarted")
    expirationd.kill_task("test")
end)

test:test("multiple expires test", function(test)
    test:plan(2)
    local tuples_count = 10
    local time = fiber.time()
    local space_name = 'exp_test'
    local expire_delta = 2

    for i = 1, tuples_count do
        box.space[space_name]:delete{i}
        box.space[space_name]:insert{i, get_email(i), time + expire_delta}
    end

    expirationd.run_task(
        "test",
         space_name,
         check_tuple_expire_by_timestamp,
         put_tuple_to_archive,
         {
             field_no = 3,
             archive_space_id = archive_space_id
         },
         5,
         1
    )
    -- test first expire part
    fiber.sleep(1 + expire_delta)
    local task = expirationd.get_task("test")
    local cnt = task.expired_tuples_count
    test:ok(cnt < tuples_count and cnt > 0, 'First part expires done')

    -- test second expire part
    fiber.sleep(1)
    test:is(expirationd.get_task("test").expired_tuples_count,
            tuples_count, 'Multiple expires done')
    expirationd.kill_task("test")
end)

test:test("default drop function test", function(test)
    test:plan(2)
    local tuples_count = 10
    local space_name = 'drop_test'
    local space = box.space[space_name]
    for i = 1, tuples_count do
        space:insert{i, 'test_data', fiber.time() + 2}
    end

    expirationd.run_task(
        "test",
         space_name,
         check_tuple_expire_by_timestamp,
         nil,
         {
             field_no = 3,
             archive_space_id = archive_space_id
         },
         10,
         1
    )

    test:is(space:len(), tuples_count, 'tuples are in space')
    fiber.sleep(3)
    test:is(space:len(), 0, 'all tuples are expired with default function')
    expirationd.kill_task("test")
end)

test:test("restart test", function(test)
    test:plan(3)
    local tuples_count = 10
    local space_name = 'restart_test'
    local space = box.space[space_name]

    local task = expirationd.run_task(
        "test",
         space_name,
         check_tuple_expire_by_timestamp,
         nil,
         {
             field_no = 3,
             archive_space_id = archive_space_id
         },
         10,
         1
    )

    local old_expd = expirationd
    expirationd.update()
    for i = 1, tuples_count do
        space:insert{i, 'test_data', fiber.time() + 1}
    end

    expirationd = require('expirationd')
    test:isnt(tostring(old_expd):match('0x.*'),
              tostring(expirationd):match('0x.*'),
              'new expirationd table')

    test:is(space:len(), tuples_count, 'tuples are in space')
    fiber.sleep(4)
    test:is(space:len(), 0, 'all tuples are expired')

    task:statistics()

    expirationd.kill_task("test")
end)

test:test("complex key test", function(test)
    test:plan(2)
    local tuples_count = 10
    local space_name = 'complex_test'
    local space = box.space[space_name]

    for i = 1, tuples_count do
        space:insert{i, i*i + 100, fiber.time() + 1}
    end

    expirationd.run_task(
        "test",
         space_name,
         check_tuple_expire_by_timestamp,
         nil,
         {
             field_no = 3,
             archive_space_id = archive_space_id
         },
         10,
         1
    )

    test:is(space:len(), tuples_count, 'tuples are in space')
    fiber.sleep(3.1)
    test:is(space:len(), 0, 'all tuples are expired with default function')
    expirationd.kill_task("test")
end)

test:check()

-- strict.off()

os.exit()
