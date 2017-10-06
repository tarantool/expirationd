#!/usr/bin/env tarantool

local fun = require('fun')
local log = require('log')
local tap = require('tap')
local yaml = require('yaml')
local fiber = require('fiber')
local strict = require('strict')
local expirationd = require('expirationd')

local test = tap.test("expirationd")

strict.on()

-- ========================================================================= --
--                          local support functions                          --
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

local function construct_key(space_id, tuple)
    return fun.map(
        function(x) return tuple[x.fieldno] end,
        box.space[space_id].index[0].parts
    ):totable()
end

local function truncate(space_id)
    local sp = box.space[space_id]
    if sp.engine == 'memtx' then
        return sp:truncate()
    else
        fun.iter(box.space[space_id]:select()):each(function(tuple)
            box.space[space_id]:delete(construct_key(space_id, tuple))
        end)
    end
end

-- ========================================================================= --
-- Expiration handlers examples
-- ========================================================================= --

local function len(arg)
    local len = 0
    for k, v in pairs(arg) do
        len = len + 1
    end
    return len
end

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

local nonexistentfunction = nil

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
    box.cfg{
        logger = 'tarantool.log'
    }
    local index_type = arg[1] or os.getenv('INDEX_TYPE') or 'TREE'
    local space_type = arg[2] or os.getenv('SPACE_TYPE') or 'memtx'
    if space_type == 'sophia' or space_type == 'phia' or space_type == 'vinyl' then
        space_type = nil
        if box.error.VINYL ~= nil then
            space_type = 'vinyl'
        else
            os.exit(0)
        end
    end
    log.info('Running tests for %s index', index_type)

    local a = box.schema.create_space('origin', {
        engine = space_type,
        if_not_exists = true
    })
    a:create_index('first', {
        type = index_type,
        parts = {1, 'NUM'},
        if_not_exists = true
    })
    truncate(a.id)

    local b = box.schema.create_space('cemetery', {
        engine = space_type,
        if_not_exists = true
    })
    b:create_index('first', {
        type = index_type,
        parts = {1, 'STR'},
        if_not_exists = true
    })
    truncate(b.id)

    local c = box.schema.create_space('exp_test', {
        engine = space_type,
        if_not_exists = true
    })
    c:create_index('first', {
        type = index_type,
        parts = {1, 'NUM'},
        if_not_exists = true
    })
    truncate(c.id)

    local d = box.schema.create_space('drop_test', {
        engine = space_type,
        if_not_exists = true
    })
    d:create_index('first', {
        type = index_type,
        parts = {1, 'NUM'},
        if_not_exists = true
    })
    truncate(d.id)

    local e = box.schema.create_space('restart_test', {
        engine = space_type,
        if_not_exists = true
    })
    e:create_index('first', {
        type = index_type,
        parts = {1, 'NUM'},
        if_not_exists = true
    })
    truncate(e.id)

    local f = box.schema.create_space('complex_test', {
        engine = space_type,
        if_not_exists = true
    })
    f:create_index('first', {
        type = index_type,
        parts = {2, 'NUM', 1, 'NUM'},
        if_not_exists = true
    })
    truncate(f.id)
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
    fiber.sleep(5)
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
    test:is(task.start_time, start_time, 'checking start time')
    test:is(task.name, "test", 'checking task name')
    local restarts = 5
    if box.space[space_id].engine == 'memtx' then
        restarts = 1
    end
    test:is(task.restarts, restarts, 'checking restart count')
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
    test:is(expirationd.get_task("test").restarts, 1, 'checking restart count')

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

    truncate(space_id)

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
    test:is(task.expired_tuples_count, 0, 'checking expired tuples empty')
    -- wait 3 seconds and check: all tuples must be expired
    fiber.sleep(3)
    test:is(task.expired_tuples_count, tuples_count, 'checking expired tuples count')
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
    test:is(fiber_obj:status(), 'suspended', 'checking status of fiber')
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
    test:is(task.restarts, 1, 'checking restart count')
    -- check is first fiber killed
    test:is(task.guardian_fiber:status(), "suspended", 'checking status of fiber')
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

    test:is(space:count{}, tuples_count, 'tuples are in space')
    fiber.sleep(3)
    test:is(space:count{}, 0, 'all tuples are expired with default function')
    expirationd.kill_task("test")
end)

test:test("restart test", function(test)
    test:plan(5)
    local tuples_count = 10
    local space_name = 'restart_test'
    local space = box.space[space_name]

    local task1 = expirationd.run_task(
        "test1", space_name, check_tuple_expire_by_timestamp,
         nil, { field_no = 3, archive_space_id = archive_space_id }, 10, 1
    )
    local task2 = expirationd.run_task(
        "test2", space_name, check_tuple_expire_by_timestamp,
         nil, { field_no = 3, archive_space_id = archive_space_id }, 10, 1
    )
    local task3 = expirationd.run_task(
        "test3", space_name, check_tuple_expire_by_timestamp,
         nil, { field_no = 3, archive_space_id = archive_space_id }, 10, 1
    )
    local task4 = expirationd.run_task(
        "test4", space_name, check_tuple_expire_by_timestamp,
         nil, { field_no = 3, archive_space_id = archive_space_id }, 10, 1
    )

    local fiber_cnt = len(fiber.info())
    local old_expd = expirationd

    local chan = fiber.channel(1)
    local fiber_update = fiber.create(function()
        expirationd.update()
        chan:put(1)
    end)
    local ok, err = pcall(function() expirationd.start() end)
    test:like(err, ".*Wait until update is done.*", "error while reloading")
    chan:get()

    for i = 1, tuples_count do
        space:insert{i, 'test_data', fiber.time() + 1}
    end

    expirationd = require('expirationd')
    test:isnt(tostring(old_expd):match('0x.*'),
              tostring(expirationd):match('0x.*'),
              'new expirationd table')

    test:is(space:count{}, tuples_count, 'tuples are in space')
    fiber.sleep(4)
    test:is(space:count{}, 0, 'all tuples are expired')

    task1:statistics()
    test:is(fiber_cnt, len(fiber.info()), "check for absence of ghost fibers")

    expirationd.kill_task("test1")
    expirationd.kill_task("test2")
    expirationd.kill_task("test3")
    expirationd.kill_task("test4")
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

    test:is(space:count{}, tuples_count, 'tuples are in space')
    fiber.sleep(3.1)
    test:is(space:count{}, 0, 'all tuples are expired with default function')
    expirationd.kill_task("test")
end)

os.exit(test:check() and 0 or 1)
