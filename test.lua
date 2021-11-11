#!/usr/bin/env tarantool

local clock = require('clock')
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

-- Strip pcall()'s true or re-raise catched error.
local function wait_cond_finish(status, ...)
    if not status then
        error((...), 2)
     end

     return ...
end

-- Block until the condition function returns a positive value
-- (anything except `nil` and `false`) or until the timeout
-- exceeds. Return the result of the last invocation of the
-- condition function (it is `false` or `nil` in case of exiting
-- by the timeout).
--
-- If the condition function raises a Lua error, wait_cond()
-- continues retrying. If the latest attempt raises an error (and
-- we hit a timeout), the error will be re-raised.
local function wait_cond(cond, timeout, delay)
    assert(type(cond) == 'function')

    local timeout = timeout or 60
    local delay = delay or 0.001

    local start_time = clock.monotonic()
    local res = {pcall(cond)}

    while not res[1] or not res[2] do
        local work_time = clock.monotonic() - start_time
        if work_time > timeout then
            return wait_cond_finish(res[1], res[2])
        end
        fiber.sleep(delay)
        res = {pcall(cond)}
    end

    return wait_cond_finish(res[1], res[2])
end

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
        log = 'tarantool.log'
    }
    local index_type = arg[1] or os.getenv('INDEX_TYPE') or 'TREE'
    local space_type = arg[2] or os.getenv('SPACE_TYPE') or 'memtx'
    log.info('Running tests for %s index and engine %s', index_type, space_type)

    local a = box.schema.create_space('origin', {
        engine = space_type,
        if_not_exists = true
    })
    a:create_index('first', {
        type = index_type,
        parts = {1, 'number'},
        if_not_exists = true
    })
    truncate(a.id)

    local b = box.schema.create_space('cemetery', {
        engine = space_type,
        if_not_exists = true
    })
    b:create_index('first', {
        type = index_type,
        parts = {1, 'string'},
        if_not_exists = true
    })
    truncate(b.id)

    local c = box.schema.create_space('exp_test', {
        engine = space_type,
        if_not_exists = true
    })
    c:create_index('first', {
        type = index_type,
        parts = {1, 'number'},
        if_not_exists = true
    })
    truncate(c.id)

    local d = box.schema.create_space('drop_test', {
        engine = space_type,
        if_not_exists = true
    })
    d:create_index('first', {
        type = index_type,
        parts = {1, 'number'},
        if_not_exists = true
    })
    truncate(d.id)

    local e = box.schema.create_space('restart_test', {
        engine = space_type,
        if_not_exists = true
    })
    e:create_index('first', {
        type = index_type,
        parts = {1, 'number'},
        if_not_exists = true
    })
    truncate(e.id)

    local f = box.schema.create_space('complex_test', {
        engine = space_type,
        if_not_exists = true
    })
    f:create_index('first', {
        type = index_type,
        parts = {2, 'number', 1, 'number'},
        if_not_exists = true
    })
    truncate(f.id)

    local g = box.schema.create_space('delays_test', {
        engine = space_type,
        if_not_exists = true
    })
    g:create_index('first', {
        type = index_type,
        parts = {1, 'number'},
        if_not_exists = true
    })
    truncate(g.id)

    local h = box.schema.create_space('error_callback_test', {
        engine = space_type,
        if_not_exists = true
    })
    h:create_index('first', {
        type = index_type,
        parts = {1, 'number'},
        if_not_exists = true
    })
    truncate(h.id)
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
-- 5. multiple expires test
-- 6. default drop function test
-- 7. restart test
-- 8. complex key test
-- 9. delays and scan callbacks test
-- 10. error callback test
-- ========================================================================= --
test:plan(10)

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
    expirationd.start(
        "test",
        space_id,
        check_tuple_expire_by_timestamp,
        {
            process_expired_tuple = put_tuple_to_archive,
            args = {
                field_no = 3,
                archive_space_id = archive_space_id
            },
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

    expirationd.tasks()

    local task = expirationd.task("test")
    test:is(task.start_time, start_time, 'checking start time')
    test:is(task.name, "test", 'checking task name')
    local restarts = 1
    test:is(task.restarts, restarts, 'checking restart count')
    local res = wait_cond(
        function()
            local task = expirationd.task("test")
            local cnt = task.expired_tuples_count
            return cnt == 7
        end
    )
    test:is(res, true, 'Test task executed and moved to archive')
    expirationd.kill("test")
end)

test:test("execution error test", function (test)
    test:plan(2)
    expirationd.start(
        "test",
        space_id,
        check_tuple_expire_by_timestamp_error,
        {
            process_expired_tuple = put_tuple_to_archive,
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
        }
    )
    test:is(expirationd.task("test").restarts, 1, 'checking restart count')

    expirationd.start("test",
        space_id,
        check_tuple_expire_by_timestamp,
        {
            process_expired_tuple = put_tuple_to_archive_error,
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
        }
    )
    local task = expirationd.task("test")
    test:is(task.restarts, 1, 'Error task executed')
    expirationd.kill("test")
end)

test:test("not expired task",  function(test)
    test:plan(2)

    truncate(space_id)

    local tuples_count = 5
    local time = fiber.time()
    for i = 1, tuples_count do
        add_entry(space_id, i, get_email(i), time + 2)
    end

    expirationd.start(
        "test",
        space_id,
        check_tuple_expire_by_timestamp,
        {
            process_expired_tuple = put_tuple_to_archive,
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
        }
    )
    local task = expirationd.task("test")
    -- after run tuples is not expired
    test:is(task.expired_tuples_count, 0, 'checking expired tuples empty')
    -- wait 3 seconds and check: all tuples must be expired
    fiber.sleep(3)
    test:is(task.expired_tuples_count, tuples_count, 'checking expired tuples count')
    expirationd.kill("test")
end)

test:test("zombie task kill", function(test)
    test:plan(4)
    local tuples_count = 10
    local time = math.floor(fiber.time())
    for i = 0, tuples_count do
        add_entry(space_id, i, get_email(i), time + i * 5)
    end
    -- first run
    expirationd.start(
        "test",
        space_id,
        check_tuple_expire_by_timestamp,
        {
            process_expired_tuple = put_tuple_to_archive,
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
        }
    )
    local fiber_obj = expirationd.task("test").guardian_fiber
    test:is(fiber_obj:status(), 'suspended', 'checking status of fiber')
    -- run again and check - it must kill first task
    expirationd.start(
        "test",
        space_id,
        check_tuple_expire_by_timestamp,
        {
            process_expired_tuple = put_tuple_to_archive,
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
        }
    )
    local task = expirationd.task("test")
    test:is(task.restarts, 1, 'checking restart count')
    -- check is first fiber killed
    test:is(task.guardian_fiber:status(), "suspended", 'checking status of fiber')
    test:is(fiber_obj:status(), 'dead', "Zombie task was killed and restarted")
    expirationd.kill("test")
end)

test:test("multiple expires test", function(test)
    test:plan(2)
    local tuples_count = 10
    local time = fiber.time()
    local space_name = 'exp_test'
    local expire_delta = 0.5

    for i = 1, tuples_count do
        box.space[space_name]:delete{i}
        if i <= tuples_count / 2 then
            time = time + expire_delta
        end
        box.space[space_name]:insert{i, get_email(i), time}
    end

    expirationd.start(
        "test",
        space_name,
        check_tuple_expire_by_timestamp,
        {
            process_expired_tuple = put_tuple_to_archive,
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
            tuples_per_iteration = 5,
            full_scan_time = 1,
        }
    )
    -- test first expire part
    local res = wait_cond(
        function()
            local task = expirationd.task("test")
            local cnt = task.expired_tuples_count
            return cnt < tuples_count and cnt > 0
        end,
        2 + expire_delta
    )
    test:ok(res, true, 'First part expires done')

    -- test second expire part
    res = wait_cond(
        function()
            local task = expirationd.task("test")
            local cnt = task.expired_tuples_count
            return cnt == tuples_count
        end,
        4
    )
    test:ok(res, true, 'Multiple expires done')
    expirationd.kill("test")
end)

test:test("default drop function test", function(test)
    test:plan(2)
    local tuples_count = 10
    local space_name = 'drop_test'
    local space = box.space[space_name]
    for i = 1, tuples_count do
        space:insert{i, 'test_data', fiber.time()}
    end
    test:is(space:count{}, tuples_count, 'tuples are in space')

    expirationd.start(
        "test",
        space_name,
        check_tuple_expire_by_timestamp,
        {
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
            tuples_per_iteration = 10,
            full_scan_time = 1,
        }
    )

    local task = expirationd.task("test")
    local res = wait_cond(
        function()
            return space:count{} == 0
        end,
        2
    )
    test:is(res, true, 'all tuples are expired with default function')
    expirationd.kill("test")
end)

test:test("restart test", function(test)
    test:plan(5)
    local tuples_count = 10
    local space_name = 'restart_test'
    local space = box.space[space_name]

    local task1 = expirationd.start(
        "test1",
        space_name,
        check_tuple_expire_by_timestamp,
        {
            args = {
                field_no = 3,
                archive_space_id = archive_space_id
            },
            tuples_per_iteration = 10,
            full_scan_time = 1,
        }
    )
    local task2 = expirationd.start(
        "test2",
        space_name,
        check_tuple_expire_by_timestamp,
        {
            args = {
                field_no = 3,
                archive_space_id = archive_space_id
            },
            tuples_per_iteration = 10,
            full_scan_time = 1,
        }
    )
    local task3 = expirationd.start(
        "test3",
        space_name,
        check_tuple_expire_by_timestamp,
        {
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
            tuples_per_iteration = 10,
            full_scan_time = 1,
        }
    )
    local task4 = expirationd.start(
        "test4",
        space_name,
        check_tuple_expire_by_timestamp,
        {
            args = {
                field_no = 3,
                archive_space_id = archive_space_id,
            },
            tuples_per_iteration = 10,
            full_scan_time = 1,
        }
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

    expirationd.kill("test1")
    expirationd.kill("test2")
    expirationd.kill("test3")
    expirationd.kill("test4")
end)

test:test("complex key test", function(test)
    test:plan(2)
    local tuples_count = 10
    local space_name = 'complex_test'
    local space = box.space[space_name]

    for i = 1, tuples_count do
        space:insert{i, i*i + 100, fiber.time() + 1}
    end

    expirationd.start(
        "test",
        space_name,
        check_tuple_expire_by_timestamp,
        {
            args = {
                field_no = 3,
                archive_space_id = archive_space_id
            },
            tuples_per_iteration = 10,
            full_scan_time = 1,
        }
    )

    test:is(space:count{}, tuples_count, 'tuples are in space')
    fiber.sleep(3.1)
    test:is(space:count{}, 0, 'all tuples are expired with default function')
    expirationd.kill("test")
end)

test:test('delays and scan callbacks test', function(test)
    test:plan(4)

    -- Prepare the space.
    local tuples_count = 10
    local time = fiber.time()
    local space_name = 'delays_test'
    local expire_delta = 10

    for i = 1, tuples_count do
        box.space[space_name]:insert{i, time + expire_delta}
    end

    -- To check all delays (iteration and full scan), two full scan
    -- iterations will be performed.
    local first_iteration_done = false
    local task_name = 'delays_task'
    local cond = fiber.cond()
    local start_time = 0
    local complete_time = 0

    local iteration_delay = 1
    local full_scan_delay = 2

    expirationd.start(
        task_name,
        space_name,
        check_tuple_expire_by_timestamp,
        {
            args = {
                field_no = 2
            },
            tuples_per_iteration = 10,
            iteration_delay = iteration_delay,
            full_scan_delay = full_scan_delay,
            on_full_scan_start = function(task)
                start_time = fiber.time()
                if first_iteration_done then
                    -- Check the full scan delay with an accuracy
                    -- of 0.1 seconds.
                    test:ok(math.abs(start_time - complete_time -
                        full_scan_delay) < 0.1, 'test full scan delay')
                end
            end,
            on_full_scan_success = function(task)
                -- Must be called twice.
                test:ok(true, 'test success callback invoke')
            end,
            on_full_scan_complete = function(task)
                complete_time = fiber.time()
                if first_iteration_done then
                    cond:signal()
                else
                    -- Check the accuracy of iteration delay,
                    -- it should be not beyond 1 second.
                    test:ok(math.abs(complete_time - start_time -
                        iteration_delay) < 1, 'test iteration delay')
                    first_iteration_done = true
                end
            end
        }
    )

    cond:wait()
    expirationd.kill(task_name)
end)

test:test('error callback test', function(test)
    test:plan(2)

    -- Prepare the space.
    local tuples_count = 1
    local time = fiber.time()
    local space_name = 'error_callback_test'
    local expire_delta = 10

    for i = 1, tuples_count do
        box.space[space_name]:insert{i, time + expire_delta}
    end

    local task_name = 'error_callback_task'
    local cond = fiber.cond()

    local error_cb_called = false
    local complete_cb_called = false
    local err_msg = 'The error is occured'

    expirationd.start(
        task_name,
        space_name,
        function(args, tuple)
            error(err_msg)
        end,
        {
            args = {
                field_no = 2
            },
            -- The callbacks can be called multiple times because guardian_loop
            -- will restart the task.
            on_full_scan_error = function(task, err)
                if err:find(err_msg) then
                    error_cb_called = true
                end
            end,
            on_full_scan_complete = function(task)
                complete_cb_called = true
                cond:signal()
            end
        }
    )

    cond:wait()
    expirationd.kill(task_name)

    test:ok(error_cb_called, 'the "error" callback has been invoked')
    test:ok(complete_cb_called, 'the "complete" callback has been invoked')
end)

os.exit(test:check() and 0 or 1)
