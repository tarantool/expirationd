#!/usr/bin/env tarantool

local expirationd = require('expirationd')
local log = require('log')
local tap = require('tap')
local test = tap.test("expirationd")

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

    local current_time = os.time()
    return current_time >= tuple_expire_time
end

-- put expired tuple in archive
local function put_tuple_to_archive(space_id, args, tuple)
    -- delete expired tuple
    box.space[space_id]:delete{tuple[1]}
    local email = get_field(tuple, 2)
    if args.archive_space_id ~= nil and email ~= nil then
        box.space[args.archive_space_id]:replace{email, os.time()}
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
    local time = math.floor(os.time())
    for i = 0, total do
        add_entry(space_id, i, get_email(i), time + i)
    end

    -- tuple w/o expiration date
    uid = total + 1
    add_entry(space_id, uid, get_email(uid), "")

    -- tuple w/ invalid expiration date
    uid = total + 2
    add_entry(space_id, uid, get_email(uid), "some string in exp field")
end

-- print test tuples
local function print_test_tuples(space_id)
    for state, tuple in box.space[space_id].index[0]:pairs(nil, {iterator = box.index.ALL}) do
        print(tuple)
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
   box.cfg{}
   if box.space.origin == nil then
      a = box.schema.create_space('origin')
      a:create_index('first', {type = 'TREE', parts = {1, 'NUM'}})
   else
      box.space.origin:truncate()
   end

   if box.space.cemetery == nil then
      b = box.schema.create_space('cemetery')
      b:create_index('first', {type = 'TREE', parts = {1, 'STR'}})
   else
      box.space.cemetery:truncate()
   end
   
   if box.space.exp_test == nil then
      b = box.schema.create_space('exp_test')
      b:create_index('first', {type = 'TREE', parts = {1, 'NUM'}})
   else
      box.space.exp_test:truncate()
   end

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
-- ========================================================================= --
test:plan(4)

test:test('simple expires test',  function(test)
    test:plan(4)
    -- put test tuples
    print("-------------- put ------------")
    print("put to space " .. prefix_space_id(space_id))
    put_test_tuples(space_id, 10)

    -- print before
    print("\n------------- print -----------")
    print("before print space " .. prefix_space_id(space_id), "\n")
    print_test_tuples(space_id)
    print("\nbefore print archive space " .. prefix_space_id(archive_space_id), "\n")
    print_test_tuples(archive_space_id)

    print("-------------- run ------------")
    expirationd.run_task(
        "test",
        space_id,
        check_tuple_expire_by_timestamp,
        put_tuple_to_archive,
        {
            field_no = 2,
            archive_space_id = archive_space_id
        }
    )

    -- wait expiration
    start_time = os.time()
    print("------------- wait ------------")
    print("before time = ", os.date('%X', start_time))
    require('fiber').sleep(5)
    end_time = os.time()
    print("after time = ", os.date('%X', end_time))

    -- print after
    print("\n------------- print -----------")
    print("After print space " .. prefix_space_id(space_id), "\n")
    print_test_tuples(space_id)
    print("\nafter print archive space " .. prefix_space_id(archive_space_id), "\n")
    print_test_tuples(archive_space_id)

    expirationd.show_task_list(true)
    log.info('')
    expirationd.task_details("test")
 
    local task = expirationd.task_list["test"]
    log.info(task.expired_tuples_count)
    test:is(task.start_time, start_time)
    test:is(task.name, "test")
    test:is(task.restarts, 1)
    test:ok(task.expired_tuples_count==13, 'Test task executed and moved to archive')
end)

test:test("execution error test", function (test)
    test:plan(2)
    expirationd.run_task(
        "test",
        space_id,
        check_tuple_expire_by_timestamp_error,
        put_tuple_to_archive,
        {
            field_no = 2,
            archive_space_id = archive_space_id
        }
    )
    test:is(expirationd.task_list["test"].restarts, 1)

    expirationd.run_task("test",
         space_id,
         check_tuple_expire_by_timestamp,
         put_tuple_to_archive_error,
         {
             field_no = 2,
             archive_space_id = archive_space_id
         }
    )
    local task = expirationd.task_list["test"]
    test:ok(task.restarts==1, 'Error task executed')
end)

test:test("not expired task",  function(test)
    test:plan(2)
    tuples_count = 10
    local time = math.floor(os.time())
    for i = 0, tuples_count do
        add_entry(space_id, i, get_email(i), time + (i + 1)* 5)
    end
    
    expirationd.run_task(
        "test",
         space_id,
         check_tuple_expire_by_timestamp,
         put_tuple_to_archive,
         {
             field_no = 2,
             archive_space_id = archive_space_id
         }
    )
    task = expirationd.task_list["test"]
    -- after run tuples is not expired
    test:is(task.expired_tuples_count, 1)
    -- wait 5 seconds and check: all tuples must be expired
    require('fiber').sleep(5)
    test:ok(task.expired_tuples_count==11)
end)

test:test("zombie task kill", function(test)
    test:plan(4)
    tuples_count = 10
    local time = math.floor(os.time())
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
             field_no = 2,
             archive_space_id = archive_space_id
         }
    )
    fiber_obj = expirationd.task_list["test"].guardian_fiber
    test:is(fiber_obj:status(), 'suspended')
    -- run again and check - it must kill first task
    expirationd.run_task(
        "test",
         space_id,
         check_tuple_expire_by_timestamp,
         put_tuple_to_archive,
         {
             field_no = 2,
             archive_space_id = archive_space_id
         }
    )
    local task = expirationd.task_list["test"]
    test:is(task.restarts, 1)
    -- check is first fiber killed   
    test:is(task.guardian_fiber:status(), "suspended")
    test:ok(fiber_obj:status() == 'dead', "Zobie task was killed and restarted")
end)

test:test("multiple expires test", function(test)
    test:plan(2)
    tuples_count = 10
    local time = math.floor(os.time())
    space_name = 'exp_test'
    expire_delta = 2
    
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
             field_no = 2,
             archive_space_id = archive_space_id
         },
         5,
         1
    )
    fiber = require('fiber')    
    -- test first expire part
    fiber.sleep(1 + expire_delta)
    log.info(task.expired_tuples_count)
    cnt = expirationd.task_list["test"].expired_tuples_count
    test:ok(cnt < tuples_count and cnt > 0, 'First part expires done')
    
    -- test second expire part
    fiber.sleep(1 + expire_delta)
    log.info(task.expired_tuples_count)
    test:ok(expirationd.task_list["test"].expired_tuples_count==tuples_count, 'Multiple expires done')  
end)

os.exit()
