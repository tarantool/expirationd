#!/usr/bin/env tarantool

local expirationd = require('expirationd')
local log = require('log')

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

-- do test
local function do_test(space_id, archive_space_id)
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
    expirationd.run_task("test",
                   space_id,
                   check_tuple_expire_by_timestamp,
                   put_tuple_to_archive,
                   {
                       field_no = 2,
                       archive_space_id = archive_space_id
                   })

    -- wait expiration
    print("------------- wait ------------")
    print("before time = ", os.date('%X', os.time()))
    require('fiber').sleep(5)
    print("after time = ", os.date('%X', os.time()))

    -- print after
    print("\n------------- print -----------")
    print("After print space " .. prefix_space_id(space_id), "\n")
    print_test_tuples(space_id)
    print("\nafter print archive space " .. prefix_space_id(archive_space_id), "\n")
    print_test_tuples(archive_space_id)

    expirationd.show_task_list(true)
    log.info('')
    expirationd.task_details("test")
    return true
end

-- do test
local function do_test_error(space_id, archive_space_id)
    print("-------------- erroneous run ------------")
    expirationd.run_task("test",
                   space_id,
                   check_tuple_expire_by_timestamp_error,
                   put_tuple_to_archive,
                   {
                       field_no = 2,
                       archive_space_id = archive_space_id
                   })
    expirationd.run_task("test",
                   space_id,
                   check_tuple_expire_by_timestamp,
                   put_tuple_to_archive_error,
                   {
                       field_no = 2,
                       archive_space_id = archive_space_id
                   })
    return true
end

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

do_test('origin', 'cemetery')
do_test_error('origin', 'cemetery')
os.exit()
