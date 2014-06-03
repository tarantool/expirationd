-- ========================================================================= --
-- Tarantool/Box expiration daemon
--
-- Daemon management functions:
--   - expirationd.run_task       -- run a new expiration task
--   - expirationd.kill_task      -- kill a running task
--   - expirationd.show_task_list -- print the task list
--   - expirationd.task_details   -- show task details
-- ========================================================================= --

-- ========================================================================= --
-- local support functions
-- ========================================================================= --

local log = require('log')
local fiber = require('fiber')

-- Create a new table with constant members. A runtime error
-- is raised on attempt to change a table field.
local function finalize_table(table)
    local str = "attempt to change constant %q to %q"
    return setmetatable ({}, {
            __index = table,
            __newindex = function(table_arg,
                                name_arg,
                                value_arg)
                error(str:format(name_arg, value_arg), 2)
            end
        })
end

-- get fiber id function
local function get_fid(fiber)
    local fid = 0
    if fiber ~= nil then
        fid = fiber:id()
    end
    return fid
end

-- get field
local function get_field(tuple, field_no)
    if tuple == nil then
        return nil
    end

    if #tuple <= field_no then
        return nil
    end

    return tuple[field_no]
end

-- ========================================================================= --
-- Expriration daemon global variables
-- ========================================================================= --

-- main table

local expirationd = {
    -- enable/disable debug functions
    _debug = false,
    -- task list
    task_list = {},
    -- constants
    constants = finalize_table({
        -- default value of number of tuples will be checked by one iteration
        default_tuples_per_iter = 1024,
        -- default value of time required for full index scan (in seconds)
        default_full_scan_time = 3600,
        -- maximal worker delay (seconds)
        max_delay = 1,
        -- check worker intarval
        check_interval = 1,
    }),
}


-- ========================================================================= --
-- Task local functions
-- ========================================================================= --

-- ------------------------------------------------------------------------- --
-- Task fibers
-- ------------------------------------------------------------------------- --

local function do_worker_iteration(task)
    local scan_space = box.space[task.space_no]

    -- full index scan loop
    local checks = 0
    for _, tuple in scan_space.index[0]:pairs(nil, {iterator = box.index.ALL}) do
        checks = checks + 1

        -- do main work
        if task.is_tuple_expired(task.args, tuple) then
            task.tuples_expired = task.tuples_expired + 1
            task.process_expired_tuple(task.space_no, task.args, tuple)
        end

        -- find out if the worker can go to sleep
        if checks >= task.tuples_per_iter then
            checks = 0
            if scan_space:len() > 0 then
                local delay = (task.tuples_per_iter * task.full_scan_time) / scan_space:len()

                if delay > expirationd.constants.max_delay then
                    delay = expirationd.constants.max_delay
                end
                fiber.sleep(delay)
            end
        end
    end
end

local function worker_loop(task)
    -- detach worker from the guardian and attach it to sched fiber
    fiber.detach()
    fiber.name(task.name)

    while true do
        if box.cfg.replication_source == nil then
            do_worker_iteration(task)
        end

        -- iteration is complete, yield
        fiber.sleep(expirationd.constants.max_delay)
    end
end

local function guardian_loop(task)
    -- detach the guardian from the creator and attach it to sched
    local str = "guardian of %q"
    fiber.detach()
    fiber.name(str:format(task.name))

    while true do
        if get_fid(task.worker_fiber) == 0 then
            -- create worker fiber
            task.worker_fiber = fiber.create(worker_loop)
            task.worker_fiber:resume(task)

            log.info("expiration: task %q restarted", task.name)
            task.restarts = task.restarts + 1
        end
        fiber.sleep(expirationd.constants.check_interval)
    end
end


-- ------------------------------------------------------------------------- --
-- Task managemet
-- ------------------------------------------------------------------------- --

-- create new expiration task
local function create_task(name)
    local task = {}
    task.name = name
    task.start_time = os.time()
    task.guardian_fiber = nil
    task.worker_fiber = nil
    task.space_no = nil
    task.tuples_expired = 0
    task.restarts = 0
    task.is_tuple_expired = nil
    task.process_expired_tuple = nil
    task.args = nil
    task.tuples_per_iter = expirationd.constants.default_tuples_per_iter
    task.full_scan_time = expirationd.constants.default_full_scan_time
    return task
end

-- get task for table
local function get_task(name)
    if name == nil then
        error("task name is nil")
    end

    -- check, does the task exist
    if expirationd.task_list[name] == nil then
        error("task '" .. name .. "' doesn't exist")
    end

    return expirationd.task_list[name]
end

-- run task
local function run_task(task)
    -- start guardian task
    task.guardian_fiber = fiber.create(guardian_loop)
    fiber.resume(task.guardian_fiber, task)
end

-- kill task
local function kill_task(task)
    if get_fid(task.guardian_fiber) ~= 0 then
        -- kill guardian fiber
        fiber.cancel(task.guardian_fiber)
        task.guardian_fiber = nil
    end
    if get_fid(task.worker_fiber) ~= 0 then
        -- kill worker fiber
        fiber.cancel(task.worker_fiber)
        task.worker_fiber = nil
    end
end


-- ========================================================================= --
-- Expriration daemon management functions
-- ========================================================================= --

--
-- Run a named task
-- params:
--    name             -- task name
--    space_no         -- space to look in for expired tuples
--    is_tuple_expired -- a function, must accept tuple and return
--                        true/false (is tuple expired or not),
--                        receives (args, tuple) as arguments
--    process_expired_tuple -- applied to expired tuples, receives
--                        (space_no, args, tuple) as arguments
--    args             -- passed to is_tuple_expired and process_expired_tuple()
--                        as additional context
--    tuples_per_iter  -- number of tuples will be checked by one itaration
--    full_scan_time   -- time required for full index scan (in seconds)
--
function expirationd.run_task(name,
                              space_no,
                              is_tuple_expired,
                              process_expired_tuple,
                              args,
                              tuples_per_iter,
                              full_scan_time)
    if name == nil then
        error("task name is nil")
    end

    -- check, does the task exist
    if expirationd.task_list[name] ~= nil then
        log.info("restart task %q", name)

        expirationd.kill_task(name)
    end
    local task = create_task(name)

    -- required params

    -- check expiration space number (required)
    if space_no == nil then
        error("space_no is nil")
    end
    task.space_no = space_no

    if is_tuple_expired == nil then
        error("is_tuple_expired is nil, please provide a check function")
    elseif type(is_tuple_expired) ~= "function" then
        error("is_tuple_expired is not a function, please provide a check function")
    end
    task.is_tuple_expired = is_tuple_expired

    -- process expired tuple handler
    if process_expired_tuple == nil then
        error("process_expired_tuple is nil, please provide a purge function")
    elseif type(process_expired_tuple) ~= "function" then
        error("process_expired_tuple is not defined, please provide a purge function")
    end
    task.process_expired_tuple = process_expired_tuple

    -- optional params

    -- check expire and process after expiration handler's arguments
    task.args = args

    -- check tuples per iteration (not required)
    if tuples_per_iter ~= nil then
        if tuples_per_iter <= 0 then
            error("invalid tuples per iteration parameter")
        end
        task.tuples_per_iter = tuples_per_iter
    end

    -- check full scan time
    if full_scan_time ~= nil then
        if full_scan_time <= 0 then
            error("invalid full scan time")
        end
        task.full_scan_time = full_scan_time
    end

    --
    -- run task
    --

    -- put the task to table
    expirationd.task_list[name] = task
    -- run
    run_task(task)
end

--
-- Kill named task
-- params:
--    name -- is taks's name
--
function expirationd.kill_task(name)
    kill_task(get_task(name))
    expirationd.task_list[name] = nil
end

--
-- Print task list in TSV table format
-- params:
--   print_head -- print table head
--
function expirationd.show_task_list(print_head)
    if print_head == nil or print_head == true then
        log.info('name\tspace\texpired\ttime')
        log.info('-----------------------------------')
    end
    for i, task in pairs(expirationd.task_list) do
            log.info("%q\t%s\t%s\t%f",
                     task.name, task.space_no, tuples_expired,
                     math.floor(os.time() - task.start_time))
    end
end

--
-- Print task details
-- params:
--   name -- task's name
--
function expirationd.task_details(name)
    local task = get_task(name)
    log.info("name: %s", task.name)
    log.info("start time: %f", math.floor(task.start_time))
    log.info("working time: %f", math.floor(os.time() - task.start_time))
    log.info("space: %s", task.space_no)
    log.info("is_tuple_expired handler: %s", task.is_tuple_expired)
    log.info("process_expired_tuple handler: %s", task.process_expired_tuple)

    if task.args ~= nil then
        log.info("args: ")
        for k, v in pairs(task.args) do
            log.info("\t%s\t%s", k, v)
        end
    else
        log.info("args: nil")
    end
    log.info("tuples per iteration: %d", task.tuples_per_iter)
    log.info("full index scan time: %f", task.full_scan_time)
    log.info("tuples expired: %d", task.tuples_expired)
    log.info("restarts: %d", task.restarts)
    log.info("guardian fid: %d", get_fid(task.guardian_fiber))
    log.info("worker fid: %d", get_fid(task.worker_fiber))
end


-- ========================================================================= --
-- Expriratiuons handlers examples
-- ========================================================================= --

-- check tuple's expiration by field witch containing
local function check_tuple_expire_by_timestamp(args, tuple)
    local tuple_expire_time = get_field(tuple, args.field_no)
    if type(tuple_expire_time) ~= 'number' then
        return true
    end

    local current_time = os.time()
    return current_time >= tuple_expire_time
end

-- put expired tuple to cemetery
local function put_tuple_to_cemetery(space_no, args, tuple)
    -- delete expired tuple
    box.space[space_no]:delete{tuple[0]}
    local email = get_field(tuple, 1)
    if args.cemetery_space_no ~= nil and email ~= nil then
        box.space[args.cemetery_space_no]:replace{email, os.time()}
    end
end

-- ========================================================================= --
-- Expriration module test functions
-- ========================================================================= --
-- Warning: for these test functions to work, you need
-- a space with a numeric primary key defined on field[0]

-- generate email string
local function get_email(uid)
    local email = "test_" .. uid .. "@sex.com"
    return email
end
-- insert entry to space
local function add_entry(space_no, uid, email, expiration_time)
    box.space[space_no]:replace{uid, email, expiration_time}
end

-- put test tuples
function expirationd.put_test_tuples(space_no, total)
    if not expirationd._debug then
        error("expiration daemon module's debug disabled")
    end

    local time = math.floor(os.time())
    for i = 0, total do
        add_entry(space_no, i, get_email(i), time + i)
    end

    -- tuple w/o expiration date
    uid = total + 1
    add_entry(space_no, uid, get_email(uid), "")

    -- tuple w/ invalid expiration date
    uid = total + 2
    add_entry(space_no, uid, get_email(uid), "some string in exp field")
end

-- print test tuples
function expirationd.print_test_tuples(space_no)
    if not expirationd._debug then
        error("expiration daemon module's debug disabled")
    end

    for state, tuple in box.space[space_no].index[0]:pairs(nil, {iterator = box.index.ALL}) do
        print(tuple)
    end
end

-- do test
function expirationd.do_test(space_no, cemetery_space_no)
    if not expirationd._debug then
        error("expiration daemon module's debug disabled")
    end

    -- put test tuples
    print("-------------- put ------------")
    print("put to space #"..space_no)
    expirationd.put_test_tuples(space_no, 10)

    -- print before
    print("\n------------- print -----------")
    print("before print space #"..space_no, "\n")
    expirationd.print_test_tuples(space_no)
    print("\nbefore print cemetery space #"..cemetery_space_no, "\n")
    expirationd.print_test_tuples(cemetery_space_no)

    print("-------------- run ------------")
    expirationd.run_task("test",
                   space_no,
                   check_tuple_expire_by_timestamp,
                   put_tuple_to_cemetery,
                   {
                       field_no = 2,
                       cemetery_space_no = cemetery_space_no
                   })

    -- wait expiration
    print("------------- wait ------------")
    print("before time = ", os.time())
    fiber.sleep(5)
    print("after time = ", os.time())

    -- print after
    print("\n------------- print -----------")
    print("After print space #"..space_no, "\n")
    expirationd.print_test_tuples(space_no)
    print("\nafter print cemetery space #"..cemetery_space_no, "\n")
    expirationd.print_test_tuples(cemetery_space_no)
    
    expirationd.show_task_list(true)
    log.info('')
    expirationd.task_details("test")
    
    return true
end

return expirationd
