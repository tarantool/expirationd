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
local function get_fiber_id(fiber)
    local fid = 0
    if fiber ~= nil and fiber:status() ~= "dead" then
        fid = fiber:id()
    end
    return fid
end

-- ========================================================================= --
-- Expiration daemon global variables
-- ========================================================================= --

-- main table

local expirationd = {
    -- task list
    task_list = {},
    -- constants
    constants = finalize_table({
        -- default value of number of tuples that
        -- will be checked by one iteration
        default_tuples_per_iteration = 1024,
        -- default value of time required for full
        -- index scan (in seconds)
        default_full_scan_time = 3600,
        -- maximal worker delay (seconds)
        max_delay = 1,
        -- check worker interval
        check_interval = 1,
    }),
}

-- ========================================================================= --
-- Task local functions
-- ========================================================================= --

-- ------------------------------------------------------------------------- --
-- Task fibers
-- ------------------------------------------------------------------------- --

local function process_batch(scan_space, task, tuples)
    for _, tuple in pairs(tuples) do
        -- do main work
        if task.is_tuple_expired(task.args, tuple) then
            task.expired_tuples_count = task.expired_tuples_count + 1
            task.process_expired_tuple(task.space_id, task.args, tuple)
        end
    end
end

local function do_worker_iteration(task)
    local scan_space = box.space[task.space_id]
    local params = {iterator = 'GT', limit = task.tuples_per_iteration}
    local last_id

    -- full index scan loop
    local tuples = scan_space.index[0]:select({}, params)
    while #tuples > 0 do
        last_id = tuples[#tuples][1]
        process_batch(scan_space, task, tuples)
        tuples = scan_space.index[0]:select({last_id}, params)

        -- delay if space is not empty
        if scan_space:len() > 0 then
            local delay = (task.tuples_per_iteration * task.full_scan_time) / scan_space:len()

            if delay > expirationd.constants.max_delay then
                delay = expirationd.constants.max_delay
            end
            fiber.sleep(delay)
        end
    end
end

local function worker_loop(task)
    -- detach worker from the guardian and attach it to sched fiber
    fiber.self():name(task.name)

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
    fiber.self():name(string.format("guardian of %q", task.name))

    while true do
        if get_fiber_id(task.worker_fiber) == 0 then
            -- create worker fiber
            task.worker_fiber = fiber.create(worker_loop, task)

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
    task.space_id = nil
    task.expired_tuples_count = 0
    task.restarts = 0
    task.is_tuple_expired = nil
    task.process_expired_tuple = nil
    task.args = nil
    task.tuples_per_iteration = expirationd.constants.default_tuples_per_iteration
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
    task.guardian_fiber = fiber.create(guardian_loop, task)
end

-- kill task
local function kill_task(task)
    if get_fiber_id(task.guardian_fiber) ~= 0 then
        -- kill guardian fiber
        fiber.cancel(task.guardian_fiber)
        task.guardian_fiber = nil
    end
    if get_fiber_id(task.worker_fiber) ~= 0 then
        -- kill worker fiber
        fiber.cancel(task.worker_fiber)
        task.worker_fiber = nil
    end
end


-- ========================================================================= --
-- Expiration daemon management functions
-- ========================================================================= --

--
-- Run a named task
-- params:
--    name                  -- task name
--    space_id              -- space to look in for expired tuples
--    is_tuple_expired      -- a function, must accept tuple and return
--                             true/false (is tuple expired or not),
--                             receives (args, tuple) as arguments
--    process_expired_tuple -- applied to expired tuples, receives
--                             (space_id, args, tuple) as arguments
--    args                  -- passed to is_tuple_expired and process_expired_tuple()
--                             as additional context
--    tuples_per_iteration  -- number of tuples will be checked by one iteration
--    full_scan_time        -- time required for full index scan (in seconds)
--
function expirationd.run_task(name,
                              space_id,
                              is_tuple_expired,
                              process_expired_tuple,
                              args,
                              tuples_per_iteration,
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
    if space_id == nil then
        error("space_id is nil")
    end
    task.space_id = space_id

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
    if tuples_per_iteration ~= nil then
        if tuples_per_iteration <= 0 then
            error("invalid tuples per iteration parameter")
        end
        task.tuples_per_iteration = tuples_per_iteration
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
--    name -- is task's name
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
                  task.name,
                  task.space_id,
                  task.expired_tuples_count,
                  math.floor(os.time() - task.start_time)
         )
    end
end

--
-- Print task details
-- params:
--   name -- task's name
--
function expirationd.task_details(name)
    local task = get_task(name)
    log.info("name: %s",                          task.name)
    log.info("start time: %f",                    math.floor(task.start_time))
    log.info("working time: %f",                  math.floor(os.time() - task.start_time))
    log.info("space: %s",                         task.space_id)
    log.info("is_tuple_expired handler: %s",      task.is_tuple_expired)
    log.info("process_expired_tuple handler: %s", task.process_expired_tuple)

    if task.args ~= nil then
        log.info("args: ")
        for k, v in pairs(task.args) do
            log.info("\t%s\t%s", k, v)
        end
    else
        log.info("args: nil")
    end
    log.info("tuples per iteration: %d", task.tuples_per_iteration)
    log.info("full index scan time: %f", task.full_scan_time)
    log.info("expired tuples count: %d", task.expired_tuples_count)
    log.info("restarts: %d",             task.restarts)
    log.info("guardian fid: %d",         get_fiber_id(task.guardian_fiber))
    log.info("worker fid: %d",           get_fiber_id(task.worker_fiber))
end

return expirationd
