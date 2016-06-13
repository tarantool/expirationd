-- ========================================================================= --
-- Tarantool/Box expiration daemon
--
-- Daemon management functions:
--   - expirationd.start    -- start a new expiration task
--   - expirationd.stats    -- show task stats
--   - expirationd.update   -- update expirationd from disk
--   - expirationd.kill     -- kill a running task
--   - expirationd.task     -- get an existing task
--   - expirationd.tasks    -- get list with all tasks
-- ========================================================================= --

-- ========================================================================= --
-- local support functions
-- ========================================================================= --

local log = require('log')
local fiber = require('fiber')
local fun = require('fun')

-- get fiber id function
local function get_fiber_id(fiber)
    local fid = 0
    if fiber ~= nil and fiber:status() ~= "dead" then
        fid = fiber:id()
    end
    return fid
end

local task_list = {}

local constants = {
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
    -- force expirationd, even if started on replica (false by default)
    force = false
}

-- ========================================================================= --
-- Expiration daemon global variables
-- ========================================================================= --

-- main table

-- ========================================================================= --
-- Task local functions
-- ========================================================================= --

-- ------------------------------------------------------------------------- --
-- Task fibers
-- ------------------------------------------------------------------------- --

local function construct_key(space_id, tuple)
    return fun.map(
        function(x) return tuple[x.fieldno] end,
        box.space[space_id].index[0].parts
    ):totable()
end

local function expiration_process(task, tuple)
    task.checked_tuples_count = task.checked_tuples_count + 1
    if task.is_tuple_expired(task.args, tuple) then
        task.expired_tuples_count = task.expired_tuples_count + 1
        task.process_expired_tuple(task.space_id, task.args, tuple)
    end
end

local function suspend(scan_space, task)
    if scan_space:len() > 0 then
        local delay = (task.tuples_per_iteration * task.full_scan_time)
        delay = delay / scan_space:len()
        if delay > constants.max_delay then
            delay = constants.max_delay
        end
        fiber.sleep(delay)
    end
end

local function tree_index_iter(scan_space, task)
    -- iteration with GT iterator
    local params = {iterator = 'GT', limit = task.tuples_per_iteration}
    local last_id
    local tuples = scan_space.index[0]:select({}, params)
    while #tuples > 0 do
        last_id = tuples[#tuples]
        for _, tuple in pairs(tuples) do
            expiration_process(task, tuple)
        end
        local key = construct_key(scan_space.id, last_id)
        tuples = scan_space.index[0]:select(key, params)
        suspend(scan_space, task)
    end
end

local function hash_index_iter(scan_space, task)
    -- iteration for hash index
    local checked_tuples_count = 0
    for _, tuple in scan_space.index[0]:pairs(nil, {iterator = box.index.ALL}) do
        checked_tuples_count = checked_tuples_count + 1
        expiration_process(task, tuple)
        -- find out if the worker can go to sleep
        if checked_tuples_count >= task.tuples_per_iteration then
            checked_tuples_count = 0
            suspend(scan_space, task)
        end
    end
end

local function do_worker_iteration(task)
    local scan_space = box.space[task.space_id]
    local index_type = scan_space.index[0].type

    -- full index scan loop
    if index_type == 'HASH' then
        hash_index_iter(scan_space, task)
    else
        tree_index_iter(scan_space, task)
    end
end

local function worker_loop(task)
    -- detach worker from the guardian and attach it to sched fiber
    fiber.self():name(task.name)

    while true do
        if box.cfg.replication_source == nil or task.force then
            do_worker_iteration(task)
        end

        -- iteration is complete, yield
        fiber.sleep(constants.max_delay)
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
        fiber.sleep(constants.check_interval)
    end
end

-- ------------------------------------------------------------------------- --
-- Task management
-- ------------------------------------------------------------------------- --

-- Task methods:
-- * task:start()      -- start task
-- * task:stop()       -- stop task
-- * task:restart()    -- restart task
-- * task:kill()       -- delete task and restart
-- * task:statistics() -- return table with statistics
local Task_methods = {
    start = function (self)
--        if (get_fiber_id(self.worker_loop) ~= 0) then
--            self.worker_loop:cancel()
--            self.guardian_fiber = nil
--        end
        self.guardian_fiber = fiber.create(guardian_loop, self)
    end,
    stop = function (self)
        if (get_fiber_id(self.guardian_fiber) ~= 0) then
            self.guardian_fiber:cancel()
            self.guardian_fiber = nil
        end
        if (get_fiber_id(self.worker_fiber) ~= 0) then
            self.worker_fiber:cancel()
            self.worker_fiber = nil
        end
    end,
    restart = function (self)
        self:stop()
        self:start()
    end,
    kill = function (self)
        self:stop()
        task_list[self.name] = nil
    end,
    statistics = function (self)
        return {
            checked_count = self.checked_tuples_count,
            expired_count = self.expired_tuples_count,
            restarts      = self.restarts,
            working_time  = math.floor(fiber.time() - self.start_time),
        }
    end,
}

-- create new expiration task
local function create_task(name)
    local task = setmetatable({}, { __index = Task_methods })
    task.name = name
    task.start_time = fiber.time()
    task.guardian_fiber = nil
    task.worker_fiber = nil
    task.space_id = nil
    task.expired_tuples_count = 0
    task.checked_tuples_count = 0
    task.restarts = 0
    task.is_tuple_expired = nil
    task.process_expired_tuple = nil
    task.args = nil
    task.tuples_per_iteration = constants.default_tuples_per_iteration
    task.full_scan_time = constants.default_full_scan_time
    return task
end

-- get task for table
local function get_task(name)
    if name == nil then
        error("task name is nil")
    end

    -- check, does the task exist
    if task_list[name] == nil then
        error("task '" .. name .. "' doesn't exist")
    end

    return task_list[name]
end

-- default process_expired_tuple function
local function default_tuple_drop(space_id, args, tuple)
    box.space[space_id]:delete(construct_key(space_id, tuple))
end


-- ========================================================================= --
-- Expiration daemon management functions
-- ========================================================================= --

-- Run a named task
-- params:
--   name             -- task name
--   space_id         -- space to look in for expired tuples
--   is_tuple_expired -- a function, must accept tuple and return
--                       true/false (is tuple expired or not),
--                       receives (args, tuple) as arguments
--   options = {      -- (table with named options)
--     * process_expired_tuple -- applied to expired tuples, receives
--                                (space_id, args, tuple) as arguments
--     * args                  -- passed to is_tuple_expired and
--                                process_expired_tuple() as additional context
--     * tuples_per_iteration  -- number of tuples will be checked by one iteration
--     * full_scan_time        -- time required for full index scan (in seconds)
--     * force                 -- run task even on replica
--  }
local function expirationd_run_task(name, space_id, is_tuple_expired, options)
    if name == nil then
        error("task name is nil")
    end

    -- check, does the task exist
    local prev = task_list[name]
    if prev ~= nil then
        log.info("restart task %q", name)
        prev:kill(name)
    end
    local task = create_task(name)

    -- required params

    -- check expiration space number (required)
    if space_id == nil then
        error("space_id is nil")
    end
    task.space_id = space_id

    if is_tuple_expired == nil or type(is_tuple_expired) ~= "function" then
        error("is_tuple_expired is not a function, please provide a check function")
    end
    task.is_tuple_expired = is_tuple_expired

    -- optional params
    if options ~= nil and type(options) ~= 'table' then
        error("options must be table or not defined")
    end
    options = options or {}

    -- process expired tuple handler
    if options.process_expired_tuple and
            type(options.process_expired_tuple) ~= "function" then
        error("process_expired_tuple is not defined, please provide a purge function")
    end
    task.process_expired_tuple = options.process_expired_tuple or default_tuple_drop

    -- check expire and process after expiration handler's arguments
    task.args = options.args

    -- check tuples per iteration (not required)
    if options.tuples_per_iteration ~= nil then
        if options.tuples_per_iteration <= 0 then
            error("invalid tuples per iteration parameter")
        end
        task.tuples_per_iteration = options.tuples_per_iteration
    end

    -- check full scan time
    if options.full_scan_time ~= nil then
        if options.full_scan_time <= 0 then
            error("invalid full scan time")
        end
        task.full_scan_time = options.full_scan_time
    end

    if options.force ~= nil then
        if type(options.force) ~= 'boolean' then
            error("Invalid type of force value")
        end
        task.force = options.force
    end

    --
    -- run task
    --

    -- put the task to table
    task_list[name] = task
    -- run
    task:start()

    return task
end

local function expirationd_run_task_obsolete(name,
                              space_id,
                              is_tuple_expired,
                              process_expired_tuple,
                              args,
                              tuples_per_iteration,
                              full_scan_time)
    return expirationd_run_task(
        name, space_id, is_tuple_expired, {
            process_expired_tuple = process_expired_tuple,
            args = args, full_scan_time = full_scan_time,
            tuples_per_iteration = tuples_per_iteration,
            force = false,
        }
    )
end

-- Kill named task
-- params:
--    name -- is task's name
local function expirationd_kill_task(name)
    return get_task(name):kill()
end

-- Return copy of task list
local function expirationd_show_task_list()
    return fun.map(function(x) return x end, fun.iter(task_list)):totable()
end

-- Return task statistics in table
-- * checked_count - count of checked tuples (expired + skipped)
-- * expired_count - count of expired tuples
-- * restarts      - count of task restarts
-- * working_time  - task operation time
-- params:
--   name -- task's name
local function expirationd_task_stats(name)
    if name ~= nil then
        return get_task(name):statistics()
    end
    local retval = {}
    for name, task in pairs(task_list) do
        retval[name] = task:statistics()
    end
    return retval
end

-- kill task
local function expirationd_kill_task(name)
    return get_task(name):kill()
end

-- get task by name
local function expirationd_get_task(name)
    return get_task(name)
end

-- Update expirationd version in running tarantool
-- * remove expirationd from package.loaded
-- * require new expirationd
-- * restart all tasks
local function expirationd_update()
    local expd_prev = require('expirationd')
    package.loaded['expirationd'] = nil
    local expd_new  = require('expirationd')
    for name, task in pairs(task_list) do
        task:kill()
        expd_new.start(
            task.name, task.space_id,
            task.is_tuple_expired, {
                process_expired_tuple = task.process_expired_tuple,
                args = task.args, tuples_per_iteration = task.tuples_per_iteration,
                full_scan_time = task.full_scan_time, force = task.force
            }
        )
    end
end

return {
    start   = expirationd_run_task,
    stats   = expirationd_task_stats,
    update  = expirationd_update,
    kill    = expirationd_kill_task,
    task    = expirationd_get_task,
    tasks   = expirationd_show_task_list,
    -- Obsolete function names, use previous, instead
    task_stats     = expirationd_task_stats,
    kill_task      = expirationd_kill_task,
    get_task       = expirationd_get_task,
    get_tasks      = expirationd_show_task_list,
    run_task       = expirationd_run_task_obsolete,
    show_task_list = expirationd_show_task_list,
}
