-- ========================================================================= --
-- Tarantool/Box expiration daemon
--
-- Daemon management functions:
--   - expirationd.start  -- start a new expiration task
--   - expirationd.stats  -- show task stats
--   - expirationd.update -- update expirationd from disk
--   - expirationd.kill   -- kill a running task
--   - expirationd.task   -- get an existing task
--   - expirationd.tasks  -- get list with all tasks
-- ========================================================================= --

-- ========================================================================= --
-- local support functions
-- ========================================================================= --

local checks = require("checks")
local fun = require("fun")
local log = require("log")
local fiber = require("fiber")

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
    -- default value of number of tuples that will be checked by one iteration
    default_tuples_per_iteration = 1024,
    -- default value of time required for full index scan (in seconds)
    default_full_scan_time = 3600,
    -- maximal worker delay (seconds)
    max_delay = 1,
    -- check worker interval
    check_interval = 1,
    -- force expirationd, even if started on replica (false by default)
    force = false,
    -- assumed size of vinyl space (in the first iteration)
    default_vinyl_assumed_space_len = math.pow(10, 7),
    -- factor for recalculation of vinyl space size
    default_vinyl_assumed_space_len_factor = 2,
    -- default function on full scan
    default_on_full_scan = function() end,
    -- default function for start_key
    start_key = function() return nil end,
    -- default function for process_while
    process_while = function() return true end,
    -- default iterating over the loop will go in ascending index
    iterator_type = "ALL",
    -- default atomic_iteration is false, batch of items doesn't include in one transaction
    atomic_iteration = false,
}

-- ========================================================================= --
-- Task local functions
-- ========================================================================= --

-- ------------------------------------------------------------------------- --
-- Task fibers
-- ------------------------------------------------------------------------- --

-- get all fields in primary key(composite possible) from tuple
local function construct_key(space_id, tuple)
    return fun.map(
        function(x) return tuple[x.fieldno] end,
        box.space[space_id].index[0].parts
    ):totable()
end

-- do expiration process on tuple
local function expiration_process(task, tuple)
    task.checked_tuples_count = task.checked_tuples_count + 1
    if task.is_tuple_expired(task.args, tuple) then
        task.expired_tuples_count = task.expired_tuples_count + 1
        task.process_expired_tuple(task.space_id, task.args, tuple, task)
    end
end

-- yield for some time
local function suspend_basic(task, len)
    local delay = (task.tuples_per_iteration * task.full_scan_time)
    delay = math.min(delay / len, task.iteration_delay)
    fiber.sleep(delay)
end

local function suspend(task)
    -- Return the number of tuples in the space
    local space_len = task.index:len()
    if space_len > 0 then
        suspend_basic(task, space_len)
    end
end

local function default_do_worker_iteration(task)
    -- full index scan loop
    local space_len = task.vinyl_assumed_space_len
    local checked_tuples_count = 0
    local vinyl_checked_tuples_count = 0
    if task.atomic_iteration then
        -- Check before starting the transaction,
        -- since a transaction can be long.
        if task.worker_cancelled then
            return true
        end
        box.begin()
    end
    for _, tuple in task:iterate_with() do
        checked_tuples_count = checked_tuples_count + 1
        vinyl_checked_tuples_count = vinyl_checked_tuples_count + 1
        expiration_process(task, tuple)
        -- find out if the worker can go to sleep
        -- if the batch is full
        if checked_tuples_count >= task.tuples_per_iteration then
            if task.atomic_iteration then
                box.commit()
                -- The suspend functions can be long.
                if task.worker_cancelled then
                    return true
                end
            end
            checked_tuples_count = 0
            if box.space[task.space_id].engine == "vinyl" then
                if vinyl_checked_tuples_count > space_len then
                    space_len = task.vinyl_assumed_space_len_factor * space_len
                end
                suspend_basic(task, space_len)
            else
                suspend(task)
            end
            if task.atomic_iteration then
                -- Check before starting the transaction,
                -- since a transaction can be long.
                if task.worker_cancelled then
                    return true
                end
                box.begin()
            end
        end
    end
    if task.atomic_iteration then
        box.commit()
    end
    if box.space[task.space_id].engine == "vinyl" then
        task.vinyl_assumed_space_len = vinyl_checked_tuples_count
    end
end

local function worker_loop(task)
    -- detach worker from the guardian and attach it to sched fiber
    fiber.name(string.format("worker of %q", task.name), { truncate = true })

    while true do
        if (box.cfg.replication_source == nil and box.cfg.replication == nil) or task.force then
            task.on_full_scan_start(task)
            local state, err = pcall(task.do_worker_iteration, task)
            -- Following functions are on_full_scan*,
            -- but we probably did not complete the full scan,
            -- so we should check for cancellation here.
            if task.worker_cancelled then
                fiber.self():cancel()
            end
            if state then
                task.on_full_scan_success(task)
            else
                task.on_full_scan_error(task, err)
            end

            task.on_full_scan_complete(task)
            if not state then
                box.rollback()
                error(err)
            end
        end

        -- If we do not check the fiber for cancellation,
        -- then the fiber may fall asleep for a long time, depending on `full_scan_delay`.
        -- And a fiber that wants to stop this task can also freeze, a kind of deadlock.
        if task.worker_cancelled then
            fiber.self():cancel()
        end
        -- Full scan iteration is complete, yield
        fiber.sleep(task.full_scan_delay)
    end
end

local function guardian_loop(task)
    -- detach the guardian from the creator and attach it to sched
    fiber.name(string.format("guardian of %q", task.name), { truncate = true })

    while true do
        -- if fiber doesn't exist
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
-- * task:kill()       -- stop task and delete
-- * task:statistics() -- return table with statistics
local Task_methods = {
    start = function (self)
        self:stop()
        self.guardian_fiber = fiber.create(guardian_loop, self)
    end,
    stop = function (self)
        if (get_fiber_id(self.guardian_fiber) ~= 0) then
            self.guardian_fiber:cancel()
            while self.guardian_fiber:status() ~= "dead" do
                fiber.sleep(0.01)
            end
            self.guardian_fiber = nil
        end
        if (get_fiber_id(self.worker_fiber) ~= 0) then
            self.worker_cancelled = true
            if not self.atomic_iteration then
                self.worker_fiber:cancel()
            end
            while self.worker_fiber:status() ~= "dead" do
                fiber.sleep(0.01)
            end
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
    local task = setmetatable({
        name                  = name,
        start_time            = fiber.time(),
        guardian_fiber        = nil,
        worker_fiber          = nil,
        space_id              = nil,
        expired_tuples_count  = 0,
        checked_tuples_count  = 0,
        restarts              = 0,
        is_tuple_expired      = nil,
        process_expired_tuple = nil,
        args                  = nil,
        index                 = nil,
        iterate_with          = nil,
        worker_cancelled      = false,
        iteration_delay                = constants.max_delay,
        full_scan_delay                = constants.max_delay,
        tuples_per_iteration           = constants.default_tuples_per_iteration,
        full_scan_time                 = constants.default_full_scan_time,
        vinyl_assumed_space_len        = constants.default_vinyl_assumed_space_len,
        vinyl_assumed_space_len_factor = constants.default_vinyl_assumed_space_len_factor,
        on_full_scan_error             = constants.default_on_full_scan,
        on_full_scan_success           = constants.default_on_full_scan,
        on_full_scan_start             = constants.default_on_full_scan,
        on_full_scan_complete          = constants.default_on_full_scan,
        start_key                      = constants.start_key,
        process_while                  = constants.process_while,
        iterator_type                  = constants.iterator_type,
        atomic_iteration               = constants.atomic_iteration,
    }, { __index = Task_methods })
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
-- luacheck: ignore unused args
local function default_tuple_drop(space_id, args, tuple)
    box.space[space_id]:delete(construct_key(space_id, tuple))
end


-- default iterate_with function
local function default_iterate_with(task)
    return task.index:pairs(task.start_key(), { iterator = task.iterator_type })
       :take_while(
            function()
                return task:process_while()
            end
        )
end

-- ========================================================================= --
-- Expiration daemon management functions
-- ========================================================================= --

-- Run a scheduled task to check and process (expire) tuples in a given space.
--
-- params:
--   name             -- task name
--   space_id         -- space to look in for expired tuples
--   is_tuple_expired -- a function, must accept tuple and return
--                       true/false (is tuple expired or not),
--                       receives (args, tuple) as arguments
--   options = {      -- (table with named options)
--     * process_expired_tuple -- Applied to expired tuples, receives (space_id, args, tuple) as arguments;
--                                can be nil: by default, tuples are removed.
--     * index                 -- Name or id of the index to iterate on; if omitted, will use the primary index;
--                                if there's no index with this name, will throw an error.
--                                supported index types are TREE and HASH, using other types will result in an error.
--     * iterator_type         -- Type of the iterator to use, as string or box.index constant,
--                                for example, 'EQ' or box.index.EQ; default is box.index.ALL;
--                                see https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/pairs/.
--     * start_key             -- Start iterating from the tuple with this index value;
--                                or when iterator is 'EQ', iterate over tuples with this index value;
--                                must be a value of the same data type as the index field or fields,
--                                or a function which returns such value;
--                                if omitted or nil, all tuples will be checked.
--     * tuples_per_iteration  -- Number of tuples to check in one batch (iteration); default is 1024.
--     * atomic_iteration      -- Boolean, false (default) to process each tuple as a single transaction;
--                                true to process tuples from each batch in a single transaction.
--     * process_while         -- Function to call before checking each tuple;
--                                if it returns false, the task will stop until next full scan.
--     * iterate_with          -- Function which returns an iterator object which provides tuples to check,
--                                considering the start_key, process_while and other options.
--                                There's a default function which can be overriden with this parameter.
--     * on_full_scan_start    -- Function to call before starting a full scan iteration.
--     * on_full_scan_complete -- Function to call after completing a full scan iteration.
--     * on_full_scan_success  -- Function to call after successfully completing a full scan iteration.
--     * on_full_scan_error    -- Function to call after terminating a full scan due to an error.
--     * args                  -- Passed to is_tuple_expired and
--                                process_expired_tuple() as additional context.
--     * full_scan_time        -- Time required for a full index scan (in seconds).
--     * iteration_delay       -- Max sleep time between iterations (in seconds).
--     * full_scan_delay       -- Sleep time between full scans (in seconds).
--     * force                 -- Run task even on replica.
--  }
local function expirationd_run_task(name, space_id, is_tuple_expired, options)
    checks('string', 'number|string', 'function', {
        args = '?',
        atomic_iteration = '?boolean',
        force = '?boolean',
        full_scan_delay = '?number|cdata',
        full_scan_time = '?number|cdata',
        index = '?number|string',
        iterate_with = '?function',
        iteration_delay = '?number|cdata',
        iterator_type = '?number|string',
        on_full_scan_complete = '?function',
        on_full_scan_error = '?function',
        on_full_scan_start = '?function',
        on_full_scan_success = '?function',
        process_expired_tuple = '?function',
        process_while = '?function',
        start_key = '?',
        tuples_per_iteration = '?number|cdata',
        vinyl_assumed_space_len_factor = '?number|cdata',
        vinyl_assumed_space_len = '?number|cdata',
    })

    -- check, does the task exist
    local prev = task_list[name]
    if prev ~= nil then
        log.info("restart task %q", name)
        prev:kill(name)
    end
    local task = create_task(name)
    task.space_id = space_id
    task.is_tuple_expired = is_tuple_expired

    options = options or {}
    task.process_expired_tuple = options.process_expired_tuple or default_tuple_drop

    -- validate index
    local expire_index = box.space[space_id].index[0]
    if options.index then
        if box.space[space_id].index[options.index] == nil then
            if type(options.index) == "string" then
                error("Index with name " .. options.index .. " does not exist")
            elseif type(options.index) == "number" then
                error("Index with id " .. options.index .. " does not exist")
            else
                error("Invalid type of index, expected string or number")
            end
        end
        expire_index = box.space[space_id].index[options.index]
        if expire_index.type ~= "TREE" and expire_index.type ~= "HASH" then
            error("Not supported index type, expected TREE or HASH")
        end
    end
    task.index = expire_index

    -- check iterator_type
    if options.iterator_type ~= nil then
        task.iterator_type = options.iterator_type
    end

    -- check start_key
    if options.start_key ~= nil or options.start_key == box.NULL then
        if type(options.start_key) == "function" then
            task.start_key = function() return options.start_key() end
        else
            task.start_key = function() return options.start_key end
        end
    end

    -- check valid of iterator_type and start key
    task.index:pairs( task.start_key(), { iterator = task.iterator_type })

    -- check process_while
    if options.process_while ~= nil then
        task.process_while = options.process_while
    end

    -- check transaction option
    if options.atomic_iteration ~= nil then
        task.atomic_iteration = options.atomic_iteration
    end

    task.iterate_with = options.iterate_with or default_iterate_with

    -- check expire and process after expiration handler's arguments
    task.args = options.args

    -- check tuples per iteration (not required)
    if options.tuples_per_iteration ~= nil then
        if options.tuples_per_iteration <= 0 then
            error("Invalid tuples per iteration parameter")
        end
        task.tuples_per_iteration = options.tuples_per_iteration
    end

    -- check full scan time
    if options.full_scan_time ~= nil then
        if options.full_scan_time <= 0 then
            error("Invalid full scan time")
        end
        task.full_scan_time = options.full_scan_time
    end

    if options.force ~= nil then
        task.force = options.force
    end

    if options.vinyl_assumed_space_len ~= nil then
        task.vinyl_assumed_space_len = options.vinyl_assumed_space_len
    end

    if options.vinyl_assumed_space_len_factor ~= nil then
        task.vinyl_assumed_space_len_factor = options.vinyl_assumed_space_len_factor
    end

    task.do_worker_iteration = default_do_worker_iteration

    if options.iteration_delay ~= nil then
        task.iteration_delay = options.iteration_delay
    end

    if options.full_scan_delay ~= nil then
        task.full_scan_delay = options.full_scan_delay
    end

    if options.on_full_scan_start ~= nil then
        task.on_full_scan_start = options.on_full_scan_start
    end

    if options.on_full_scan_success ~= nil then
        task.on_full_scan_success = options.on_full_scan_success
    end

    if options.on_full_scan_complete ~= nil then
        task.on_full_scan_complete = options.on_full_scan_complete
    end

    if options.on_full_scan_error ~= nil then
        task.on_full_scan_error = options.on_full_scan_error
    end

    -- put the task to table
    task_list[name] = task
    -- run
    task:start()

    return task
end

local function run_task_obsolete(name,
                              space_id,
                              is_tuple_expired,
                              process_expired_tuple,
                              args,
                              tuples_per_iteration,
                              full_scan_time)
    log.info("expirationd.run_task() is obsolete, please consider a switching to expirationd.start()")
    return expirationd_run_task(
        name, space_id, is_tuple_expired, {
            process_expired_tuple = process_expired_tuple,
            args = args,
            full_scan_time = full_scan_time,
            tuples_per_iteration = tuples_per_iteration,
            force = false,
        }
    )
end

-- Kill named task
-- params:
--    name -- is task's name
local function expirationd_kill_task(name)
    checks('string')

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
    checks('?string')

    if name ~= nil then
        return get_task(name):statistics()
    end
    local retval = {}
    for task_name, task in pairs(task_list) do
        retval[task_name] = task:statistics()
    end
    return retval
end



-- get task by name
local function expirationd_get_task(name)
    checks('string')

    return get_task(name)
end

-- Update expirationd version in running tarantool
-- * remove expirationd from package.loaded
-- * require new expirationd
-- * restart all tasks
local function expirationd_update()
    local expd_prev = require("expirationd")
    table.clear(expd_prev)
    setmetatable(expd_prev, {
        __index = function()
            error("Wait until update is done before using expirationd", 2)
        end
    })
    package.loaded["expirationd"] = nil
    local expd_new  = require("expirationd")
    local tmp_task_list = task_list; task_list = {}
    for _, task in pairs(tmp_task_list) do
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
    -- update old function table to represent new reloaded expirationd
    -- some kind of dirty hack if user forgot to require new expirationd
    setmetatable(expd_prev, nil)
    for name, func in pairs(expd_new) do
        expd_prev[name] = func
    end
end

local function task_stats_obsolete(...)
    log.info("expirationd.task_stats() is obsolete, please consider a switching to expirationd.stats()")
    return expirationd_task_stats(...)
end

local function kill_task_obsolete(...)
    log.info("expirationd.kill_task() is obsolete, please consider a switching to expirationd.kill()")
    return expirationd_kill_task(...)
end

local function get_task_obsolete(...)
    log.info("expirationd.get_task() is obsolete, please consider a switching to expirationd.task()")
    return expirationd_get_task(...)
end

local function get_tasks_obsolete(...)
    log.info("expirationd.get_tasks() is obsolete, please consider a switching to expirationd.tasks()")
    return expirationd_get_task(...)
end

local function show_task_list_obsolete(...)
    log.info("expirationd.show_task_list() is obsolete, please consider a switching to expirationd.tasks()")
    return expirationd_get_task(...)
end

return {
    start   = expirationd_run_task,
    stats   = expirationd_task_stats,
    update  = expirationd_update,
    kill    = expirationd_kill_task,
    task    = expirationd_get_task,
    tasks   = expirationd_show_task_list,
    -- Obsolete function names, use previous, instead
    task_stats     = task_stats_obsolete,
    kill_task      = kill_task_obsolete,
    get_task       = get_task_obsolete,
    get_tasks      = get_tasks_obsolete,
    run_task       = run_task_obsolete,
    show_task_list = show_task_list_obsolete,
}
