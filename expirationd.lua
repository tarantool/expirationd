--- expirationd - data expiration with custom quirks.
--
-- @module expirationd

-- ========================================================================= --
-- local support functions
-- ========================================================================= --

local checks = require("checks")
local fun = require("fun")
local log = require("log")
local fiber = require("fiber")
local is_metrics_package, metrics = pcall(require, "metrics")
local is_hotreload_package, hotreload = pcall(require, "cartridge.hotreload")

-- get fiber id function
local function get_fiber_id(fiber)
    local fid = 0
    if fiber ~= nil and fiber:status() ~= "dead" then
        fid = fiber:id()
    end
    return fid
end

local stash_names = {
    cfg = '__expirationd_cfg',
    metrics_stats = '__expirationd_metrics_stats',
    task_continue = '__expirationd_task_continue',
}

if is_hotreload_package then
    for _, name in pairs(stash_names) do
        hotreload.whitelist_globals({ name })
    end
end

-- get a stash instance, initialize if needed
local function stash_get(name)
    local instance = rawget(_G, name) or {}
    rawset(_G, name, instance)
    return instance
end

local task_list = {}
local task_continue = stash_get(stash_names.task_continue)
local cfg = stash_get(stash_names.cfg)
if cfg.metrics == nil then
    cfg.metrics = true
end
local metrics_stats = stash_get(stash_names.metrics_stats)

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

local function is_metrics_v_0_11_installed()
    if not is_metrics_package or metrics.unregister_callback == nil then
        return false
    end
    local counter = require('metrics.collectors.counter')
    return counter.remove and true or false
end

local function metrics_enable()
    if not is_metrics_v_0_11_installed() then
        error("metrics >= 0.11.0 is required", 3)
    end

    -- Workaround for a cartridge role reload:
    --
    -- Metrics package does not lose observation after a cartridge reload since
    -- 0.13.0. expirationd does not automatically restart tasks after the
    -- cartridge reload. So, we are acting here as if all expirationd tasks
    -- have been killed: reset all collectors and the callback.
    if metrics_stats.callback then
        metrics.unregister_callback(metrics_stats.callback)
    end
    metrics_stats.callback = nil
    if metrics_stats.collectors then
        for _, c in pairs(metrics_stats.collectors) do
            metrics.registry:unregister(c.collector)
        end
    end
    metrics_stats.collectors = nil

    local create_collector = function(name, description)
        return {
            collector = metrics.counter(name, description),
            task_value = {},
        }
    end

    metrics_stats.collectors = {
        ["checked_count"] = create_collector(
            "expirationd_checked_count",
            "expirationd task's a number of checked tuples"
        ),
        ["expired_count"] = create_collector(
            "expirationd_expired_count",
            "expirationd task's a number of expired tuples"
        ),
        ["restarts"] = create_collector(
            "expirationd_restarts",
            "expirationd task's a number of restarts"
        ),
        ["working_time"] = create_collector(
            "expirationd_working_time",
            "expirationd task's operation time"
        ),
    }

    local callback = function()
        for task_name, task in pairs(task_list) do
            local stats = task:statistics()
            for k, v in pairs(stats) do
                local prev_v = metrics_stats.collectors[k].task_value[task_name] or 0
                local v_inc = v - prev_v
                metrics_stats.collectors[k].collector:inc(v_inc, {name = task_name})
                metrics_stats.collectors[k].task_value[task_name] = v
            end
        end
    end
    metrics.register_callback(callback)
    metrics_stats.callback = callback
end

local function metrics_disable()
    for _, c in pairs(metrics_stats.collectors) do
        metrics.registry:unregister(c.collector)
    end
    metrics_stats.collectors = nil
    metrics.unregister_callback(metrics_stats.callback)
    metrics_stats.callback = nil
end

if cfg.metrics then
    local enabled, _ = pcall(metrics_enable)
    cfg.metrics = enabled
end

-- ========================================================================= --
-- Task local functions
-- ========================================================================= --

-- ------------------------------------------------------------------------- --
-- Task fibers
-- ------------------------------------------------------------------------- --

-- get all fields in key(composite possible) from a tuple
local function construct_key(space_id, index_id, tuple)
    return fun.map(
        function(x) return tuple[x.fieldno] end,
        box.space[space_id].index[index_id].parts
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

    -- https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-read_only
    local space = box.space[task.space_id]
    local is_ro_writable = space.temporary or space.is_local
    local is_skipped = false
    while true do
        local box_info = box.info
        if not box_info.ro or is_ro_writable or task.force then
            if is_skipped then
                -- Wait for maximum upstream lag * 2 if we became a master.
                -- It helps to don't hit a conflict due to parallel processing
                -- of remaining transactions from the old master and from the
                -- new master.
                local max_lag = 0
                for i = 1, table.maxn(box_info.replication) do
                    local r = box_info.replication[i]
                    if r and r.upstream and max_lag < r.upstream.lag then
                        max_lag = r.upstream.lag
                    end
                end
                if max_lag > 0 then
                    fiber.sleep(max_lag * 2)
                end
            end
            is_skipped = false

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
        else
            is_skipped = true
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

-- {{{ Task instance methods

--- Task instance methods.
--
-- NOTE: task object contains a number of properties that available for users.
-- However these properties are not a part of expirationd API. Property name
-- can be changed or property itself can be removed in future version. Be
-- careful!
--
-- @section Methods
--
local Task_methods = {
    --- Start a task. It continues processing from a last tuple if the task
    --  was previously stopped by @{task.stop} and the task has default
    --  `start_key` and `iterate_with` functions.
    --
    -- @param  self
    --     Task instance.
    --
    -- @return None
    --
    -- @function task.start
    start = function (self)
        self:stop()
        self.guardian_fiber = fiber.create(guardian_loop, self)
    end,

    --- Stop a task.
    --
    -- @param  self
    --     Task instance.
    --
    -- @return None
    --
    -- @function task.stop
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

    --- Restart a task.
    --
    -- @param  self
    --     Task instance.
    --
    -- @return None
    --
    -- @function task.restart
    restart = function (self)
        self:stop()
        task_continue[self.name] = nil
        self:start()
    end,

    --- Kill a task.
    --
    -- Stop a task and delete it from list of tasks.
    --
    -- @param  self
    --     Task instance.
    --
    -- @return None
    --
    -- @function task.kill
    kill = function (self)
        self:stop()
        if metrics_stats.collectors then
            for _, c in pairs(metrics_stats.collectors) do
                c.collector:remove({name = self.name})
                c.task_value[self.name] = nil
            end
        end
        task_list[self.name] = nil
        task_continue[self.name] = nil
    end,

    --- Get a statistics about a task.
    --
    -- @param  self
    --     Task instance.
    --
    -- @return Response of the following structure:
    --
    -- ```
    -- {
    --     checked_count = number,
    --     expired_count = number,
    --     restarts = number,
    --     working_time = number,
    -- }
    -- ```
    --
    -- where:
    --
    -- `checked_count` is a number of tuples checked for expiration (expired + skipped).
    --
    -- `expired_count` is a number of expired tuples.
    --
    -- `restarts` is a number of restarts since start. From the start `restarts` is equal to 1.
    --
    -- `working_time` is a task's operation time.
    --
    -- @function task.statistics
    statistics = function (self)
        return {
            checked_count = self.checked_tuples_count,
            expired_count = self.expired_tuples_count,
            restarts      = self.restarts,
            working_time  = math.floor(fiber.time() - self.start_time),
        }
    end,
}

-- }}} Task instance methods

--- create new expiration task
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
    box.space[space_id]:delete(construct_key(space_id, 0, tuple))
end

local function create_continue_key(tuple, old_parts, task)
    if tuple == nil or #old_parts ~= #task.index.parts then
        return nil
    end

    for i, part in ipairs(old_parts) do
        for k, v in pairs(part) do
            if task.index.parts[i][k] == nil or task.index.parts[i][k] ~= v then
                return nil
            end
        end
    end

    return construct_key(task.space_id, task.index.id, tuple)
end

local continue_iterators_map = {}
for _, ge in ipairs({box.index.ALL, box.index.GE, box.index.GT, box.index.EQ,
                     "ALL", "GE", "GT", "EQ"}) do
    -- default start_key() == nil, so it's ok that EQ -> GE because the
    -- continue logic does not work with non-default start_key callback
    continue_iterators_map[ge] = box.index.GE
end
for _, le in ipairs({box.index.LE, box.index.LT, box.index.REQ,
                     "LE", "LT", "REQ"}) do
    continue_iterators_map[le] = box.index.LE
end

local function create_continue_state(task)
    if task.start_key ~= constants.start_key then
        task_continue[task.name] = nil
        return nil, nil
    end

    local key = nil
    local it = nil
    local c = task_continue[task.name]
    if c and c.space_id == task.space_id and c.index_id == task.index.id and c.it == task.iterator_type then
        key = create_continue_key(c.tuple, c.index_parts, task)
        it = continue_iterators_map[task.iterator_type]
    end

    task_continue[task.name] = {
        space_id = task.space_id,
        index_id = task.index.id,
        index_parts = table.deepcopy(task.index.parts),
        it = task.iterator_type,
        tuple = nil,
    }

    if key == nil or it == nil then
        return nil, nil
    end
    return key, it
end

-- default iterate_with function
local function default_iterate_with(task)
    local continue_key, continue_it = create_continue_state(task)
    local iter, param, state = task.index:pairs(continue_key or task.start_key(),
                                                { iterator = continue_it or task.iterator_type })
       :take_while(
            function()
                return task:process_while()
            end
        )

    if task_continue[task.name] then
        return function(p, s)
            local i, tuple = iter(p, s)
            if tuple then
                task_continue[task.name].tuple = tuple
            else
                task_continue[task.name] = nil
            end
            return i, tuple
        end, param, state
    else
        return iter, param, state
    end
end


-- ========================================================================= --
-- Expiration daemon management functions
-- ========================================================================= --
--
-- {{{ Module functions
--

--- Module functions
--
-- @section Functions

--- Configure expirationd.
--
-- Since version 1.2.0.
--
-- How to set up a configuration option:
--
-- ```
-- expirationd.cfg({metrics = true})
-- ```
--
-- How to get an option value:
--
-- ```
-- print(expirationd.cfg.metrics)
-- true
-- ```
--
-- @table options
--
-- @bool[opt] options.metrics
--     Enable or disable stats collection by [metrics][1]. metrics >= 0.11.0
--     is required. It is enabled by default.
--
--     If enabled it creates four counter collectors, see @{task.statistics}:
--
--     1. `expirationd_checked_count`
--
--     2. `expirationd_expired_count`
--
--     3. `expirationd_restarts`
--
--     4. `expirationd_working_time`
--
--     Labeled with `name = task_name`.
--
--     [1]: https://github.com/tarantool/metrics/
--
-- @return None
--
-- @function expirationd.cfg
local function expirationd_cfg(self, options)
    checks('table', {
        metrics = '?boolean',
    })

    if options.metrics == nil then
        return
    end

    if cfg.metrics ~= options.metrics then
        if options.metrics == true then
            metrics_enable()
        else
            metrics_disable()
        end
        rawset(cfg, 'metrics', options.metrics)
    end
end

--- Run a scheduled task to check and process (expire) tuples in a given space.
--
-- How expirationd works in general:
--
-- 1. Process min(`space_length`, `tuples_per_iteration`) tuples at once.
--
-- 2. Sleep `tuples_per_iteration` × `full_scan_time` / `space_length` (but not
--    beyond 1 second).
--
-- 3. Repeat 1-2 until the whole space will be traversed.
--
-- 4. Sleep 1 second.
--
-- 5. Repeat 1-4.
--
-- NOTE: By default expirationd does not start tasks on an read-only instance
-- for non-local persistent spaces, see the `force` option.
--
-- NOTE: By default expirationd continues processing from a last processed
-- tuple if a task with same name has not been killed properly with
-- @{task.kill} or @{kill}. This behavior only works with default functions
-- `start_key` and `iterate_with`. You need to set at least one function if
-- another behavior is needed, see `start_key` and `iterate_with` options.
--
-- @string name
--     Task name.
-- @string space_id
--     Space to look in for expired tuples. `space_id` can be numeric or
--     string.
-- @func is_tuple_expired
--     Function, must accept tuple and return `true` or `false` (is tuple
--     expired or not), receives `args` and `tuple` as arguments.
--
--
-- Example of function:
--
-- ```
-- local function is_tuple_expired(args, tuple)
--     local tuple_expire_time = get_field(tuple, args.field_no)
--     local current_time = fiber.time()
--     return current_time >= tuple_expire_time
-- end
-- ```
--
-- @table[opt] options
--     Table with named options.
-- @param[opt] options.args
--     Passed to `is_tuple_expired()` and `process_expired_tuple()` as
--     an additional context.
-- @boolean[opt] options.atomic_iteration
--     False (default) to process each tuple as a single transaction and true
--     to process tuples from each batch in a single transaction.
-- @boolean[opt] options.force
--     By default expirationd processes tasks for all types of spaces only on
--     an writable instance. It does not process tasks on an read-only instance
--     for [non-local persistent spaces][1]. It means that expirationd will not
--     start the task processing on a replica for regular spaces. Set the
--     option to `true` to force enable the task processing anyway.
--
--     [1]: https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-read_only
--
-- @boolean[opt] options.force_allow_functional_index
--     By default expirationd returns an error on iteration through a functional
--     index for Tarantool < 2.8.4 because it may cause a crash, see
--     https://github.com/tarantool/expirationd/issues/101
--     You can skip the error using the option if you know what you are doing
--     (implement your own `iterate_with` as example).
-- @number[opt] options.full_scan_delay
--     Sleep time between full scans (in seconds). It is allowed to pass an FFI
--     number: `1LL`, `1ULL` etc. Default value is 1 sec.
-- @number[opt] options.full_scan_time
--     Time required for a full index scan (in seconds). It is allowed to pass
--     an FFI number: `1LL`, `1ULL` etc. `full_scan_time` used for calculation
--     of time during which fiber sleeps between iterations. Default value is
--     3600.
-- @string[opt] options.index
--     Name or id of the index to iterate on. If omitted, will use the primary
--     index. If there's no index with this name, will throw an error.
--     Supported index types are TREE and HASH, using other types will result
--     in an error.
-- @func[opt] options.iterate_with
--     Function which returns an iterator object which provides tuples to
--     check, considering the `start_key`, `process_while` and other options.
--     When option is nil default function is used. Function must accept a task
--     instance object. Default function returns iterator returned by
--     [index_object:pairs()][1], where `index` is a primary index or index
--     that specified with argument `options.index`:
--
-- Example of function:
--
-- ```
--  local function iterate_with()
--      index:pairs(option.start_key(), {
--         iterator = option.iterator_type
--      }):take_while(
--             function()
--                 return option.process_while()
--             end
--        )
--  end
-- ```
--
--     [1]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/pairs/.
--
-- @number[opt] options.iteration_delay
--     Max sleep time between iterations (in seconds). It is allowed to pass
--     an FFI number: `1LL`, `1ULL` etc. Default value is 1 sec.
--     Fiber sleeps min(`tuples_per_iteration` × `full_scan_time` / `space_length`, `iteration_delay`).
-- @string[opt] options.iterator_type
--     Type of the iterator to use, as string or box.index constant, for
--     example, `EQ` or `box.index.EQ`, default is `box.index.ALL`. See more
--     about index iterators in [index_object:pairs()][1].
--
--     [1]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/pairs/.
--
-- @func[opt] options.on_full_scan_complete
--     Function to call after completing a full scan iteration. Default value
--     is a function that do nothing.
-- @func[opt] options.on_full_scan_error
--     Function to call after terminating a full scan due to an error. Default
--     value is a function that do nothing.
--
-- Example of function:
--
-- ```
-- local function on_full_scan_error()
--     pcall(fiber.sleep, 1)
-- end
-- ```
-- @func[opt] options.on_full_scan_start
--     Function to call before starting a full scan iteration. Default value
--     is a function that do nothing.
-- @func[opt] options.on_full_scan_success
--     Function to call after successfully completing a full scan iteration.
--     Default value is a function that do nothing.
-- @func[opt] options.process_expired_tuple
--     Applied to expired tuples, receives `space_id`, `args`, `tuple` as
--     arguments. When `process_expired_tuple` is not passed (or `nil` passed),
--     tuples are removed.
--
-- Example of function:
--
-- ```
-- local function put_tuple_to_archive(space_id, args, tuple)
--     box.space[space_id]:delete{tuple[1]}
--     local email = tuple[2]
--     if args.archive_space_id ~= nil and email ~= nil then
--         box.space[args.archive_space_id]:replace{email, fiber.time()}
--     end
-- end
-- ```
--
-- @func[opt] options.process_while
--     Function to call before checking each tuple. If it returns false, the
--     task will stop until next full scan. Default is a function that always
--     return `true`.
--
-- Example of function:
--
-- ```
-- local function process_while()
--     return false
-- end
-- ```
--
-- @param[opt] options.start_key
--     Start iterating from the tuple with this index value. Or when iterator
--     is 'EQ', iterate over tuples with this index value. Must be a value of
--     the same data type as the index field or fields, or a function which
--     returns such value. If omitted or nil, all tuples will be checked.
-- @number[opt] options.tuples_per_iteration
--     Number of tuples to check in one batch (iteration). It is allowed to
--     pass an FFI number: `1LL`, `1ULL` etc. Default value is 1024.
-- @number[opt] options.vinyl_assumed_space_len_factor
--     Factor for recalculation of vinyl space size. Vinyl space size can't be
--     counted (since many operations, `upsert` for example, are applied when
--     you address some data), so you should count (approximate space size)
--     tuples with the first start. `vinyl_assumed_space_len` is approximate
--     count for first run and `vinyl_assumed_space_len_factor` for next
--     milestone (after we've reached next milestone is `*` and so on). It is
--     allowed to pass an FFI number: `1LL`, `1ULL` etc. Default value is 2.
-- @number[opt] options.vinyl_assumed_space_len
--     Assumed size of vinyl space (in the first iteration).
--     Vinyl space size can't be counted (since many operations, `upsert` for
--     example, are applied when you address some data), so you should count
--     (approximate space size) tuples with the first start.
--     `vinyl_assumed_space_len` is approximate count for first run and
--     `vinyl_assumed_space_len_factor` for next milestone (after we've reached
--     next milestone is `*` and so on). It is allowed to pass an FFI number:
--     `1LL`, `1ULL` etc. Default value is 10^7.
--
-- @return task instance
--
-- @usage
--
-- local expirationd = require('expirationd')
--
-- box.cfg{}
--
-- local space = box.space.old
-- local job_name = "clean_all"
--
-- local function is_expired(args, tuple)
--     return true
-- end
--
-- local function delete_tuple(space_id, args, tuple)
--     box.space[space_id]:delete{tuple[1]}
-- end
--
-- expirationd.start(job_name, space.id, is_expired, {
--     process_expired_tuple = delete_tuple,
--     args = nil,
--     tuples_per_iteration = 50,
--     full_scan_time = 3600
-- })
--
-- @function expirationd.start
local function expirationd_run_task(name, space_id, is_tuple_expired, options)
    checks('string', 'number|string', 'function', {
        args = '?',
        atomic_iteration = '?boolean',
        force = '?boolean',
        force_allow_functional_index = '?boolean',
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
        local tmp = task_continue[name]
        prev:kill(name)
        task_continue[name] = tmp
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
        local engine = box.space[space_id].engine
        if engine == "memtx" and expire_index.func ~= nil then
            local supported = false
            local version = rawget(_G, "_TARANTOOL"):split('-', 1)[1]
            local major_minor_patch = version:split('.', 2)

            local major = tonumber(major_minor_patch[1])
            local minor = tonumber(major_minor_patch[2])
            local patch = tonumber(major_minor_patch[3])
            -- https://github.com/tarantool/expirationd/issues/101
            -- fixed since 2.8.4 and 2.10
            if (major > 2) or (major == 2 and minor == 8 and patch >= 4)
               or (major == 2 and minor >= 10) then
                 supported = true
            end
            local force_allow = options.force_allow_functional_index or false
            if not supported and not force_allow then
                error("Functional indices are not supported for" ..
                      " Tarantool < 2.8.4, see options.force_allow_functional_index")
            end
        end
    end
    task.index = expire_index

    -- check iterator_type
    if options.iterator_type ~= nil then
        task.iterator_type = options.iterator_type
    end

    -- check start_key
    if options.start_key ~= nil then
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

--- Kill an existing task.
--
-- @string name
--     Task name.
--
-- @return None
--
-- @function expirationd.kill
local function expirationd_kill_task(name)
    checks('string')

    return get_task(name):kill()
end

--- Return a list with task's names.
--
-- @return Response of the following structure:
--
-- ```
-- {
--     "expirationd-1"
--     "expirationd-2",
--     "expirationd-3",
-- }
-- ```
--
-- @function expirationd.tasks
local function expirationd_show_task_list()
    return fun.map(function(x) return x end, fun.iter(task_list)):totable()
end

--- Return task statistics in table.
--
-- @string[opt] name
--     Task name. If `name` is nil, then return map of `name`:`stats`, else
--     return map with stats.
--
-- @return Response of the following structure:
--
-- ```
-- {
--     checked_count = number,
--     expired_count = number,
--     restarts = number,
--     working_time = number,
-- }
-- ```
--
-- where:
--
-- `checked_count` is a number of tuples checked for expiration (expired + skipped).
--
-- `expired_count` is a number of expired tuples.
--
-- `restarts` is a number of restarts since start. From the start
-- `restarts` is equal to 1.
--
-- `working_time` is a task's operation time.
--
-- @function expirationd.stats
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

--- Get task by name.
--
-- @string name
--     Task name.
--
-- @return task instance
--
-- @function expirationd.task
local function expirationd_get_task(name)
    checks('string')

    return get_task(name)
end

--- Reload module.
--
-- Update expirationd version in a running Tarantool and restart all tasks.
-- Reload process step by step: remove expirationd module from
-- `package.loaded`, import new version of expirationd using `require` and
-- finally restart all tasks.
--
-- @return None
--
-- @function expirationd.update
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
    local tmp_task_continue = table.deepcopy(task_continue)
    for _, task in pairs(tmp_task_list) do
        task:kill()
        -- kill() resets a continue state, we should restore the state
        if tmp_task_continue[task.name] then
            task_continue[task.name] = tmp_task_continue[task.name]
        end
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
    cfg     = setmetatable({}, {
        __index = cfg,
        __newindex = function() error("Use expirationd.cfg{} instead", 2) end,
        __call = expirationd_cfg,
        __serialize = function() return cfg end,
    }),
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

-- }}} Module functions
