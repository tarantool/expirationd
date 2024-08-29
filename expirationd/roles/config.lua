local expirationd = require('expirationd')
local checks = require('checks')

local started = {}

local function check_task_options(opts) -- luacheck: ignore 212
    checks({
        args = '?',
        atomic_iteration = '?boolean',
        force = '?boolean',
        force_allow_functional_index = '?boolean',
        full_scan_delay = '?number',
        full_scan_time = '?number',
        index = '?number|string',
        iterate_with = '?string',
        iteration_delay = '?number',
        iterator_type = '?number|string',
        on_full_scan_complete = '?string',
        on_full_scan_error = '?string',
        on_full_scan_start = '?string',
        on_full_scan_success = '?string',
        process_expired_tuple = '?string',
        process_while = '?string',
        start_key = '?',
        tuples_per_iteration = '?number',
        vinyl_assumed_space_len_factor = '?number',
        vinyl_assumed_space_len = '?number',
    })
end

local function check_default_is_expired_args(args) -- luacheck: ignore 212
    checks({
        lifetime_in_seconds = 'number',
        time_create_field = 'string',
    })
end

local function check_task_description(task_conf) -- luacheck: ignore 212
    checks({
        space = 'string|number',
        is_expired = '?string',
        is_master_only = '?boolean',
        options = '?table',
    })

    if task_conf.options then
        check_task_options(task_conf.options)
    end

    if task_conf.is_expired == nil then
        if task_conf.options == nil or task_conf.options.args == nil then
            error('is_expired is required or options.args is required')
        end

        check_default_is_expired_args(task_conf.options.args)
    end
end

local function get_task_config(task_conf)
    check_task_description(task_conf)

    -- setmetatable resets __newindex write protection on a copy
    return setmetatable(table.deepcopy(task_conf), {})
end

local function get_cfg(cfg)
    checks({
        metrics = '?boolean',
    })

    return setmetatable(table.deepcopy(cfg), {})
end

local function validate_config(conf)
    checks('table')

    for task_name, task_conf in pairs(conf) do
        if type(task_name) ~= 'string' then
            error("task name must be a string")
        end

        if type(task_conf) ~= 'table' then
            error("task configuration must be a table")
        end

        if task_name == 'cfg' then
            get_cfg(task_conf)
        else
            check_task_description(task_conf)
        end
    end

    return true
end

local function apply_config(conf, opts) -- luacheck: ignore 212
    -- finishes tasks from an old configuration
    for i = #started, 1, -1 do
        local task_name = started[i]
        local ok, task = pcall(expirationd.task, task_name)
        -- We don't need to do anything if there is no task
        if ok then
            if conf[task_name] then
                task:stop()
            else
                task:kill()
            end
        end
        table.remove(started, i)
    end

    if conf['cfg'] ~= nil then
        local ok = pcall(get_task_config, conf['cfg'])
        if not ok then
            local cfg = get_cfg(conf['cfg'])
            expirationd.cfg(cfg)
            conf['cfg'] = nil
        end
    end

    for task_name, task_conf in pairs(conf) do
        task_conf = get_task_config(task_conf)

        local skip = task_conf.is_master_only and box.info.ro
        if not skip then
            local task = expirationd.start(task_name, task_conf.space,
                                           task_conf.is_expired,
                                           task_conf.options)
            if task == nil then
                error('unable to start task ' .. task_name)
            end
            table.insert(started, task_name)
        end
    end
end

return {
    validate_config = validate_config,
    apply_config = apply_config,
}
