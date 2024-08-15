local expirationd = require("expirationd")
local config = require('config')
local fiber = require("fiber")
local log = require("log")

local role_name = "roles.expirationd"
local started = {}

function _G.expirationd_enable_issue(task_name, message, ...)
    config._aboard:set(
        {
            type = 'warn',
            message = ('EXPIRATIOND, task name "%s": ' .. message):format(task_name, ...),
        },
        {
            key = task_name,
        }
    )
end

function _G.expirationd_disable_issue(task_name)
    config._aboard:drop(task_name)
end

local types_map = {
    b = {type = "boolean", err = "a boolean"},
    n = {type = "number", err = "a number"},
    s = {type = "string", err = "a string"},
    f = {type = "string", err = "a string"},
    t = {type = "table", err = "a table"},
    any = {err = "any type"},
}

local opts_map = {
    args = {"any"},
    atomic_iteration = {"b"},
    force = {"b"},
    force_allow_functional_index = {"b"},
    full_scan_delay = {"n"},
    full_scan_time = {"n"},
    index = {"n", "s"},
    iterate_with = {"f"},
    iteration_delay = {"n"},
    iterator_type = {"n", "s"},
    on_full_scan_complete = {"f"},
    on_full_scan_error = {"f"},
    on_full_scan_start = {"f"},
    on_full_scan_success = {"f"},
    process_expired_tuple = {"f"},
    process_while = {"f"},
    start_key = {"f", "t"},
    tuples_per_iteration = {"n"},
    vinyl_assumed_space_len_factor = {"n"},
    vinyl_assumed_space_len = {"n"},
}

local function table_contains(table, element)
    for _, value in pairs(table) do
      if value == element then
        return true
      end
    end
    return false
end


local function get_param(param_name, value, types)
    local found = false
    for _, t in ipairs(types) do
        local type_opts = types_map[t]
        if type_opts == nil then
            error(role_name .. ": unsupported type option")
        end
        if not type_opts.type or type(value) == type_opts.type then
            if type_opts.transform then
                local tmp = type_opts.transform(value)
                if tmp then
                    value = tmp
                    found = true
                    break
                end
            else
                found = true
                break
            end
        end
    end

    -- Small hack because in tarantool role we wait for functions to be created.
    -- So, if type of value is function and it is allowed
    -- and it is not found we do not return an error.
    if table_contains(types, "f") and not found then
        for _, t in ipairs(types) do
            local type_opts = types_map[t]
            if t == "f" and type(value) == type_opts.type then
                return nil, true, nil
            end
        end
    end

    if not found then
        local err = role_name .. ": " .. param_name .. " must be "
        for i, t in ipairs(types) do
            err = err .. types_map[t].err
            if i ~= #types then
                err = err .. " or "
            end
        end
        return nil, false, err
    end

    return value, true, nil
end

local function get_task_options(opts)
    if opts == nil then
        return
    end

    local missed_functions = {}

    for opt, val in pairs(opts) do
        if type(opt) ~= "string" then
            error(role_name .. ": an option must be a string")
        end
        if opts_map[opt] == nil then
            error(role_name .. ": unsupported option '" .. opt .. "'")
        end
        local res, ok, err = get_param("options." .. opt, val, opts_map[opt])
        if not ok then
            error(err)
        end
        if ok and res == nil and opts_map[opt][1] == "f" then
            table.insert(missed_functions, val)
        end
        opts[opt] = res
    end

    return opts, missed_functions
end

local function get_task_config(task_conf)
    -- setmetatable resets __newindex write protection on a copy.
    local conf = setmetatable(table.deepcopy(task_conf), {})
    local params_map = {
        space = {required = true, types = {"n", "s"}},
        is_expired = {required = true, types = {"f"}},
        is_master_only = {required = false, types = {"b"}},
        options = {required = false, types = {"t"}},
    }
    for k, _ in pairs(conf) do
        if type(k) ~= "string" then
            error(role_name .. ": param must be a string")
        end
        if params_map[k] == nil then
            error(role_name .. ": unsupported param " .. k)
        end
    end
    local missed_functions = {}
    for param, opts in pairs(params_map) do
        if opts.required and conf[param] == nil then
            error(role_name .. ": " .. param .. " is required")
        end
        if conf[param] ~= nil then
            local res, ok, err = get_param(param, conf[param], opts.types)
            if not ok then
                 error(err)
            end
            if ok and res == nil and opts.types[1] == "f" then
                table.insert(missed_functions, conf[param])
            end
            conf[param] = res
        end
    end

    local missed_functions_opts
    conf.options, missed_functions_opts = get_task_options(conf.options)
    if missed_functions_opts ~= nil then
        for _, func in pairs(missed_functions_opts) do
            table.insert(missed_functions, func)
        end
    end
    return conf, missed_functions
end

local function get_cfg(cfg)
    local conf = setmetatable(table.deepcopy(cfg), {})
    local params_map = {
        metrics = {"b"},
    }

    for k, _ in pairs(conf) do
        if type(k) ~= "string" then
            error(role_name .. ": config option must be a string")
        end
        if params_map[k] == nil then
            error(role_name .. ": unsupported config option " .. k)
        end
    end

    for param, types in pairs(params_map) do
        if conf[param] ~= nil then
            local _, ok, err = get_param(param, conf[param], types)
            if not ok then
                error(err)
            end
        end
    end

    return conf
end

local function validate_config(conf_new)
    local conf = conf_new or {}

    for task_name, task_conf in pairs(conf) do
        local _, ok, err = get_param("task name", task_name, {"s"})
        if not ok then
            error(err)
        end
        local _, ok, err = get_param("task params", task_conf, {"t"})
        if not ok then
            error(err)
        end
        local ok, ret = pcall(get_task_config, task_conf)
        if not ok then
            if task_name == "cfg" then
                get_cfg(task_conf)
            else
                error(ret)
            end
        end
    end

    return true
end

local function load_task(task_conf, task_name)
    local timeout = 1
    local warning_delay = 60
    local start = fiber.clock()
    local task_config, missed_functions = get_task_config(task_conf)

    fiber.name(role_name .. ":" .. task_name)

    local skip = task_conf.is_master_only and box.info.ro
    if skip then
        return
    end

    while #missed_functions ~= 0 do
        fiber.sleep(timeout)
        if fiber.clock() - start > warning_delay then
            local message = role_name .. ": " .. task_name .. ": waiting for functions: "
            for i, func in pairs(missed_functions) do
                if i == #missed_functions then
                    message = message .. func .. '.'
                else
                    message = message .. func .. ', '
                end
            end

            log.warn(message)
            start = fiber.clock()
        end
        task_config, missed_functions = get_task_config(task_conf)
    end

    local task = expirationd.start(task_name, task_config.space,
                                   task_config.is_expired,
                                   task_config.options)
    if task == nil then
        error(role_name .. ": unable to start task " .. task_name)
    end
    table.insert(started, task_name)
end

local function apply_config(conf)
    -- Finishes tasks from an old configuration
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

    if conf["cfg"] ~= nil then
        local ok = pcall(get_task_config, conf["cfg"])
        if not ok then
            local cfg = get_cfg(conf["cfg"])
            expirationd.cfg(cfg)
            conf["cfg"] = nil
        end
    end

    for task_name, task_conf in pairs(conf) do
        fiber.new(load_task, task_conf, task_name)
    end
end

local function stop()
    for _, task_name in pairs(expirationd.tasks()) do
        local task = expirationd.task(task_name)
        task:stop()
    end
end

return {
    validate = validate_config,
    apply = apply_config,
    stop = stop,
}
