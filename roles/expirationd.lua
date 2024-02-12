local expirationd = require("expirationd")
local role_name = "expirationd-role"
local fiber = require("fiber")
local log = require("log")
local config = require("config")

local started = {}


local function load_function(func_name)
    if func_name == nil or type(func_name) ~= 'string' then
        return nil
    end

    local func = rawget(_G, func_name)
    if func == nil then
        if not box.schema.func.exists(func_name) then
            return nil
        end

        return function(...)
            return box.func[func_name]:call({...})
        end
    end

    if type(func) ~= 'function' then
        return nil
    end

    return func
end

local function get_param(param_name, value, types)
    local types_map = {
        b = {type = "boolean", err = "a boolean"},
        n = {type = "number", err = "a number"},
        s = {type = "string", err = "a string"},
        f = {type = "string", transform = load_function, err = "a function name in _G or in box.func"},
        t = {type = "table", err = "a table"},
        any = {err = "any type"},
    }

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

    if not found then
        local err = role_name .. ": " .. param_name .. " must be "
        for i, t in ipairs(types) do
            if t ~= 'f' then
                err = err .. types_map[t].err
                if i ~= #types then
                    err = err .. " or "
                end
                return false, err
            end
        end
    end

    -- Small hack because in tarantool role we wait for functions to be created.
    for i, t in ipairs(types) do
        if not found and t == "f" then
            return true, nil
        end
    end

    return true, value
end

local function get_task_options(opts)
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
        local ok, res = get_param("options." .. opt, val, opts_map[opt])
        if not ok then
            error(res)
        end
        if ok and res == nil and opts_map[opt][1] == "f" then
            table.insert(missed_functions, val)
        end
        opts[opt] = res
    end

    return opts, missed_functions
end

local function get_task_config(task_conf)
    -- setmetatable resets __newindex write protection on a copy
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
            local ok, res = get_param(param, conf[param], opts.types)
            if not ok then
                 error(res)
            end
            if ok and res == nil and opts.types[1] == "f" then
                table.insert(missed_functions, conf[param])
            end
            conf[param] = res
        end
    end

    local missed_functions_opts = {}
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
            local ok, res = get_param(param, conf[param], types)
            if not ok then
                error(res)
            end
        end
    end

    return conf
end

local function validate_config(conf_new)
    local conf = conf_new or {}

    for task_name, task_conf in pairs(conf) do
        local ok, res = get_param("task name", task_name, {"s"})
        if not ok then
            error(res)
        end
        local ok, res = get_param("task params", task_conf, {"t"})
        if not ok then
            error(res)
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
    local start = fiber.clock()
    local task_config, missed_functions = get_task_config(task_conf)
    while true do
        local alert_key = role_name .. ": " .. task_name
        if #missed_functions == 0 then
            config._aboard:drop(alert_key)
            break
        end
        fiber.sleep(1)
        if fiber.clock() - start > 60 then
            local message = role_name .. ": " .. task_name .. ": waiting for functions: "
            for _, func in pairs(missed_functions) do
                message = message .. func .. ', '
            end
            config._aboard:set({type = 'warn', message = message}, {key = alert_key})
            start = fiber.clock()
        end
        task_config, missed_functions = get_task_config(task_conf)
    end

    local skip = task_conf.is_master_only and not box.info.ro
    local skip = false
    if not skip then
        local task = expirationd.start(task_name, task_config.space,
                                       task_config.is_expired,
                                       task_config.options)
        if task == nil then
            error(role_name .. ": unable to start task " .. task_name)
        end
        table.insert(started, task_name)
    end

end

local function apply_config(conf)

    -- finishes tasks from an old configuration
    for i=#started,1,-1 do
        local task_name = started[i]
        local ok, _ = pcall(expirationd.task, task_name)
        if ok then
            if conf[task_name] then
                expirationd.task(task_name):stop()
            else
                expirationd.task(task_name):kill()
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
