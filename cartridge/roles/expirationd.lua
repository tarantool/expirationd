local expirationd = require("expirationd")
local role_name = "expirationd"
local started = require("cartridge.vars").new(role_name)

local issue_map = {}

function _G.expirationd_enable_issue(task_name, message, ...)
    issue_map[task_name] = {
        level = 'warning',
        topic = 'expirationd',
        message = ('EXPIRATIOND, task name "%s": ' .. message):format(task_name, ...),
    }
end

function _G.expirationd_disable_issue(task_name)
    issue_map[task_name] = nil
end

local function get_issues()
    local res = {}
    for _, issue in pairs(issue_map) do
        table.insert(res, issue)
    end

    return res
end

local function get_param(param_name, value, types)
    local types_map = {
        b = {type = "boolean", err = "a boolean"},
        n = {type = "number", err = "a number"},
        s = {type = "string", err = "a string"},
        f = {type = "string", err = "a string"},
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
            err = err .. types_map[t].err
            if i ~= #types then
                err = err .. " or "
            end
        end
        return false, err
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
        opts[opt] = res
    end

    return opts
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

    for param, opts in pairs(params_map) do
        if opts.required and conf[param] == nil then
            error(role_name .. ": " .. param .. " is required")
        end
        if conf[param] ~= nil then
            local ok, res = get_param(param, conf[param], opts.types)
            if not ok then
                error(res)
            end
            conf[param] = res
        end
    end

    conf.options = get_task_options(conf.options)
    return conf
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

local function init()

end

local function validate_config(conf_new)
    local conf = conf_new[role_name] or {}

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

local function apply_config(conf_new, opts)
    local conf = conf_new[role_name] or {}

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
        task_conf = get_task_config(task_conf)

        local skip = task_conf.is_master_only and not opts.is_master
        if not skip then
            local task = expirationd.start(task_name, task_conf.space,
                                           task_conf.is_expired,
                                           task_conf.options)
            if task == nil then
                error(role_name .. ": unable to start task " .. task_name)
            end
            table.insert(started, task_name)
        end
    end
end

local function stop()
    for _, task_name in pairs(expirationd.tasks()) do
        local task = expirationd.task(task_name)
        task:stop()
    end
end

return setmetatable({
    role_name = role_name,
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,
    stop = stop,
    get_issues = get_issues,
}, { __index = expirationd })
