local expirationd = require("expirationd")
local expirationd_roles_config = require("expirationd.roles.config")

local role_name = "expirationd"
local issue_map = {}

function _G.expirationd_enable_issue(task_name, message)
    issue_map[task_name] = {
        level = 'warning',
        topic = 'expirationd',
        message = message,
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

local function init()

end

local function validate_config(conf_new)
    local conf = conf_new[role_name] or {}
    local ok, res = pcall(expirationd_roles_config.validate_config, conf)
    if not ok then
        error(role_name .. ": " .. res)
    end

    return true
end

local function apply_config(conf_new, opts)
    local conf = conf_new[role_name] or {}
    local ok, res = pcall(expirationd_roles_config.apply_config, conf, opts)
    if not ok then
        error(role_name .. ": " .. res)
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
