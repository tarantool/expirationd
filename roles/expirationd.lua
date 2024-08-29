local expirationd = require("expirationd")
local expirationd_roles_config = require("expirationd.roles.config")
local config = require('config')

local role_name = "roles.expirationd"

if config._aboard ~= nil then
    rawset(_G, "expirationd_enable_issue", function(task_name, message)
        config._aboard:set(
            {
                type = 'warn',
                message = message,
            },
            {
                key = task_name,
            }
        )
    end)
    rawset(_G, "expirationd_disable_issue", function(task_name)
        config._aboard:drop(task_name)
    end)
end

local function validate(conf_new)
    local conf = conf_new or {}
    local ok, res = pcall(expirationd_roles_config.validate_config, conf)
    if not ok then
        error(role_name .. ":" .. res)
    end

    return true
end

local function apply(conf)
    local conf_safe = conf or {}
    local ok, res = pcall(expirationd_roles_config.apply_config, conf_safe)
    if not ok then
        error(role_name .. ":" .. res)
    end
end

local function stop()
    for _, task_name in pairs(expirationd.tasks()) do
        local task = expirationd.task(task_name)
        task:stop()
    end
end

return {
    validate = validate,
    apply = apply,
    stop = stop,
}
