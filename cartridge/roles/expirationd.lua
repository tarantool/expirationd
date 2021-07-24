local expirationd = require("expirationd")
local vars = require("cartridge.vars").new("expirationd")

local function init()
    rawset(_G, "expirationd", expirationd)
end

local function stop()
    for _, name in pairs(expirationd.tasks()) do
        local task = expirationd.get_task(name)
        task:kill()
        -- save the state
        local func, state, var = task:iterate_with()
        local _, start_tuple = func(state, var)

        vars[name] = start_tuple
    end
    rawset(_G, "expirationd", nil)
end

return {
    role_name = "expirationd",
    permanent = true,

    init = init,
    stop = stop
}
