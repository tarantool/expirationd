local expirationd = require("expirationd")
local t = require("luatest")
local g = t.group("iterate_with")

local helpers = require("test.helper")

g.before_all(function()
    helpers.init_spaces(g)
end)

g.after_each(function()
    helpers.truncate_spaces(g)
end)

function g.test_passing()
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            { iterate_with = helpers.is_expired_true })
    -- default process_while always return false, iterations never stopped by this function
    t.assert_equals(task.iterate_with, helpers.is_expired_true)
    task:kill()

    -- errors
    t.assert_error_msg_contains("bad argument options.iterate_with to nil (?function expected, got string)",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            { iterate_with = "" })
end
