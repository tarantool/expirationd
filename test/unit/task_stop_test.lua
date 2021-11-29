local expirationd = require("expirationd")
local fiber = require("fiber")
local t = require("luatest")

local helpers = require("test.helper")

local g = t.group('task_stop', t.helpers.matrix({
    engine = {
        'memtx',
        'vinyl',
    },
}))

g.before_each(function(cg)
    g.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.after_each(function(g)
    g.space:drop()
end)

function g.test_cancel_on_pcall(cg)
    local function on_full_scan_complete()
        pcall(fiber.sleep, 1)
    end
    local one_hour = 3600
    local task = expirationd.start("clean_all", cg.space.id, helpers.is_expired_true, {
        full_scan_delay = one_hour,
        on_full_scan_complete = on_full_scan_complete
    })
    helpers.retrying({}, function()
        t.assert(task.worker_fiber)
    end)
    -- We need to execute in a separate fiber,
    -- since pcall does not check testcancel and stop may freeze up.
    local f = fiber.create(task.stop, task)
    helpers.retrying({timeout = 5}, function()
        t.assert_equals(f:status(), "dead")
    end)
    -- Also delete from task_list
    task:kill()
end
