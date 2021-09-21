local expirationd = require("expirationd")
local fiber = require("fiber")
local t = require("luatest")
local g = t.group("task_stop")

local helpers = require("test.helper")

g.before_each(function()
    g.tree = helpers.create_space_with_tree_index()
    g.hash = helpers.create_space_with_hash_index()
    g.bitset = helpers.create_space_with_bitset_index()
    g.vinyl = helpers.create_space_with_vinyl()
end)

g.after_each(function()
    g.tree:drop()
    g.hash:drop()
    g.bitset:drop()
    g.vinyl:drop()
end)

function g.test_cancel_on_pcall()
    local function on_full_scan_complete()
        pcall(fiber.sleep, 1)
    end
    local one_hour = 3600
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true, {
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
end
