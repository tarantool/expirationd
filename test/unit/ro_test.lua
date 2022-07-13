local expirationd = require("expirationd")
local fiber = require("fiber")
local t = require("luatest")
local helpers = require("test.helper")
local g = t.group('expirationd_ro')

g.before_all(function()
    -- we need to restore a state after fail
    g.default_box_cfg = {read_only = box.cfg.read_only}
end)

g.before_each(function()
    box.cfg({read_only = false})
    g.space = helpers.create_space_with_tree_index('memtx')
    box.cfg(g.default_box_cfg)
end)

local local_space = nil
g.after_each(function()
    box.cfg({read_only = false})
    g.space:drop()
    if local_space ~= nil then
        local_space:drop()
        local_space = nil
    end
    box.cfg(g.default_box_cfg)
end)

local function create_id_index(space)
    space:create_index("primary", {
        type = "TREE",
        parts = {
            {
                field = 1
            }
        }
    })
end

local tuples = {{1, "1"}, {2, "2"}, {3, "3"}}
local function insert_tuples(space)
    for i = 1,3 do
        space:insert({i, tostring(i)})
    end
end

function g.test_ro_temporary()
    box.cfg({read_only = false})
    local_space = box.schema.create_space("temporary", {temporary = true})
    create_id_index(local_space)
    insert_tuples(local_space)

    box.cfg({read_only = true})

    helpers.iteration_result = {}
    local task = expirationd.start("clean_all", local_space.id, helpers.is_expired_debug,
            {full_scan_delay = 0})

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples)
    end)

    task:kill()
    box.cfg({read_only = false})
end

function g.test_ro_local()
    box.cfg({read_only = false})
    local_space = box.schema.create_space("is_local", {is_local = true})
    create_id_index(local_space)
    insert_tuples(local_space)

    box.cfg({read_only = true})

    helpers.iteration_result = {}
    local task = expirationd.start("clean_all", local_space.id, helpers.is_expired_debug,
            {full_scan_delay = 0})

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples)
    end)

    task:kill()
    box.cfg({read_only = false})
end

function g.test_switch_ro_to_rw()
    box.cfg({read_only = false})
    insert_tuples(g.space)

    box.cfg({read_only = true})

    helpers.iteration_result = {}
    local task = expirationd.start("clean_all", g.space.id, helpers.is_expired_debug,
            {full_scan_delay = 0})

    fiber.yield()
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {})
    end)

    box.cfg({read_only = false})

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, tuples)
    end)

    task:kill()
end

function g.test_switch_rw_to_ro()
    box.cfg({read_only = false})
    insert_tuples(g.space)

    local is_expired_yield = function()
        fiber.yield()
        return true
    end
    helpers.iteration_result = {}
    local task = expirationd.start("clean_all", g.space.id, is_expired_yield,
            {full_scan_delay = 0})

    box.cfg({read_only = true})

    for _ = 1, 10 do
        fiber.yield()
    end

    helpers.retrying({}, function()
        t.assert_equals(g.space:select({}, {limit = 10}), tuples)
    end)

    task:kill()
end
