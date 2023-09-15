local expirationd = require("expirationd")
local t = require("luatest")
local luatest_capture = require("luatest.capture")
local helpers = require("test.helper")
local g = t.group("expirationd_space_index")

g.before_each(function(cg)
    cg.space = helpers.create_space_with_tree_index("memtx")
    cg.space:insert({1, "1"})
    -- kill live tasks (it can still live after failed tests)
    for _, t in ipairs(expirationd.tasks()) do
        expirationd.kill(t)
    end
    cg.case_space = nil
end)

g.after_each(function(cg)
    if box.space[cg.space.name] then
        cg.space:drop()
    end
    if cg.case_space then
        cg.case_space:drop()
        cg.case_space = nil
    end
end)

-- in some cases we need to create an additional space
local function create_case_space(cg, space_name)
    cg.case_space = box.schema.create_space(space_name)
    cg.case_space:create_index("primary", {type = "TREE", parts = {{field = 1}}})
    cg.case_space:insert({2, "2"})
end

local start_cases = {
    by_name = {
        space = function(cg)
            return cg.space.name
        end,
        index = function()
            return "index_for_first_name"
        end,
    },
    by_id = {
        space = function(cg)
            return cg.space.id
        end,
        index = function(cg)
            return cg.space.index["index_for_first_name"].id
        end,
    },
}

for name, case in pairs(start_cases) do
    g["test_start_on_existing_space_and_index_" .. name] = function(cg)
        helpers.iteration_result = {}
        local task = expirationd.start("clean_all", case.space(cg), helpers.is_expired_debug,
                                       {index = case.index(cg)})
        helpers.retrying({}, function()
            t.assert_equals({{1, "1"}}, helpers.iteration_result)
        end)
        t.assert_not_equals(task, nil)
        t.assert_equals(task:statistics().restarts, 1)
        task:kill()
    end
end

local non_existing_start_cases = {
    non_existing_index_name = {
        index = "non_existing_name",
        msg = "expiration: postpone a task clean_all, reason: Index with name non_existing_name does not exist",
    },
    non_existing_index_id = {
        index = 67,
        msg = "expiration: postpone a task clean_all, reason: Index with id 67 does not exist",
    },
    non_existing_space_name = {
        space = "non_existing_name",
        index = 0,
        msg = "expiration: postpone a task clean_all, reason: Space with name non_existing_name does not exist",
    },
    non_existing_space_id = {
        space = 337,
        index = 0,
        msg = "expiration: postpone a task clean_all, reason: Space with id 337 does not exist",
    },
}

for name, case in pairs(non_existing_start_cases) do
    g["test_start_" .. name] = function(cg)
        local task
        local capture = luatest_capture:new()
        capture:wrap(true, function()
            helpers.iteration_result = {}
            local space = case.space or cg.space.id
            task = expirationd.start("clean_all", space, helpers.is_expired_debug,
                                     {index = case.index})
        end)

        t.assert_str_contains(capture:flush().stderr, case.msg)

        t.assert_not_equals(task, nil)
        t.assert_not_equals(expirationd.task("clean_all"), nil)
        t.assert_equals(#expirationd.tasks(), 1)
        t.assert_equals(task:statistics().restarts, 0)

        task:kill()

        t.assert_equals(helpers.iteration_result, {})
    end
end

function g.test_run_after_non_existing_index_created(cg)
    local task_name = "clean_all"
    local index_name = "non_existing_name"

    helpers.iteration_result = {}
    local task = expirationd.start(task_name, cg.space.id, helpers.is_expired_debug,
                                   {index = index_name})
    cg.space:create_index(index_name, {type = "TREE", parts = {{field = 2}}})

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{1, "1"}})
    end)
    t.assert_equals(#expirationd.tasks(), 1)
    t.assert_not_equals(expirationd.task(task_name), nil)
    t.assert_equals(task:statistics().restarts, 1)

    task:kill()
end

function g.test_run_after_non_existing_space_created(cg)
    local task_name = "clean_all"
    local space_name = "tmp"

    helpers.iteration_result = {}
    local task = expirationd.start(task_name, space_name, helpers.is_expired_debug)

    create_case_space(cg, space_name)

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{2, "2"}})
    end)
    t.assert_equals(#expirationd.tasks(), 1)
    t.assert_not_equals(expirationd.task(task_name), nil)
    t.assert_equals(task:statistics().restarts, 1)

    task:kill()
end

function g.test_run_after_non_existing_space_and_index_created(cg)
    local task_name = "clean_all"
    local space_name = "tmp"
    local index_name = "non_primary"

    helpers.iteration_result = {}
    local task = expirationd.start(task_name, space_name, helpers.is_expired_debug,
                                   {index = index_name})

    create_case_space(cg, space_name)

    t.assert_equals(helpers.iteration_result, {})
    t.assert_equals(#expirationd.tasks(), 1)
    t.assert_not_equals(expirationd.task(task_name), nil)
    t.assert_equals(task:statistics().restarts, 0)

    g.case_space:create_index(index_name, {type = "TREE", parts = {{field = 1}}})

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{2, "2"}})
    end)
    t.assert_equals(#expirationd.tasks(), 1)
    t.assert_not_equals(expirationd.task(task_name), nil)
    t.assert_equals(task:statistics().restarts, 1)

    task:kill()
end

local rename_cases = {
    index_rename = {
        fun = function(space, _)
            space:rename("XXX")
        end
    },
    space_rename = {
        fun = function(_, index)
            index:rename("XXX")
        end
    },
}

for name, case in pairs(rename_cases) do
    g["test_stop_after_" .. name .. "_if_name_used"] = function(cg)
        local task_name = "clean_all"
        local space_name = cg.space.name
        local index_name = "index_for_first_name"

        helpers.iteration_result = {}
        local task = expirationd.start(task_name, space_name, helpers.is_expired_debug,
                                       {index = index_name})
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {{1, "1"}})
        end)
        helpers.iteration_result = {}

        local capture = luatest_capture:new()
        capture:wrap(true, function()
            local space = box.space[space_name]
            case.fun(space, space.index[index_name])
            space:insert({1, "1"})

            local stderr = ""
            helpers.retrying({}, function()
                stderr = stderr .. capture:flush().stderr
                t.assert_str_contains(stderr, "expiration: stop task")
            end)
        end)

        t.assert_equals(task.worker_fiber:status(), "dead")
        t.assert_equals(task.guardian_fiber:status(), "dead")
        t.assert_equals(helpers.iteration_result, {})
        t.assert_equals(#expirationd.tasks(), 1)
        t.assert_not_equals(expirationd.task(task_name), nil)
        t.assert_equals(task:statistics().restarts, 1)

        task:kill()
    end
    g["test_not_stop_after_" .. name .. "_if_id_used"] = function(cg)
        local task_name = "clean_all"
        local space_id = cg.space.id
        local index_id = 1

        helpers.iteration_result = {}
        local task = expirationd.start(task_name, space_id, helpers.is_expired_debug,
                                       {index = index_id})
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {{1, "1"}})
        end)
        helpers.iteration_result = {}

        local space = box.space[space_id]
        case.fun(space, space.index[index_id])
        space:insert({1, "1"})

        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {{1, "1"}})
        end)
        t.assert_equals(#expirationd.tasks(), 1)
        t.assert_not_equals(expirationd.task(task_name), nil)
        t.assert_equals(task:statistics().restarts, 1)

        task:kill()
    end
end

local drop_cases = {
    index_drop = {
        fun = function(_, index)
            index:drop()
        end,
    },
    space_drop = {
        fun = function(space, _)
            space:drop()
        end,
    }
}

for name, case in pairs(drop_cases) do
    g["test_stop_after_" .. name] = function(cg)
        local task_name = "clean_all"
        local index_id = 1

        helpers.iteration_result = {}
        local task = expirationd.start(task_name, cg.space.id, helpers.is_expired_debug,
                                       {index = index_id})
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {{1, "1"}})
        end)
        helpers.iteration_result = {}

        local capture = luatest_capture:new()
        capture:wrap(true, function()
            case.fun(cg.space, cg.space.index[index_id])

            local stderr = ""
            helpers.retrying({}, function()
                stderr = stderr .. capture:flush().stderr
                t.assert_str_contains(stderr, "expiration: stop task")
            end)
        end)

        t.assert_equals(task.worker_fiber:status(), "dead")
        t.assert_equals(task.guardian_fiber:status(), "dead")
        t.assert_equals(helpers.iteration_result, {})
        t.assert_equals(#expirationd.tasks(), 1)
        t.assert_not_equals(expirationd.task(task_name), nil)
        t.assert_equals(task:statistics().restarts, 1)

        task:kill()
    end
end

function g.test_stop_after_drop_stop_and_recreate(cg)
    local space_name = "tmp"
    local task_name = "clean_all"

    create_case_space(cg, space_name)

    helpers.iteration_result = {}
    local task = expirationd.start(task_name, space_name, helpers.is_expired_debug)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{2, "2"}})
    end)

    local capture = luatest_capture:new()
    capture:wrap(true, function()
        cg.case_space:drop()
        local stderr = ""
        helpers.retrying({}, function()
            stderr = stderr .. capture:flush().stderr
            t.assert_str_contains(stderr, "expiration: stop task")
        end)
    end)

    helpers.iteration_result = {}
    create_case_space(cg, space_name)

    t.assert_equals(task.worker_fiber:status(), "dead")
    t.assert_equals(task.guardian_fiber:status(), "dead")
    t.assert_equals(helpers.iteration_result, {})
    t.assert_equals(#expirationd.tasks(), 1)
    t.assert_not_equals(expirationd.task(task_name), nil)
    t.assert_equals(task:statistics().restarts, 1)

    task:kill()
end

function g.test_not_stop_after_drop_and_recreate(cg)
    local space_name = "tmp"
    local task_name = "clean_all"

    create_case_space(cg, space_name)

    helpers.iteration_result = {}
    local task = expirationd.start(task_name, space_name, helpers.is_expired_debug)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{2, "2"}})
    end)

    helpers.iteration_result = {}

    -- This should be atomic to prevent yielding into the expirationd task with
    -- partially applied changes (when the old space is dropped but the new one
    -- is not created yet).
    --
    -- The space drop is transactional if the tarantool/taratool#8751 is merged,
    -- so it yields on commit. After that point the space is dropped.
    --
    -- The expirationd task yields on each tuple drop if the task does not have
    -- the atomic_iteration option set.
    --
    -- So without this begin/commit guard the following situation is possible:
    -- 1. The expirationd task has started.
    -- 2. The task removes one tuple, which causes yield.
    -- 3. The current fiber drops the space and yields to the expirationd task.
    -- 4. The expirationd task finishes the iteration and checks if the space
    --    is vinyl to update the vinyl_assumed_space_len, but the space does
    --    not exist anymore, so it fails causing the task restart.

    -- Since the test is also executed against old versions of Tarantool, let's
    -- only wrap the space drop and recreation into transaction on the versions
    -- that support at least single-yield transactional DDL.
    if helpers.single_yield_transactional_ddl_is_supported() then
        box.begin()
    end

    cg.case_space:drop()
    create_case_space(cg, space_name)

    if helpers.single_yield_transactional_ddl_is_supported() then
        box.commit()
    end

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{2, "2"}})
    end)
    t.assert_equals(#expirationd.tasks(), 1)
    t.assert_not_equals(expirationd.task(task_name), nil)
    t.assert_equals(task:statistics().restarts, 1)

    task:kill()
end

function g.test_not_stop_after_truncate(cg)
    local task_name = "clean_all"

    helpers.iteration_result = {}
    local task = expirationd.start(task_name, cg.space.id, helpers.is_expired_debug)
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{1, "1"}})
    end)

    helpers.iteration_result = {}
    cg.space:truncate()
    cg.space:insert({1, "1"})

    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {{1, "1"}})
    end)
    t.assert_equals(#expirationd.tasks(), 1)
    t.assert_not_equals(expirationd.task(task_name), nil)
    t.assert_equals(task:statistics().restarts, 1)

    task:kill()
end
