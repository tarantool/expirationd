local t = require('luatest')
local g = t.group()

local fio = require('fio')
local helpers = require('test.helper')
local Server = require('test.helper_server')

g.before_all(function (cg)
    t.skip_if(not helpers.tarantool_role_is_supported(),
             'Tarantool role is supported only for Tarantool starting from v3.0.0')

    local workdir = fio.abspath('tmp')
    cg.router = Server:new({
        config_file = fio.abspath(fio.pathjoin('test', 'integration', 'simple_app', 'config_for_deploy_test.yaml')),
        env = {LUA_PATH = helpers.lua_path},
        chdir = workdir,
        alias = 'master',
        workdir = workdir,
    })
end)

g.before_each(function(cg)
    fio.mktree(cg.router.workdir)

    -- We start instance before each test because
    -- we need to force reload of expirationd role and also instance environment
    -- from previous tests can influence test result.
    -- (e.g function creation, when testing that role doesn't start w/o it)
    -- Restarting instance is the easiest way to achieve it.
    -- It takes around 1s to start an instance, which considering small amount
    -- of integration tests is not a problem.
    cg.router:start{wait_until_ready = true}
end)

g.after_each(function(cg)
    cg.router:stop()
    fio.rmtree(cg.router.workdir)
end)

local function check_issues(server, expected_issue)
    local retry_opt = {
        timeout = 5,
        delay = 0.25,
    }

    t.helpers.retrying(retry_opt, function()
        local issue = server:exec(function()
            local config = require('config')
            local expirationd = require('expirationd')

            if config._aboard == nil then
                local task = expirationd.task('task_name1')
                if task == nil or task.alert == nil then
                    return nil
                end

                return {
                    type = 'warn',
                    message = task.alert,
                }
            else
                return config._aboard:get('task_name1')
            end
        end)
        if type(issue) == 'table' then
            issue.timestamp = nil
        end
        t.assert_equals(issue, expected_issue)
    end)
end

function g.test_nonstandard_startup_order(cg)
    local storage_master = cg.router
    local retry_opt = {
        timeout = 5,
        delay = 0.25,
    }

    check_issues(storage_master, {
            type = 'warn',
            message = 'Expirationd warning, task "task_name1": Space with name customers does not exist',
    })

    storage_master:exec(function()
        local customers_space = box.schema.space.create('customers', {
            format = {
                {name = 'id', type = 'unsigned'},
            },
            if_not_exists = true,
            engine = 'memtx',
        })

        customers_space:create_index('id', {
            parts = { {field = 'id'} },
            unique = true,
            type = 'TREE',
            if_not_exists = true,
        })
    end)

    check_issues(storage_master, {
            type = 'warn',
            message = 'Expirationd warning, task "task_name1": Function "test_is_expired" '
                .. '(for option "is_tuple_expired") -- not loaded',
    })

    storage_master:exec(function()
        box.schema.func.create('test_is_expired', {
            language = 'LUA',
            if_not_exists = true,
            body = 'function() return true end'
        })
    end)

    check_issues(storage_master, {
            type = 'warn',
            message = 'Expirationd warning, task "task_name1": Function "test_iterate_with" '
                .. '(for option "iterate_with") -- not loaded',
    })

    storage_master:exec(function()
        box.schema.func.create('test_iterate_with', {
            language = 'LUA',
            if_not_exists = true,
            body = 'function() return box.space.customers:pairs() end'
        })
    end)

    check_issues(storage_master, {
            type = 'warn',
            message = 'Expirationd warning, task "task_name1": Function "test_process_expired_tuple" '
                .. '(for option "process_expired_tuple") -- not loaded',
    })

    storage_master:exec(function()
        box.schema.func.create('test_process_expired_tuple', {
            language = 'LUA',
            if_not_exists = true,
            body = 'function(space, args, tuple) box.space[space]:delete({tuple.id}) end'
        })
    end)

    check_issues(storage_master, nil)

    storage_master:exec(function()
        for id = 1, 100 do
            box.space.customers:insert({ id })
        end
    end)

    t.helpers.retrying(retry_opt, function()
        local space_count = storage_master:eval([[ return box.space.customers:count() ]])
        t.assert_equals(space_count, 0)
    end)

    check_issues(storage_master, nil)
end
