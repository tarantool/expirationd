local t = require('luatest')
local fio = require('fio')

--local helpers = require('test.helper')
local helpers = require('test.helper')
local Server = require('test.helper_server')

local g = t.group('tarantool_role_integration_test')

g.before_all(function (cg)
    t.skip_if(not helpers.tarantool_role_is_supported(),
             'Tarantool role is supported only for Tarantool starting from v3.0.0')

    local workdir = fio.tempdir()
    cg.router = Server:new({
        config_file = fio.abspath(fio.pathjoin('test', 'integration', 'simple_app', 'config.yaml')),
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
    cg.router:start()

    cg.router:exec(function()
        box.watch('box.status', function(_, status)
            if status.is_ro == false then
                box.schema.create_space('users', {if_not_exists = true})

                box.space.users:format({
                    {name = 'id', type = 'unsigned'},
                    {name = 'first name', type = 'string'},
                    {name = 'second name', type = 'string', is_nullable = true},
                    {name = 'age', type = 'number', is_nullable = false},
                })

                box.space.users:create_index('primary', {
                    parts = {
                        {field = 1, type = 'unsigned'},
                    },
                })

                box.space.users:insert{1, 'Samantha', 'Carter', 30}
                box.space.users:insert{2, 'Fay', 'Rivers', 41}
                box.space.users:insert{3, 'Zachariah', 'Peters', 13}
                box.space.users:insert{4, 'Milo', 'Walters', 74}
            end
        end)
    end)
end)

g.after_each(function(cg)
    cg.router:stop()
    fio.rmtree(cg.router.workdir)
end)

g.test_simple_role_config = function(cg)
    cg.router:exec(function()
        box.schema.func.create('forever_true_test', {
            body = "function(...) return true end",
            if_not_exists = true
        })
    end)

    helpers.retrying({}, function()
        t.assert_equals(cg.router:exec(function()
            return #box.space.users:select({}, {limit = 10})
        end), 0)
    end)
end

g.test_waiting_for_functions = function(cg)
    -- Function for is_expired config entry was not created, so expirationd
    -- is waiting for it's creation.
    helpers.retrying({}, function()
        t.assert_equals(cg.router:exec(function()
            return box.space.users:select({}, {limit = 10})
        end), {
            {1, 'Samantha', 'Carter', 30},
            {2, 'Fay', 'Rivers', 41},
            {3, 'Zachariah', 'Peters', 13},
            {4, 'Milo', 'Walters', 74},
        })
    end)

    -- Create is_expired function and check that expirationd task started.
    cg.router:exec(function()
        box.schema.func.create('forever_true_test', {
            body = "function(...) return true end",
            if_not_exists = true
        })
    end)

    helpers.retrying({}, function()
        t.assert_equals(cg.router:exec(function()
            return #box.space.users:select({}, {limit = 10})
        end), 0)
    end)

end

g.test_function_created_in_g = function(cg)
    cg.router:exec(function()
        rawset(_G, 'forever_true_test', function()
            return true
        end)
    end)

    helpers.retrying({}, function()
        t.assert_equals(cg.router:exec(function()
            return #box.space.users:select({}, {limit = 10})
        end), 0)
    end)
end
