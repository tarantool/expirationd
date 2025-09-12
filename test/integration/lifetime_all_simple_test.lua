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
        config_file = fio.abspath(fio.pathjoin('test', 'integration', 'simple_app', 'config_without_user_logic.yaml')),
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
end)

g.after_each(function(cg)
    cg.router:stop()
    fio.rmtree(cg.router.workdir)
end)

function g.test_case(cg)
    local storage_master = cg.router

    storage_master:exec(function()
        local datetime = require('datetime')

        local s = box.schema.space.create('test', {
            format = {
                { name = 'id', type = 'unsigned' },
                { name = 'dt', type = 'datetime' },
                { name = 'data', type = 'any' },
            }
        })

        s:create_index('id', {
            parts = { {field = 'id'} },
            unique = true,
            type = 'TREE',
        })

        s:insert({123, datetime.now(), 'Too foo bar'})
    end)

    -- In config lifetime_in_seconds = 3 second
    local retry_opt = {
        timeout = 5,
        delay = 0.25,
    }
    t.helpers.retrying(retry_opt, function()
        local space_count = storage_master:eval([[ return box.space.test:count() ]])
        t.assert_equals(space_count, 0)
    end)
end
