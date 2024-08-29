local t = require('luatest')
local g = t.group()

local is_cartridge_helpers, cartridge_helpers = pcall(require, 'cartridge.test-helpers')

local fio = require('fio')
local helpers = require('test.helper')

g.before_all(function(cg)
    t.skip_if(not is_cartridge_helpers, "cartridge is not installed")

    local entrypoint_path = fio.pathjoin(helpers.project_root,
                                         'test',
                                         'entrypoint',
                                         'empty.lua')
    cg.cluster = cartridge_helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = entrypoint_path,
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'router',
                roles = { 'vshard-router' },
                servers = {
                    { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
                },
            },
            {
                uuid = helpers.uuid('b'),
                alias = 's-1',
                roles = { 'vshard-storage', 'expirationd' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-slave' },
                }
            }
        },
    })
    cg.cluster:start()
end)

g.after_all(function(cg)
    cg.cluster:stop()
    fio.rmtree(cg.cluster.datadir)
end)

local function check_issues(server, expected_issues)
    local retry_opt = {
        timeout = 5,
        delay = 0.25,
    }

    t.helpers.retrying(retry_opt, function()
        local issues = server:eval([[ return require('cartridge.roles.expirationd').get_issues() ]])
        t.assert_equals(issues, expected_issues)
    end)
end

function g.test_nonstandard_startup_order(cg)
    local storage_master = cg.cluster:server('s1-master')
    local storage_slave = cg.cluster:server('s1-slave')
    local retry_opt = {
        timeout = 5,
        delay = 0.25,
    }

    storage_master:upload_config({
        expirationd = {
            test_task = {
                space = "customers",
                is_expired = "test_is_expired",
                is_master_only = true,
                options = {
                    iterate_with = "test_iterate_with",
                    process_expired_tuple = "test_process_expired_tuple",
                },
            }
        },
    })

    check_issues(storage_master, {{
            level = 'warning',
            topic = 'expirationd',
            message = 'Expirationd warning, task "test_task": Space with name customers does not exist',
    }})
    check_issues(storage_slave, {})

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

    check_issues(storage_master, {{
        level = 'warning',
        topic = 'expirationd',
        message = 'Expirationd warning, task "test_task": Function "test_is_expired" '
            .. '(for option "is_tuple_expired") -- not loaded',
    }})
    check_issues(storage_slave, {})

    helpers.create_persistent_function_on_server(
        storage_master,
        'test_is_expired',
        [[
            function()
                return true
            end
        ]]
    )

    check_issues(storage_master, {{
            level = 'warning',
            topic = 'expirationd',
            message = 'Expirationd warning, task "test_task": Function "test_iterate_with" '
                .. '(for option "iterate_with") -- not loaded',
    }})
    check_issues(storage_slave, {})

    helpers.create_persistent_function_on_server(
        storage_master,
        'test_iterate_with',
        [[
            function()
                return box.space.customers:pairs()
            end
        ]]
    )

    check_issues(storage_master, {{
            level = 'warning',
            topic = 'expirationd',
            message = 'Expirationd warning, task "test_task": Function "test_process_expired_tuple" '
                .. '(for option "process_expired_tuple") -- not loaded',
    }})
    check_issues(storage_slave, {})

    helpers.create_persistent_function_on_server(
        storage_master,
        'test_process_expired_tuple',
        [[
            function(space, args, tuple)
                box.space[space]:delete({tuple.id})
            end
        ]]
    )

    check_issues(storage_master, {})
    check_issues(storage_slave, {})

    storage_master:exec(function()
        for id = 1, 100 do
            box.space.customers:insert({ id })
        end
    end)

    t.helpers.retrying(retry_opt, function()
        local space_count = storage_master:eval([[ return box.space.customers:count() ]])
        t.assert_equals(space_count, 0)
    end)

    check_issues(storage_master, {})
    check_issues(storage_slave, {})
end
