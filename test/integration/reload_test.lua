local fio = require('fio')
local t = require('luatest')
local helpers = require('test.helper')
local g = t.group('expirationd_reload')
local is_cartridge_helpers, cartridge_helpers = pcall(require, 'cartridge.test-helpers')

g.before_all(function(cg)
    if is_cartridge_helpers then
        local entrypoint_path = fio.pathjoin(helpers.project_root,
                                             'test',
                                             'entrypoint',
                                             'srv_reload.lua')
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
                    roles = { 'vshard-storage', 'customers-storage' },
                    servers = {
                        { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    }
                }
            },
        })
        cg.cluster:start()
    end
end)

g.after_all(function(cg)
    if is_cartridge_helpers then
        cg.cluster:stop()
        fio.rmtree(cg.cluster.datadir)
    end
end)

g.before_each(function()
    t.skip_if(not is_cartridge_helpers, "cartridge is not installed")
    t.skip_if(not helpers.is_metrics_supported(),
              "metrics >= 0.11.0 is not installed")
end)


local function reload_roles(srv)
    local ok, err = srv.net_box:eval([[
        return require('cartridge.roles').reload()
    ]])

    t.assert_equals({ok, err}, {true, nil})
end

function g.test_cfg_metrics_disable_after_reload(cg)
    cg.cluster:server('router').net_box:eval([[
        local expirationd = require('expirationd')
        expirationd.cfg({metrics = false})
    ]])

    reload_roles(cg.cluster:server('router'))

    local ret = cg.cluster:server('router').net_box:eval([[
        return require('expirationd').cfg.metrics
    ]])
    t.assert_equals(ret, false)
end

function g.test_cfg_metrics_enable_after_reload(cg)
    cg.cluster:server('router').net_box:eval([[
        local expirationd = require('expirationd')
        expirationd.cfg({metrics = true})
    ]])

    reload_roles(cg.cluster:server('router'))

    local ret = cg.cluster:server('router').net_box:eval([[
        return require('expirationd').cfg.metrics
    ]])
    t.assert_equals(ret, true)
end

function g.test_cfg_metrics_clean_after_reload(cg)
    local metrics = cg.cluster:server('s1-master').net_box:eval([[
        local metrics = require('metrics')
        local expirationd = require('expirationd')

        expirationd.cfg({metrics = true})
        expirationd.start("stats_basic", 'customers',
                          function()
                              return true
                          end)
        metrics.invoke_callbacks()
        return metrics.collect()
    ]])
    local restarts = nil
    for _, v in ipairs(metrics) do
        if v.metric_name == "expirationd_restarts" then
            restarts = v.value
        end
    end
    t.assert_equals(restarts, 1)

    reload_roles(cg.cluster:server('s1-master'))

    local metrics = cg.cluster:server('s1-master').net_box:eval([[
        local metrics = require('metrics')
        local expirationd = require('expirationd')

        metrics.invoke_callbacks()
        return metrics.collect()
    ]])
    t.assert_equals(metrics, {})
end
