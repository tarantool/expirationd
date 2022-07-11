local expirationd = require("expirationd")
local t = require("luatest")
local helpers = require("test.helper")
local g = t.group('expirationd_cfg')

local metrics_required_msg = "metrics >= 0.11.0 is not installed"
local metrics_not_required_msg = "metrics >= 0.11.0 is installed"

g.before_all(function()
    g.default_cfg = { metrics = expirationd.cfg.metrics }
end)

g.after_each(function()
    expirationd.cfg(g.default_cfg)
end)

function g.test_cfg_default_if_installed()
    t.skip_if(not helpers.is_metrics_supported(), metrics_required_msg)
    t.assert_equals(expirationd.cfg.metrics, true)
end

function g.test_cfg_default_if_uninstalled()
    t.skip_if(helpers.is_metrics_supported(), metrics_not_required_msg)
    t.assert_equals(expirationd.cfg.metrics, false)
end

function g.test_cfg_newindex()
    t.assert_error_msg_content_equals("Use expirationd.cfg{} instead",
                                      function()
                                          expirationd.cfg.any_key = false
                                      end)
end

function g.test_cfg_metrics_set_unset()
    t.skip_if(not helpers.is_metrics_supported(), metrics_required_msg)

    expirationd.cfg({metrics = true})
    t.assert_equals(expirationd.cfg.metrics, true)
    expirationd.cfg({metrics = false})
    t.assert_equals(expirationd.cfg.metrics, false)
end

function g.test_cfg_metrics_multiple_set_unset()
    t.skip_if(not helpers.is_metrics_supported(), metrics_required_msg)

    expirationd.cfg({metrics = true})
    expirationd.cfg({metrics = true})
    t.assert_equals(expirationd.cfg.metrics, true)
    expirationd.cfg({metrics = false})
    expirationd.cfg({metrics = false})
    t.assert_equals(expirationd.cfg.metrics, false)
end

function g.test_cfg_metrics_set_unsupported()
    t.skip_if(helpers.is_metrics_supported(), metrics_not_required_msg)

    t.assert_error_msg_content_equals("metrics >= 0.11.0 is required",
                                      function()
                                          expirationd.cfg({metrics = true})
                                      end)
    t.assert_equals(expirationd.cfg.metrics, false)
end
