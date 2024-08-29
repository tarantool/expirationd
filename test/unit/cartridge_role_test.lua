local expirationd = require("expirationd")
local fiber = require("fiber")
local t = require("luatest")
local helpers = require("test.helper")
local g = t.group('cartridge_expirationd_role')
local is_cartridge_roles, _ = pcall(require, 'cartridge.roles')

local always_true_func_name = "expirationd_test_always_true"
local always_true_func_name_in_box_func = 'expirationd_test_always_true_bf'
local always_true_func_name_with_side_effect = 'expirationd_test_always_true_se'
local always_true_func_name_with_side_effect_flag = 'expirationd_test_always_true_se_called'
local iterate_pairs_func_name = "expirationd_test_pairs"

g.before_all(function()
    g.default_cfg = { metrics = expirationd.cfg.metrics }
end)

g.before_each(function()
    t.skip_if(not is_cartridge_roles, "cartridge is not installed")

    g.role = require("cartridge.roles.expirationd")
    g.space = helpers.create_space_with_tree_index('memtx')

    -- kill live tasks (it can still live after failed tests)
    for _, t in ipairs(g.role.tasks()) do
        g.role.kill(t)
    end

    g.is_metrics_supported = helpers.is_metrics_supported()

    if _TARANTOOL >= '2' then
        helpers.create_persistent_function(always_true_func_name_in_box_func)
        helpers.create_persistent_function(always_true_func_name_with_side_effect, [[
            function(...)
                return false
            end
        ]])
    end

    rawset(_G, always_true_func_name_with_side_effect_flag, false)
    rawset(_G, always_true_func_name, function() return true end)
    rawset(_G, iterate_pairs_func_name, function() return pairs({}) end)
    rawset(_G, always_true_func_name_with_side_effect, function()
        rawset(_G, always_true_func_name_with_side_effect_flag, true)
    end)
end)

g.after_each(function(g)
    expirationd.cfg(g.default_cfg)
    g.space:drop()
    g.role.stop()
    for _, t in ipairs(g.role.tasks()) do
        g.role.kill(t)
    end

    rawset(_G, always_true_func_name, nil)
    rawset(_G, iterate_pairs_func_name, nil)
end)

function g.test_expirationd_naming_no_role_intersections()
    -- https://www.tarantool.io/ru/doc/latest/book/cartridge/cartridge_api/modules/custom-role/
    for k, _ in pairs(expirationd) do
         for _, reserved in ipairs({"init", "stop", "validate_config",
                                    "apply_config", "get_issues",
                                    "role_name", "hidden", "permanent",}) do
            t.assert_not_equals(k, reserved)
         end
    end
end

function g.test_expirationd_export_all_to_role(cg)
    for k, _ in pairs(expirationd) do
         t.assert_not_equals(cg.role[k], nil)
    end
end

function g.test_expirationd_task_in_role(cg)
    local task_name = "test_task_expirationd"
    local task = expirationd.start(task_name, cg.space.id, helpers.is_expired_debug, {})
    t.assert_not_equals(task, nil)

    t.assert_equals(#cg.role.tasks(), 1)
    t.assert_not_equals(cg.role.task(task_name), nil)

    task:kill()
end

function g.test_stop_all_tasks(cg)
    local is_task_processes = function(name)
        local prev_checked = cg.role.task(name):statistics().checked_count
        local new_checked = nil
        for _ = 1,100 do
            new_checked = cg.role.task(name):statistics().checked_count
            if new_checked ~= prev_checked then
                break
            end
            fiber.yield()
        end
        return new_checked ~= prev_checked
    end

    cg.space:insert({1, "1"})
    local opts = {
        process_expired_tuple = function() return true end,
        iteration_delay = 0,
        full_scan_delay = 0,
    }
    local e_task = expirationd.start("e", cg.space.id, helpers.is_expired_debug, opts)
    t.assert_not_equals(e_task, nil)
    local r_task = cg.role.start("r", cg.space.id, helpers.is_expired_debug, opts)
    t.assert_not_equals(r_task, nil)
    t.assert_equals(is_task_processes("e"), true)
    t.assert_equals(is_task_processes("r"), true)

    cg.role.stop()

    t.assert_equals(is_task_processes("e"), false)
    t.assert_equals(is_task_processes("r"), false)

    e_task:kill()
    r_task:kill()
end

function g.test_validate_config_empty(cg)
    t.assert_equals(cg.role.validate_config({}), true)
    t.assert_equals(cg.role.validate_config({["expirationd"] = {}}), true)
end

local required_test_cases = {
    cfg_empty = {
        ok = true,
        cfg = { ["cfg"] = {
        }}
    },
    cfg_invalid_param = {
        ok = false,
        err = "unexpected argument cfg.any to get_cfg",
        cfg = { ["cfg"] = {
            any = 1,
        }}
    },
    cfg_metrics = {
        ok = true,
        cfg = { ["cfg"] = {
            metrics = true,
        }}
    },
    cfg_metrics_invalid_value = {
        ok = false,
        err = "bad argument cfg.metrics to get_cfg (?boolean expected, got number)",
        cfg = { ["cfg"] = {
            metrics = 12,
        }}
    },
    all_ok_space_number = {
        ok = true,
        cfg = { ["task_name"] = {
            space = 1,
            is_expired = always_true_func_name,
        }}
    },
    all_ok_space_string = {
        ok = true,
        cfg = { ["task_name"] = {
            space = "space name",
            is_expired = always_true_func_name,
        }}
    },
    all_ok_empty_opts = {
        ok = true,
        cfg = { ["task_name"] = {
            space = 1,
            is_expired = always_true_func_name,
            options = {}
        }}
    },
    task_cfg = {
        ok = false,
        err = "unexpected argument cfg.space to get_cfg",
        cfg = { ["cfg"] = {
            space = "space name",
            is_expired = always_true_func_name,
        }}
    },
    not_table = {
        ok = false,
        err = "task configuration must be a table",
        cfg = { ["task_name"] = 123 },
    },
    no_space = {
        ok = false,
        err = "bad argument task_conf.space to check_task_description (string|number expected, got nil)",
        cfg = { ["task_name"] = {
            is_expired = always_true_func_name,
        }}
    },
    no_expired = {
        ok = false,
        err = "is_expired is required or options.args is required",
        cfg = { ["task_name"] = {
            space = 1,
        }}
    },
    invalid_name = {
        ok = false,
        err = "task name must be a string",
        cfg = { [3] = {
            space = "space name",
            is_expired = always_true_func_name,
        }}
    },
    invalid_space = {
        ok = false,
        err = "bad argument task_conf.space to check_task_description (string|number expected, got table)",
        cfg = { ["task_name"] = {
            space = {},
            is_expired = always_true_func_name,
        }}
    },
    invalid_expired = {
        ok = false,
        err = "bad argument task_conf.is_expired to check_task_description (?string expected, got number)",
        cfg = { ["task_name"] = {
            space = 1,
            is_expired = 0,
        }}
    },
    lifetime_in_seconds_string = {
        ok = false,
        err = "bad argument args.lifetime_in_seconds to check_default_is_expired_args (number expected, got string)",
        cfg = { ["task_name"] = {
            space = 1,
            options = {
                args = {
                    time_create_field = "dt",
                    lifetime_in_seconds = "too",
                },
            },
        }},
    },
    time_create_field_number = {
        ok = false,
        err = "bad argument args.time_create_field to check_default_is_expired_args (string expected, got number)",
        cfg = { ["task_name"] = {
            space = 1,
            options = {
                args = {
                    time_create_field = 123,
                    lifetime_in_seconds = 123,
                },
            },
        }},
    },
    declare_only_lifetime_in_seconds = {
        ok = false,
        err = "bad argument args.time_create_field to check_default_is_expired_args (string expected, got nil)",
        cfg = { ["task_name"] = {
            space = 1,
            options = {
                args = {
                    lifetime_in_seconds = 123,
                },
            },
        }},
    },
    declare_only_time_create_field = {
        ok = false,
        err = "bad argument args.lifetime_in_seconds to check_default_is_expired_args (number expected, got nil)",
        cfg = { ["task_name"] = {
            space = 1,
            options = {
                args = {
                    time_create_field = 'dt',
                },
            },
        }},
    },
    declare_all_opt_without_is_expired = {
        ok = true,
        cfg = { ["task_name"] = {
            space = 1,
            options = {
                args = {
                    lifetime_in_seconds = 123,
                    time_create_field = 'dt',
                },
            },
        }},
    },
}

for k, case in pairs(required_test_cases) do
    g["test_validate_config_required_" .. k] = function(cg)
        local status, res = pcall(cg.role.validate_config, {expirationd = case.cfg})
        if case.ok then
            t.assert_equals(status, true)
            t.assert_equals(res, true)
        else
            t.assert_equals(status, false)
            t.assert_str_contains(res, case.err)
        end
    end
end

local function create_valid_required(args)
    local new_args = table.deepcopy(args)
    new_args["space"] = 1
    new_args["is_expired"] = always_true_func_name
    return {expirationd = {["task_name"] = new_args}}
end

local additional_opts_test_cases = {
   is_master_boolean = {
        ok = true,
        cfg = {is_master_only = true}
   },
   is_master_not_boolean = {
        ok = false,
        err = "bad argument task_conf.is_master_only to check_task_description (?boolean expected, got table)",
        cfg = {is_master_only = {}}
   },
}

for k, case in pairs(additional_opts_test_cases) do
    g["test_validate_config_additional_" .. k] = function(cg)
        local cfg = create_valid_required(case.cfg)
        local status, res = pcall(cg.role.validate_config, cfg)
        if case.ok then
            t.assert_equals(status, true)
            t.assert_equals(res, true)
        else
            t.assert_equals(status, false)
            t.assert_str_contains(res, case.err)
        end
    end
end

local options_cases = {
    nilval = {
        ok = true,
        options = nil,
    },
    empty = {
        ok = true,
        options = {},
    },
    number = {
        ok = false,
        err = "unexpected argument opts[1] to check_task_options",
        options = {[1] = "any"},
    },
    unsupported = {
        ok = false,
        err = "unexpected argument opts.unsupported_option to check_task_options",
        options = {unsupported_option = "any"},
    },
    args_table = {
        ok = true,
        options = {args = {}},
    },
    args_string = {
        ok = true,
        options = {args = "string"},
    },
    args_number = {
        ok = true,
        options = {args = 13},
    },
    args_boolean = {
        ok = true,
        options = {args = true},
    },
    atomic_iteration_boolean = {
        ok = true,
        options = {atomic_iteration = true},
    },
    atomic_iteration_invalid = {
        ok = false,
        err = "bad argument opts.atomic_iteration to check_task_options (?boolean expected, got string)",
        options = {atomic_iteration = "string"},
    },
    force_boolean = {
        ok = true,
        options = {force = false},
    },
    force_invalid = {
        ok = false,
        err = "bad argument opts.force to check_task_options (?boolean expected, got string)",
        options = {force = "string"},
    },
    force_allow_functional_index_boolean = {
        ok = true,
        options = {force_allow_functional_index = true },
    },
    force_allow_functional_index_invalid = {
        ok = false,
        err = "bad argument opts.force_allow_functional_index to check_task_options (?boolean expected, got number)",
        options = {force_allow_functional_index = 13},
    },
    full_scan_delay_number = {
        ok = true,
        options = {full_scan_delay = 13},
    },
    full_scan_delay_invalid = {
        ok = false,
        err = "bad argument opts.full_scan_delay to check_task_options (?number expected, got string)",
        options = {full_scan_delay = "string"},
    },
    full_scan_time_number = {
        ok = true,
        options = {full_scan_time = 23},
    },
    full_scan_time_invalid = {
        ok = false,
        err = "bad argument opts.full_scan_time to check_task_options (?number expected, got boolean)",
        options = {full_scan_time = true},
    },
    index_number = {
        ok = true,
        options = {index = 0},
    },
    index_string = {
        ok = true,
        options = {index = "string"},
    },
    index_invalid = {
        ok = false,
        err = "bad argument opts.index to check_task_options (?number|string expected, got boolean)",
        options = {index = true},
    },
    iterate_with_func = {
        ok = true,
        options = {iterate_with = always_true_func_name},
    },
    iterate_with_string = {
        ok = true,
        options = {iterate_with = "always_true_func_name"},
    },
    iterate_with_number = {
        ok = false,
        err = "bad argument opts.iterate_with to check_task_options (?string expected, got number)",
        options = {iterate_with = 123},
    },
    iterate_with_table = {
        ok = false,
        err = "bad argument opts.iterate_with to check_task_options (?string expected, got table)",
        options = {iterate_with = {}},
    },
    iteration_delay_number = {
        ok = true,
        options = {iteration_delay = 876},
    },
    iteration_delay_invalid = {
        ok = false,
        err = "bad argument opts.iteration_delay to check_task_options (?number expected, got table)",
        options = {iteration_delay = {"table"}},
    },
    iterator_type_number = {
        ok = true,
        options = {iterator_type = box.index.GE},
    },
    iterator_type_string = {
        ok = true,
        options = {iterator_type = "GE"},
    },
    iterator_type_invalid = {
        ok = false,
        err = "bad argument opts.iterator_type to check_task_options (?number|string expected, got boolean)",
        options = {iterator_type = false},
    },
    on_full_scan_complete_func = {
        ok = true,
        options = {on_full_scan_complete = always_true_func_name},
    },
    on_full_scan_complete_number = {
        ok = false,
        err = "bad argument opts.on_full_scan_complete to check_task_options (?string expected, got number)",
        options = {on_full_scan_complete = 123},
    },
    on_full_scan_error_func = {
        ok = true,
        options = {on_full_scan_error = always_true_func_name},
    },
    on_full_scan_error_number = {
        ok = false,
        err = "bad argument opts.on_full_scan_error to check_task_options (?string expected, got number)",
        options = {on_full_scan_error = 123},
    },
    on_full_scan_start_func = {
        ok = true,
        options = {on_full_scan_start = always_true_func_name},
    },
    on_full_scan_start_number = {
        ok = false,
        err = "bad argument opts.on_full_scan_start to check_task_options (?string expected, got number)",
        options = {on_full_scan_start = 123},
    },
    on_full_scan_success_func = {
        ok = true,
        options = {on_full_scan_success = always_true_func_name},
    },
    on_full_scan_success_number = {
        ok = false,
        err = "bad argument opts.on_full_scan_success to check_task_options (?string expected, got number)",
        options = {on_full_scan_success = 123},
    },
    process_expired_tuple_func = {
        ok = true,
        options = {process_expired_tuple = always_true_func_name},
    },
    process_expired_tuple_number = {
        ok = false,
        err = "bad argument opts.process_expired_tuple to check_task_options (?string expected, got number)",
        options = {process_expired_tuple = 123},
    },
    process_while_func = {
        ok = true,
        options = {process_while = always_true_func_name},
    },
    process_while_number = {
        ok = false,
        err = "bad argument opts.process_while to check_task_options (?string expected, got number)",
        options = {process_while = 123},
    },
    start_key_func = {
        ok = true,
        options = {start_key = always_true_func_name},
    },
    start_key_table = {
        ok = true,
        options = {start_key = {1, 2, 3}},
    },
    tuples_per_iteration_number = {
        ok = true,
        options = {tuples_per_iteration = 11},
    },
    tuples_per_iteration_invalid = {
        ok = false,
        err = "bad argument opts.tuples_per_iteration to check_task_options (?number expected, got table)",
        options = {tuples_per_iteration = {"table"}},
    },
    vinyl_assumed_space_len_factor_number = {
        ok = true,
        options = {vinyl_assumed_space_len_factor = 333},
    },
    vinyl_assumed_space_len_factor_invalid = {
        ok = false,
        err = "bad argument opts.vinyl_assumed_space_len_factor to check_task_options (?number expected, got boolean)",
        options = {vinyl_assumed_space_len_factor = false},
    },
    vinyl_assumed_space_len_number = {
        ok = true,
        options = {vinyl_assumed_space_len = 1},
    },
    vinyl_assumed_space_len_invalid = {
        ok = false,
        err = "bad argument opts.vinyl_assumed_space_len to check_task_options (?number expected, got string)",
        options = {vinyl_assumed_space_len = "string"},
    },
}

for k, case in pairs(options_cases) do
    g["test_validate_config_option_" .. k] = function(cg)
        local cfg = create_valid_required({options = case.options})
        local status, res = pcall(cg.role.validate_config, cfg)
        if case.ok then
            t.assert_equals(status, true)
            t.assert_equals(res, true)
        else
            t.assert_equals(status, false)
            t.assert_str_contains(res, case.err)
        end
    end
end

function g.test_apply_config_start_tasks(cg)
    local task_name1 = "apply_config_test1"
    local task_name2 = "apply_config_test2"

    cg.role.apply_config({
        expirationd = {
            [task_name1] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
            [task_name2] = {
                space = g.space.id,
                is_expired = always_true_func_name,
                options = {}
            },
        }
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 2)
    t.assert_not_equals(cg.role.task(task_name1), nil)
    t.assert_not_equals(cg.role.task(task_name2), nil)
end

function g.test_apply_config_cfg_empty_do_nothing(cg)
    t.skip_if(not g.is_metrics_supported,
              "metrics >= 0.11.0 is not installed")
    local task_name1 = "apply_config_test1"

    expirationd.cfg({metrics = not g.default_cfg.metrics})
    t.assert_equals(expirationd.cfg.metrics, not g.default_cfg.metrics)

    cg.role.apply_config({
        expirationd = {
            ["cfg"] = {},
            [task_name1] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
        }
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 1)
    t.assert_not_equals(cg.role.task(task_name1), nil)
    t.assert_equals(expirationd.cfg.metrics, not g.default_cfg.metrics)
end

function g.test_apply_config_cfg_metrics_default(cg)
    t.skip_if(not g.is_metrics_supported,
              "metrics >= 0.11.0 is not installed")
    local task_name1 = "apply_config_test1"

    cg.role.apply_config({
        expirationd = {
            [task_name1] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
        }
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 1)
    t.assert_not_equals(cg.role.task(task_name1), nil)
    t.assert_equals(expirationd.cfg.metrics, g.default_cfg.metrics)
end

function g.test_apply_config_cfg_metrics(cg)
    t.skip_if(not g.is_metrics_supported,
              "metrics >= 0.11.0 is not installed")
    local task_name1 = "apply_config_test1"

    for _, value in ipairs({false, true}) do
        cg.role.apply_config({
            expirationd = {
                ["cfg"] = {
                    metrics = value,
                },
                [task_name1] = {
                    space = g.space.id,
                    is_expired = always_true_func_name,
                },
            }
        }, {is_master = false})

        t.assert_equals(#cg.role.tasks(), 1)
        t.assert_not_equals(cg.role.task(task_name1), nil)
        t.assert_equals(expirationd.cfg.metrics, value)
    end
end

function g.test_apply_config_start_cfg_task(cg)
    local task_name = "cfg"
    cg.role.apply_config({
        expirationd = {
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
        }
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 1)
    t.assert_not_equals(cg.role.task(task_name), nil)
end

function g.test_apply_config_start_cfg_task_with_box_func(cg)
    t.skip_if(_TARANTOOL < '2', 'Restricted support in Tarantool 1.10')
    local task_name = 'cfg'
    cg.role.apply_config({
        expirationd = {
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name_in_box_func
            }
        }
    }, { is_master = false })

    t.assert_equals(#cg.role.tasks(), 1)
    t.assert_not_equals(cg.role.task(task_name), nil)
end

function g.test_apply_config_start_cfg_task_with_correct_order(cg)
    t.skip_if(_TARANTOOL < '2', 'Restricted support in Tarantool 1.10')
    t.assert_not(rawget(_G, always_true_func_name_with_side_effect_flag))
    local task_name = 'cfg'
    cg.space:insert({1, "1"})
    cg.role.apply_config({
        expirationd = {
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name_with_side_effect
            }
        }
    }, { is_master = false })

    t.assert_equals(#cg.role.tasks(), 1)
    t.assert_not_equals(cg.role.task(task_name), nil)
    t.assert(rawget(_G, always_true_func_name_with_side_effect_flag))
end

function g.test_apply_config_start_task_with_all_options(cg)
    local task_name = "apply_config_test"
    local options =  {
        args = {"any"},
        atomic_iteration = false,
        force = false,
        force_allow_functional_index = true,
        full_scan_delay = 1,
        full_scan_time = 1,
        index = 0,
        iterate_with = iterate_pairs_func_name,
        iteration_delay = 1,
        iterator_type = "ALL",
        on_full_scan_complete = always_true_func_name,
        on_full_scan_error = always_true_func_name,
        on_full_scan_start = always_true_func_name,
        on_full_scan_success = always_true_func_name,
        process_expired_tuple = always_true_func_name,
        process_while = always_true_func_name,
        start_key = {1},
        tuples_per_iteration = 100,
        vinyl_assumed_space_len_factor = 1,
        vinyl_assumed_space_len = 100,
    }

    cg.role.apply_config({
        expirationd = {
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name,
                options = options,
            }
        },
    }, {is_master = true})

    t.assert_equals(#cg.role.tasks(), 1)
    t.assert_not_equals(cg.role.task(task_name), nil)

    -- pass an invalid option
    options.full_scan_time = -100
    local ok, _ = pcall(cg.role.apply_config, {
        expirationd = {
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name,
                options = options,
            }
        },
    }, {is_master = true})
    t.assert_equals(ok, false)
    t.assert_equals(#cg.role.tasks(), 0)
end

g.before_test('test_apply_config_skip_is_master_only', function()
    box.cfg({read_only = true})
end)

g.after_test('test_apply_config_skip_is_master_only', function()
    box.cfg({read_only = false})
end)

function g.test_apply_config_skip_is_master_only(cg)
    local task_name = "apply_config_test"

    cg.role.apply_config({
        expirationd = {
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name,
                is_master_only = true,
            },
        },
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 0)
end

function g.test_apply_config_start_is_master_only(cg)
    local task_name = "apply_config_test"

    cg.role.apply_config({
        expirationd = {
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name,
                is_master_only = true,
            },
        },
    }, {is_master = true})

    t.assert_equals(#cg.role.tasks(), 1)
    t.assert_not_equals(cg.role.task(task_name), nil)
end

function g.test_apply_config_empty_repeat_kill_all_config_tasks(cg)
    local local_name1 = "local_task1"
    local local_name2 = "local_task2"
    local config_name1 = "config_task1"
    local config_name2 = "config_task2"
    local config_name3 = "config_task3"

    expirationd.start(local_name1, cg.space.id, function() return true end)
    expirationd.start(local_name2, cg.space.id, function() return true end)

    cg.role.apply_config({
        expirationd = {
            [config_name1] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
            [config_name2] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
            [config_name3] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
        },
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 5)
    t.assert_not_equals(cg.role.task(local_name1), nil)
    t.assert_not_equals(cg.role.task(local_name2), nil)
    t.assert_not_equals(cg.role.task(config_name1), nil)
    t.assert_not_equals(cg.role.task(config_name2), nil)
    t.assert_not_equals(cg.role.task(config_name3), nil)

    cg.role.apply_config({}, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 2)
    t.assert_not_equals(cg.role.task(local_name1), nil)
    t.assert_not_equals(cg.role.task(local_name2), nil)
end

function g.test_apply_config_empty_repeat_handle_custom_stops(cg)
    local config_name1 = "config_task1"
    local config_name2 = "config_task2"
    local config_name3 = "config_task3"

    cg.role.apply_config({
        expirationd = {
            [config_name1] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
            [config_name2] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
            [config_name3] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
        },
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 3)
    t.assert_not_equals(cg.role.task(config_name1), nil)
    t.assert_not_equals(cg.role.task(config_name2), nil)
    t.assert_not_equals(cg.role.task(config_name3), nil)

    cg.role.task(config_name1):stop()
    cg.role.task(config_name2):kill()
    cg.role.apply_config({}, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 0)
end

function g.test_apply_config_repeat_kill_old_config_tasks(cg)
    local local_name1 = "local_task1"
    local local_name2 = "local_task2"
    local config_name1 = "config_task1"
    local config_name2 = "config_task2"
    local config_name3 = "config_task3"

    expirationd.start(local_name1, cg.space.id, function() return true end)
    expirationd.start(local_name2, cg.space.id, function() return true end)

    cg.role.apply_config({
        expirationd = {
            [config_name1] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
            [config_name2] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
            [config_name3] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
        },
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 5)
    t.assert_not_equals(cg.role.task(local_name1), nil)
    t.assert_not_equals(cg.role.task(local_name2), nil)
    t.assert_not_equals(cg.role.task(config_name1), nil)
    t.assert_not_equals(cg.role.task(config_name2), nil)
    t.assert_not_equals(cg.role.task(config_name3), nil)

    cg.role.apply_config({
        expirationd = {
            [config_name2] = {
                space = cg.space.id,
                is_expired = always_true_func_name,
            },
        },
    }, {is_master = false})

    t.assert_equals(#cg.role.tasks(), 3)
    t.assert_not_equals(cg.role.task(local_name1), nil)
    t.assert_not_equals(cg.role.task(local_name2), nil)
    t.assert_not_equals(cg.role.task(config_name2), nil)
end
