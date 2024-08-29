local expirationd = require('expirationd')
local fiber = require('fiber')
local t = require('luatest')
local helpers = require('test.helper')

local g = t.group('tarantool_expirationd_role')

local always_true_func_name = 'expirationd_test_always_true'
local always_true_func_name_in_box_func = 'expirationd_test_always_true_bf'
local always_true_func_name_with_side_effect = 'expirationd_test_always_true_se'
local always_true_func_name_with_side_effect_flag = 'expirationd_test_always_true_se_called'
local iterate_pairs_func_name = 'expirationd_test_pairs'

g.before_all(function()
    t.skip_if(not helpers.tarantool_role_is_supported(),
             'Tarantool role is supported only for Tarantool starting from v3.0.0')
    g.default_cfg = { metrics = expirationd.cfg.metrics }
end)

g.before_each(function()
    g.role = require('roles.expirationd')
    g.space = helpers.create_space_with_tree_index('memtx')

    -- Kill live tasks (it can still live after failed tests).
    for _, t in ipairs(expirationd.tasks()) do
        local task = expirationd.task(t)
        task:stop()
    end

    g.is_metrics_supported = helpers.is_metrics_supported()

    helpers.create_persistent_function(always_true_func_name_in_box_func)
    helpers.create_persistent_function(always_true_func_name_with_side_effect, [[
        function(...)
            return false
        end
    ]])

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
    for _, t in ipairs(expirationd.tasks()) do
        local task = expirationd.task(t)
        task:stop()
    end

    rawset(_G, always_true_func_name, nil)
    rawset(_G, iterate_pairs_func_name, nil)
end)

local required_test_cases = {
    cfg_empty = {
        ok = true,
        cfg = { ["cfg"] = {
        }},
    },
    cfg_invalid_param = {
        ok = false,
        err = "unexpected argument cfg.any to get_cfg",
        cfg = { ["cfg"] = {
            any = 1,
        }},
    },
    cfg_metrics = {
        ok = true,
        cfg = { ["cfg"] = {
            metrics = true,
        }},
    },
    cfg_metrics_invalid_value = {
        ok = false,
        err = "bad argument cfg.metrics to get_cfg (?boolean expected, got number)",
        cfg = { ["cfg"] = {
            metrics = 12,
        }},
    },
    all_ok_space_number = {
        ok = true,
        cfg = { ["task_name"] = {
            space = 1,
            is_expired = always_true_func_name,
        }},
    },
    all_ok_space_string = {
        ok = true,
        cfg = { ["task_name"] = {
            space = "space name",
            is_expired = always_true_func_name,
        }},
    },
    all_ok_empty_opts = {
        ok = true,
        cfg = { ["task_name"] = {
            space = 1,
            is_expired = always_true_func_name,
            options = {}
        }},
    },
    all_ok_task_cfg = {
        ok = false,
        err = "unexpected argument cfg.space to get_cfg",
        cfg = { ["cfg"] = {
            space = "space name",
            is_expired = always_true_func_name,
        }},
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
        }},
    },
    no_expired = {
        ok = false,
        err = "is_expired is required or options.args is required",
        cfg = { ["task_name"] = {
            space = 1,
        }},
    },
    invalid_name = {
        ok = false,
        err = "task name must be a string",
        cfg = { [3] = {
            space = "space name",
            is_expired = always_true_func_name,
        }},
    },
    invalid_space = {
        ok = false,
        err = "bad argument task_conf.space to check_task_description (string|number expected, got table)",
        cfg = { ["task_name"] = {
            space = {},
            is_expired = always_true_func_name,
        }},
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
                }
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
                }
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
        local status, res = pcall(cg.role.validate, case.cfg)
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
    return {["task_name"] = new_args}
end

local additional_opts_test_cases = {
   is_master_boolean = {
        ok = true,
        cfg = {is_master_only = true},
   },
   is_master_not_boolean = {
        ok = false,
        err = "bad argument task_conf.is_master_only to check_task_description (?boolean expected, got table)",
        cfg = {is_master_only = {}},
   },
}

for k, case in pairs(additional_opts_test_cases) do
    g["test_validate_config_additional_" .. k] = function(cg)
        local cfg = create_valid_required(case.cfg)
        local status, res = pcall(cg.role.validate, cfg)
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
    on_full_scan_error_func = {
        ok = true,
        options = {on_full_scan_error = always_true_func_name},
    },
    on_full_scan_start_func = {
        ok = true,
        options = {on_full_scan_start = always_true_func_name},
    },
    on_full_scan_success_func = {
        ok = true,
        options = {on_full_scan_success = always_true_func_name},
    },
    process_expired_tuple_func = {
        ok = true,
        options = {process_expired_tuple = always_true_func_name},
    },
    process_while_func = {
        ok = true,
        options = {process_while = always_true_func_name},
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
        local status, res = pcall(cg.role.validate, cfg)
        if case.ok then
            t.assert_equals(status, true)
            t.assert_equals(res, true)
        else
            t.assert_equals(status, false)
            t.assert_str_contains(res, "roles.expirationd:")
            t.assert_str_contains(res, case.err)
        end
    end
end

function g.test_apply_config_start_tasks(cg)
    local task_name1 = "apply_config_test1"
    local task_name2 = "apply_config_test2"
    pcall(cg.role.apply, {
        [task_name1] = {
            space = g.space.id,
            is_expired = always_true_func_name,
        },
        [task_name2] = {
            space = g.space.id,
            is_expired = always_true_func_name,
            options = {}
        },
    }, {is_master = false})

    helpers.retrying({}, function()
        t.assert_not_equals(expirationd.task(task_name1), nil)
        t.assert_not_equals(expirationd.task(task_name2), nil)
    end)
end

function g.test_apply_config_cfg_metrics_default(cg)
    t.skip_if(not g.is_metrics_supported,
              "metrics >= 0.11.0 is not installed")
    local task_name1 = "apply_config_test1"

    cg.role.apply({
            [task_name1] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
    }, {is_master = false})

    helpers.retrying({}, function()
        t.assert_not_equals(expirationd.task(task_name1), nil)
        t.assert_equals(expirationd.cfg.metrics, g.default_cfg.metrics)
    end)
end

function g.test_apply_config_cfg_metrics(cg)
    t.skip_if(not g.is_metrics_supported,
              "metrics >= 0.11.0 is not installed")
    local task_name1 = "apply_config_test1"

    for _, value in ipairs({false, true}) do
        cg.role.apply({
                ["cfg"] = {
                    metrics = value,
                },
                [task_name1] = {
                    space = g.space.id,
                    is_expired = always_true_func_name,
                },
        }, {is_master = false})

    helpers.retrying({}, function()
        t.assert_not_equals(expirationd.task(task_name1), nil)
        t.assert_equals(expirationd.cfg.metrics, value)
    end)
    end
end

function g.test_apply_config_start_cfg_task(cg)
    local task_name = "cfg"
    cg.role.apply({
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name,
            },
    }, {is_master = false})

    helpers.retrying({}, function()
        t.assert_not_equals(expirationd.task(task_name), nil)
    end)
end

function g.test_apply_config_start_cfg_task_with_box_func(cg)
    local task_name = 'cfg'
    cg.role.apply({
            [task_name] = {
                space = g.space.id,
                is_expired = always_true_func_name_in_box_func
            },
    }, { is_master = false })

    helpers.retrying({}, function()
        t.assert_not_equals(expirationd.task(task_name), nil)
    end)
end

function g.test_apply_config_start_cfg_task_with_correct_order(cg)
    t.assert_not(rawget(_G, always_true_func_name_with_side_effect_flag))
    local task_name = 'cfg'
    cg.space:insert({1, "1"})
    cg.role.apply({
        [task_name] = {
            space = g.space.id,
            is_expired = always_true_func_name_with_side_effect
        },
    }, { is_master = false })

    helpers.retrying({}, function()
        t.assert_not_equals(expirationd.task(task_name), nil)
        t.assert(rawget(_G, always_true_func_name_with_side_effect_flag))
    end)
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

    cg.role.apply({
        [task_name] = {
            space = g.space.id,
            is_expired = always_true_func_name,
            options = options,
        },
    }, {is_master = true})

    helpers.retrying({}, function()
        t.assert_not_equals(expirationd.task(task_name), nil)
    end)

    -- pass an invalid option
    options.full_scan_time = -100
    pcall(cg.role.apply, {
        [task_name] = {
            space = g.space.id,
            is_expired = always_true_func_name,
            options = options,
        },
    }, {is_master = true})

    helpers.retrying({}, function()
        local err = pcall(expirationd.task, task_name)
        t.assert_equals(err, false)
    end)
end

function g.test_apply_config_skip_is_master_only(cg)
    local task_name = "apply_config_test"

    t.assert_equals(box.info.ro, false)
    box.cfg{read_only=true}

    cg.role.apply({
        [task_name] = {
            space = g.space.id,
            is_expired = always_true_func_name,
            is_master_only = true,
        },
    }, {is_master = false})

    fiber.yield()
    helpers.retrying({}, function()
        t.assert_equals(#expirationd.tasks(), 0)
    end)

    box.cfg{read_only=false}
end
