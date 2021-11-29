local expirationd = require('expirationd')
local fiber = require('fiber')
local t = require('luatest')

local helpers = require('test.helper')

local g = t.group('callbacks_and_delays', {
    {index_type = 'TREE', engine = 'vinyl'},
    {index_type = 'TREE', engine = 'memtx'},
    {index_type = 'HASH', engine = 'memtx'},
})

g.before_each({index_type = 'TREE'}, function(cg)
    t.skip_if(cg.params.engine == 'vinyl' and not helpers.vinyl_is_supported(),
        'Blocked by https://github.com/tarantool/tarantool/issues/6448')
    cg.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.before_each({index_type = 'HASH'}, function(cg)
    cg.space = helpers.create_space_with_hash_index(cg.params.engine)
end)

g.before_each(function(cg)
    cg.task_name = 'test'
end)

g.after_each(function(cg)
    if cg.task ~= nil then
        cg.task:kill()
    end
    cg.space:drop()
end)

function g.test_delays_and_scan_callbacks(cg)
    local space = cg.space
    local task_name = cg.task_name

    local total = 10
    for i = 1, total do
        space:insert({i, tostring(i)})
    end
    t.assert_equals(space:count{}, total)

    -- To check all delays (iteration and full scan), two full scan
    -- iterations will be performed.
    local first_fullscan_done = false
    local cond = fiber.cond()
    local start_time = 0
    local complete_time = 0

    local check_full_scan_delay = true
    local check_iteration_delay = true
    local iteration_delay = 1
    local full_scan_delay = 2
    local full_scan_success_counter = 0

    local check_full_scan_delay_cb = function()
        start_time = fiber.time()
        if first_fullscan_done and check_full_scan_delay then
            -- Check the full scan delay with an accuracy of 0.1 seconds.
            -- Difference between start time of the second full scan
            -- and complete_time of the first full scan.
            check_full_scan_delay = math.abs(start_time - complete_time
                - full_scan_delay) < 0.1
        end
    end

    local call_counter = function()
        -- Must be called twice.
        full_scan_success_counter = full_scan_success_counter + 1
    end

    local check_iteration_delay_cb = function()
        complete_time = fiber.time()
        if first_fullscan_done then
            cond:signal()
        else
            first_fullscan_done = true
            -- Check the accuracy of iteration delay.
            -- Difference between start time and complete_time of the first full scan.
            if check_iteration_delay then
                check_iteration_delay = math.abs(complete_time - start_time -
                    iteration_delay) < 2
            end
        end
    end

    cg.task = expirationd.start(task_name, space.id,
        helpers.is_expired_true,
        {
            iteration_delay = iteration_delay,
            full_scan_delay = full_scan_delay,
            tuples_per_iteration = 5,
            on_full_scan_start = check_full_scan_delay_cb,
            on_full_scan_success = call_counter,
            on_full_scan_complete = check_iteration_delay_cb,
            vinyl_assumed_space_len = 5, -- iteration_delay will be 1 sec
        }
    )

    cond:wait()
    cg.task:kill()
    cg.task = nil

    t.assert(check_full_scan_delay)
    t.assert(check_iteration_delay)
    t.assert_equals(full_scan_success_counter, 2)
    t.assert_equals(space:count{}, 0)
end
