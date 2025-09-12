local t = require("luatest")

local clock = require('clock')
local expirationd = require("expirationd")
local fiber = require("fiber")

local case_list = {
    {
        num = 1,
        type_name = 'boolean',
        now = function()
            return true
        end,
        err = 'is_tuple_expired not supported field type boolean'
    },
    {
        num = 2,
        type_name = 'unsigned',
        now = function()
            return math.floor(clock.time())
        end
    },
    {
        num = 3,
        type_name = 'integer',
        now = function()
            return math.floor(clock.time())
        end
    },
    {
        num = 4,
        type_name = 'number',
        now = function()
            return clock.time()
        end
    },
    {
        num = 5,
        type_name = 'string',
        now = function()
            return 'now'
        end,
        err = 'is_tuple_expired not supported field type string'
    },
    {
        num = 6,
        type_name = 'map',
        now = function()
            return { too = 'foo'}
        end,
        err = 'is_tuple_expired not supported field type map'
    },
}

if _TARANTOOL >= '2.11' then
    local datetime = require("datetime")
    table.insert(case_list, {
        num = #case_list + 1,
        type_name = 'datetime',
        now = function()
            return datetime.now()
        end,
    })
end

local g = t.group(nil, case_list)
local task_name = "lifetime_all"
local issue_message

g.before_all(function ()
    t.skip_if(_TARANTOOL > '2', 'Not supported on 3.* versions')
end)

local function set_issue(issue_task_name, message, ...)
    issue_message = ('Expirationd warning, task "%s": ' .. message):format(issue_task_name, ...)
end

local function unset_issue(_)
    issue_message = nil
end

g.before_each(function()
    for _, task in ipairs(expirationd.tasks()) do
         if task == task_name then
             expirationd.task(task_name):kill()
         end
    end

    rawset(_G, 'expirationd_enable_issue', set_issue)
    rawset(_G, 'expirationd_disable_issue', unset_issue)
end)

g.after_each(function()
    rawset(_G, 'expirationd_enable_issue', nil)
    rawset(_G, 'expirationd_disable_issue', nil)

    local s = box.space.lifetime_all_test
    if s ~= nil then
        s:drop()
    end
end)

function g.test_case(case)
    local s = box.schema.space.create('lifetime_all_test')
    s:format({
        {name = 'id', type = 'unsigned'},
        {name = 'dt', type = case.params.type_name},
        {name = 'data', type = 'any'},
    })

    s:create_index('id', {
        parts = { {field = 'id'} },
        unique = true,
        type = 'TREE',
    })

    local task = expirationd.start(
        task_name,
        s.id,
        nil,
        {
            args = {
                lifetime_in_seconds = 3,
                time_create_field = 'dt',
            }
        }
    )

    local t0 = clock.time()
    s:insert({1, case.params.now(), 'Foo bar'})

    fiber.sleep(2.5)
    s:insert({2, case.params.now(), 'Bazz'})


    local target
    while true do
        local t1 = clock.time()
        target = t1 - t0
        if target > 5 then
            break
        end

        local tuple_1 = s:get(1)
        local tuple_2 = s:get(2)
        if tuple_1 == nil and tuple_2 ~= nil then
            break
        end

        fiber.sleep(0.1)
    end

    local actual_issue_message = issue_message
    task:stop()

    if case.params.err == nil then
        t.assert_ge(target, 3.0)
        t.assert_lt(target, 4.5)
        t.assert_equals(actual_issue_message, nil)
    else
        t.assert_ge(target, 5.0)
        t.assert_str_contains(actual_issue_message, case.params.err)
    end
end
