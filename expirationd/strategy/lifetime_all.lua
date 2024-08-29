local clock = require("clock")
local _, datetime = pcall(require, "datetime")

local function is_tuple_expired_datetime_type(args, tuple)
    local dt = tuple[args.time_create_field]

    local interval = datetime.interval.new({ sec = args.lifetime_in_seconds })
    local now = datetime.now()
    local threshold = now:sub(interval)

    return dt < threshold
end

local function is_tuple_expired_number_type(args, tuple)
    local dt = tuple[args.time_create_field]
    local threshold = clock.time() - args.lifetime_in_seconds

    return dt < threshold
end

local function get_method_is_tuple_expired(space_name, time_create_field)
    local fields = box.space[space_name]:format()
    for _, field in ipairs(fields) do
        if field.name == time_create_field then
            if field.type == 'number' or field.type == 'integer' or field.type == 'unsigned' then
                return is_tuple_expired_number_type
            elseif field.type == 'datetime' then
                return is_tuple_expired_datetime_type
            else
                error('is_tuple_expired not supported field type ' .. field.type)
            end
        end
    end

    local errmsg = ('Field %s not finded in space %s'):format(time_create_field, space_name)
    error(errmsg)
end

return {
    get_method_is_tuple_expired = get_method_is_tuple_expired,
}
