# Tarantool expiriation daemon

## API

### `expirationd.run_task (name, space_no, is_tuple_expired, process_expired_tuple, args, tuples_per_item, full_scan_time)`
* `name` - task name
* `space_no` - space to look in for expired tuples
* `is_tuple_expired` - a function, must accept tuple and return true/false (is tuple expired or not), receives `(args, tuple)` as arguments
* `process_expired_tuple` - applied to expired tuples, receives `(space_no, args, tuple)` as arguments
* `args` - passed to `is_tuple_expired()` and `process_expired_tuple()` as additional context
* `tuples_per_iter` - number of tuples will be checked by one itaration
* `full_scan_time` - time required for full index scan (in seconds)

### `expirationd.kill_task (name)`
* `name` - task's name

### `expirationd.show_task_list (print_head)`
* `print_head` - print table head

### `expirationd.task_details (task_name)`
* `name` - task's name

## Testing

Simply start `tarantool test.lua`
