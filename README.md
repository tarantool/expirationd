# expirationd -  data expiration with custom quirks.

This package can turn Tarantool into a persistent memcache replacement,
but is powerful enough so that  your own expiration strategy can be defined.

You define two functions: one takes a tuple as an input and returns
true in case it's expirted and false otherwise. The other takes the
tuple and performs the expiry itself: either deletes it (memcache), or
does something smarter, like put a smaller representation of the data
being deleted into some other space.

### Example
``` lua
box.cfg{}
space = box.space.old
job_name = 'clean_all'
expirationd = require('expirationd')
function is_expired(args, tuple)
  return true
end
function delete_tuple(space_id, args, tuple)
  box.space[space_id]:delete{tuple[1]}
end
expirationd.start(job_name, space.id, is_expired, {
    process_expired_tuple = delete_tuple, args = nil,
    tuple_per_item = 50, full_scan_time = 3600
})
```

## Expirationd API

### `expirationd.start (name, space_id, is_tuple_expired, options)`

Run a named task

* `name` - task name
* `space_id` - space to look in for expired tuples
* `is_tuple_expired` - a function, must accept tuple and return true/false
  (is tuple expired or not), receives `(args, tuple)` as arguments
opt
* `options` -- (table with named options, may be nil)
  * `process_expired_tuple` - applied to expired tuples, receives `(space_id, args, tuple)`
    as arguments. Can be nil: by default tuples are removed
  * `args` - passed to `is_tuple_expired()` and `process_expired_tuple()` as additional context
  * `tuples_per_iteration` - number of tuples will be checked by one iteration
  * `full_scan_time` - time required for full index scan (in seconds)
  * `force` - run, even on replica

### `expirationd.kill (name)`

Kill an existing task with name 'name'

* `name` - task's name

### `expirationd.stats ([name])`

if `name` is nil, then return map of `name`:`stats`, else return map with stats.

* `name` - task's name, may be nil

### `expirationd.task (name)`

Get task with name `name`

### `expirationd.tasks ()`

Get copy of task list

### `expirationd.update ()`

Update expirationd version and restart all tasks

## Task API

### `task:start()`

Force start `task` (old guardian_fiber will be garbage collected,
so do `task:stop()` before or `task:restart()` instead)

### `task:stop()`

### `task:restart()`

### `task:kill()`

Stop task and delete it from list of tasks.

### `task:statistics()`

Get statistics of task

## Testing

Simply start `tarantool test.lua`
