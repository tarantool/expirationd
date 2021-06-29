# expirationd -  data expiration with custom quirks.

This package can turn Tarantool into a persistent memcache replacement,
but is powerful enough so that  your own expiration strategy can be defined.

You define two functions: one takes a tuple as an input and returns
true in case it's expired and false otherwise. The other takes the
tuple and performs the expiry itself: either deletes it (memcache), or
does something smarter, like put a smaller representation of the data
being deleted into some other space.

### Example
``` lua
box.cfg{}
space = box.space.old
job_name = "clean_all"
expirationd = require("expirationd")
function is_expired(args, tuple)
  return true
end
function delete_tuple(space_id, args, tuple)
  box.space[space_id]:delete{tuple[1]}
end
expirationd.start(job_name, space.id, is_expired, {
    process_expired_tuple = delete_tuple, args = nil,
    tuples_per_iteration = 50, full_scan_time = 3600
})
```

## Expirationd API

### `expirationd.start (name, space_id, is_tuple_expired, options)`

Run a scheduled task to check and process (expire) tuples in a given space.

* `name` - task name
* `space_id` - space to look in for expired tuples
* `is_tuple_expired` - a function, must accept tuple and return true/false
  (is tuple expired or not), receives `(args, tuple)` as arguments
* `options` -- (table with named options, may be nil)
    * `process_expired_tuple` - Applied to expired tuples, receives (space_id, args, tuple) as arguments.
     Can be nil: by default, tuples are removed.
    * `index` - Name or id of the index to iterate on. If omitted, will use the primary index.
     If there's no index with this name, will throw an error.
     Supported index types are TREE and HASH, using other types will result in an error.
    * `iterator_type` - Type of the iterator to use, as string or box.index constant, for example, "EQ" or box.index.EQ.
     Default is box.index.ALL.
     See https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/pairs/.
    * `start_key` - Start iterating from the tuple with this index value. If the iterator is "EQ", iterate over tuples with this index value.
     The index value may be a single value, if the index consists of one field, a tuple with the index key parts, or a function which returns such value.
     If omitted or nil, all tuples will be checked.
    * `tuples_per_iteration` - Number of tuples to check in one batch (iteration). Default is 1024.
    * `on_full_scan_start` - Function to call before starting a tuple scan.
    * `on_full_scan_complete` - Function to call after completing a full scan.
    * `on_full_scan_success` - Function to call after successfully completing a full scan.
    * `on_full_scan_error` - Function to call after terminating a full scan due to an error.
    * `args` - Passed to is_tuple_expired and process_expired_tuple() as an additional context.
    * `full_scan_time` - Time required for a full index scan (in seconds).
    * `iteration_delay` - Max sleep time between batches (in seconds).
    * `full_scan_delay` - Sleep time between full scans (in seconds).
    * `force` - Run task even on replica.


### `expirationd.kill (name)`

Kill an existing task with name "name"

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
