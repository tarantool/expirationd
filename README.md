[![Run tests](https://github.com/tarantool/expirationd/actions/workflows/test_on_push.yaml/badge.svg)](https://github.com/tarantool/expirationd/actions/workflows/test_on_push.yaml)
<a href='https://coveralls.io/github/tarantool/expirationd?branch=master'>
<img src='https://coveralls.io/repos/github/tarantool/expirationd/badge.svg?branch=master' alt='Coverage Status' />
</a>

# expirationd -  data expiration with custom quirks.

This package can turn Tarantool into a persistent memcache replacement,
but is powerful enough so that  your own expiration strategy can be defined.

You define two functions: one takes a tuple as an input and returns
true in case it's expired and false otherwise. The other takes the
tuple and performs the expiry itself: either deletes it (memcache), or
does something smarter, like put a smaller representation of the data
being deleted into some other space.

There are a number of similar modules:
- [moonwalker](https://github.com/tarantool/moonwalker) triggered manually,
useful for batch transactions, a performance about 600/700k rec/sec
- [expirationd](https://github.com/tarantool/expirationd/issues/53) always
expires tuples with using indices and using any condition, without guarantee
for time expiration.
- [indexpirationd](https://github.com/moonlibs/indexpiration) always expires
tuples with indices, has a nice precision (up to ms) for time to expire.

Table below may help you to choose a proper module for your requirements:

| Module        | Reaction time | Uses indices | Arbitrary condition | Expiration trigger                 |
|---------------|---------------|--------------|---------------------|------------------------------------|
| indexpiration | High (ms)     | Yes          | No                  | synchronous (fiber with condition) |
| expirationd   | Medium (sec)  | Yes          | Yes                 | synchronous (fiber with condition) |
| moonwalker    | NA            | No           | Yes                 | asynchronous (using crontab etc)   |

### Prerequisites

* Tarantool 1.10+ (`tarantool` package, see [documentation](https://www.tarantool.io/en/download/)).

### Installation

You can:

* Install the module using `tarantoolctl`:

  ``` bash
  tarantoolctl rocks install expirationd
  ```

* Install the module using LuaRocks:

  ``` bash
  luarocks install --local --server=https://rocks.tarantool.org expirationd
  ```

### Documentation

See API documentation in https://tarantool.github.io/expirationd/

Note about using expirationd with replication: by default expirationd should
process tasks only on the writeable instance, it means that expirationd *will
not* start task processing on a replica. Here the word 'replica' means an
instance with at least one configured upstream, it's an option
`box.cfg.replication_source`. One can force running task on replica with option
`force` in `start()` module function. The option force let a user control where
to start task processing and where don't.

### Examples

Simple version:

```lua
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
    process_expired_tuple = delete_tuple,
    args = nil,
    tuples_per_iteration = 50,
    full_scan_time = 3600
})
```

Сustomized version:

```lua
expirationd.start(job_name, space.id, is_expired, {
    -- name or id of the index in the specified space to iterate over
    index = "exp",
    -- one transaction per batch
    -- default is false
    atomic_iteration = true,
    -- delete data that was added a year ago
    -- default is nil
    start_key = function( task )
        return clock.time() - (365*24*60*60)
    end,
    -- delete it from the oldest to the newest
    -- default is ALL
    iterator_type = "GE",
    -- stop full_scan if delete a lot
    -- returns true by default
    process_while = function( task )
        if task.args.max_expired_tuples >= task.expired_tuples_count then
            task.expired_tuples_count = 0
            return false
        end
        return true
    end,
    -- this function must return an iterator over the tuples
    iterate_with = function( task )
        return task.index:pairs({ task.start_key() }, { iterator = task.iterator_type })
            :take_while( function( tuple )
                return task:process_while()
            end )
    end,
    args = {
        max_expired_tuples = 1000
    }
})
```

## Testing

```
$ make deps
$ make test
```
