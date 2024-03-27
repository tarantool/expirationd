[![Run tests](https://github.com/tarantool/expirationd/actions/workflows/fast_testing.yaml/badge.svg)](https://github.com/tarantool/expirationd/actions/workflows/fast_testing.yaml)
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

* Install the module using [tt](https://github.com/tarantool/tt):

  ``` bash
  tt rocks install expirationd
  ```

* Install the module using LuaRocks:

  ``` bash
  luarocks install --local --server=https://rocks.tarantool.org expirationd
  ```

### Documentation

See API documentation in https://tarantool.github.io/expirationd/

Note about using expirationd with replication: by default expirationd processes
tasks for all types of spaces only on the writable instance. It does not
process tasks on read-only instance for [non-local persistent spaces](https://www.tarantool.io/en/doc/latest/reference/configuration/#confval-read_only).
It means that expirationd *will not* start task processing on a replica for
regular spaces. One can force running task on replica with option `force` in
`start()` module function. The option force let a user control where to start
task processing and where don't.

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

function delete_tuple(space, args, tuple)
  box.space[space]:delete{tuple[1]}
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
$ make deps-full
$ make test
```

Regression tests running in continuous integration that uses luatest are
executed in shuffle mode. It means that every time order of tests is
pseudorandom with predefined seed. If tests in CI are failed it is better to
reproduce these failures with the same seed:

```sh
$ make SEED=1334 test
luatest -v --coverage --shuffle all:1334
...
```

## Cartridge role

`cartridge.roles.expirationd` is a Tarantool Cartridge role for the expirationd
package with features:

* It registers expirationd as a Tarantool Cartridge service for easy access to
  all [API calls](https://tarantool.github.io/expirationd/#Module_functions):
  ```Lua
  local task = cartridge.service_get('expirationd').start("task_name", id, is_expired)
  task:kill()
  ```
* You could configure the expirationd role with `cfg` entry.
  [expirationd.cfg()](https://tarantool.github.io/expirationd/#cfg) has the
  same parameters with the same meaning.

  Be careful, values from the clusterwide configuration are applied by default
  to all nodes on each
  [apply_config()](https://www.tarantool.io/en/doc/latest/book/cartridge/cartridge_dev/).
  Changing the configuration manually with
  [expirationd.cfg()](https://tarantool.github.io/expirationd/#cfg)
  only affects the current node and does not update values in the clusterwide
  configuration. The manual change will be overwritten by a next
  `apply_config` call.
* You can use persistent functions (i.e. created by `box.schema.func.create`).
  When configuring, role tries firstly get function from global namespace
  (`_G`) and if function was not found then role tries search in `box.func` for
  function with the same name.

  Be careful! At the moment of validating and applying config of expirationd
  role all persistent functions must be created before, so to configure
  cartridge application correctly you must do it in two steps: at the first
  step you have to confgure migrations with creating persistent functions and
  run them, at the second one put expirationd config.
* The role stops all expirationd tasks on an instance on the role termination.
* The role can automatically start or kill old tasks from the role
  configuration:

  ```yaml
  expirationd:
    cfg:
      metrics: true
    task_name1:
      space: 579
      is_expired: is_expired_func_name_in__G
      is_master_only: true
      options:
        args:
          - any
        atomic_iteration: false
        force: false
        force_allow_functional_index: true
        full_scan_delay: 1
        full_scan_time: 1
        index: 0
        iterate_with: iterate_with_func_name_in__G
        iteration_delay: 1
        iterator_type: ALL
        on_full_scan_complete: on_full_scan_complete_func_name_in__G
        on_full_scan_error: on_full_scan_error_func_name_in__G
        on_full_scan_start: on_full_scan_start_func_name_in__G
        on_full_scan_success: on_full_scan_success_func_name_in__G
        process_expired_tuple: process_expired_tuple_func_name_in__G
        process_while: process_while_func_name_in__G
        start_key:
        - 1
        tuples_per_iteration: 100
        vinyl_assumed_space_len: 100
        vinyl_assumed_space_len_factor: 1
    task_name2:
      ...
  ```

  [expirationd.start()](https://tarantool.github.io/expirationd/#start) has
  the same parameters with the same meaning except for the additional optional
  param `is_master_only`. If `true`, the task should run only on a master
  instance. By default, the value is `false`.

  You need to be careful with parameters-functions. The string is a key in
  the global variable `_G`, the value must be a function. You need to define
  the key before initializing the role:

  ```Lua
  rawset(_G, "is_expired_func_name_in__G", function(args, tuple)
      -- code of the function
  end)
  ```

## Tarantool 3.0 role

`roles.expirationd` is a Tarantool 3.0 role for the expirationd
package with the following features:

* You can configure the expirationd role with `cfg` entry (check example).
  Cluster configuration allows to set the same parameters as
  in [expirationd.cfg()](https://tarantool.github.io/expirationd/#cfg)
* You can use persistent functions (i.e. created by `box.schema.func.create`)
  for expirationd `cfg` entries.
  When configuring, role tries first to get a function from global namespace (`_G`)
  and if the function was not found then role tries to search in `box.func`
  for a function with the same name.
  If some functions from config are missing,
  expirationd will wait for their creation and start tasks when all of them are found.
  You can check logs to see what functions are missing.
* The role stops all expirationd tasks on an instance on the role termination.
* The role can automatically start or kill old tasks from the role
  configuration.

  ```yaml
  roles: [roles.expirationd]
  roles_cfg:
    roles.expirationd:
      cfg:
        metrics: true
      task_name1:
        space: users
        is_expired: is_expired_func_name
        is_master_only: true
        options:
          args:
            - any
          atomic_iteration: false
          force: false
          force_allow_functional_index: true
          full_scan_delay: 1
          full_scan_time: 1
          index: 0
          iterate_with: iterate_with_func_name_in__G
          iteration_delay: 1
          iterator_type: ALL
          on_full_scan_complete: on_full_scan_complete_func_name_in__G
          on_full_scan_error: on_full_scan_error_func_name_in__G
          on_full_scan_start: on_full_scan_start_func_name_in__G
          on_full_scan_success: on_full_scan_success_func_name_in__G
          process_expired_tuple: process_expired_tuple_func_name_in__G
          process_while: process_while_func_name_in__G
          start_key:
            - 1
          tuples_per_iteration: 100
          vinyl_assumed_space_len: 100
          vinyl_assumed_space_len_factor: 1
  ```

  [expirationd.start()](https://tarantool.github.io/expirationd/#start) has
  the same parameters with the same meaning except for the additional optional
  param `is_master_only`. If `true`, the task should run only on a master
  instance. By default, the value is `false`.

  You need to be careful with function parameters. Task will not start until it
  finds all functions from config. You can define them in user code:

  ```Lua
  box.schema.func.create('is_expired_func_name', {
      body = "function(...) return true end",
      if_not_exists = true
  })

  -- Or you could define a global variable.
  rawset(_G, "process_while_func_name_in__G", function(...)
      return true
  end)
  ```

