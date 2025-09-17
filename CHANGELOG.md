# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Fixed

## 1.7.0 - 2025-09-17

This release improves flexibility of configuration and fixes role handling issues.

### Added

- Added the ability to run without a user Lua-code.

### Fixed

- Fix `is_master_only` option. Now it runs on master correctly. 
- Fixed restrictions on the order of role enable.

## 1.6.0 - 2024-03-25

The release introduces a role for Tarantool 3.0.

### Added

- Tarantool 3.0 role (#160).

### Changed

- Updated the 'space_index_test.lua' to drop and recreate the test space
  atomically. This prevents the space access failure in the expirationd
  task fiber if the `space:drop` function is transactional (#157).
- Updated version of `luatest` in `make deps` to 1.0.1 to support Tarantool 3.0
  role tests (#160).

## 1.5.0 - 2023-08-23

The release adds an ability to use functions from `box.func` with the Tarantool
Cartridge role.

### Added

- An ability to use persistent functions in `box.func` with cartridge. A user
  can configure the role with persistent functions as callback for a
  task (#153).

## 1.4.0 - 2023-03-16

The release adds `_VERSION` constant for the module.

### Added

- Add versioning support (#136).

## 1.3.1 - 2023-01-17

The release adds a missed ability to configure the expirationd using
Tarantool Cartridge role configuration.

### Fixed

- Incorrect check of the Tarantool version in tests to determine a bug in
  the vinyl engine that breaks the tests (#103).
- There is no way to configure the module using Tarantool Cartridge role
  configuration (#131).

## 1.3.0 - 2022-08-11

This release adds a Tarantool Cartridge role for expirationd package and
improves the default behavior.

### Added

- Continue a task from a last tuple (#54).
- Process a task on a writable space by default (#42).
- Wait until a space or an index is created (#68, #116).
- Tarantool Cartridge role (#107).
- Shuffle tests (#118).
- GitHub Actions workflow with debug Tarantool build (#102).
- GitHub Actions workflow for deploying module packages to S3 based
  repositories (#43).

### Changed

- Decrease tarantool-checks dependency from 3.1 to 2.1 (#124).
- expirationd.start() parameter `space_id` has been renamed to `space` (#112).

### Deprecated

- Obsolete functions: task_stats, kill_task, get_task, get_tasks, run_task,
  show_task_list.

### Fixed

- Do not restart a work fiber if an index does not exist (#64).
- Build and installation of rpm/deb packages (#124).
- test_mvcc_vinyl_tx_conflict (#104, #105).
- Flaky 'simple expires test' (#90).
- Changelogs.

## 1.2.0 - 2022-06-27

This release adds a lot of test fixes, documentation and CI improvements. The
main new feature is support of metrics package. Collecting statistics using the
metrics package is enabled by default if the package metrics >= 0.11.0
is installed.

4 counters will be created:

1. expirationd_checked_count
2. expirationd_expired_count
3. expirationd_restarts
4. expirationd_working_time

The meaning of counters is same as for expirationd.stats().

It can be disabled using the expirationd.cfg call:

```Lua
expirationd.cfg({metrics = false})
```

### Added

- Check types of function arguments with checks module (#58).
- Messages about obsolete methods.
- Metrics support (#100).
- Tests use new version of API.
- Tests for expirationd.stats() (#77).
- Gather code coverage and send report to coveralls on GitHub CI (#85).
- Print engine passed to tests (#76).
- Support to generate documentation using make (#79).
- Note about using expirationd with replication (#14).
- New target deps to Makefile that install lua dependencies (#79).
- GitHub CI for publishing API documentation (#79).
- Describe prerequisites and installation steps in README.md.

### Changed

- Update documentation and convert to LDoc format (#60).
- Update comparison table in README.md (#53).
- Bump luatest version to 0.5.6.

### Fixed

- Prevent iteration through a functional index for Tarantool < 2.8.4 (#101).
- Processing tasks with zero length box.cfg.replication (#95).
- Remove check for vinyl engine (#76).
- Flakiness (#76, #90, #80).
- Make iterate_with() conform to declared interface (#84).
- Use default 'vinyl_memory' quota for tests (#104).
- A typo in the rpm-package description.
- Function name in example:
  function on_full_scan_complete -> function on_full_scan_error.
- Incorrect description of the force option for the expirationd.start (#92,
  #96).

## 1.1.1 - 2021-09-13

This release adds a fix for a bug with freezing on stop a task.

### Added

- Enable Lua source code analysis with luacheck (#57).

### Fixed

- Freezes when stopping a task (#69).

## 1.1.0 - 2021-07-06

This release adds a number of features and fixes a bug.

### Added

- The ability to set iteration and full scan delays for a task (#38).
- Callbacks for a task at various stages of the full scan iteration (#25).
- The ability to specify from where to start the iterator (option start_key)
  and specify the type of the iterator itself (option iterator_type).
  Start key can be set as a function (dynamic parameter) or just a static
  value. The type of the iterator can be specified either with the
  `box.index.*` constant, or with the name for example, 'EQ' or
  box.index.EQ (#50).
- The ability to create a custom iterator that will be created at the selected
  index (option iterate_with). One can also pass a predicate that will stop the
  full-scan process, if required (process_while) (#50).
- An option atomic_iteration that allows making only one transaction per batch
  option. With task:kill(), the batch with transactions will be finalized, and
  only after that, the fiber will complete its work (#50).

### Fixed

- Worker iteration for a tree index. The bug can cause an array of tuples for a
  check on expiration to be obtained before suspending during the worker
  iteration (in case of using a tree index), and some tuples can be
  modified/deleted from another fiber while the worker fiber is sleeping.

## 1.0.1 - 2018-01-22

First release with rockspecs.

### Added

- rockspecs.
