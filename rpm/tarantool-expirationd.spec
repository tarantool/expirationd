Name: tarantool-expirationd
Version: 1.0.0
Release: 1%{?dist}
Summary: Expiration daemon for Tarantool
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/tarantool-expirationd
Source0: https://github.com/tarantool/%{name}/archive/%{version}/%{name}-%{version}.tar.gz
BuildArch: noarch
BuildRequires: tarantool >= 1.7.4.0
BuildRequires: /usr/bin/prove
Requires: tarantool >= 1.7.4.0
Requires: tarantool-checks >= 2.1
%description
This package can turn Tarantool into a persistent memcache replacement,
but is powerful enough so that your own expiration strategy can be defined.

You define two functions: one takes a tuple as an input and returns true in
case it's expired and false otherwise. The other takes the tuple and
performs the expiry itself: either deletes it (memcache), or does something
smarter, like put a smaller representation of the data being deleted into
some other space.

%prep
%setup -q -n %{name}-%{version}

%install
install -d %{buildroot}%{_datarootdir}/tarantool/
install -m 0644 expirationd.lua %{buildroot}%{_datarootdir}/tarantool/
install -d %{buildroot}%{_datarootdir}/tarantool/cartridge/roles/
install -m 0644 cartridge/roles/expirationd.lua %{buildroot}%{_datarootdir}/tarantool/cartridge/roles/expirationd.lua

%files
%{_datarootdir}/tarantool/expirationd.lua
%{_datarootdir}/tarantool/cartridge
%doc README.md
%{!?_licensedir:%global license %doc}
%license LICENSE

%changelog
* Thu Aug 11 2022 Oleg Jukovec <oleg.jukovec@tarantool.org> 1.3.0-1
- Continue a task from a last tuple
- Decrease tarantool-checks dependency from 3.1 to 2.1
- Process a task on a writable space by default
- Wait until a space or an index is created
- Tarantool Cartridge role
- Fix build and installation of rpm/deb packages
- Do not restart work a fiber if an index does not exist
- expirationd.start() parameter space_id has been renamed to space

* Mon Jun 27 2022 Oleg Jukovec <oleg.jukovec@tarantool.org> 1.2.0-1
- Check types of function arguments with checks module
- Add messages about obsolete methods
- Add metrics support
- Prevent iteration through a functional index for Tarantool < 2.8.4
- Fix processing tasks with zero length box.cfg.replication
- Make iterate_with() conform to declared interface
- Update documentation and convert to LDoc format
- Support to generate documentation using make
- Update comparison table in README.md
- Add note about using expirationd with replication
- Fix a typo in the rpm-package description
- Fix function name in example:
  function on_full_scan_complete -> function on_full_scan_error
- Describe prerequisites and installation steps in README.md
- Bump luatest version to 0.5.6
- Fix incorrect description of the force option for the expirationd.start

* Mon Sep 13 2021 Sergey Bronnikov <sergeyb@tarantool.org> 1.1.1-1
- Fix freezes when stopping a task
- Enable Lua source code analysis with luacheck

* Tue Jul 06 2021 Sergey Bronnikov <sergeyb@tarantool.org> 1.1.0-1
- Add the ability to set iteration and full scan delays for a task.
- Add callbacks for a task at various stages of the full scan iteration.
- Add the ability to specify from where to start the iterator
  (option start_key) and specify the type of the iterator itself
  (option iterator_type)
- Add the ability to create a custom iterator that will be created at the
  selected index (option iterate_with)
- Add an option atomic_iteration that allows making only one transaction per
  batch option
- Fix worker iteration for a tree index

* Sat Jan 22 2018 Roman Tsisyk <roman@tarantool.org> 1.0.1-1
- First release with rockspecs

* Thu Jun 18 2015 Roman Tsisyk <roman@tarantool.org> 1.0.0-1
- Initial version of the RPM spec
