Name: tarantool-expirationd
Version: 1.0.0
Release: 1%{?dist}
Summary: Expiration daemon for Tarantool
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/tarantool-expirationd
Source0: https://github.com/tarantool/%{name}/archive/%{version}/%{name}-%{version}.tar.gz
BuildArch: noarch
BuildRequires: tarantool >= 1.6.8.0
BuildRequires: /usr/bin/prove
Requires: tarantool >= 1.6.8.0
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

%check
make test

%install
install -d %{buildroot}%{_datarootdir}/tarantool/
install -m 0644 expirationd.lua %{buildroot}%{_datarootdir}/tarantool/

%files
%{_datarootdir}/tarantool/expirationd.lua
%doc README.md
%{!?_licensedir:%global license %doc}
%license LICENSE

%changelog
* Thu Jun 18 2015 Roman Tsisyk <roman@tarantool.org> 1.0.0-1
- Initial version of the RPM spec
