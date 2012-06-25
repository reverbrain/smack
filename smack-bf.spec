Summary:	low-level IO storage which packs data into sorted (optionally compressed) blobs
Name:		smack
Version:	0.2.0
Release:	1%{?dist}.1

License:	GPLv2+
Group:		System Environment/Libraries
URL:		http://www.ioremap.net/projects/smack
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if %{defined rhel} && 0%{?rhel} < 6
BuildRequires:	boost141-devel, boost141-iostreams, boost141-filesystem, boost141-system
%else
BuildRequires:  boost-devel, boost-filesystem, boost-thread, boost-system, boost-iostreams
%endif
BuildRequires:	cmake snappy-devel

%description
  SMACK - low-level IO storage which packs data into sorted (zlib/bzip2/snappy compressed) blobs

%package devel
Summary: Development files for %{name}
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}


%description devel
SMACK - low-level IO storage which packs data into sorted (zlib/bzip2/snappy compressed) blobs

This package contains libraries, header files and developer documentation
needed for developing software which uses the eblob library.

%prep
%setup -q

%build
export LDFLAGS="-Wl,-z,defs"
%if %{defined rhel} && 0%{?rhel} < 6
cmake -DCMAKE_INSTALL_PREFIX:PATH=%{buildroot}%{_prefix} -DCMAKE_INSTALL_LIBDIR:PATH=%{buildroot}%{_libdir} -DBOOST_INCLUDEDIR:PATH=/usr/include/boost141 -DBOOST_LIBRARYDIR:PATH=/usr/lib64/boost141 .
%else
cmake -DCMAKE_INSTALL_PREFIX:PATH=%{buildroot}%{_prefix} -DCMAKE_INSTALL_LIBDIR:PATH=%{buildroot}%{_libdir} .
%endif

make VERBOSE=1 %{?_smp_mflags}

%install
rm -rf %{buildroot}

make install
rm -f %{buildroot}%{_libdir}/*.a
rm -f %{buildroot}%{_libdir}/*.la

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
#%{_bindir}/*
%{_libdir}/lib*.so.*


%files devel
%defattr(-,root,root,-)
%{_includedir}/smack/*
%{_libdir}/lib*.so

%changelog
* Mon Jun 25 2012 Evgeniy Polyakov <zbr@ioremap.net> - 0.2.0
- Added lz4 (default and high) compression
- Added zlib high compression
- Interface updates

* Fri Jun 22 2012 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.1
- Do not use lib64 as default install library dir

* Fri Jun 22 2012 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.0
- Initial build for Fedora
