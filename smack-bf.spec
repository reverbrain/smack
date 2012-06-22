Summary:	low-level IO storage which packs data into sorted (optionally compressed) blobs
Name:		smack
Version:	0.1.0
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
cmake -DCMAKE_INSTALL_PREFIX:PATH=%{buildroot}%{_prefix} -DCMAKE_INSTALL_LIBDIR:PATH=%{buildroot}%{_libdir} .
make %{?_smp_mflags}

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
* Fri Jun 22 2012 Evgeniy Polyakov <zbr@ioremap.net> - 0.1.0
- Initial build for Fedora
