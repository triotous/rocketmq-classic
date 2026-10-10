use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs},
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Address {
    ip: IpAddr,
    scope: u32,
    scope_name: Option<String>,
}

fn internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            matches!(octets[0], 10 | 127)
                || (octets[0] == 172 && (16..=31).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 168)
        }
        IpAddr::V6(ip) => {
            ip.is_unspecified()
                || ip.is_loopback()
                || ip.segments()[0] & 0xffc0 == 0xfe80
                || ip.segments()[0] & 0xffc0 == 0xfec0
        }
    }
}

fn normalize(address: &Address) -> String {
    match address.ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => {
            let host = ip
                .segments()
                .iter()
                .map(|part| format!("{part:x}"))
                .collect::<Vec<_>>()
                .join(":");
            if let Some(name) = &address.scope_name {
                format!("[{host}%{name}]")
            } else if address.scope == 0 {
                format!("[{host}]")
            } else {
                format!("[{host}%{}]", address.scope)
            }
        }
    }
}

fn select(addresses: Vec<Address>) -> Option<Address> {
    let mut v4 = Vec::new();
    let mut v6 = Vec::new();
    for address in addresses
        .into_iter()
        .filter(|address| !address.ip.is_loopback())
    {
        if address.ip.is_ipv4() {
            v4.push(address);
        } else {
            v6.push(address);
        }
    }
    if !v4.is_empty() {
        v4.iter()
            .find(|address| !internal(address.ip))
            .cloned()
            .or_else(|| v4.pop())
    } else {
        v6.iter()
            .find(|address| !internal(address.ip))
            .cloned()
            .or_else(|| v6.into_iter().next())
    }
}

#[cfg(any(windows, test))]
fn java_windows_interface_is_eligible(
    admin_up: bool,
    operational_up: bool,
    point_to_point: bool,
) -> bool {
    admin_up && operational_up && !point_to_point
}

#[cfg(any(windows, test))]
fn java_windows_unicast_address_is_usable(dad_state: i32) -> bool {
    // Windows NL_DAD_STATE values are 3 for deprecated and 4 for preferred.
    matches!(dad_state, 3 | 4)
}

#[cfg(any(windows, test))]
fn java_windows_interface_candidate_order(
    mut unicast: Vec<Address>,
    mut anycast: Vec<Address>,
) -> Vec<Address> {
    // OpenJDK prepends each table row to its native address list, so the
    // resulting enumeration is reverse table order, with anycast before
    // unicast because its rows are prepended second.
    unicast.reverse();
    anycast.reverse();
    anycast.extend(unicast);
    anycast
}

#[cfg(target_os = "macos")]
fn java_candidate_order(mut addresses: Vec<Address>) -> Vec<Address> {
    // Pinned Java's NetworkUtil returns IPv4 candidates in the opposite order
    // from macOS getifaddrs; preserve that order because its private-address
    // fallback selects the last IPv4 candidate. IPv6 candidate order is kept.
    let mut ipv4 = addresses
        .iter()
        .filter(|address| address.ip.is_ipv4())
        .cloned()
        .collect::<Vec<_>>();
    ipv4.reverse();
    for (address, replacement) in addresses
        .iter_mut()
        .filter(|address| address.ip.is_ipv4())
        .zip(ipv4)
    {
        *address = replacement;
    }
    addresses
}

#[cfg(not(target_os = "macos"))]
fn java_candidate_order(addresses: Vec<Address>) -> Vec<Address> {
    addresses
}

#[cfg(any(target_os = "linux", test))]
fn java_linux_ipv6_scope(_native_scope: u32, interface_index: u32) -> u32 {
    // OpenJDK's Linux enumerator sets sin6_scope_id from /proc/net/if_inet6's
    // interface index for every IPv6 address. glibc getifaddrs only supplies
    // a scope for link-local and link-scoped multicast addresses.
    interface_index
}

pub(super) fn get() -> io::Result<String> {
    resolve(interfaces().map(java_candidate_order), local_host)
}

fn resolve(
    addresses: io::Result<Vec<Address>>,
    fallback: impl FnOnce() -> io::Result<Address>,
) -> io::Result<String> {
    if let Some(address) = select(addresses?) {
        return Ok(normalize(&address));
    }
    fallback().map(|address| normalize(&address))
}

fn local_host() -> io::Result<Address> {
    // Java InetAddress.getLocalHost resolves the OS host name, not a hardcoded
    // loopback or a UDP route probe to an unrelated Internet destination.
    let host = hostname::get()?
        .into_string()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-Unicode local host name"))?;
    let socket = (host.as_str(), 0)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "local host has no addresses",
            )
        })?;
    let scope = match socket {
        std::net::SocketAddr::V6(socket) => socket.scope_id(),
        _ => 0,
    };
    Ok(Address {
        ip: socket.ip(),
        scope,
        scope_name: None,
    })
}

#[cfg(unix)]
fn eligible(name: &str, flags: u32, bridge: bool) -> bool {
    flags & libc::IFF_UP as u32 != 0
        && flags & libc::IFF_RUNNING as u32 != 0
        && flags & libc::IFF_POINTOPOINT as u32 == 0
        && !name.contains(':')
        && !bridge
}

#[cfg(unix)]
fn interfaces() -> io::Result<Vec<Address>> {
    use std::{ffi::CStr, ptr};
    let mut first = ptr::null_mut();
    // SAFETY: getifaddrs writes an owned linked list; the guard releases it once.
    if unsafe { libc::getifaddrs(&mut first) } != 0 {
        return Err(io::Error::last_os_error());
    }
    struct List(*mut libc::ifaddrs);
    impl Drop for List {
        fn drop(&mut self) {
            // SAFETY: this guard owns exactly the list returned by getifaddrs.
            unsafe {
                libc::freeifaddrs(self.0);
            }
        }
    }
    let _guard = List(first);
    let mut current = first;
    let mut addresses = Vec::new();
    while !current.is_null() {
        // SAFETY: nodes/names/sockaddrs live until the list guard is dropped.
        let interface = unsafe { &*current };
        current = interface.ifa_next;
        if interface.ifa_addr.is_null() || interface.ifa_name.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(interface.ifa_name) }.to_string_lossy();
        let flags = interface.ifa_flags;
        let bridge = cfg!(target_os = "linux")
            && std::path::Path::new("/sys/class/net")
                .join(name.as_ref())
                .join("bridge")
                .exists();
        if !eligible(&name, flags, bridge) {
            continue;
        }
        let family = unsafe { (*interface.ifa_addr).sa_family } as i32;
        let address = match family {
            libc::AF_INET => {
                let socket = unsafe { &*(interface.ifa_addr as *const libc::sockaddr_in) };
                Address {
                    ip: IpAddr::V4(Ipv4Addr::from(socket.sin_addr.s_addr.to_ne_bytes())),
                    scope: 0,
                    scope_name: None,
                }
            }
            libc::AF_INET6 => {
                let socket = unsafe { &*(interface.ifa_addr as *const libc::sockaddr_in6) };
                #[cfg(target_os = "linux")]
                let scope = java_linux_ipv6_scope(socket.sin6_scope_id, unsafe {
                    libc::if_nametoindex(interface.ifa_name)
                });
                #[cfg(not(target_os = "linux"))]
                let scope = socket.sin6_scope_id;
                Address {
                    ip: IpAddr::V6(Ipv6Addr::from(socket.sin6_addr.s6_addr)),
                    scope,
                    // Java's Inet6Address stores the NetworkInterface itself
                    // for scoped addresses, so getHostAddress prints its name.
                    scope_name: (scope != 0).then(|| name.to_string()),
                }
            }
            _ => continue,
        };
        addresses.push(address);
    }
    Ok(addresses)
}

#[cfg(windows)]
fn interfaces() -> io::Result<Vec<Address>> {
    use windows_sys::Win32::{
        NetworkManagement::{
            IpHelper::{
                ConvertInterfaceLuidToNameW, FreeMibTable, GetAnycastIpAddressTable, GetIfEntry2,
                GetIfTable2, GetUnicastIpAddressTable, MIB_ANYCASTIPADDRESS_TABLE, MIB_IF_ROW2,
                MIB_IF_TABLE2, MIB_UNICASTIPADDRESS_TABLE,
            },
            Ndis::{
                IfOperStatusUp, NET_IF_ACCESS_POINT_TO_POINT, NET_IF_ADMIN_STATUS_UP, NET_LUID_LH,
            },
        },
        Networking::WinSock::AF_UNSPEC,
    };

    struct MibTable(*const std::ffi::c_void);
    impl Drop for MibTable {
        fn drop(&mut self) {
            // SAFETY: each pointer is returned by an IP Helper table API and
            // remains owned by this guard until FreeMibTable is called.
            unsafe { FreeMibTable(self.0) };
        }
    }

    fn interface_name(luid: &NET_LUID_LH) -> io::Result<String> {
        // OpenJDK obtains NetworkInterface.getName() from this LUID conversion.
        let mut name = [0u16; 257];
        let result = unsafe {
            ConvertInterfaceLuidToNameW(std::ptr::from_ref(luid), name.as_mut_ptr(), name.len())
        };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        let length = name
            .iter()
            .position(|unit| *unit == 0)
            .unwrap_or(name.len());
        String::from_utf16(&name[..length])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn luid_value(luid: &NET_LUID_LH) -> u64 {
        // SAFETY: the IP Helper APIs initialize the canonical 64-bit view of
        // every NET_LUID_LH value returned in these tables.
        unsafe { luid.Value }
    }

    fn interface_row(index: u32) -> io::Result<MIB_IF_ROW2> {
        let mut row = MIB_IF_ROW2 {
            InterfaceIndex: index,
            ..Default::default()
        };
        let result = unsafe { GetIfEntry2(&mut row) };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        Ok(row)
    }

    let mut interfaces = std::ptr::null_mut::<MIB_IF_TABLE2>();
    let result = unsafe { GetIfTable2(&mut interfaces) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    if interfaces.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "GetIfTable2 returned a null table",
        ));
    }
    let _interfaces_guard = MibTable(interfaces.cast());

    let mut unicast = std::ptr::null_mut::<MIB_UNICASTIPADDRESS_TABLE>();
    let result = unsafe { GetUnicastIpAddressTable(AF_UNSPEC, &mut unicast) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    if unicast.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "GetUnicastIpAddressTable returned a null table",
        ));
    }
    let _unicast_guard = MibTable(unicast.cast());

    let mut anycast = std::ptr::null_mut::<MIB_ANYCASTIPADDRESS_TABLE>();
    let result = unsafe { GetAnycastIpAddressTable(AF_UNSPEC, &mut anycast) };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    if anycast.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "GetAnycastIpAddressTable returned a null table",
        ));
    }
    let _anycast_guard = MibTable(anycast.cast());

    // SAFETY: the IP Helper APIs allocate each flexible table with at least
    // `NumEntries` initialized rows, and all guards outlive these slices.
    let interfaces = unsafe {
        std::slice::from_raw_parts(
            (*interfaces).Table.as_ptr(),
            (*interfaces).NumEntries as usize,
        )
    };
    let unicast = unsafe {
        std::slice::from_raw_parts((*unicast).Table.as_ptr(), (*unicast).NumEntries as usize)
    };
    let anycast = unsafe {
        std::slice::from_raw_parts((*anycast).Table.as_ptr(), (*anycast).NumEntries as usize)
    };

    let mut addresses = Vec::new();
    for interface in interfaces {
        let interface_name = interface_name(&interface.InterfaceLuid)?;
        // NetworkInterface.isUp() and isPointToPoint() each query the current
        // interface row after Java has enumerated the interface table.
        let up_state = interface_row(interface.InterfaceIndex)?;
        let point_to_point_state = interface_row(interface.InterfaceIndex)?;
        if !java_windows_interface_is_eligible(
            up_state.AdminStatus == NET_IF_ADMIN_STATUS_UP,
            up_state.OperStatus == IfOperStatusUp,
            point_to_point_state.AccessType == NET_IF_ACCESS_POINT_TO_POINT,
        ) {
            continue;
        }

        let interface_unicast = unicast
            .iter()
            .filter(|row| {
                luid_value(&row.InterfaceLuid) == luid_value(&interface.InterfaceLuid)
                    && java_windows_unicast_address_is_usable(row.DadState)
            })
            .filter_map(|row| windows_address(row.Address, &interface_name))
            .collect::<Vec<_>>();
        let interface_anycast = anycast
            .iter()
            .filter(|row| luid_value(&row.InterfaceLuid) == luid_value(&interface.InterfaceLuid))
            .filter_map(|row| windows_address(row.Address, &interface_name))
            .collect::<Vec<_>>();
        addresses.extend(java_windows_interface_candidate_order(
            interface_unicast,
            interface_anycast,
        ));
    }
    Ok(addresses)
}

#[cfg(windows)]
fn windows_address(
    address: windows_sys::Win32::Networking::WinSock::SOCKADDR_INET,
    interface_name: &str,
) -> Option<Address> {
    use windows_sys::Win32::Networking::WinSock::{AF_INET, AF_INET6};

    // SAFETY: `si_family` is the active discriminator for this union.
    match unsafe { address.si_family } {
        AF_INET => {
            // SAFETY: AF_INET identifies the IPv4 member of SOCKADDR_INET.
            let socket = unsafe { address.Ipv4 };
            Some(Address {
                ip: IpAddr::V4(Ipv4Addr::from(
                    unsafe { socket.sin_addr.S_un.S_addr }.to_ne_bytes(),
                )),
                scope: 0,
                scope_name: None,
            })
        }
        AF_INET6 => {
            // SAFETY: AF_INET6 identifies the IPv6 member of SOCKADDR_INET.
            let socket = unsafe { address.Ipv6 };
            let scope = unsafe { socket.Anonymous.sin6_scope_id };
            Some(Address {
                ip: IpAddr::V6(Ipv6Addr::from(unsafe { socket.sin6_addr.u.Byte })),
                scope,
                scope_name: (scope != 0).then(|| interface_name.to_owned()),
            })
        }
        _ => None,
    }
}

#[cfg(not(any(unix, windows)))]
fn interfaces() -> io::Result<Vec<Address>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "native interface enumeration unavailable",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn windows_interface_filter_matches_java_admin_oper_and_access_checks() {
        assert!(java_windows_interface_is_eligible(true, true, false));
        assert!(!java_windows_interface_is_eligible(false, true, false));
        assert!(!java_windows_interface_is_eligible(true, false, false));
        assert!(!java_windows_interface_is_eligible(true, true, true));
    }
    #[test]
    fn windows_unicast_address_state_matches_java_preferred_and_deprecated_filter() {
        assert!(!java_windows_unicast_address_is_usable(0)); // invalid
        assert!(!java_windows_unicast_address_is_usable(1)); // tentative
        assert!(!java_windows_unicast_address_is_usable(2)); // duplicate
        assert!(java_windows_unicast_address_is_usable(3)); // deprecated
        assert!(java_windows_unicast_address_is_usable(4)); // preferred
    }
    #[test]
    fn windows_candidate_order_matches_java_native_address_list() {
        assert_eq!(
            java_windows_interface_candidate_order(
                vec![
                    address("10.0.0.1"),
                    address("10.0.0.2"),
                    address("10.0.0.3"),
                ],
                vec![address("192.0.2.1"), address("192.0.2.2")],
            ),
            vec![
                address("192.0.2.2"),
                address("192.0.2.1"),
                address("10.0.0.3"),
                address("10.0.0.2"),
                address("10.0.0.1"),
            ]
        );
    }
    #[cfg(unix)]
    #[test]
    fn interface_filter_matches_java_up_running_and_exclusions() {
        assert!(eligible(
            "veth0",
            (libc::IFF_UP | libc::IFF_RUNNING) as u32,
            false
        ));
        assert!(eligible(
            "eth0",
            (libc::IFF_UP | libc::IFF_RUNNING) as u32,
            false
        ));
        assert!(!eligible("eth0", libc::IFF_UP as u32, false));
        assert!(!eligible("eth0", libc::IFF_RUNNING as u32, false));
        assert!(!eligible(
            "eth0:1",
            (libc::IFF_UP | libc::IFF_RUNNING) as u32,
            false
        ));
        assert!(!eligible(
            "br0",
            (libc::IFF_UP | libc::IFF_RUNNING) as u32,
            true
        ));
        assert!(!eligible(
            "ppp0",
            (libc::IFF_UP | libc::IFF_RUNNING | libc::IFF_POINTOPOINT) as u32,
            false
        ));
    }
    #[test]
    fn fallback_only_runs_for_successful_empty_or_loopback_only_enumeration() {
        assert_eq!(
            resolve(Ok(vec![address("127.0.0.1"), address("::1")]), || Ok(
                address("::1")
            ))
            .unwrap(),
            "[0:0:0:0:0:0:0:1]"
        );
        assert_eq!(
            resolve(Ok(vec![address("10.0.0.1")]), || panic!(
                "must not resolve hostname"
            ))
            .unwrap(),
            "10.0.0.1"
        );
        let error = resolve(
            Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            || panic!("enumeration failed"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        let error = resolve(Ok(vec![]), || {
            Err(io::Error::from(io::ErrorKind::AddrNotAvailable))
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrNotAvailable);
    }
    #[test]
    fn address_helpers_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        for case in fixture["addresses"].as_array().unwrap() {
            let address = Address {
                ip: case["ip"].as_str().unwrap().parse().unwrap(),
                scope: case["scope"].as_u64().unwrap() as u32,
                scope_name: None,
            };
            assert_eq!(
                internal(address.ip),
                case["internal"].as_bool().unwrap(),
                "{case}"
            );
            assert_eq!(
                address.ip.is_loopback(),
                case["loopback"].as_bool().unwrap(),
                "{case}"
            );
            assert_eq!(
                normalize(&address),
                case["normalized"].as_str().unwrap(),
                "{case}"
            );
        }
        for case in fixture["scopeCases"].as_array().unwrap() {
            let address = Address {
                ip: case["ip"].as_str().unwrap().parse().unwrap(),
                scope: case["scope"].as_u64().unwrap() as u32,
                scope_name: Some(case["interfaceName"].as_str().unwrap().to_owned()),
            };
            assert_eq!(
                address.scope_name.as_deref(),
                case["interfaceName"].as_str()
            );
            assert_eq!(
                normalize(&address),
                case["normalized"].as_str().unwrap(),
                "{case}"
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn interface_enumeration_retains_java_named_ipv6_scope() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        let addresses = interfaces().unwrap();
        for case in fixture["scopeCases"].as_array().unwrap() {
            let expected = Address {
                ip: case["ip"].as_str().unwrap().parse().unwrap(),
                scope: case["scope"].as_u64().unwrap() as u32,
                scope_name: Some(case["interfaceName"].as_str().unwrap().to_owned()),
            };
            assert!(
                addresses.contains(&expected),
                "Java interface scope missing from native enumeration: {case}; {addresses:?}"
            );
            assert_eq!(
                normalize(&expected),
                case["normalized"].as_str().unwrap(),
                "{case}"
            );
        }
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn local_address_selection_matches_invoked_java_candidates_and_ipv6_fallback() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let java_candidates = fixture["networkUtilLocalAddressCandidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|candidate| Address {
                ip: candidate
                    .as_str()
                    .unwrap()
                    .split('%')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap(),
                scope: 0,
                scope_name: None,
            })
            .collect::<Vec<_>>();
        let native_candidates = java_candidate_order(interfaces().unwrap());
        let java_v4 = java_candidates
            .iter()
            .filter(|address| address.ip.is_ipv4())
            .map(|address| address.ip)
            .collect::<Vec<_>>();
        let native_v4 = native_candidates
            .iter()
            .filter(|address| address.ip.is_ipv4())
            .map(|address| address.ip)
            .collect::<Vec<_>>();
        assert_eq!(native_v4, java_v4);

        let java_selected = select(java_candidates.clone()).unwrap();
        let native_selected = select(native_candidates.clone()).unwrap();
        assert_eq!(native_selected.ip, java_selected.ip);
        assert_eq!(
            normalize(&native_selected),
            fixture["networkUtilLocalAddress"].as_str().unwrap()
        );

        let java_v6 = java_candidates
            .into_iter()
            .filter(|address| address.ip.is_ipv6())
            .collect::<Vec<_>>();
        let native_v6 = native_candidates
            .into_iter()
            .filter(|address| address.ip.is_ipv6())
            .collect::<Vec<_>>();
        assert_eq!(
            select(native_v6).map(|address| address.ip),
            select(java_v6).map(|address| address.ip)
        );
    }
    fn address(ip: &str) -> Address {
        Address {
            ip: ip.parse().unwrap(),
            scope: 0,
            scope_name: None,
        }
    }
    #[test]
    fn java_selection_matches_java_family_and_candidate_order() {
        assert_eq!(
            select(vec![
                address("::1"),
                address("2001:db8::1"),
                address("10.1.1.1"),
                address("192.0.2.1"),
                address("192.0.2.2")
            ]),
            Some(address("192.0.2.1"))
        );
        assert_eq!(
            select(vec![
                address("10.1.1.1"),
                address("2001:db8::1"),
                address("192.168.1.2")
            ]),
            Some(address("192.168.1.2"))
        );
        assert_eq!(
            select(vec![
                address("fe80::1"),
                address("fec0::1"),
                address("fc00::1"),
                address("2001:db8::2")
            ]),
            Some(address("fc00::1"))
        );
        assert_eq!(
            select(vec![address("fe80::1"), address("fec0::1")]),
            Some(address("fe80::1"))
        );
        assert_eq!(select(vec![address("127.0.0.2"), address("::1")]), None);
        assert!(!internal("169.254.1.1".parse().unwrap()));
        assert!(!internal("fc00::1".parse().unwrap()));
        assert_eq!(
            normalize(&Address {
                ip: "fe80::1".parse().unwrap(),
                scope: 7,
                scope_name: None,
            }),
            "[fe80:0:0:0:0:0:0:1%7]"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn java_candidate_order_reverses_only_macos_ipv4_candidates() {
        assert_eq!(
            java_candidate_order(vec![
                address("10.10.10.111"),
                address("fe80::1"),
                address("192.168.139.3"),
            ]),
            vec![
                address("192.168.139.3"),
                address("fe80::1"),
                address("10.10.10.111"),
            ]
        );
    }

    #[test]
    fn linux_ipv6_scope_uses_interface_index_even_when_native_scope_is_zero() {
        assert_eq!(java_linux_ipv6_scope(0, 7), 7);
        let scoped = Address {
            ip: "2001:db8::1".parse().unwrap(),
            scope: java_linux_ipv6_scope(0, 7),
            scope_name: Some("eth0".to_owned()),
        };
        assert_eq!(normalize(&scoped), "[2001:db8:0:0:0:0:0:1%eth0]");
    }
}
