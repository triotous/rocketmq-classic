use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs},
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Address {
    ip: IpAddr,
    scope: u32,
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
            if address.scope == 0 {
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

pub(super) fn get() -> io::Result<String> {
    resolve(interfaces(), local_host)
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
    })
}

#[cfg(unix)]
fn eligible(name: &str, flags: u32, bridge: bool) -> bool {
    flags & libc::IFF_UP as u32 != 0
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
                }
            }
            libc::AF_INET6 => {
                let socket = unsafe { &*(interface.ifa_addr as *const libc::sockaddr_in6) };
                Address {
                    ip: IpAddr::V6(Ipv6Addr::from(socket.sin6_addr.s6_addr)),
                    scope: socket.sin6_scope_id,
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
    use windows_sys::Win32::{NetworkManagement::IpHelper::*, Networking::WinSock::*};
    let mut length = 15_000u32;
    for _ in 0..3 {
        // u64 storage supplies adapter structure alignment and a bounded buffer.
        let mut storage = vec![0u64; (length as usize).div_ceil(8)];
        let first = storage.as_mut_ptr().cast::<IP_ADAPTER_ADDRESSES_LH>();
        // SAFETY: storage has 8-byte alignment and at least `length` bytes;
        // the synchronous API retains no pointers after returning.
        let result = unsafe {
            GetAdaptersAddresses(
                AF_UNSPEC as u32,
                GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER,
                std::ptr::null(),
                first,
                &mut length,
            )
        };
        if result == 111 {
            continue;
        } // ERROR_BUFFER_OVERFLOW, retry new size.
        if result == 232 {
            return Ok(Vec::new()); // ERROR_NO_DATA: permit hostname fallback.
        }
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        let mut current = first;
        let mut addresses = Vec::new();
        while !current.is_null() {
            // SAFETY: on success the API initializes this linked list entirely
            // inside storage, which remains owned through traversal.
            let adapter = unsafe { &*current };
            current = adapter.Next;
            if adapter.OperStatus != 1 || adapter.IfType == 23 || adapter.IfType == 28 {
                continue;
            }
            let mut unicast = adapter.FirstUnicastAddress;
            while !unicast.is_null() {
                // SAFETY: this entry and its sockaddr are owned by storage.
                let entry = unsafe { &*unicast };
                unicast = entry.Next;
                let pointer = entry.Address.lpSockaddr;
                if pointer.is_null()
                    || entry.Address.iSockaddrLength < std::mem::size_of::<SOCKADDR>() as i32
                {
                    continue;
                }
                let address = match unsafe { (*pointer).sa_family } {
                    AF_INET
                        if entry.Address.iSockaddrLength
                            >= std::mem::size_of::<SOCKADDR_IN>() as i32 =>
                    {
                        let socket = unsafe { &*pointer.cast::<SOCKADDR_IN>() };
                        Address {
                            ip: IpAddr::V4(Ipv4Addr::from(
                                unsafe { socket.sin_addr.S_un.S_addr }.to_ne_bytes(),
                            )),
                            scope: 0,
                        }
                    }
                    AF_INET6
                        if entry.Address.iSockaddrLength
                            >= std::mem::size_of::<SOCKADDR_IN6>() as i32 =>
                    {
                        let socket = unsafe { &*pointer.cast::<SOCKADDR_IN6>() };
                        Address {
                            ip: IpAddr::V6(Ipv6Addr::from(unsafe { socket.sin6_addr.u.Byte })),
                            scope: unsafe { socket.Anonymous.sin6_scope_id },
                        }
                    }
                    _ => continue,
                };
                addresses.push(address);
            }
        }
        return Ok(addresses);
    }
    Err(io::Error::other(
        "interface list changed during enumeration",
    ))
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
    #[cfg(unix)]
    #[test]
    fn interface_filter_uses_up_not_running_and_only_excludes_subinterfaces() {
        assert!(eligible("veth0", libc::IFF_UP as u32, false));
        assert!(eligible("eth0", libc::IFF_UP as u32, false));
        assert!(!eligible("eth0", libc::IFF_RUNNING as u32, false));
        assert!(!eligible("eth0:1", libc::IFF_UP as u32, false));
        assert!(!eligible("br0", libc::IFF_UP as u32, true));
        assert!(!eligible(
            "ppp0",
            (libc::IFF_UP | libc::IFF_POINTOPOINT) as u32,
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
    }
    fn address(ip: &str) -> Address {
        Address {
            ip: ip.parse().unwrap(),
            scope: 0,
        }
    }
    #[test]
    fn java_selection_prefers_external_v4_then_last_internal_v4_then_v6() {
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
                address("fc00::1")
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
                scope: 7
            }),
            "[fe80:0:0:0:0:0:0:1%7]"
        );
    }
}
