use std::{
    hash::{BuildHasher, Hash, Hasher},
    net::IpAddr,
    sync::{
        OnceLock,
        atomic::{AtomicU32, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use rocketmq_classic_common::message::{
    Message, constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
};

static COUNTER: AtomicU32 = AtomicU32::new(0);
static PROCESS_HASH: OnceLock<u32> = OnceLock::new();

/// Emits the same 16/28-byte hexadecimal layout as Java's
/// `MessageClientIDSetter`: IP, process ID, process identity, month-relative
/// milliseconds, and a wrapping 16-bit counter.
#[doc(hidden)]
pub struct UniqueIdGenerator {
    ip: Vec<u8>,
}

impl UniqueIdGenerator {
    pub fn new(client_ip: Option<&str>) -> Self {
        let ip = client_ip
            .and_then(|value| value.parse::<IpAddr>().ok())
            .map(|address| match address {
                IpAddr::V4(address) => address.octets().to_vec(),
                IpAddr::V6(address) => address.octets().to_vec(),
            })
            .unwrap_or_else(fake_ip);
        Self { ip }
    }

    pub fn set_if_missing(&self, message: &mut Message) -> String {
        if let Some(id) = message
            .properties()
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .cloned()
        {
            return id;
        }
        let id = self.create();
        let _ = message.set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, id.clone());
        id
    }

    pub fn create(&self) -> String {
        let millis = now_millis();
        let month_start = month_start_millis(millis);
        let mut bytes = Vec::with_capacity(self.ip.len() + 12);
        bytes.extend_from_slice(&self.ip);
        bytes.extend_from_slice(&(std::process::id() as u16).to_be_bytes());
        bytes.extend_from_slice(&process_hash().to_be_bytes());
        let mut diff = millis.wrapping_sub(month_start) as i32;
        if diff < 0 && diff > -1_000_000 {
            diff = 0;
        }
        bytes.extend_from_slice(&diff.to_be_bytes());
        bytes.extend_from_slice(&(COUNTER.fetch_add(1, Ordering::Relaxed) as u16).to_be_bytes());
        encode_hex(&bytes)
    }
}

fn process_hash() -> u32 {
    *PROCESS_HASH.get_or_init(|| {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        std::process::id().hash(&mut hasher);
        SystemTime::now().hash(&mut hasher);
        std::thread::current().id().hash(&mut hasher);
        hasher.finish() as u32
    })
}

fn fake_ip() -> Vec<u8> {
    now_millis().to_be_bytes()[4..].to_vec()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn month_start_millis(millis: i64) -> i64 {
    const DAY_MILLIS: i64 = 86_400_000;
    let day = millis.div_euclid(DAY_MILLIS);
    let (year, month, _) = civil_from_days(day);
    days_from_civil(year, month, 1) * DAY_MILLIS
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_hex(input: &str) -> Vec<u8> {
        input
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |value: u8| match value {
                    b'0'..=b'9' => value - b'0',
                    b'A'..=b'F' => value - b'A' + 10,
                    _ => panic!("invalid hex digit"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }

    #[test]
    fn generated_id_uses_the_java_client_id_layout_and_advancing_counter() {
        let generator = UniqueIdGenerator::new(Some("10.10.10.111"));
        let first = generator.create();
        let second = generator.create();
        let first_bytes = decode_hex(&first);
        let second_bytes = decode_hex(&second);
        assert_eq!(first.len(), 32);
        assert_eq!(&first_bytes[..4], &[10, 10, 10, 111]);
        assert_eq!(first_bytes.len(), 16);
        let first_counter = u16::from_be_bytes([first_bytes[14], first_bytes[15]]);
        let second_counter = u16::from_be_bytes([second_bytes[14], second_bytes[15]]);
        assert_ne!(first_counter, second_counter);
        assert_ne!(second_counter.wrapping_sub(first_counter), 0);
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_lowercase())
        );
    }

    #[test]
    fn existing_unique_id_is_preserved_when_preparing_a_message() {
        let generator = UniqueIdGenerator::new(Some("10.10.10.111"));
        let mut message = Message::new("orders", b"body".to_vec());
        message
            .set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "existing")
            .unwrap();
        assert_eq!(generator.set_if_missing(&mut message), "existing");
        assert_eq!(
            message.properties()[PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX],
            "existing"
        );
    }

    #[test]
    fn generated_id_layout_matches_the_pinned_java_unique_id_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let shape = &fixture["uniqueId"];
        assert_eq!(shape["processIdBytes"], 2);
        assert_eq!(shape["processIdentityBytes"], 4);
        assert_eq!(shape["monthTimestampBytes"], 4);
        assert_eq!(shape["counterBytes"], 2);
        assert!(shape["hexUppercase"].as_bool().unwrap());

        for (address, ip_length) in [("10.10.10.111", 4usize), ("2001:db8::1", 16)] {
            let generator = UniqueIdGenerator::new(Some(address));
            let id = generator.create();
            let bytes = decode_hex(&id);
            assert_eq!(bytes.len(), ip_length + 12);
            assert_eq!(
                id.len(),
                (ip_length
                    + shape["processIdBytes"].as_u64().unwrap() as usize
                    + shape["processIdentityBytes"].as_u64().unwrap() as usize
                    + shape["monthTimestampBytes"].as_u64().unwrap() as usize
                    + shape["counterBytes"].as_u64().unwrap() as usize)
                    * 2
            );
            assert!(
                id.bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_lowercase())
            );
        }
    }
}
