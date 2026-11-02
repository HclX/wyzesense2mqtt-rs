//! OpenWyzeSense gateway extensions: `0x53F0` (host → gateway) / `0x53F1`
//! (gateway → host), each multiplexed by a sub-ID in the first payload byte.
//!
//! Only stock-incompatible gateways running OpenWyzeSense firmware answer
//! these; a stock dongle stays silent, which is how the host tells them apart.
//! Spec: OpenWyzeSense `docs/roaming.md` and issue https://github.com/HclX/openwyzesense/issues/41.

pub const CMD_OWS: u16 = 0x53F0;
pub const CMD_OWS_RESPONSE: u16 = 0x53F1;

pub const SUB_FLAGS: u8 = 0x01;
pub const SUB_BUILD_INFO: u8 = 0x02;
pub const SUB_NETWORK: u8 = 0x04;
pub const SUB_NV: u8 = 0x06;
pub const SUB_OTA_OFFER: u8 = 0x10;
pub const SUB_OTA_NEED: u8 = 0x11;
pub const SUB_OTA_DATA: u8 = 0x12;
pub const SUB_OTA_DONE: u8 = 0x13;
pub const SUB_OVERHEARD: u8 = 0x20;
pub const SUB_ADDR_RANGE: u8 = 0x23;

pub const NV_ADD_SENSOR: u8 = 0x03;
pub const NV_GET_SENSOR: u8 = 0x04;

pub const FLAG_DEDUP: u32 = 1 << 0;
pub const FLAG_OTA: u32 = 1 << 2;
pub const FLAG_FIX_ZERO_MAC: u32 = 1 << 1;
pub const FLAG_ANNOUNCE: u32 = 1 << 3;
pub const FLAG_ROAM: u32 = 1 << 4;
/// The flags this host sets from its configuration; others (OTA) it leaves alone.
pub const MANAGED_FLAGS: u32 = FLAG_DEDUP | FLAG_FIX_ZERO_MAC | FLAG_ANNOUNCE | FLAG_ROAM;

pub const STATUS_OK: u8 = 0x00;

/// Lowest and highest short addresses a gateway may hand out.
pub const ADDR_FIRST: u16 = 0x0001;
pub const ADDR_LAST: u16 = 0xFFFD;

/// The key a `0x53F1` reply completes in the engine's pending-request map.
/// Replies are told apart by sub-ID, so each sub-ID gets its own slot; the
/// `0xF1` high byte cannot collide with a `0x43xx`/`0x53xx` command.
pub fn response_key(sub: u8) -> u16 {
    0xF100 | sub as u16
}

/// A v1 contact reports an all-zero MAC (OpenWyzeSense known-issues.md §1).
pub fn is_zero_mac(mac: &str) -> bool {
    mac.bytes().all(|b| b == 0) || mac == "00000000"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Flags {
    pub enabled: u32,
    pub supported: u32,
}

/// `[0x01, enabled_be32, supported_be32]`
pub fn parse_flags(p: &[u8]) -> Result<Flags, String> {
    if p.len() < 9 || p[0] != SUB_FLAGS {
        return Err(format!("bad flags reply: {:02x?}", p));
    }
    Ok(Flags {
        enabled: u32::from_be_bytes([p[1], p[2], p[3], p[4]]),
        supported: u32::from_be_bytes([p[5], p[6], p[7], p[8]]),
    })
}

/// The coordinator identity a gateway presents to sensors. Gateways sharing
/// one can hand sensors to each other without re-pairing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkIdentity {
    pub pan: u16,
    pub short: u16,
    pub ext: u64,
}

impl std::fmt::Display for NetworkIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PAN {:04X} / {:04X} / {:016X}", self.pan, self.short, self.ext)
    }
}

/// `[0x04, status, pan_le16, short_le16, ext_le64]`
pub fn parse_network(p: &[u8]) -> Result<NetworkIdentity, String> {
    if p.len() < 14 || p[0] != SUB_NETWORK {
        return Err(format!("bad network reply: {:02x?}", p));
    }
    if p[1] != STATUS_OK {
        return Err(format!("network read refused (status {:02X})", p[1]));
    }
    Ok(NetworkIdentity {
        pan: u16::from_le_bytes([p[2], p[3]]),
        short: u16::from_le_bytes([p[4], p[5]]),
        ext: u64::from_le_bytes(p[6..14].try_into().unwrap()),
    })
}

/// What a gateway says about its firmware (`OWS_SUB_BUILD_INFO`, pages 0..2).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BuildInfo {
    pub id: String,
    pub product: String,
    pub app: String,
    pub board: String,
    pub version: String,
}

const BUILD_INFO_MAGIC: u32 = 0x4253_574F; // "OWSB"

fn c_str(b: &[u8]) -> String {
    let end = b.iter().position(|c| *c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).to_string()
}

/// Pages are the `0x53F1` payloads (sub-ID included): 53, 55 and 27 bytes.
/// Page 2 (the rest of a long version string) is optional.
pub fn parse_build_info(p0: &[u8], p1: &[u8], p2: Option<&[u8]>) -> Result<BuildInfo, String> {
    let page = |p: &'_ [u8], len: usize| -> Option<Vec<u8>> {
        (p.len() == len && p[0] == SUB_BUILD_INFO).then(|| p[1..].to_vec())
    };
    let (p0, p1) = match (page(p0, 53), page(p1, 55)) {
        (Some(a), Some(b)) => (a, b),
        _ => return Err("bad build-info pages".into()),
    };
    if u32::from_le_bytes(p0[0..4].try_into().unwrap()) != BUILD_INFO_MAGIC || p0[4] != 1 || p0[8..16] != p1[..8] {
        return Err("build-info pages do not match".into());
    }
    let mut version = p1[32..54].to_vec();
    if let Some(p2) = p2.and_then(|p| page(p, 27)).filter(|p| p[..8] == p0[8..16]) {
        version.extend_from_slice(&p2[8..26]);
    }
    Ok(BuildInfo {
        id: p0[8..16].iter().map(|b| format!("{:02x}", b)).collect(),
        product: c_str(&p0[16..40]),
        app: c_str(&p0[40..52]),
        board: c_str(&p1[8..32]),
        version: c_str(&version),
    })
}

/// One entry of a gateway's sensor table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NvSensor {
    pub mac: String,
    pub short: u16,
    /// Sensor type byte, 0 when the gateway has not learned it.
    pub sensor_type: u8,
}

/// Request payload for `OWS_NV_GET_SENSOR`.
pub fn build_get_sensor(index: u8) -> Vec<u8> {
    vec![SUB_NV, NV_GET_SENSOR, index]
}

/// Reply to `OWS_NV_GET_SENSOR`:
/// `[0x06, 0x04, status, count, index, sub_le16, mac8, short_le16, type]`.
/// Returns the table size and the entry (`None` once `index` is past the end).
pub fn parse_get_sensor(p: &[u8]) -> Result<(u8, u8, Option<NvSensor>), String> {
    if p.len() < 5 || p[0] != SUB_NV || p[1] != NV_GET_SENSOR {
        return Err(format!("bad get-sensor reply: {:02x?}", p));
    }
    let (count, index) = (p[3], p[4]);
    if p[2] != STATUS_OK {
        return Ok((count, index, None));
    }
    if p.len() < 18 {
        return Err(format!("short get-sensor reply: {:02x?}", p));
    }
    let mac = String::from_utf8(p[7..15].to_vec()).map_err(|_| "non-ASCII MAC in sensor table".to_string())?;
    Ok((
        count,
        index,
        Some(NvSensor { mac, short: u16::from_le_bytes([p[15], p[16]]), sensor_type: p[17] }),
    ))
}

/// Request payload for `OWS_NV_ADD_SENSOR`: `[0x06, 0x03, mac8, short_le16, type]`.
pub fn build_add_sensor(mac: &str, short: u16, sensor_type: u8) -> Result<Vec<u8>, String> {
    if mac.len() != 8 {
        return Err(format!("MAC {:?} is not 8 characters", mac));
    }
    let mut p = vec![SUB_NV, NV_ADD_SENSOR];
    p.extend_from_slice(mac.as_bytes());
    p.extend_from_slice(&short.to_le_bytes());
    p.push(sensor_type);
    Ok(p)
}

/// Reply to `OWS_NV_ADD_SENSOR`: `[0x06, 0x03, status, count]` → status.
pub fn parse_add_sensor(p: &[u8]) -> Result<u8, String> {
    if p.len() < 3 || p[0] != SUB_NV || p[1] != NV_ADD_SENSOR {
        return Err(format!("bad add-sensor reply: {:02x?}", p));
    }
    Ok(p[2])
}

/// Request payload for `OWS_SUB_ADDR_RANGE` (set).
pub fn build_addr_range(first: u16, last: u16) -> Vec<u8> {
    let mut p = vec![SUB_ADDR_RANGE];
    p.extend_from_slice(&first.to_le_bytes());
    p.extend_from_slice(&last.to_le_bytes());
    p
}

/// `[0x23, status, first_le16, last_le16]`
pub fn parse_addr_range(p: &[u8]) -> Result<(u8, u16, u16), String> {
    if p.len() < 6 || p[0] != SUB_ADDR_RANGE {
        return Err(format!("bad address-range reply: {:02x?}", p));
    }
    Ok((p[1], u16::from_le_bytes([p[2], p[3]]), u16::from_le_bytes([p[4], p[5]])))
}

/// `OWS_SUB_OVERHEARD`: `[0x20, short_le16, <the 0x5319 payload>]` → (short, 0x5319 payload).
pub fn parse_overheard(p: &[u8]) -> Result<(u16, &[u8]), String> {
    if p.len() < 3 || p[0] != SUB_OVERHEARD {
        return Err(format!("bad overheard report: {:02x?}", p));
    }
    Ok((u16::from_le_bytes([p[1], p[2]]), &p[3..]))
}

/// The address range a gateway gets right before it pairs: everything above
/// the highest short address any gateway in the network already uses, so the
/// new sensor cannot collide with one paired elsewhere.
pub fn pairing_range<'a>(shorts: impl IntoIterator<Item = &'a u16>) -> Option<(u16, u16)> {
    let max = shorts.into_iter().copied().filter(|s| *s <= ADDR_LAST).max().unwrap_or(0);
    let first = max.checked_add(1)?.max(ADDR_FIRST);
    (first <= ADDR_LAST).then_some((first, ADDR_LAST))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_reply() {
        let f = parse_flags(&[0x01, 0, 0, 0, 0x11, 0, 0, 0, 0x1F]).unwrap();
        assert_eq!(f, Flags { enabled: FLAG_ROAM | FLAG_DEDUP, supported: 0x1F });
        assert!(parse_flags(&[0x04, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(parse_flags(&[0x01, 0, 0]).is_err());
    }

    #[test]
    fn network_reply() {
        let mut p = vec![0x04, 0x00, 0xFE, 0x65, 0x00, 0x00];
        p.extend_from_slice(&0x00124B00E2E0000Au64.to_le_bytes());
        let id = parse_network(&p).unwrap();
        assert_eq!(id, NetworkIdentity { pan: 0x65FE, short: 0, ext: 0x00124B00E2E0000A });
        p[1] = 0xE0;
        assert!(parse_network(&p).is_err());
    }

    #[test]
    fn get_sensor_reply() {
        let mut p = vec![0x06, 0x04, 0x00, 2, 1, 0x05, 0x00];
        p.extend_from_slice(b"77BD514D");
        p.extend_from_slice(&[0x34, 0x12, 0x0E]);
        let (count, index, s) = parse_get_sensor(&p).unwrap();
        assert_eq!((count, index), (2, 1));
        assert_eq!(s, Some(NvSensor { mac: "77BD514D".into(), short: 0x1234, sensor_type: 0x0E }));
        // past the end: [0x06, 0x04, refused, count, index]
        assert_eq!(parse_get_sensor(&[0x06, 0x04, 0xE0, 2, 2]).unwrap(), (2, 2, None));
    }

    #[test]
    fn add_sensor_round_trip() {
        let p = build_add_sensor("77BD514D", 0x4001, 0x0E).unwrap();
        assert_eq!(p[..2], [0x06, 0x03]);
        assert_eq!(&p[2..10], b"77BD514D");
        assert_eq!(p[10..], [0x01, 0x40, 0x0E]);
        assert!(build_add_sensor("short", 1, 1).is_err());
        assert_eq!(parse_add_sensor(&[0x06, 0x03, 0x00, 1]).unwrap(), 0);
    }

    #[test]
    fn addr_range() {
        assert_eq!(build_addr_range(0x4000, 0xFFFD), vec![0x23, 0x00, 0x40, 0xFD, 0xFF]);
        assert_eq!(parse_addr_range(&[0x23, 0, 0x00, 0x40, 0xFD, 0xFF]).unwrap(), (0, 0x4000, 0xFFFD));
    }

    #[test]
    fn overheard_report() {
        let (short, rest) = parse_overheard(&[0x20, 0x01, 0x40, 0xAA, 0xBB]).unwrap();
        assert_eq!(short, 0x4001);
        assert_eq!(rest, &[0xAA, 0xBB]);
        assert!(parse_overheard(&[0x01, 0, 0]).is_err());
    }

    #[test]
    fn pairing_range_is_above_every_used_address() {
        assert_eq!(pairing_range(&[]), Some((0x0001, 0xFFFD)));
        assert_eq!(pairing_range(&[0x0003, 0x4002, 0x0010]), Some((0x4003, 0xFFFD)));
        assert_eq!(pairing_range(&[0xFFFC]), Some((0xFFFD, 0xFFFD)));
        assert_eq!(pairing_range(&[0xFFFD]), None);
    }

    #[test]
    fn build_info_pages() {
        let id = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let mut p0 = vec![SUB_BUILD_INFO];
        p0.extend_from_slice(&BUILD_INFO_MAGIC.to_le_bytes());
        p0.extend_from_slice(&[1, 0, 0, 0]);
        p0.extend_from_slice(&id);
        let mut product = b"OpenWyzeSense".to_vec();
        product.resize(24, 0);
        p0.extend_from_slice(&product);
        let mut app = b"gateway".to_vec();
        app.resize(12, 0);
        p0.extend_from_slice(&app);
        assert_eq!(p0.len(), 53);
        let mut p1 = vec![SUB_BUILD_INFO];
        p1.extend_from_slice(&id);
        let mut board = b"host".to_vec();
        board.resize(24, 0);
        p1.extend_from_slice(&board);
        let mut ver = b"0.0.0.47 V1.8 Gateway ".to_vec();
        ver.resize(22, 0);
        p1.extend_from_slice(&ver);
        assert_eq!(p1.len(), 55);
        let mut p2 = vec![SUB_BUILD_INFO];
        p2.extend_from_slice(&id);
        let mut rest = b"GW3U".to_vec();
        rest.resize(18, 0);
        p2.extend_from_slice(&rest);
        let info = parse_build_info(&p0, &p1, Some(&p2)).unwrap();
        assert_eq!(info.version, "0.0.0.47 V1.8 Gateway GW3U");
        assert_eq!((info.product.as_str(), info.app.as_str(), info.board.as_str()), ("OpenWyzeSense", "gateway", "host"));
        assert_eq!(info.id, "0102030405060708");
        assert_eq!(parse_build_info(&p0, &p1, None).unwrap().version, "0.0.0.47 V1.8 Gateway ");
        p1[1] = 9;
        assert!(parse_build_info(&p0, &p1, None).is_err());
    }

    #[test]
    fn zero_mac() {
        assert!(is_zero_mac("\0\0\0\0\0\0\0\0"));
        assert!(is_zero_mac("00000000"));
        assert!(!is_zero_mac("77BD514D"));
    }
}
