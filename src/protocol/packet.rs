use std::fmt;

pub mod commands {
    // Sync commands (0x43XX)
    pub const CMD_INQUIRY: u16 = 0x4327;
    pub const CMD_INQUIRY_RESPONSE: u16 = 0x4328;
    pub const CMD_GET_ENR: u16 = 0x4302;
    pub const CMD_ENR_RESPONSE: u16 = 0x4303;
    pub const CMD_GET_MAC: u16 = 0x4304;
    pub const CMD_MAC_RESPONSE: u16 = 0x4305;

    // Async commands (0x53XX)
    pub const CMD_GET_VERSION: u16 = 0x5316;
    pub const CMD_VERSION_RESPONSE: u16 = 0x5317;
    pub const CMD_FINISH_AUTH: u16 = 0x5314;
    pub const CMD_FINISH_AUTH_RESPONSE: u16 = 0x5315;
    pub const CMD_SET_SCAN: u16 = 0x531C;
    pub const CMD_SET_SCAN_RESPONSE: u16 = 0x531D;
    pub const CMD_SENSOR_SCAN: u16 = 0x5320;
    pub const CMD_GET_R1: u16 = 0x5321;
    pub const CMD_R1_RESPONSE: u16 = 0x5322;
    pub const CMD_VERIFY_SENSOR: u16 = 0x5323;
    pub const CMD_VERIFY_SENSOR_RESPONSE: u16 = 0x5324;
    pub const CMD_DELETE_SENSOR: u16 = 0x5325;
    pub const CMD_DELETE_SENSOR_RESPONSE: u16 = 0x5326;
    pub const CMD_ALARM1: u16 = 0x5319;
    pub const CMD_ALARM2: u16 = 0x5355;
    pub const CMD_GET_SENSOR_COUNT: u16 = 0x532E;
    pub const CMD_SENSOR_COUNT_RESPONSE: u16 = 0x532F;
    pub const CMD_GET_SENSOR_LIST: u16 = 0x5330;
    pub const CMD_SENSOR_LIST_ITEM: u16 = 0x5331;
    pub const CMD_TIME_SYNC: u16 = 0x5332;
    pub const CMD_TIME_SYNC_RESPONSE: u16 = 0x5333;
    pub const CMD_EVENT_LOG: u16 = 0x5335;
    pub const CMD_PLAY_CHIME: u16 = 0x5370;
    pub const CMD_PLAY_CHIME_RESPONSE: u16 = 0x5371;
    pub const CMD_ASYNC_ACK: u16 = 0x53FF;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CommandType {
    Sync = 0x43,
    Async = 0x53,
}

impl TryFrom<u8> for CommandType {
    type Error = &'static str;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x43 => Ok(CommandType::Sync),
            0x53 => Ok(CommandType::Async),
            _ => Err("Invalid CommandType byte"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PacketPayload {
    Ack(u16),         // The acknowledged command ID (cmd_type << 8 | b2)
    Bytes(Vec<u8>),   // Raw payload bytes
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub cmd_type: CommandType,
    pub command_id: u8,
    pub payload: PacketPayload,
}

impl Packet {
    pub const ASYNC_ACK: u16 = commands::CMD_ASYNC_ACK;

    pub fn new_sync(command_id: u8, payload: Vec<u8>) -> Self {
        Self {
            cmd_type: CommandType::Sync,
            command_id,
            payload: PacketPayload::Bytes(payload),
        }
    }

    pub fn new_async(command_id: u8, payload: Vec<u8>) -> Self {
        Self {
            cmd_type: CommandType::Async,
            command_id,
            payload: PacketPayload::Bytes(payload),
        }
    }

    pub fn new_ack(ack_cmd: u16) -> Self {
        Self {
            cmd_type: CommandType::Async,
            command_id: 0xFF,
            payload: PacketPayload::Ack(ack_cmd),
        }
    }

    pub fn cmd(&self) -> u16 {
        ((self.cmd_type as u16) << 8) | (self.command_id as u16)
    }

    /// Returns the payload bytes, or None if this is an ACK packet.
    pub fn payload_bytes(&self) -> Option<&[u8]> {
        match &self.payload {
            PacketPayload::Bytes(b) => Some(b),
            PacketPayload::Ack(_) => None,
        }
    }

    /// Calculates the 16-bit checksum of a byte slice.
    pub fn calculate_checksum(data: &[u8]) -> u16 {
        let sum: u32 = data.iter().map(|&b| b as u32).sum();
        (sum & 0xFFFF) as u16
    }

    /// Parses a packet from a byte slice.
    /// Returns the parsed Packet and the total number of bytes consumed if successful.
    pub fn parse(s: &[u8]) -> Result<(Self, usize), &'static str> {
        if s.len() < 5 {
            return Err("Buffer too short for header");
        }

        // 1. Verify Magic Prefix
        let magic = ((s[0] as u16) << 8) | (s[1] as u16);
        if magic != 0x55AA && magic != 0xAA55 {
            return Err("Invalid packet magic prefix");
        }

        // 2. Extract Header
        let cmd_type_val = s[2];
        let b2 = s[3];
        let cmd_id = s[4];

        let cmd_type = CommandType::try_from(cmd_type_val)?;
        let cmd = ((cmd_type_val as u16) << 8) | (cmd_id as u16);

        if cmd == Self::ASYNC_ACK {
            if s.len() < 7 {
                return Err("Buffer too short for ACK packet");
            }
            // Verify checksum
            let cs_remote = ((s[5] as u16) << 8) | (s[6] as u16);
            let cs_local = Self::calculate_checksum(&s[..5]);
            if cs_remote != cs_local {
                return Err("Mismatched checksum in ACK packet");
            }

            let ack_cmd = ((cmd_type_val as u16) << 8) | (b2 as u16);
            let pkt = Packet {
                cmd_type,
                command_id: cmd_id,
                payload: PacketPayload::Ack(ack_cmd),
            };
            return Ok((pkt, 7));
        }

        // For normal packets, length is b2 + 4
        let expected_len = (b2 as usize) + 4;
        if s.len() < expected_len {
            return Err("Buffer too short for expected packet length");
        }

        // Verify checksum
        let cs_remote = ((s[expected_len - 2] as u16) << 8) | (s[expected_len - 1] as u16);
        let cs_local = Self::calculate_checksum(&s[..expected_len - 2]);
        if cs_remote != cs_local {
            return Err("Mismatched checksum in packet");
        }

        // Extract payload: bytes from offset 5 to (expected_len - 2)
        let payload_bytes = s[5..expected_len - 2].to_vec();
        let pkt = Packet {
            cmd_type,
            command_id: cmd_id,
            payload: PacketPayload::Bytes(payload_bytes),
        };

        Ok((pkt, expected_len))
    }

    /// Serializes the packet into bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut pkt = Vec::new();
        // 1. Magic
        pkt.push(0xAA);
        pkt.push(0x55);

        // 2. Command type
        pkt.push(self.cmd_type as u8);

        match &self.payload {
            PacketPayload::Ack(ack_cmd) => {
                // b2 is the lower byte of the ACK command ID
                pkt.push((ack_cmd & 0xFF) as u8);
                // Command ID (always 0xFF for Async ACK)
                pkt.push(self.command_id);
            }
            PacketPayload::Bytes(bytes) => {
                // b2 = len(payload) + 3
                let b2 = (bytes.len() + 3) as u8;
                pkt.push(b2);
                // Command ID
                pkt.push(self.command_id);
                // Payload
                pkt.extend_from_slice(bytes);
            }
        }

        // Checksum
        let checksum = Self::calculate_checksum(&pkt);
        pkt.push((checksum >> 8) as u8);
        pkt.push((checksum & 0xFF) as u8);

        pkt
    }
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.payload {
            PacketPayload::Ack(ack_cmd) => write!(
                f,
                "Packet: Cmd={:04X}, Payload=ACK({:04X})",
                self.cmd(),
                ack_cmd
            ),
            PacketPayload::Bytes(bytes) => {
                let hex_str = bytes
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<String>>()
                    .join(",");
                write!(
                    f,
                    "Packet: Cmd={:04X}, Payload=[{}]",
                    self.cmd(),
                    if hex_str.is_empty() { "<None>".to_string() } else { hex_str }
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum() {
        let data = [0xAA, 0x55, 0x43, 0x03, 0x27];
        assert_eq!(Packet::calculate_checksum(&data), 0x016C);
    }

    #[test]
    fn test_serialize_inquiry() {
        let pkt = Packet::new_sync(0x27, vec![]);
        let bytes = pkt.to_bytes();
        assert_eq!(bytes, vec![0xAA, 0x55, 0x43, 0x03, 0x27, 0x01, 0x6C]);
    }

    #[test]
    fn test_parse_inquiry() {
        let bytes = [0xAA, 0x55, 0x43, 0x03, 0x27, 0x01, 0x6C];
        let (pkt, len) = Packet::parse(&bytes).unwrap();
        assert_eq!(len, 7);
        assert_eq!(pkt.cmd_type, CommandType::Sync);
        assert_eq!(pkt.command_id, 0x27);
        assert_eq!(pkt.payload, PacketPayload::Bytes(vec![]));
    }

    #[test]
    fn test_parse_inquiry_response() {
        // Magic: 55 AA, Type: 43, Length: 04, Cmd: 28, Payload: 01, Checksum: 01 6F
        let bytes = [0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F];
        let (pkt, len) = Packet::parse(&bytes).unwrap();
        assert_eq!(len, 8);
        assert_eq!(pkt.cmd_type, CommandType::Sync);
        assert_eq!(pkt.command_id, 0x28);
        assert_eq!(pkt.payload, PacketPayload::Bytes(vec![0x01]));

        // Test round-trip
        assert_eq!(pkt.to_bytes(), vec![0xAA, 0x55, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F]);
    }

    #[test]
    fn test_parse_ack() {
        // Magic: 55 AA, Type: 53, b2: 27, Cmd: FF, Checksum: 02 78 (0x55+0xAA+0x53+0x27+0xFF = 632 = 0x0278)
        let bytes = [0x55, 0xAA, 0x53, 0x27, 0xFF, 0x02, 0x78];
        let (pkt, len) = Packet::parse(&bytes).unwrap();
        assert_eq!(len, 7);
        assert_eq!(pkt.cmd_type, CommandType::Async);
        assert_eq!(pkt.command_id, 0xFF);
        assert_eq!(pkt.payload, PacketPayload::Ack(0x5327));

        // Round trip serialization
        assert_eq!(pkt.to_bytes(), vec![0xAA, 0x55, 0x53, 0x27, 0xFF, 0x02, 0x78]);
    }
}
