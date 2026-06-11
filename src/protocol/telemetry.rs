use std::time::{SystemTime, Duration};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SensorType {
    ContactV1,
    MotionV1,
    LeakV2,
    ClimateV2,
    Chime,
    ContactV2,
    MotionV2,
    Unknown(u8),
}

impl From<u8> for SensorType {
    fn from(val: u8) -> Self {
        match val {
            0x01 => SensorType::ContactV1,
            0x02 => SensorType::MotionV1,
            0x03 => SensorType::LeakV2,
            0x07 => SensorType::ClimateV2,
            0x0C => SensorType::Chime,
            0x0E => SensorType::ContactV2,
            0x0F => SensorType::MotionV2,
            other => SensorType::Unknown(other),
        }
    }
}

impl SensorType {
    pub fn to_u8(&self) -> u8 {
        match self {
            SensorType::ContactV1 => 0x01,
            SensorType::MotionV1 => 0x02,
            SensorType::LeakV2 => 0x03,
            SensorType::ClimateV2 => 0x07,
            SensorType::Chime => 0x0C,
            SensorType::ContactV2 => 0x0E,
            SensorType::MotionV2 => 0x0F,
            SensorType::Unknown(val) => *val,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SensorType::ContactV1 => "switch",
            SensorType::ContactV2 => "switchv2",
            SensorType::MotionV1 => "motion",
            SensorType::MotionV2 => "motionv2",
            SensorType::LeakV2 => "leak",
            SensorType::ClimateV2 => "climate",
            SensorType::Chime => "chime",
            SensorType::Unknown(_) => "unknown",
        }
    }

    pub fn model_str(&self) -> &'static str {
        match self {
            SensorType::ContactV1 => "Contact Sensor V1",
            SensorType::ContactV2 => "Contact Sensor V2",
            SensorType::MotionV1 => "Motion Sensor V1",
            SensorType::MotionV2 => "Motion Sensor V2",
            SensorType::LeakV2 => "Leak Sensor V2",
            SensorType::ClimateV2 => "Climate Sensor V2",
            SensorType::Chime => "Chime/Alarm V1",
            SensorType::Unknown(_) => "Unknown Sensor",
        }
    }
}

impl std::str::FromStr for SensorType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "switch" | "contact" | "ContactV1" => Ok(SensorType::ContactV1),
            "switchv2" | "contactv2" | "ContactV2" => Ok(SensorType::ContactV2),
            "motion" | "MotionV1" => Ok(SensorType::MotionV1),
            "motionv2" | "MotionV2" => Ok(SensorType::MotionV2),
            "leak" | "LeakV2" => Ok(SensorType::LeakV2),
            "climate" | "ClimateV2" => Ok(SensorType::ClimateV2),
            "chime" | "Chime" => Ok(SensorType::Chime),
            "unknown" => Ok(SensorType::Unknown(0)),
            other => Err(format!("Unknown sensor type string: {}", other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TelemetryData {
    Heartbeat {
        battery: u8,
        /// RSSI in negative dBm. Dongle-appended — NOT part of the sensor's
        /// transmitted RF payload; measured by the dongle during reception.
        rssi: i8,
        /// On-chip die temperature in °C, read from AON_BATMON:TEMP with
        /// battery-voltage compensation (§6.4). NOT ambient temperature;
        /// typically a few degrees warmer than ambient.
        die_temperature_c: i8,
        /// Monotonic event sequence counter (16-bit BE). Increments by 1 per
        /// state-transition event, NOT per reboot (§6.6 correction #3).
        event_sequence: u16,
    },
    Alarm {
        battery: u8,
        rssi: i8,
        state: u8,
        /// On-chip die temperature in °C (same source as Heartbeat).
        die_temperature_c: i8,
        /// Monotonic event sequence counter.
        event_sequence: u16,
    },
    /// Alarm ring buffer history: 12 bytes, each representing the count of
    /// alarm events in a time slot. Only sent by Motion V1 sensors (§6.1, 0xAB).
    AlarmData {
        rssi: i8,
        ring_buffer: [u8; 12],
    },
    Climate {
        battery: u8,
        rssi: i8,
        temperature: f32,
        humidity: u8,
    },
    Leak {
        battery: u8,
        rssi: i8,
        state: u8,
        probe_state: u8,
        probe_available: bool,
    },
    UnknownEvent(Vec<u8>),
    Scanned { version: u8 },
    Offline,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DongleEvent {
    pub mac: String,
    pub timestamp: SystemTime,
    pub sensor_type: SensorType,
    pub event_type: u8,
    pub data: TelemetryData,
}

impl DongleEvent {
    pub const EVENT_TYPE_HEARTBEAT: u8 = 0xA1;
    pub const EVENT_TYPE_ALARM: u8 = 0xA2;
    /// 0xAB: Alarm ring buffer history — 12 slots of per-interval event counts.
    /// Only observed from Motion V1 sensors.
    pub const EVENT_TYPE_ALARM_DATA: u8 = 0xAB;
    pub const EVENT_TYPE_CLIMATE: u8 = 0xE8;
    pub const EVENT_TYPE_LEAK: u8 = 0xEA;
    // Documented in firmware RE (§5.10) but rare/unobserved in live captures.
    pub const EVENT_TYPE_EXTENDED_DATA: u8 = 0xD1;
    pub const EVENT_TYPE_EXTENDED_EVENT: u8 = 0xE1;
    pub const EVENT_TYPE_EXTENDED_STATUS: u8 = 0xE3;

    /// Parses a scan event from the payload of a NOTIFY_SENSOR_SCAN (0x5320) packet.
    pub fn parse_scan(payload: &[u8]) -> Result<Self, &'static str> {
        if payload.len() < 11 {
            return Err("Scan payload too short (need 11 bytes: event_type + mac[8] + type + version)");
        }
        let event_type = payload[0]; // 0xA3
        let mac_bytes = &payload[1..9];
        let sensor_type_val = payload[9];
        let version = payload[10];

        let mac = String::from_utf8(mac_bytes.to_vec())
            .map_err(|_| "Invalid MAC characters (non-UTF8)")?;
        let sensor_type = SensorType::from(sensor_type_val);

        Ok(DongleEvent {
            mac,
            timestamp: SystemTime::now(),
            sensor_type,
            event_type,
            data: TelemetryData::Scanned { version },
        })
    }

    /// Parses a telemetry event from the payload of a NOTIFY_SENSOR_ALARM (0x5319) packet.
    pub fn parse_alarm1(payload: &[u8]) -> Result<Self, &'static str> {
        if payload.len() < 18 {
            return Err("Alarm1 payload too short for header");
        }

        let timestamp_ms = u64::from_be_bytes(payload[0..8].try_into().unwrap());
        let event_type = payload[8];
        let mac_bytes = &payload[9..17];
        let sensor_type_val = payload[17];

        let mac = String::from_utf8(mac_bytes.to_vec())
            .map_err(|_| "Invalid MAC characters (non-UTF8)")?;
        let sensor_type = SensorType::from(sensor_type_val);
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp_ms);
        let remaining = &payload[18..];

        let data = match event_type {
            Self::EVENT_TYPE_HEARTBEAT => {
                // 0xA1 Status Report — AES-encrypted, 32B (2 blocks)
                // remaining layout (after timestamp + event_type + MAC + sensor_type):
                //   [0] = die temperature °C (AON_BATMON:TEMP, battery-compensated)
                //   [1] = battery voltage (AON_BATMON:BAT >> 3, voltage ≈ byte/32.0)
                //   [2] = config flags (typically 0x00)
                //   [3] = marker (constant 0x01)
                //   [4] = state/flags
                //   [5..7] = event sequence counter (16-bit BE, monotonic +1 per event)
                //   [7] = RSSI (dongle-appended, negate for dBm)
                if remaining.len() < 8 {
                    return Err("Heartbeat payload too short");
                }
                let die_temperature_c = remaining[0] as i8;
                let battery = remaining[1];
                let event_sequence = u16::from_be_bytes([remaining[5], remaining[6]]);
                let rssi = (remaining[7] as i8).saturating_neg();
                TelemetryData::Heartbeat { battery, rssi, die_temperature_c, event_sequence }
            }
            Self::EVENT_TYPE_ALARM => {
                // 0xA2 Registration — AES-encrypted, 32B (2 blocks)
                // Same byte layout as 0xA1; byte[4] = sensor state (0=inactive, 1=active)
                if remaining.len() < 8 {
                    return Err("Alarm payload too short");
                }
                let die_temperature_c = remaining[0] as i8;
                let battery = remaining[1];
                let state = remaining[4];
                let event_sequence = u16::from_be_bytes([remaining[5], remaining[6]]);
                let rssi = (remaining[7] as i8).saturating_neg();
                TelemetryData::Alarm { battery, rssi, state, die_temperature_c, event_sequence }
            }
            Self::EVENT_TYPE_ALARM_DATA => {
                // 0xAB Alarm Data — AES-encrypted, 32B (2 blocks)
                // Contains a 12-byte ring buffer of per-slot alarm event counts.
                // remaining layout:
                //   [0..12] = ring buffer (12 bytes, each = event count per time slot)
                //   [12..14] = flag bytes (typically 0x01, 0x01)
                //   RSSI: last byte if present (dongle-appended)
                if remaining.len() < 12 {
                    return Err("AlarmData payload too short");
                }
                let mut ring_buffer = [0u8; 12];
                ring_buffer.copy_from_slice(&remaining[0..12]);
                let rssi = if remaining.len() > 12 {
                    (remaining[remaining.len() - 1] as i8).saturating_neg()
                } else {
                    0
                };
                TelemetryData::AlarmData { rssi, ring_buffer }
            }
            Self::EVENT_TYPE_CLIMATE => {
                // 0xE8 Climate Data — dongle-synthesized from Climate V2 sensor
                if remaining.len() < 10 {
                    return Err("Climate payload too short");
                }
                let battery = remaining[1];
                let temp_hi = remaining[4] as i8;
                let temp_lo = remaining[5];
                let humidity = remaining[6];
                // RSSI is the last byte, dongle-appended
                let rssi = (remaining[9] as i8).saturating_neg();
                let temperature = (temp_hi as f32) + ((temp_lo as f32) / 100.0);
                TelemetryData::Climate {
                    battery,
                    rssi,
                    temperature,
                    humidity,
                }
            }
            Self::EVENT_TYPE_EXTENDED_DATA | Self::EVENT_TYPE_EXTENDED_EVENT
            | Self::EVENT_TYPE_EXTENDED_STATUS => {
                // Documented in firmware RE (§5.10) but rare/unobserved in live captures.
                // Log at debug level and treat as unknown for now.
                tracing::debug!(
                    "Received known-but-unimplemented event type 0x{:02X} from {} ({} bytes)",
                    event_type, mac, remaining.len()
                );
                TelemetryData::UnknownEvent(remaining.to_vec())
            }
            _ => TelemetryData::UnknownEvent(remaining.to_vec()),
        };

        Ok(DongleEvent {
            mac,
            timestamp,
            sensor_type,
            event_type,
            data,
        })
    }

    /// Parses a telemetry event from the payload of a NOTIFY_SENSOR_ALARM2 (0x5355) packet.
    pub fn parse_alarm2(payload: &[u8]) -> Result<Self, &'static str> {
        if payload.len() < 10 {
            return Err("Alarm2 payload too short for header");
        }

        let event_type = payload[0];
        let mac_bytes = &payload[1..9];
        let sensor_type_val = payload[9];

        let mac = String::from_utf8(mac_bytes.to_vec())
            .map_err(|_| "Invalid MAC characters (non-UTF8)")?;
        let sensor_type = SensorType::from(sensor_type_val);
        let timestamp = SystemTime::now();
        let remaining = &payload[10..];

        let data = match event_type {
            Self::EVENT_TYPE_LEAK => {
                if remaining.len() < 11 {
                    return Err("Leak payload too short");
                }
                let battery = remaining[2];
                let state = remaining[5];
                let probe_state = remaining[6];
                let probe_available = remaining[7] == 1;
                let rssi = (remaining[10] as i8).saturating_neg();
                TelemetryData::Leak {
                    battery,
                    rssi,
                    state,
                    probe_state,
                    probe_available,
                }
            }
            _ => TelemetryData::UnknownEvent(remaining.to_vec()),
        };

        Ok(DongleEvent {
            mac,
            timestamp,
            sensor_type,
            event_type,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_scan_extracts_version() {
        // Payload: event_type=0xA3, MAC="ABCD1234", type=ContactV1(0x01), version=23
        let payload = vec![
            0xA3,                               // event_type
            b'A', b'B', b'C', b'D', b'1', b'2', b'3', b'4', // MAC
            0x01,                               // ContactV1
            0x17,                               // version = 23
        ];
        let event = DongleEvent::parse_scan(&payload).unwrap();
        assert_eq!(event.mac, "ABCD1234");
        assert_eq!(event.sensor_type, SensorType::ContactV1);
        assert_eq!(event.event_type, 0xA3);
        assert_eq!(event.data, TelemetryData::Scanned { version: 23 });
    }

    #[test]
    fn test_parse_scan_motion_v2_version() {
        let payload = vec![
            0xA3,
            b'M', b'O', b'T', b'I', b'O', b'N', b'0', b'1',
            0x0F,  // MotionV2
            0x19,  // version = 25
        ];
        let event = DongleEvent::parse_scan(&payload).unwrap();
        assert_eq!(event.mac, "MOTION01");
        assert_eq!(event.sensor_type, SensorType::MotionV2);
        assert_eq!(event.data, TelemetryData::Scanned { version: 25 });
    }

    #[test]
    fn test_parse_scan_rejects_short_payload() {
        // Only 10 bytes — missing the version byte
        let payload = vec![
            0xA3,
            b'A', b'B', b'C', b'D', b'1', b'2', b'3', b'4',
            0x01,
        ];
        let result = DongleEvent::parse_scan(&payload);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too short"));
    }

    #[test]
    fn test_parse_scan_version_zero() {
        let payload = vec![
            0xA3,
            b'Z', b'E', b'R', b'O', b'V', b'E', b'R', b'S',
            0x07,  // ClimateV2
            0x00,  // version = 0
        ];
        let event = DongleEvent::parse_scan(&payload).unwrap();
        assert_eq!(event.sensor_type, SensorType::ClimateV2);
        assert_eq!(event.data, TelemetryData::Scanned { version: 0 });
    }
}
