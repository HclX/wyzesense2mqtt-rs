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
        rssi: i8,
    },
    Alarm {
        battery: u8,
        rssi: i8,
        state: u8,
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
    Raw(Vec<u8>),
    Scanned,
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
    pub const EVENT_TYPE_CLIMATE: u8 = 0xE8;
    pub const EVENT_TYPE_LEAK: u8 = 0xEA;

    /// Parses a scan event from the payload of a NOTIFY_SENSOR_SCAN (0x5320) packet.
    pub fn parse_scan(payload: &[u8]) -> Result<Self, &'static str> {
        if payload.len() < 10 {
            return Err("Scan payload too short");
        }
        let event_type = payload[0]; // 0xA3
        let mac_bytes = &payload[1..9];
        let sensor_type_val = payload[9];

        let mac = String::from_utf8(mac_bytes.to_vec())
            .map_err(|_| "Invalid MAC characters (non-UTF8)")?;
        let sensor_type = SensorType::from(sensor_type_val);

        Ok(DongleEvent {
            mac,
            timestamp: SystemTime::now(),
            sensor_type,
            event_type,
            data: TelemetryData::Scanned,
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
                if remaining.len() < 8 {
                    return Err("Heartbeat payload too short");
                }
                let battery = remaining[1];
                let rssi = (remaining[7] as i8).saturating_neg();
                TelemetryData::Heartbeat { battery, rssi }
            }
            Self::EVENT_TYPE_ALARM => {
                if remaining.len() < 8 {
                    return Err("Alarm payload too short");
                }
                let battery = remaining[1];
                let state = remaining[4];
                let rssi = (remaining[7] as i8).saturating_neg();
                TelemetryData::Alarm { battery, rssi, state }
            }
            Self::EVENT_TYPE_CLIMATE => {
                if remaining.len() < 10 {
                    return Err("Climate payload too short");
                }
                let battery = remaining[1];
                let temp_hi = remaining[4] as i8;
                let temp_lo = remaining[5];
                let humidity = remaining[6];
                let rssi = (remaining[9] as i8).saturating_neg();
                let temperature = (temp_hi as f32) + ((temp_lo as f32) / 100.0);
                TelemetryData::Climate {
                    battery,
                    rssi,
                    temperature,
                    humidity,
                }
            }
            _ => TelemetryData::Raw(remaining.to_vec()),
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
            _ => TelemetryData::Raw(remaining.to_vec()),
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
