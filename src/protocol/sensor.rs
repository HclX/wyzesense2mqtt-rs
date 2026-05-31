use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::SystemTime;
use crate::protocol::telemetry::{DongleEvent, SensorType, TelemetryData};

pub trait WyzeSensor: Send + Sync {
    fn mac(&self) -> &str;
    fn sensor_type(&self) -> SensorType;
    fn battery_pct(&self) -> u8;
    fn rssi_dbm(&self) -> i8;
    fn sw_version(&self) -> &str;
    fn is_online(&self) -> bool;
    fn set_online(&mut self, online: bool);
    fn friendly_name(&self) -> &str;
    fn set_friendly_name(&mut self, name: String);
    fn last_seen(&self) -> u64;
    fn set_last_seen(&mut self, time: u64);
    fn timeout_sec(&self) -> u64;
    fn set_timeout_sec(&mut self, timeout: u64);

    fn get_state_payload(&self) -> Value;
    fn get_discovery_payloads(&self, topic_root: &str) -> Vec<(String, Value)>;
    fn update_from_event(&mut self, event: &DongleEvent) -> Result<(), &'static str>;
}

// Helper to build standard Home Assistant discovery device metadata
fn build_device_metadata(mac: &str, friendly_name: &str, sensor_type: SensorType) -> Value {
    let device_id = format!("wyzesense_{}", mac);
    json!({
        "identifiers": [device_id.clone(), mac.to_string()],
        "name": friendly_name,
        "model": sensor_type.model_str(),
        "manufacturer": "Wyze",
        "via_device": "wyzesense2mqtt"
    })
}

// Helper to build common sensor configs (battery, signal)
fn push_common_discovery_payloads(
    mac: &str,
    friendly_name: &str,
    sensor_type: SensorType,
    topic_root: &str,
    payloads: &mut Vec<(String, Value)>,
) {
    let device_id = format!("wyzesense_{}", mac);
    let device = build_device_metadata(mac, friendly_name, sensor_type);
    let state_topic = format!("{}/{}", topic_root, mac);
    let availability = json!([
        { "topic": format!("{}/status", topic_root) },
        { "topic": format!("{}/{}/status", topic_root, mac) }
    ]);

    payloads.push((
        format!("homeassistant/sensor/{}/battery/config", device_id),
        json!({
            "state_topic": state_topic,
            "value_template": "{{ value_json.battery }}",
            "device_class": "battery",
            "unit_of_measurement": "%",
            "state_class": "measurement",
            "unique_id": format!("{}_battery", device_id),
            "device": device,
            "availability": availability,
            "availability_mode": "all",
            "entity_category": "diagnostic",
        })
    ));

    payloads.push((
        format!("homeassistant/sensor/{}/signal_strength/config", device_id),
        json!({
            "state_topic": state_topic,
            "value_template": "{{ value_json.signal_strength }}",
            "device_class": "signal_strength",
            "unit_of_measurement": "dBm",
            "state_class": "measurement",
            "unique_id": format!("{}_signal_strength", device_id),
            "device": device,
            "availability": availability,
            "availability_mode": "all",
            "entity_category": "diagnostic",
        })
    ));
}

// Helper function to update the common fields of any sensor event
fn update_metadata(
    is_online: &mut bool,
    last_seen: &mut u64,
    battery_pct: &mut u8,
    rssi_dbm: &mut i8,
    event: &DongleEvent,
    sensor_type: SensorType,
) {
    match &event.data {
        TelemetryData::Offline => {
            *is_online = false;
            return;
        }
        TelemetryData::UnknownEvent(_) => {
            *is_online = true;
            return;
        }
        _ => {}
    }

    *is_online = true;
    *last_seen = event.timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let (battery, rssi) = match &event.data {
        TelemetryData::Heartbeat { battery, rssi } => (Some(*battery), Some(*rssi)),
        TelemetryData::Alarm { battery, rssi, .. } => (Some(*battery), Some(*rssi)),
        TelemetryData::Climate { battery, rssi, .. } => (Some(*battery), Some(*rssi)),
        TelemetryData::Leak { battery, rssi, .. } => (Some(*battery), Some(*rssi)),
        TelemetryData::Scanned => (Some(100), Some(0)),
        _ => unreachable!(),
    };

    if let (Some(b), Some(r)) = (battery, rssi) {
        let mut pct = b;
        if sensor_type == SensorType::ContactV2 {
            pct = b.saturating_mul(2);
        }
        *battery_pct = pct.min(100);
        *rssi_dbm = r;
    }
}

// ---------------------------------------------------------
// 1. Contact Sensor
// ---------------------------------------------------------
pub struct ContactSensor {
    mac: String,
    sensor_type: SensorType,
    friendly_name: String,
    battery_pct: u8,
    rssi_dbm: i8,
    sw_version: String,
    is_online: bool,
    last_seen: u64,
    is_open: bool,
    timeout_sec: u64,
}

impl ContactSensor {
    pub fn new(
        mac: String,
        sensor_type: SensorType,
        friendly_name: String,
    ) -> Self {
        let default_timeout = match sensor_type {
            SensorType::ContactV1 => 3600 * 8, // 8 hours for V1
            _ => 3600 * 4,                     // 4 hours for V2
        };
        Self {
            mac,
            sensor_type,
            friendly_name,
            battery_pct: 100,
            rssi_dbm: -60,
            sw_version: "unknown".to_string(),
            is_online: true,
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            is_open: false,
            timeout_sec: default_timeout,
        }
    }
}

impl WyzeSensor for ContactSensor {
    fn mac(&self) -> &str { &self.mac }
    fn sensor_type(&self) -> SensorType { self.sensor_type }
    fn battery_pct(&self) -> u8 { self.battery_pct }
    fn rssi_dbm(&self) -> i8 { self.rssi_dbm }
    fn sw_version(&self) -> &str { &self.sw_version }
    fn is_online(&self) -> bool { self.is_online }
    fn set_online(&mut self, online: bool) { self.is_online = online; }
    fn friendly_name(&self) -> &str { &self.friendly_name }
    fn set_friendly_name(&mut self, name: String) { self.friendly_name = name; }
    fn last_seen(&self) -> u64 { self.last_seen }
    fn set_last_seen(&mut self, time: u64) { self.last_seen = time; }
    fn timeout_sec(&self) -> u64 { self.timeout_sec }
    fn set_timeout_sec(&mut self, timeout: u64) { self.timeout_sec = timeout; }

    fn get_state_payload(&self) -> Value {
        json!({
            "battery": self.battery_pct,
            "mac": self.mac,
            "name": self.friendly_name,
            "online": self.is_online,
            "sensor_type": self.sensor_type.as_str(),
            "signal_strength": self.rssi_dbm,
            "sw_version": self.sw_version,
            "timestamp": self.last_seen as f64,
            "state": if self.is_open { "open" } else { "closed" },
        })
    }

    fn get_discovery_payloads(&self, topic_root: &str) -> Vec<(String, Value)> {
        let mut payloads = Vec::new();
        let device_id = format!("wyzesense_{}", self.mac);
        let device = build_device_metadata(&self.mac, &self.friendly_name, self.sensor_type);
        let state_topic = format!("{}/{}", topic_root, self.mac);
        let availability = json!([
            { "topic": format!("{}/status", topic_root) },
            { "topic": format!("{}/{}/status", topic_root, self.mac) }
        ]);

        push_common_discovery_payloads(&self.mac, &self.friendly_name, self.sensor_type, topic_root, &mut payloads);

        payloads.push((
            format!("homeassistant/binary_sensor/{}/state/config", device_id),
            json!({
                "name": null,
                "state_topic": state_topic.clone(),
                "value_template": "{{ value_json.state }}",
                "device_class": "opening",
                "payload_on": "open",
                "payload_off": "closed",
                "unique_id": format!("{}_state", device_id),
                "device": device,
                "availability": availability,
                "availability_mode": "all",
                "json_attributes_topic": state_topic,
            })
        ));

        payloads
    }

    fn update_from_event(&mut self, event: &DongleEvent) -> Result<(), &'static str> {
        update_metadata(
            &mut self.is_online,
            &mut self.last_seen,
            &mut self.battery_pct,
            &mut self.rssi_dbm,
            event,
            self.sensor_type,
        );

        match &event.data {
            TelemetryData::Alarm { state, .. } => {
                self.is_open = *state == 1;
                Ok(())
            }
            TelemetryData::Heartbeat { .. } | TelemetryData::Scanned | TelemetryData::Offline | TelemetryData::UnknownEvent(_) => Ok(()),
            other => {
                warn!("ContactSensor (MAC={}) received unexpected telemetry event variant: {:?}", self.mac, other);
                Err("Unexpected event type for contact sensor")
            }
        }
    }
}

// ---------------------------------------------------------
// 2. Motion Sensor
// ---------------------------------------------------------
pub struct MotionSensor {
    mac: String,
    sensor_type: SensorType,
    friendly_name: String,
    battery_pct: u8,
    rssi_dbm: i8,
    sw_version: String,
    is_online: bool,
    last_seen: u64,
    is_active: bool,
    timeout_sec: u64,
}

impl MotionSensor {
    pub fn new(
        mac: String,
        sensor_type: SensorType,
        friendly_name: String,
    ) -> Self {
        let default_timeout = match sensor_type {
            SensorType::MotionV1 => 3600 * 8, // 8 hours for V1
            _ => 3600 * 4,                    // 4 hours for V2
        };
        Self {
            mac,
            sensor_type,
            friendly_name,
            battery_pct: 100,
            rssi_dbm: -60,
            sw_version: "unknown".to_string(),
            is_online: true,
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            is_active: false,
            timeout_sec: default_timeout,
        }
    }
}

impl WyzeSensor for MotionSensor {
    fn mac(&self) -> &str { &self.mac }
    fn sensor_type(&self) -> SensorType { self.sensor_type }
    fn battery_pct(&self) -> u8 { self.battery_pct }
    fn rssi_dbm(&self) -> i8 { self.rssi_dbm }
    fn sw_version(&self) -> &str { &self.sw_version }
    fn is_online(&self) -> bool { self.is_online }
    fn set_online(&mut self, online: bool) { self.is_online = online; }
    fn friendly_name(&self) -> &str { &self.friendly_name }
    fn set_friendly_name(&mut self, name: String) { self.friendly_name = name; }
    fn last_seen(&self) -> u64 { self.last_seen }
    fn set_last_seen(&mut self, time: u64) { self.last_seen = time; }
    fn timeout_sec(&self) -> u64 { self.timeout_sec }
    fn set_timeout_sec(&mut self, timeout: u64) { self.timeout_sec = timeout; }

    fn get_state_payload(&self) -> Value {
        json!({
            "battery": self.battery_pct,
            "mac": self.mac,
            "name": self.friendly_name,
            "online": self.is_online,
            "sensor_type": self.sensor_type.as_str(),
            "signal_strength": self.rssi_dbm,
            "sw_version": self.sw_version,
            "timestamp": self.last_seen as f64,
            "state": if self.is_active { "active" } else { "inactive" },
        })
    }

    fn get_discovery_payloads(&self, topic_root: &str) -> Vec<(String, Value)> {
        let mut payloads = Vec::new();
        let device_id = format!("wyzesense_{}", self.mac);
        let device = build_device_metadata(&self.mac, &self.friendly_name, self.sensor_type);
        let state_topic = format!("{}/{}", topic_root, self.mac);
        let availability = json!([
            { "topic": format!("{}/status", topic_root) },
            { "topic": format!("{}/{}/status", topic_root, self.mac) }
        ]);

        push_common_discovery_payloads(&self.mac, &self.friendly_name, self.sensor_type, topic_root, &mut payloads);

        payloads.push((
            format!("homeassistant/binary_sensor/{}/state/config", device_id),
            json!({
                "name": null,
                "state_topic": state_topic.clone(),
                "value_template": "{{ value_json.state }}",
                "device_class": "motion",
                "payload_on": "active",
                "payload_off": "inactive",
                "unique_id": format!("{}_state", device_id),
                "device": device,
                "availability": availability,
                "availability_mode": "all",
                "json_attributes_topic": state_topic,
            })
        ));

        payloads
    }

    fn update_from_event(&mut self, event: &DongleEvent) -> Result<(), &'static str> {
        update_metadata(
            &mut self.is_online,
            &mut self.last_seen,
            &mut self.battery_pct,
            &mut self.rssi_dbm,
            event,
            self.sensor_type,
        );

        match &event.data {
            TelemetryData::Alarm { state, .. } => {
                self.is_active = *state == 1;
                Ok(())
            }
            TelemetryData::Heartbeat { .. } | TelemetryData::Scanned | TelemetryData::Offline | TelemetryData::UnknownEvent(_) => Ok(()),
            other => {
                warn!("MotionSensor (MAC={}) received unexpected telemetry event variant: {:?}", self.mac, other);
                Err("Unexpected event type for motion sensor")
            }
        }
    }
}

// ---------------------------------------------------------
// 3. Leak Sensor
// ---------------------------------------------------------
pub struct LeakSensor {
    mac: String,
    sensor_type: SensorType,
    friendly_name: String,
    battery_pct: u8,
    rssi_dbm: i8,
    sw_version: String,
    is_online: bool,
    last_seen: u64,
    is_wet: bool,
    probe_connected: bool,
    probe_available: bool,
    timeout_sec: u64,
}

impl LeakSensor {
    pub fn new(
        mac: String,
        sensor_type: SensorType,
        friendly_name: String,
    ) -> Self {
        Self {
            mac,
            sensor_type,
            friendly_name,
            battery_pct: 100,
            rssi_dbm: -60,
            sw_version: "unknown".to_string(),
            is_online: true,
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            is_wet: false,
            probe_connected: false,
            probe_available: false,
            timeout_sec: 3600 * 4, // 4 hours for V2 leak sensor
        }
    }
}

impl WyzeSensor for LeakSensor {
    fn mac(&self) -> &str { &self.mac }
    fn sensor_type(&self) -> SensorType { self.sensor_type }
    fn battery_pct(&self) -> u8 { self.battery_pct }
    fn rssi_dbm(&self) -> i8 { self.rssi_dbm }
    fn sw_version(&self) -> &str { &self.sw_version }
    fn is_online(&self) -> bool { self.is_online }
    fn set_online(&mut self, online: bool) { self.is_online = online; }
    fn friendly_name(&self) -> &str { &self.friendly_name }
    fn set_friendly_name(&mut self, name: String) { self.friendly_name = name; }
    fn last_seen(&self) -> u64 { self.last_seen }
    fn set_last_seen(&mut self, time: u64) { self.last_seen = time; }
    fn timeout_sec(&self) -> u64 { self.timeout_sec }
    fn set_timeout_sec(&mut self, timeout: u64) { self.timeout_sec = timeout; }

    fn get_state_payload(&self) -> Value {
        json!({
            "battery": self.battery_pct,
            "mac": self.mac,
            "name": self.friendly_name,
            "online": self.is_online,
            "sensor_type": self.sensor_type.as_str(),
            "signal_strength": self.rssi_dbm,
            "sw_version": self.sw_version,
            "timestamp": self.last_seen as f64,
            "state": if self.is_wet { "wet" } else { "dry" },
            "probe_state": if self.probe_connected { "wet" } else { "dry" },
            "probe_available": self.probe_available,
        })
    }

    fn get_discovery_payloads(&self, topic_root: &str) -> Vec<(String, Value)> {
        let mut payloads = Vec::new();
        let device_id = format!("wyzesense_{}", self.mac);
        let device = build_device_metadata(&self.mac, &self.friendly_name, self.sensor_type);
        let state_topic = format!("{}/{}", topic_root, self.mac);
        let availability = json!([
            { "topic": format!("{}/status", topic_root) },
            { "topic": format!("{}/{}/status", topic_root, self.mac) }
        ]);

        push_common_discovery_payloads(&self.mac, &self.friendly_name, self.sensor_type, topic_root, &mut payloads);

        payloads.push((
            format!("homeassistant/binary_sensor/{}/state/config", device_id),
            json!({
                "name": null,
                "state_topic": state_topic.clone(),
                "value_template": "{{ value_json.state }}",
                "device_class": "moisture",
                "payload_on": "wet",
                "payload_off": "dry",
                "unique_id": format!("{}_state", device_id),
                "device": device.clone(),
                "availability": availability.clone(),
                "availability_mode": "all",
                "json_attributes_topic": state_topic.clone(),
            })
        ));

        payloads.push((
            format!("homeassistant/binary_sensor/{}_probe/config", device_id),
            json!({
                "name": "Probe Connected",
                "state_topic": state_topic.clone(),
                "value_template": "{{ 'ON' if value_json.probe_available else 'OFF' }}",
                "device_class": "connectivity",
                "unique_id": format!("{}_probe", device_id),
                "device": device,
                "availability": availability,
                "availability_mode": "all",
                "entity_category": "diagnostic",
                "json_attributes_topic": state_topic,
            })
        ));

        payloads
    }

    fn update_from_event(&mut self, event: &DongleEvent) -> Result<(), &'static str> {
        update_metadata(
            &mut self.is_online,
            &mut self.last_seen,
            &mut self.battery_pct,
            &mut self.rssi_dbm,
            event,
            self.sensor_type,
        );

        match &event.data {
            TelemetryData::Leak {
                state,
                probe_state,
                probe_available,
                ..
            } => {
                self.is_wet = *state == 1;
                self.probe_connected = *probe_state == 1;
                self.probe_available = *probe_available;
                Ok(())
            }
            TelemetryData::Heartbeat { .. } | TelemetryData::Scanned | TelemetryData::Offline | TelemetryData::UnknownEvent(_) => Ok(()),
            other => {
                warn!("LeakSensor (MAC={}) received unexpected telemetry event variant: {:?}", self.mac, other);
                Err("Unexpected event type for leak sensor")
            }
        }
    }
}

// ---------------------------------------------------------
// 4. Climate Sensor
// ---------------------------------------------------------
pub struct ClimateSensor {
    mac: String,
    sensor_type: SensorType,
    friendly_name: String,
    battery_pct: u8,
    rssi_dbm: i8,
    sw_version: String,
    is_online: bool,
    last_seen: u64,
    temperature: f32,
    humidity: u8,
    timeout_sec: u64,
}

impl ClimateSensor {
    pub fn new(
        mac: String,
        sensor_type: SensorType,
        friendly_name: String,
    ) -> Self {
        Self {
            mac,
            sensor_type,
            friendly_name,
            battery_pct: 100,
            rssi_dbm: -60,
            sw_version: "unknown".to_string(),
            is_online: true,
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            temperature: 0.0,
            humidity: 0,
            timeout_sec: 3600 * 4, // 4 hours for V2 climate sensor
        }
    }
}

impl WyzeSensor for ClimateSensor {
    fn mac(&self) -> &str { &self.mac }
    fn sensor_type(&self) -> SensorType { self.sensor_type }
    fn battery_pct(&self) -> u8 { self.battery_pct }
    fn rssi_dbm(&self) -> i8 { self.rssi_dbm }
    fn sw_version(&self) -> &str { &self.sw_version }
    fn is_online(&self) -> bool { self.is_online }
    fn set_online(&mut self, online: bool) { self.is_online = online; }
    fn friendly_name(&self) -> &str { &self.friendly_name }
    fn set_friendly_name(&mut self, name: String) { self.friendly_name = name; }
    fn last_seen(&self) -> u64 { self.last_seen }
    fn set_last_seen(&mut self, time: u64) { self.last_seen = time; }
    fn timeout_sec(&self) -> u64 { self.timeout_sec }
    fn set_timeout_sec(&mut self, timeout: u64) { self.timeout_sec = timeout; }

    fn get_state_payload(&self) -> Value {
        json!({
            "battery": self.battery_pct,
            "mac": self.mac,
            "name": self.friendly_name,
            "online": self.is_online,
            "sensor_type": self.sensor_type.as_str(),
            "signal_strength": self.rssi_dbm,
            "sw_version": self.sw_version,
            "timestamp": self.last_seen as f64,
            "temperature": format!("{:.2}", self.temperature),
            "humidity": self.humidity,
        })
    }

    fn get_discovery_payloads(&self, topic_root: &str) -> Vec<(String, Value)> {
        let mut payloads = Vec::new();
        let device_id = format!("wyzesense_{}", self.mac);
        let device = build_device_metadata(&self.mac, &self.friendly_name, self.sensor_type);
        let state_topic = format!("{}/{}", topic_root, self.mac);
        let availability = json!([
            { "topic": format!("{}/status", topic_root) },
            { "topic": format!("{}/{}/status", topic_root, self.mac) }
        ]);

        push_common_discovery_payloads(&self.mac, &self.friendly_name, self.sensor_type, topic_root, &mut payloads);

        payloads.push((
            format!("homeassistant/sensor/{}/temperature/config", device_id),
            json!({
                "name": "Temperature",
                "state_topic": state_topic.clone(),
                "value_template": "{{ value_json.temperature }}",
                "device_class": "temperature",
                "state_class": "measurement",
                "unit_of_measurement": "°C",
                "unique_id": format!("{}_temperature", device_id),
                "device": device.clone(),
                "availability": availability.clone(),
                "availability_mode": "all",
                "json_attributes_topic": state_topic.clone(),
            })
        ));

        payloads.push((
            format!("homeassistant/sensor/{}/humidity/config", device_id),
            json!({
                "name": "Humidity",
                "state_topic": state_topic,
                "value_template": "{{ value_json.humidity }}",
                "device_class": "humidity",
                "state_class": "measurement",
                "unit_of_measurement": "%",
                "unique_id": format!("{}_humidity", device_id),
                "device": device,
                "availability": availability,
                "availability_mode": "all",
                "json_attributes_topic": state_topic,
            })
        ));

        payloads
    }

    fn update_from_event(&mut self, event: &DongleEvent) -> Result<(), &'static str> {
        update_metadata(
            &mut self.is_online,
            &mut self.last_seen,
            &mut self.battery_pct,
            &mut self.rssi_dbm,
            event,
            self.sensor_type,
        );

        match &event.data {
            TelemetryData::Climate {
                temperature,
                humidity,
                ..
            } => {
                self.temperature = *temperature;
                self.humidity = *humidity;
                Ok(())
            }
            TelemetryData::Heartbeat { .. } | TelemetryData::Scanned | TelemetryData::Offline | TelemetryData::UnknownEvent(_) => Ok(()),
            other => {
                warn!("ClimateSensor (MAC={}) received unexpected telemetry event variant: {:?}", self.mac, other);
                Err("Unexpected event type for climate sensor")
            }
        }
    }
}

// ---------------------------------------------------------
// 5. Unknown Sensor
// ---------------------------------------------------------
pub struct UnknownSensor {
    mac: String,
    sensor_type: SensorType,
    friendly_name: String,
    battery_pct: u8,
    rssi_dbm: i8,
    sw_version: String,
    is_online: bool,
    last_seen: u64,
    timeout_sec: u64,
}

impl UnknownSensor {
    pub fn new(mac: String, sensor_type: SensorType, friendly_name: String) -> Self {
        Self {
            mac,
            sensor_type,
            friendly_name,
            battery_pct: 100,
            rssi_dbm: -60,
            sw_version: "unknown".to_string(),
            is_online: true,
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            timeout_sec: 1800, // 30 minutes default for unknown sensors
        }
    }
}

impl WyzeSensor for UnknownSensor {
    fn mac(&self) -> &str { &self.mac }
    fn sensor_type(&self) -> SensorType { self.sensor_type }
    fn battery_pct(&self) -> u8 { self.battery_pct }
    fn rssi_dbm(&self) -> i8 { self.rssi_dbm }
    fn sw_version(&self) -> &str { &self.sw_version }
    fn is_online(&self) -> bool { self.is_online }
    fn set_online(&mut self, online: bool) { self.is_online = online; }
    fn friendly_name(&self) -> &str { &self.friendly_name }
    fn set_friendly_name(&mut self, name: String) { self.friendly_name = name; }
    fn last_seen(&self) -> u64 { self.last_seen }
    fn set_last_seen(&mut self, time: u64) { self.last_seen = time; }
    fn timeout_sec(&self) -> u64 { self.timeout_sec }
    fn set_timeout_sec(&mut self, timeout: u64) { self.timeout_sec = timeout; }

    fn get_state_payload(&self) -> Value {
        json!({
            "battery": self.battery_pct,
            "mac": self.mac,
            "name": self.friendly_name,
            "online": self.is_online,
            "sensor_type": self.sensor_type.as_str(),
            "signal_strength": self.rssi_dbm,
            "sw_version": self.sw_version,
            "timestamp": self.last_seen as f64,
            "state": "unknown",
        })
    }

    fn get_discovery_payloads(&self, _topic_root: &str) -> Vec<(String, Value)> {
        Vec::new() // No discovery payloads for unknown sensors
    }

    fn update_from_event(&mut self, event: &DongleEvent) -> Result<(), &'static str> {
        update_metadata(
            &mut self.is_online,
            &mut self.last_seen,
            &mut self.battery_pct,
            &mut self.rssi_dbm,
            event,
            self.sensor_type,
        );
        Ok(())
    }
}

// ---------------------------------------------------------
// Sensor Factory
// ---------------------------------------------------------
pub struct SensorFactory;

impl SensorFactory {
    pub fn create(
        mac: String,
        sensor_type: SensorType,
        friendly_name: String,
    ) -> Result<Box<dyn WyzeSensor>, String> {
        match sensor_type {
            SensorType::ContactV1 | SensorType::ContactV2 => {
                Ok(Box::new(ContactSensor::new(mac, sensor_type, friendly_name)))
            }
            SensorType::MotionV1 | SensorType::MotionV2 => {
                Ok(Box::new(MotionSensor::new(mac, sensor_type, friendly_name)))
            }
            SensorType::LeakV2 => {
                Ok(Box::new(LeakSensor::new(mac, sensor_type, friendly_name)))
            }
            SensorType::ClimateV2 => {
                Ok(Box::new(ClimateSensor::new(mac, sensor_type, friendly_name)))
            }
            SensorType::Unknown(_) => {
                Ok(Box::new(UnknownSensor::new(mac, sensor_type, friendly_name)))
            }
            other => Err(format!("Unsupported sensor type for factory instantiation: {:?}", other)),
        }
    }

    /// Creates a sensor from string representations (e.g., loaded from YAML config)
    pub fn create_from_str(
        mac: String,
        sensor_type_str: &str,
        friendly_name: String,
    ) -> Result<Box<dyn WyzeSensor>, String> {
        let sensor_type = sensor_type_str.parse::<SensorType>()?;
        Self::create(mac, sensor_type, friendly_name)
    }
}

// ---------------------------------------------------------
// Sensor Manager
// ---------------------------------------------------------
use crate::config::sensors::{SensorsConfig, SensorMetadata};
use crate::config::state::{SystemState, SensorState};
use tracing::{info, warn, error};

pub struct SensorManager {
    sensors: HashMap<String, Box<dyn WyzeSensor>>,
    config_path: String,
    state_path: String,
}

impl SensorManager {
    pub fn new(config_path: String, state_path: String) -> Self {
        Self {
            sensors: HashMap::new(),
            config_path,
            state_path,
        }
    }

    pub fn get_sensors(&self) -> &HashMap<String, Box<dyn WyzeSensor>> {
        &self.sensors
    }

    pub fn get_sensors_mut(&mut self) -> &mut HashMap<String, Box<dyn WyzeSensor>> {
        &mut self.sensors
    }

    /// Load all sensors by merging the user config and the dynamic state config
    pub fn load_sensors(&mut self, nvram_macs: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Load user config (config/sensors.yaml)
        let mut sensors_config = SensorsConfig::load_from_yaml(&self.config_path).unwrap_or_else(|_| SensorsConfig {
            sensors: HashMap::new(),
        });

        // 2. Load system state cache (config/state.yaml)
        let system_state = SystemState::load_from_yaml(&self.state_path).unwrap_or_default();

        let mut config_changed = false;

        // 3. Populate self.sensors by iterating over nvram_macs
        for mac in nvram_macs {
            // Find custom config metadata
            let metadata = sensors_config.sensors.get(mac);
            
            // Determine friendly name
            let friendly_name = metadata
                .map(|m| m.name.clone())
                .unwrap_or_else(|| format!("Wyze Sense {}", mac));

            // Determine sensor type (first from user config, then system state, else default to "unknown")
            let type_str = if let Some(m) = metadata {
                m.r#type.clone()
            } else if let Some(s) = system_state.sensors.get(mac) {
                s.sensor_type.clone()
            } else {
                "unknown".to_string()
            };

            // Create the sensor object
            match SensorFactory::create_from_str(mac.clone(), &type_str, friendly_name.clone()) {
                Ok(mut sensor) => {
                    // Load custom timeout if it exists in user config
                    if let Some(m) = metadata {
                        if let Some(t) = m.timeout_sec {
                            sensor.set_timeout_sec(t);
                        }
                    }

                    // Auto-populate missing NVRAM sensors in sensors.yaml
                    if !sensors_config.sensors.contains_key(mac) {
                        sensors_config.sensors.insert(mac.clone(), SensorMetadata {
                            name: friendly_name.clone(),
                            r#type: type_str.clone(),
                            timeout_sec: Some(sensor.timeout_sec()),
                        });
                        config_changed = true;
                    }

                    // Warm up battery, rssi, and version from system state cache if available
                    if let Some(cached) = system_state.sensors.get(mac) {
                        let event = DongleEvent {
                            mac: mac.clone(),
                            timestamp: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(cached.last_seen),
                            sensor_type: sensor.sensor_type(),
                            event_type: 0xA1, // Default
                            data: TelemetryData::Heartbeat {
                                battery: cached.battery,
                                rssi: cached.signal,
                            },
                        };
                        let _ = sensor.update_from_event(&event);
                    }
                    self.sensors.insert(mac.clone(), sensor);
                }
                Err(e) => {
                    warn!("Failed to construct sensor {} from type {}: {}", mac, type_str, e);
                }
            }
        }

        // If new sensors were auto-populated, save sensors.yaml back to disk atomically
        if config_changed {
            info!("Auto-generating sensors.yaml with unknown types for newly discovered NVRAM sensors.");
            sensors_config.save_to_yaml_atomic(&self.config_path)?;
        }

        // Also save loaded sensors back to system state cache to ensure consistency
        self.save_state_to_disk()?;
        Ok(())
    }

    /// Saves the dynamic in-memory sensor state back to config/state.yaml
    pub fn save_state_to_disk(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = SystemState::default();
        for (mac, sensor) in &self.sensors {
            state.sensors.insert(mac.clone(), SensorState {
                mac: mac.clone(),
                sensor_type: sensor.sensor_type().as_str().to_string(),
                version: sensor.sw_version().to_string(),
                last_seen: sensor.last_seen(),
                battery: sensor.battery_pct(),
                signal: sensor.rssi_dbm(),
            });
        }
        state.save_to_yaml_atomic(&self.state_path)?;
        Ok(())
    }

    /// Registers a newly paired sensor and persists its metadata back to config/sensors.yaml and config/state.yaml.
    pub fn register_and_persist_sensor(
        &mut self,
        mac: String,
        sensor_type: SensorType,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let type_str = sensor_type.as_str();
        
        // 1. Load existing user configuration
        let mut config = SensorsConfig::load_from_yaml(&self.config_path).unwrap_or_else(|_| SensorsConfig {
            sensors: HashMap::new(),
        });

        // 2. Determine settings: preserve existing custom metadata if re-pairing
        let (name, mut custom_timeout) = if let Some(existing) = config.sensors.get(&mac) {
            let current_name = if existing.name.starts_with("Wyze Sense") || existing.name.starts_with("Wyze Sensor") || existing.name.starts_with("Wyze UNKNOWN") {
                let suffix = &mac[mac.len() - 4..];
                format!("Wyze {} {}", type_str.to_uppercase(), suffix)
            } else {
                existing.name.clone()
            };
            (
                current_name,
                existing.timeout_sec,
            )
        } else {
            let suffix = &mac[mac.len() - 4..];
            (
                format!("Wyze {} {}", type_str.to_uppercase(), suffix),
                None,
            )
        };

        // 3. Construct and insert the in-memory sensor object
        let mut sensor = SensorFactory::create(
            mac.clone(),
            sensor_type,
            name.clone(),
        )?;

        // Load custom timeout if it exists in user config
        if let Some(t) = custom_timeout {
            sensor.set_timeout_sec(t);
        } else {
            custom_timeout = Some(sensor.timeout_sec());
        }

        // 4. Update metadata in config
        config.sensors.insert(mac.clone(), SensorMetadata {
            name,
            r#type: type_str.to_string(),
            timeout_sec: custom_timeout,
        });

        self.sensors.insert(mac.clone(), sensor);

        // 5. Save both configurations atomically to disk
        config.save_to_yaml_atomic(&self.config_path)?;
        self.save_state_to_disk()?;

        Ok(())
    }

    /// Deletes a sensor from both configuration files and in-memory registry
    pub fn delete_and_persist_sensor(&mut self, mac: &str) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Remove from memory
        self.sensors.remove(mac);

        // 2. Remove from sensors.yaml
        if let Ok(mut config) = SensorsConfig::load_from_yaml(&self.config_path) {
            if config.sensors.remove(mac).is_some() {
                config.save_to_yaml_atomic(&self.config_path)?;
            }
        }

        // 3. Save updated states to state.yaml
        self.save_state_to_disk()?;
        Ok(())
    }

    /// Dispatches a telemetry event to the correct sensor in memory and saves updated states.
    /// If the sensor is not yet in our registry, dynamically registers and persists it.
    pub fn dispatch_event(&mut self, event: &DongleEvent) -> bool {
        if !self.sensors.contains_key(&event.mac) {
            if matches!(event.data, TelemetryData::Scanned) || matches!(event.data, TelemetryData::Offline) {
                return false;
            }
            info!("Auto-Discovering and registering newly paired sensor: {}", event.mac);
            if let Err(e) = self.register_and_persist_sensor(event.mac.clone(), event.sensor_type) {
                error!("Failed to auto-register discovered sensor {}: {}", event.mac, e);
                return false;
            }
        } else {
            // Check if the existing sensor is registered as "Unknown" and we've received a concrete known type
            let is_unknown = self.sensors.get(&event.mac)
                .map(|s| matches!(s.sensor_type(), SensorType::Unknown(_)))
                .unwrap_or(false);

            if is_unknown && !matches!(event.sensor_type, SensorType::Unknown(_)) {
                info!("Upgrading auto-discovered sensor {} from Unknown to {:?}", event.mac, event.sensor_type);
                if let Err(e) = self.register_and_persist_sensor(event.mac.clone(), event.sensor_type) {
                    error!("Failed to upgrade sensor type for {}: {}", event.mac, e);
                }
            }
        }

        let mut success = false;
        if let Some(sensor) = self.sensors.get_mut(&event.mac) {
            if let Err(e) = sensor.update_from_event(event) {
                error!("Failed to update sensor state for {}: {}", event.mac, e);
            } else {
                success = true;
            }
        }
        if success {
            let _ = self.save_state_to_disk();
        }
        success
    }

    /// Periodically sweep all registered sensors to verify availability timeouts.
    /// Returns a list of MAC addresses of sensors that newly went offline.
    pub fn check_timeouts(&mut self) -> Vec<String> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut newly_offline = Vec::new();

        for (mac, sensor) in &mut self.sensors {
            if !sensor.is_online() {
                continue; // Already offline, skip
            }

            let timeout_sec = sensor.timeout_sec();

            if now.saturating_sub(sensor.last_seen()) > timeout_sec {
                warn!("Sensor {} timed out (last seen {} seconds ago). Setting offline.", mac, now.saturating_sub(sensor.last_seen()));
                sensor.set_online(false);
                newly_offline.push(mac.clone());
            }
        }

        // If any sensor went offline, persist the dynamic states to state.yaml
        if !newly_offline.is_empty() {
            if let Err(e) = self.save_state_to_disk() {
                error!("Failed to save system state after timeout check: {}", e);
            }
        }

        newly_offline
    }
}
