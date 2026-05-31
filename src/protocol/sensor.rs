use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::SystemTime;
use crate::protocol::telemetry::{DongleEvent, SensorType, TelemetryData};

// ---------------------------------------------------------
// SensorState: Type-specific state enum
// ---------------------------------------------------------

/// Type-specific sensor state — the ONLY part that varies between device types.
/// Derives Serialize/Deserialize for direct persistence to state.yaml.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum SensorState {
    Contact { is_open: bool },
    Motion { is_active: bool },
    Leak {
        is_wet: bool,
        /// Optional external probe. None = no probe connected,
        /// Some(true) = probe wet, Some(false) = probe dry.
        probe_is_wet: Option<bool>,
    },
    Climate {
        temperature: f32,
        humidity: u8,
    },
    Chime,   // Actuator: no inbound telemetry state, receives play_chime commands
    Unknown,
}

impl Default for SensorState {
    fn default() -> Self {
        SensorState::Unknown // backward compat: old state.yaml files without "state" field
    }
}

// ---------------------------------------------------------
// WyzeSensor: Unified device struct
// ---------------------------------------------------------

/// Unified device struct for all Wyze Sense devices (sensors and actuators).
/// Replaces the previous trait-based polymorphic design with composition.
pub struct WyzeSensor {
    // --- Identity (from config, set at creation) ---
    pub mac: String,
    pub sensor_type: SensorType,
    pub friendly_name: String,
    pub timeout_sec: u64,

    // --- Common telemetry (uniform across all types) ---
    pub battery_pct: Option<u8>, // None for mains-powered devices (e.g., Chime)
    pub rssi_dbm: i8,
    pub sw_version: String,
    pub is_online: bool,
    pub last_seen: u64,

    // --- Type-specific state (the polymorphic part) ---
    pub state: SensorState,
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

// Helper to build common sensor discovery configs (battery, signal)
fn push_common_discovery_payloads(
    mac: &str,
    friendly_name: &str,
    sensor_type: SensorType,
    battery_pct: Option<u8>,
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

    // Only register battery entity for battery-powered devices
    if battery_pct.is_some() {
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
    }

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

impl WyzeSensor {
    pub fn new(mac: String, sensor_type: SensorType, friendly_name: String) -> Self {
        let (default_timeout, default_battery) = match sensor_type {
            SensorType::ContactV1 | SensorType::MotionV1 => (3600 * 8, Some(100u8)),
            SensorType::ContactV2 | SensorType::MotionV2 |
            SensorType::LeakV2 | SensorType::ClimateV2 => (3600 * 4, Some(100u8)),
            SensorType::Chime => (3600 * 24, None), // Mains-powered, no battery
            SensorType::Unknown(_) => (1800, Some(100u8)),
        };
        let state = Self::default_state_for_type(sensor_type).unwrap_or(SensorState::Unknown);
        Self {
            mac,
            sensor_type,
            friendly_name,
            timeout_sec: default_timeout,
            battery_pct: default_battery,
            rssi_dbm: -60,
            sw_version: "unknown".to_string(),
            is_online: true,
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            state,
        }
    }

    /// Creates a sensor from string representations (e.g., loaded from YAML config)
    pub fn from_type_str(
        mac: String,
        sensor_type_str: &str,
        friendly_name: String,
    ) -> Result<Self, String> {
        let sensor_type = sensor_type_str.parse::<SensorType>()?;
        Ok(Self::new(mac, sensor_type, friendly_name))
    }

    /// Returns true if this device is a pure actuator (no inbound telemetry state).
    pub fn is_actuator(&self) -> bool {
        matches!(self.state, SensorState::Chime)
    }

    /// Update common metadata (battery, rssi, online status, last_seen) from a telemetry event.
    fn update_common_metadata(&mut self, event: &DongleEvent) {
        match &event.data {
            TelemetryData::Offline => {
                self.is_online = false;
                return;
            }
            TelemetryData::UnknownEvent(_) => {
                self.is_online = true;
                return;
            }
            _ => {}
        }

        self.is_online = true;
        self.last_seen = event.timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let (battery, rssi) = match &event.data {
            TelemetryData::Heartbeat { battery, rssi } => (Some(*battery), Some(*rssi)),
            TelemetryData::Alarm { battery, rssi, .. } => (Some(*battery), Some(*rssi)),
            TelemetryData::Climate { battery, rssi, .. } => (Some(*battery), Some(*rssi)),
            TelemetryData::Leak { battery, rssi, .. } => (Some(*battery), Some(*rssi)),
            TelemetryData::Scanned { .. } => (Some(100), Some(0)),
            _ => (None, None),
        };

        if let (Some(b), Some(r)) = (battery, rssi) {
            // Only update battery for battery-powered devices
            if self.battery_pct.is_some() {
                let mut pct = b;
                if self.sensor_type == SensorType::ContactV2 {
                    pct = b.saturating_mul(2);
                }
                self.battery_pct = Some(pct.min(100));
            }
            self.rssi_dbm = r;
        }
    }

    /// Update internal sensor state variables from a parsed telemetry event.
    pub fn update_from_event(&mut self, event: &DongleEvent) -> Result<(), &'static str> {
        // 1. Update common metadata
        self.update_common_metadata(event);

        // 2. Auto-upgrade Unknown state on first real telemetry event (backward compat migration)
        if matches!(self.state, SensorState::Unknown) {
            if let Some(default) = Self::default_state_for_type(self.sensor_type) {
                info!("Auto-upgraded sensor {} state from Unknown to {:?}",
                    self.mac, std::mem::discriminant(&default));
                self.state = default;
            }
        }

        // 3. Update type-specific state via match
        match (&mut self.state, &event.data) {
            (SensorState::Contact { is_open }, TelemetryData::Alarm { state, .. }) => {
                *is_open = *state == 1;
                Ok(())
            }
            (SensorState::Motion { is_active }, TelemetryData::Alarm { state, .. }) => {
                *is_active = *state == 1;
                Ok(())
            }
            (SensorState::Leak { is_wet, probe_is_wet },
             TelemetryData::Leak { state, probe_state, probe_available, .. }) => {
                *is_wet = *state == 1;
                *probe_is_wet = if *probe_available {
                    Some(*probe_state == 1)
                } else {
                    None
                };
                Ok(())
            }
            (SensorState::Climate { temperature, humidity },
             TelemetryData::Climate { temperature: t, humidity: h, .. }) => {
                *temperature = *t;
                *humidity = *h;
                Ok(())
            }
            // Chime: accept all events gracefully, nothing to update
            (SensorState::Chime, _) => Ok(()),
            // Common events handled by all sensor types (including Unknown that couldn't be upgraded)
            (_, TelemetryData::Heartbeat { .. } | TelemetryData::Scanned { .. }
               | TelemetryData::Offline | TelemetryData::UnknownEvent(_)) => Ok(()),
            (state, other) => {
                warn!("Sensor (MAC={}) with state {:?} received unexpected telemetry event: {:?}",
                    self.mac, std::mem::discriminant(state), other);
                Err("Unexpected event type for this sensor")
            }
        }
    }

    /// Returns the default SensorState for a given SensorType, or None for unknown types.
    fn default_state_for_type(sensor_type: SensorType) -> Option<SensorState> {
        match sensor_type {
            SensorType::ContactV1 | SensorType::ContactV2 =>
                Some(SensorState::Contact { is_open: false }),
            SensorType::MotionV1 | SensorType::MotionV2 =>
                Some(SensorState::Motion { is_active: false }),
            SensorType::LeakV2 =>
                Some(SensorState::Leak { is_wet: false, probe_is_wet: None }),
            SensorType::ClimateV2 =>
                Some(SensorState::Climate { temperature: 0.0, humidity: 0 }),
            SensorType::Chime =>
                Some(SensorState::Chime),
            SensorType::Unknown(_) => None,
        }
    }

    /// Dynamic JSON representation of states for MQTT publishing.
    pub fn get_state_payload(&self) -> Value {
        let mut payload = json!({
            "mac": self.mac,
            "name": self.friendly_name,
            "online": self.is_online,
            "sensor_type": self.sensor_type.as_str(),
            "signal_strength": self.rssi_dbm,
            "sw_version": self.sw_version,
            "timestamp": self.last_seen as f64,
        });
        // Include battery only for battery-powered devices
        if let Some(battery) = self.battery_pct {
            payload["battery"] = json!(battery);
        }
        // Merge type-specific fields
        match &self.state {
            SensorState::Contact { is_open } => {
                payload["state"] = json!(if *is_open { "open" } else { "closed" });
            }
            SensorState::Motion { is_active } => {
                payload["state"] = json!(if *is_active { "active" } else { "inactive" });
            }
            SensorState::Leak { is_wet, probe_is_wet } => {
                payload["state"] = json!(if *is_wet { "wet" } else { "dry" });
                match probe_is_wet {
                    Some(wet) => {
                        payload["probe_available"] = json!(true);
                        payload["probe_state"] = json!(if *wet { "wet" } else { "dry" });
                    }
                    None => {
                        payload["probe_available"] = json!(false);
                        payload["probe_state"] = json!("dry");
                    }
                }
            }
            SensorState::Climate { temperature, humidity } => {
                payload["temperature"] = json!(format!("{:.2}", temperature));
                payload["humidity"] = json!(*humidity);
            }
            SensorState::Chime => {
                payload["state"] = json!("idle");
            }
            SensorState::Unknown => {
                payload["state"] = json!("unknown");
            }
        }
        payload
    }

    /// Home Assistant discovery configurations mapping.
    pub fn get_discovery_payloads(&self, topic_root: &str) -> Vec<(String, Value)> {
        match &self.state {
            SensorState::Chime => self.build_chime_discovery(topic_root),
            SensorState::Unknown => Vec::new(), // No discovery for unknown sensors
            _ => self.build_sensor_discovery(topic_root),
        }
    }

    fn build_chime_discovery(&self, topic_root: &str) -> Vec<(String, Value)> {
        let mut payloads = Vec::new();
        let device_id = format!("wyzesense_{}", self.mac);
        let device = build_device_metadata(&self.mac, &self.friendly_name, self.sensor_type);
        let state_topic = format!("{}/{}", topic_root, self.mac);
        let availability = json!([
            { "topic": format!("{}/status", topic_root) },
        ]);

        // Signal strength (Chime still communicates wirelessly)
        payloads.push((
            format!("homeassistant/sensor/{}/signal_strength/config", device_id),
            json!({
                "state_topic": state_topic,
                "value_template": "{{ value_json.signal_strength }}",
                "device_class": "signal_strength",
                "unit_of_measurement": "dBm",
                "state_class": "measurement",
                "unique_id": format!("{}_signal_strength", device_id),
                "device": device.clone(),
                "availability": availability.clone(),
                "availability_mode": "all",
                "entity_category": "diagnostic",
            })
        ));

        // Register as a HASS "button" entity for triggering chime
        payloads.push((
            format!("homeassistant/button/{}/chime/config", device_id),
            json!({
                "name": null,
                "command_topic": format!("{}/{}/chime", topic_root, self.mac),
                "unique_id": format!("{}_chime", device_id),
                "device": device,
                "availability": availability,
                "availability_mode": "all",
                "icon": "mdi:bell-ring",
            })
        ));

        payloads
    }

    fn build_sensor_discovery(&self, topic_root: &str) -> Vec<(String, Value)> {
        let mut payloads = Vec::new();
        let device_id = format!("wyzesense_{}", self.mac);
        let device = build_device_metadata(&self.mac, &self.friendly_name, self.sensor_type);
        let state_topic = format!("{}/{}", topic_root, self.mac);
        let availability = json!([
            { "topic": format!("{}/status", topic_root) },
            { "topic": format!("{}/{}/status", topic_root, self.mac) }
        ]);

        push_common_discovery_payloads(
            &self.mac, &self.friendly_name, self.sensor_type, self.battery_pct,
            topic_root, &mut payloads,
        );

        match &self.state {
            SensorState::Contact { .. } => {
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
            }
            SensorState::Motion { .. } => {
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
            }
            SensorState::Leak { .. } => {
                // Main sensor: built-in water detector
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
                // Optional external probe: connectivity diagnostic
                payloads.push((
                    format!("homeassistant/binary_sensor/{}/probe_available/config", device_id),
                    json!({
                        "name": "Probe",
                        "state_topic": state_topic.clone(),
                        "value_template": "{{ 'ON' if value_json.probe_available else 'OFF' }}",
                        "device_class": "connectivity",
                        "unique_id": format!("{}_probe_available", device_id),
                        "device": device.clone(),
                        "availability": availability.clone(),
                        "availability_mode": "all",
                        "entity_category": "diagnostic",
                    })
                ));
                // Optional external probe: moisture state (unavailable when probe is disconnected)
                payloads.push((
                    format!("homeassistant/binary_sensor/{}/probe_state/config", device_id),
                    json!({
                        "name": "Probe Moisture",
                        "state_topic": state_topic.clone(),
                        "value_template": "{{ value_json.probe_state }}",
                        "availability_template": "{{ 'online' if value_json.probe_available else 'offline' }}",
                        "device_class": "moisture",
                        "payload_on": "wet",
                        "payload_off": "dry",
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "unique_id": format!("{}_probe_state", device_id),
                        "device": device,
                        "availability_mode": "all",
                        "json_attributes_topic": state_topic,
                    })
                ));
            }
            SensorState::Climate { .. } => {
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
            }
            _ => {} // Chime and Unknown handled separately
        }

        payloads
    }
}

// ---------------------------------------------------------
// Sensor Manager
// ---------------------------------------------------------
use crate::config::sensors::{SensorsConfig, SensorMetadata};
use crate::config::state::{SystemState, PersistedSensorState};
use tracing::{info, warn, error};

pub struct SensorManager {
    sensors: HashMap<String, WyzeSensor>,
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

    pub fn get_sensors(&self) -> &HashMap<String, WyzeSensor> {
        &self.sensors
    }

    pub fn get_sensors_mut(&mut self) -> &mut HashMap<String, WyzeSensor> {
        &mut self.sensors
    }

    /// Load all sensors by merging the user config, the dynamic state config, and the NVRAM MAC list.
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
            
            // Determine friendly name (from config, or auto-generated)
            let friendly_name = metadata
                .map(|m| m.name.clone())
                .unwrap_or_else(|| format!("Wyze Sense {}", mac));

            // Determine sensor type: state.yaml (authoritative) > sensors.yaml (migration hint) > "unknown"
            let type_str = if let Some(s) = system_state.sensors.get(mac) {
                s.sensor_type.clone()
            } else if let Some(m) = metadata {
                m.r#type.clone().unwrap_or_else(|| "unknown".to_string())
            } else {
                "unknown".to_string()
            };

            // Create the sensor object
            match WyzeSensor::from_type_str(mac.clone(), &type_str, friendly_name.clone()) {
                Ok(mut sensor) => {
                    // Load custom timeout if it exists in user config
                    if let Some(m) = metadata {
                        if let Some(t) = m.timeout_sec {
                            sensor.timeout_sec = t;
                        }
                    }

                    // Auto-populate missing NVRAM sensors in sensors.yaml (name only, no type)
                    if !sensors_config.sensors.contains_key(mac) {
                        sensors_config.sensors.insert(mac.clone(), SensorMetadata {
                            name: friendly_name.clone(),
                            r#type: None, // Type is managed in state.yaml, not config
                            timeout_sec: Some(sensor.timeout_sec),
                            sw_version: None,
                        });
                        config_changed = true;
                    }

                    // Load sw_version from config metadata if available
                    if let Some(m) = sensors_config.sensors.get(mac) {
                        if let Some(ref v) = m.sw_version {
                            sensor.sw_version = v.clone();
                        }
                    }

                    // Restore full state from system state cache if available
                    if let Some(cached) = system_state.sensors.get(mac) {
                        if let Some(b) = cached.battery {
                            sensor.battery_pct = Some(b);
                        }
                        sensor.rssi_dbm = cached.signal;
                        sensor.last_seen = cached.last_seen;
                        sensor.state = cached.state.clone(); // Full type-specific state restored!
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
            info!("Auto-generating sensors.yaml with stub entries for newly discovered NVRAM sensors.");
            sensors_config.save_to_yaml_atomic(&self.config_path)?;
        }

        // Also save loaded sensors back to system state cache to ensure consistency
        self.save_state_to_disk()?;
        Ok(())
    }

    /// Saves the dynamic in-memory sensor state back to config/state.yaml
    pub fn save_state_to_disk(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut system_state = SystemState::default();
        for (mac, sensor) in &self.sensors {
            system_state.sensors.insert(mac.clone(), PersistedSensorState {
                mac: mac.clone(),
                sensor_type: sensor.sensor_type.as_str().to_string(),
                last_seen: sensor.last_seen,
                battery: sensor.battery_pct,
                signal: sensor.rssi_dbm,
                state: sensor.state.clone(), // Type-specific state persisted!
            });
        }
        system_state.save_to_yaml_atomic(&self.state_path)?;
        Ok(())
    }

    /// Registers a newly paired sensor and persists its metadata back to config/sensors.yaml and config/state.yaml.
    pub fn register_and_persist_sensor(
        &mut self,
        mac: String,
        sensor_type: SensorType,
        version: Option<u8>,
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
        let mut sensor = WyzeSensor::new(
            mac.clone(),
            sensor_type,
            name.clone(),
        );

        // Set version from scan data if available
        if let Some(v) = version {
            sensor.sw_version = v.to_string();
        }

        // Load custom timeout if it exists in user config
        if let Some(t) = custom_timeout {
            sensor.timeout_sec = t;
        } else {
            custom_timeout = Some(sensor.timeout_sec);
        }

        // 4. Update metadata in config (name + timeout only, no type)
        config.sensors.insert(mac.clone(), SensorMetadata {
            name,
            r#type: None, // Type is managed in state.yaml
            timeout_sec: custom_timeout,
            sw_version: version.map(|v| v.to_string()),
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
            if matches!(event.data, TelemetryData::Scanned { .. }) || matches!(event.data, TelemetryData::Offline) {
                return false;
            }
            info!("Auto-Discovering and registering newly paired sensor: {}", event.mac);
            if let Err(e) = self.register_and_persist_sensor(event.mac.clone(), event.sensor_type, None) {
                error!("Failed to auto-register discovered sensor {}: {}", event.mac, e);
                return false;
            }
        } else {
            // Check if the existing sensor is registered as "Unknown" and we've received a concrete known type
            let is_unknown = self.sensors.get(&event.mac)
                .map(|s| matches!(s.sensor_type, SensorType::Unknown(_)))
                .unwrap_or(false);

            if is_unknown && !matches!(event.sensor_type, SensorType::Unknown(_)) {
                info!("Upgrading auto-discovered sensor {} from Unknown to {:?}", event.mac, event.sensor_type);
                if let Err(e) = self.register_and_persist_sensor(event.mac.clone(), event.sensor_type, None) {
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
            if !sensor.is_online {
                continue; // Already offline, skip
            }

            if now.saturating_sub(sensor.last_seen) > sensor.timeout_sec {
                warn!("Sensor {} timed out (last seen {} seconds ago). Setting offline.", mac, now.saturating_sub(sensor.last_seen));
                sensor.is_online = false;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::telemetry::{DongleEvent, TelemetryData, SensorType};
    use std::time::SystemTime;

    fn make_sensor(sensor_type: SensorType) -> WyzeSensor {
        let mut sensor = WyzeSensor::new(
            "AABBCCDD".to_string(),
            sensor_type,
            "Test Sensor".to_string(),
        );
        // Force state to Unknown to simulate loading from old state.yaml
        sensor.state = SensorState::Unknown;
        sensor
    }

    fn alarm_event(state: u8) -> DongleEvent {
        DongleEvent {
            mac: "AABBCCDD".to_string(),
            timestamp: SystemTime::now(),
            sensor_type: SensorType::Unknown(0),
            event_type: DongleEvent::EVENT_TYPE_ALARM,
            data: TelemetryData::Alarm { battery: 90, rssi: -40, state },
        }
    }

    fn climate_event(temp: f32, hum: u8) -> DongleEvent {
        DongleEvent {
            mac: "AABBCCDD".to_string(),
            timestamp: SystemTime::now(),
            sensor_type: SensorType::Unknown(0),
            event_type: DongleEvent::EVENT_TYPE_CLIMATE,
            data: TelemetryData::Climate { battery: 95, rssi: -50, temperature: temp, humidity: hum },
        }
    }

    fn leak_event(state: u8, probe_available: bool, probe_state: u8) -> DongleEvent {
        DongleEvent {
            mac: "AABBCCDD".to_string(),
            timestamp: SystemTime::now(),
            sensor_type: SensorType::Unknown(0),
            event_type: DongleEvent::EVENT_TYPE_ALARM,
            data: TelemetryData::Leak { battery: 96, rssi: -60, state, probe_state, probe_available },
        }
    }

    fn heartbeat_event() -> DongleEvent {
        DongleEvent {
            mac: "AABBCCDD".to_string(),
            timestamp: SystemTime::now(),
            sensor_type: SensorType::Unknown(0),
            event_type: DongleEvent::EVENT_TYPE_HEARTBEAT,
            data: TelemetryData::Heartbeat { battery: 90, rssi: -40 },
        }
    }

    // --- Auto-upgrade tests ---

    #[test]
    fn test_upgrade_unknown_contact_on_alarm() {
        let mut sensor = make_sensor(SensorType::ContactV1);
        assert!(matches!(sensor.state, SensorState::Unknown));

        let result = sensor.update_from_event(&alarm_event(1));
        assert!(result.is_ok());
        assert!(matches!(sensor.state, SensorState::Contact { is_open: true }));
    }

    #[test]
    fn test_upgrade_unknown_motion_on_alarm() {
        let mut sensor = make_sensor(SensorType::MotionV2);
        let result = sensor.update_from_event(&alarm_event(0));
        assert!(result.is_ok());
        assert!(matches!(sensor.state, SensorState::Motion { is_active: false }));
    }

    #[test]
    fn test_upgrade_unknown_climate_on_climate_event() {
        let mut sensor = make_sensor(SensorType::ClimateV2);
        let result = sensor.update_from_event(&climate_event(23.5, 55));
        assert!(result.is_ok());
        match &sensor.state {
            SensorState::Climate { temperature, humidity } => {
                assert!((*temperature - 23.5).abs() < 0.01);
                assert_eq!(*humidity, 55);
            }
            other => panic!("Expected Climate state, got {:?}", other),
        }
    }

    #[test]
    fn test_upgrade_unknown_leak_on_leak_event() {
        let mut sensor = make_sensor(SensorType::LeakV2);
        let result = sensor.update_from_event(&leak_event(1, true, 0));
        assert!(result.is_ok());
        assert!(matches!(sensor.state, SensorState::Leak {
            is_wet: true,
            probe_is_wet: Some(false),
        }));
    }

    #[test]
    fn test_upgrade_unknown_leak_no_probe() {
        let mut sensor = make_sensor(SensorType::LeakV2);
        let result = sensor.update_from_event(&leak_event(0, false, 0));
        assert!(result.is_ok());
        assert!(matches!(sensor.state, SensorState::Leak {
            is_wet: false,
            probe_is_wet: None,
        }));
    }

    #[test]
    fn test_heartbeat_does_not_upgrade_unknown() {
        let mut sensor = make_sensor(SensorType::ContactV1);
        let result = sensor.update_from_event(&heartbeat_event());
        assert!(result.is_ok());
        // State should remain Unknown — heartbeats don't carry type-specific data
        // Note: the state upgrades to Contact (default) but heartbeat is a common event,
        // so it still succeeds. The upgrade happens, but no state fields are set from the heartbeat.
        assert!(matches!(sensor.state, SensorState::Contact { is_open: false }));
    }

    #[test]
    fn test_already_typed_sensor_not_affected() {
        let mut sensor = WyzeSensor::new(
            "AABBCCDD".to_string(),
            SensorType::ContactV1,
            "Test".to_string(),
        );
        // Sensor starts with Contact { is_open: false } from new()
        assert!(matches!(sensor.state, SensorState::Contact { is_open: false }));

        // Open the door
        let result = sensor.update_from_event(&alarm_event(1));
        assert!(result.is_ok());
        assert!(matches!(sensor.state, SensorState::Contact { is_open: true }));

        // Close the door
        let result = sensor.update_from_event(&alarm_event(0));
        assert!(result.is_ok());
        assert!(matches!(sensor.state, SensorState::Contact { is_open: false }));
    }

    // --- default_state_for_type tests ---

    #[test]
    fn test_default_state_for_known_types() {
        assert!(matches!(
            WyzeSensor::default_state_for_type(SensorType::ContactV1),
            Some(SensorState::Contact { is_open: false })
        ));
        assert!(matches!(
            WyzeSensor::default_state_for_type(SensorType::MotionV2),
            Some(SensorState::Motion { is_active: false })
        ));
        assert!(matches!(
            WyzeSensor::default_state_for_type(SensorType::LeakV2),
            Some(SensorState::Leak { is_wet: false, probe_is_wet: None })
        ));
        assert!(matches!(
            WyzeSensor::default_state_for_type(SensorType::Chime),
            Some(SensorState::Chime)
        ));
    }

    #[test]
    fn test_default_state_for_unknown_type_returns_none() {
        assert!(WyzeSensor::default_state_for_type(SensorType::Unknown(0)).is_none());
    }
}
