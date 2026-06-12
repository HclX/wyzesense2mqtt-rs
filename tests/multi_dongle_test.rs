use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use wyzesense2mqtt_rs::config::state::{SystemState, PersistedSensorState};
use wyzesense2mqtt_rs::protocol::sensor::{SensorManager, SensorState, WyzeSensor};
use wyzesense2mqtt_rs::protocol::telemetry::SensorType;

fn get_temp_file(name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(name);
    dir
}

// --- SensorManager multi-dongle tests ---

#[test]
fn test_assign_dongle_updates_existing_sensors() {
    let config_path = get_temp_file("test_assign_config.yaml");
    let state_path = get_temp_file("test_assign_state.yaml");
    let _ = fs::remove_file(&config_path);
    let _ = fs::remove_file(&state_path);

    let mut manager = SensorManager::new(
        config_path.to_str().unwrap().to_string(),
        state_path.to_str().unwrap().to_string(),
    );

    // Pre-populate with a sensor (simulating load_all_from_state)
    let sensor = WyzeSensor::new(
        "AABB1234".to_string(),
        SensorType::ContactV2,
        "Test Contact".to_string(),
    );
    manager.get_sensors_mut().insert("AABB1234".to_string(), sensor);

    // Verify sensor starts unassociated
    assert!(manager.get_sensors().get("AABB1234").unwrap().dongle_mac.is_none());

    // Assign dongle
    manager.assign_dongle("DONGLE01", &["AABB1234".to_string()]);

    // Sensor should now be assigned
    assert_eq!(
        manager.get_sensors().get("AABB1234").unwrap().dongle_mac.as_deref(),
        Some("DONGLE01")
    );

    let _ = fs::remove_file(config_path);
    let _ = fs::remove_file(state_path);
}

#[test]
fn test_assign_dongle_creates_new_sensors_for_unknown_macs() {
    let config_path = get_temp_file("test_assign_new_config.yaml");
    let state_path = get_temp_file("test_assign_new_state.yaml");
    let _ = fs::remove_file(&config_path);
    let _ = fs::remove_file(&state_path);

    let mut manager = SensorManager::new(
        config_path.to_str().unwrap().to_string(),
        state_path.to_str().unwrap().to_string(),
    );

    // Empty manager — sensor doesn't exist yet
    assert!(manager.get_sensors().get("NEWMAC01").is_none());

    // Assign dongle with an unknown MAC
    manager.assign_dongle("DONGLE01", &["NEWMAC01".to_string()]);

    // Sensor should be created with unknown type and assigned to dongle
    let sensor = manager.get_sensors().get("NEWMAC01").unwrap();
    assert_eq!(sensor.dongle_mac.as_deref(), Some("DONGLE01"));
    assert!(matches!(sensor.sensor_type, SensorType::Unknown(_)));

    let _ = fs::remove_file(config_path);
    let _ = fs::remove_file(state_path);
}

#[test]
fn test_unassign_dongle_clears_only_matching_sensors() {
    let config_path = get_temp_file("test_unassign_config.yaml");
    let state_path = get_temp_file("test_unassign_state.yaml");
    let _ = fs::remove_file(&config_path);
    let _ = fs::remove_file(&state_path);

    let mut manager = SensorManager::new(
        config_path.to_str().unwrap().to_string(),
        state_path.to_str().unwrap().to_string(),
    );

    // Create two sensors assigned to different dongles
    let mut sensor_a = WyzeSensor::new("SENSORAA".to_string(), SensorType::ContactV2, "A".to_string());
    sensor_a.dongle_mac = Some("DONGLE01".to_string());
    manager.get_sensors_mut().insert("SENSORAA".to_string(), sensor_a);

    let mut sensor_b = WyzeSensor::new("SENSORBB".to_string(), SensorType::MotionV2, "B".to_string());
    sensor_b.dongle_mac = Some("DONGLE02".to_string());
    manager.get_sensors_mut().insert("SENSORBB".to_string(), sensor_b);

    // Unassign DONGLE01
    manager.unassign_dongle("DONGLE01");

    // Sensor A should be unassigned, Sensor B should remain
    assert!(manager.get_sensors().get("SENSORAA").unwrap().dongle_mac.is_none());
    assert_eq!(
        manager.get_sensors().get("SENSORBB").unwrap().dongle_mac.as_deref(),
        Some("DONGLE02")
    );

    let _ = fs::remove_file(config_path);
    let _ = fs::remove_file(state_path);
}

#[test]
fn test_dongle_mac_persistence_roundtrip() {
    let state_path = get_temp_file("test_dongle_mac_persist.yaml");
    let _ = fs::remove_file(&state_path);

    // 1. Create state with dongle_mac and save
    let mut state = SystemState::default();
    state.sensors.insert(
        "AABB1234".to_string(),
        PersistedSensorState {
            mac: "AABB1234".to_string(),
            sensor_type: "contact".to_string(),
            last_seen: 1620000000,
            battery: Some(90),
            signal: -60,
            state: SensorState::Contact { is_open: true },
            dongle_mac: Some("DONGLE01".to_string()),
        },
    );
    state.save_to_yaml_atomic(&state_path).unwrap();

    // 2. Load it back and verify dongle_mac is preserved
    let loaded = SystemState::load_from_yaml(&state_path).unwrap();
    let sensor = loaded.sensors.get("AABB1234").unwrap();
    assert_eq!(sensor.dongle_mac.as_deref(), Some("DONGLE01"));
    assert_eq!(sensor.state, SensorState::Contact { is_open: true });

    // 3. Verify backward compat: missing dongle_mac defaults to None
    let yaml_no_dongle = r#"
sensors:
  "CCDD5678":
    mac: "CCDD5678"
    sensor_type: "motion"
    last_seen: 1620000000
    battery: 100
    signal: -55
"#;
    fs::write(&state_path, yaml_no_dongle).unwrap();
    let loaded = SystemState::load_from_yaml(&state_path).unwrap();
    let sensor = loaded.sensors.get("CCDD5678").unwrap();
    assert!(sensor.dongle_mac.is_none());

    let _ = fs::remove_file(state_path);
}

#[test]
fn test_load_all_from_state_preserves_dongle_mac() {
    let config_path = get_temp_file("test_load_all_config.yaml");
    let state_path = get_temp_file("test_load_all_state.yaml");
    let _ = fs::remove_file(&config_path);
    let _ = fs::remove_file(&state_path);

    // Write a state file with dongle_mac populated
    let state_yaml = r#"
sensors:
  "AABB1234":
    mac: "AABB1234"
    sensor_type: "contact"
    last_seen: 1620000000
    battery: 85
    signal: -50
    state:
      kind: Contact
      is_open: false
    dongle_mac: "DONGLE99"
"#;
    fs::write(&state_path, state_yaml).unwrap();

    let mut manager = SensorManager::new(
        config_path.to_str().unwrap().to_string(),
        state_path.to_str().unwrap().to_string(),
    );
    let count = manager.load_all_from_state().unwrap();
    assert_eq!(count, 1);

    let sensor = manager.get_sensors().get("AABB1234").unwrap();
    assert_eq!(sensor.dongle_mac.as_deref(), Some("DONGLE99"));
    assert_eq!(sensor.battery_pct, Some(85));
    assert!(matches!(sensor.state, SensorState::Contact { is_open: false }));

    let _ = fs::remove_file(config_path);
    let _ = fs::remove_file(state_path);
}

#[test]
fn test_sensor_new_has_no_dongle_mac() {
    let sensor = WyzeSensor::new(
        "AABBCCDD".to_string(),
        SensorType::ContactV2,
        "Test".to_string(),
    );
    assert!(sensor.dongle_mac.is_none());
}

// --- ScanRequest deserialization tests (requires dongle_mac as mandatory String) ---

#[test]
fn test_scan_request_dongle_mac_is_required() {
    // Deserializing without dongle_mac should fail
    let json_no_mac = r#"{"enable": true}"#;
    let result: Result<wyzesense2mqtt_rs::web::ScanRequest, _> = serde_json::from_str(json_no_mac);
    assert!(result.is_err(), "ScanRequest should require dongle_mac");

    // With dongle_mac should succeed
    let json_with_mac = r#"{"enable": true, "dongle_mac": "DONGLE01"}"#;
    let result: Result<wyzesense2mqtt_rs::web::ScanRequest, _> = serde_json::from_str(json_with_mac);
    assert!(result.is_ok());
    let req = result.unwrap();
    assert_eq!(req.dongle_mac, "DONGLE01");
    assert!(req.enable);
}
