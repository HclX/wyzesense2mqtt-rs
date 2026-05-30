use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use wyzesense2mqtt_rs::config::sensors::{SensorsConfig, SensorMetadata};
use wyzesense2mqtt_rs::config::state::{SystemState, SensorState};
use wyzesense2mqtt_rs::config::monitor::AvailabilityMonitor;
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;

fn get_temp_file(name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(name);
    dir
}

#[test]
fn test_sensors_config_load() {
    let yaml_content = r#"
sensors:
  "777A1234":
    name: "Front Door"
    type: "contact"
    timeout_sec: 30
  "777B5678":
    name: "Living Room Motion"
    type: "motion"
"#;
    let temp_path = get_temp_file("test_sensors.yaml");
    fs::write(&temp_path, yaml_content).unwrap();

    let config_res = SensorsConfig::load_from_yaml(&temp_path);
    assert!(config_res.is_ok(), "Failed to load config: {:?}", config_res.err());
    let config = config_res.unwrap();

    assert_eq!(config.sensors.len(), 2);
    
    let front_door = config.sensors.get("777A1234").unwrap();
    assert_eq!(front_door.name, "Front Door");
    assert_eq!(front_door.r#type, "contact");
    assert_eq!(front_door.timeout_sec, Some(30));

    let motion = config.sensors.get("777B5678").unwrap();
    assert_eq!(motion.name, "Living Room Motion");
    assert_eq!(motion.r#type, "motion");
    assert_eq!(motion.timeout_sec, None);

    fs::remove_file(temp_path).unwrap();
}

#[test]
fn test_system_state_persistence() {
    let temp_path = get_temp_file("test_state.yaml");
    if temp_path.exists() {
        fs::remove_file(&temp_path).unwrap();
    }

    // 1. Load non-existent should return default
    let state = SystemState::load_from_yaml(&temp_path).unwrap();
    assert_eq!(state, SystemState::default());

    // 2. Save some state
    let mut state = SystemState::default();
    state.sensors.insert(
        "777A1234".to_string(),
        SensorState {
            mac: "777A1234".to_string(),
            sensor_type: "contact".to_string(),
            version: "1".to_string(),
            last_seen: 1620000000,
            battery: 90,
            signal: -60,
        },
    );

    state.save_to_yaml_atomic(&temp_path).unwrap();

    // Verify file exists
    assert!(temp_path.exists());
    // Verify tmp file does not exist
    assert!(!temp_path.with_extension("tmp").exists());

    // 3. Load it back
    let loaded_state = SystemState::load_from_yaml(&temp_path).unwrap();
    assert_eq!(loaded_state, state);

    fs::remove_file(temp_path).unwrap();
}

#[tokio::test]
async fn test_availability_monitor() {
    let _ = tracing_subscriber::fmt::try_init();

    let config_path = get_temp_file("test_monitor_sensors.yaml");
    let state_path = get_temp_file("test_monitor_state.yaml");

    // 1. Clean up any stale files
    let _ = fs::remove_file(&config_path);
    let _ = fs::remove_file(&state_path);

    // 2. Write a test config with a 3 seconds timeout
    let config_yaml = r#"
sensors:
  "777A1234":
    name: "Front Door"
    type: "contact"
    timeout_sec: 3
"#;
    fs::write(&config_path, config_yaml).unwrap();

    // 3. Write a warm state file with last seen = now
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let state_yaml = format!(
        r#"
sensors:
  "777A1234":
    mac: "777A1234"
    sensor_type: "switch"
    version: "1"
    last_seen: {}
    battery: 100
    signal: -55
"#,
        now
    );
    fs::write(&state_path, state_yaml).unwrap();

    // 4. Setup SensorManager and load
    let sensor_manager = Arc::new(std::sync::Mutex::new(SensorManager::new(
        config_path.to_str().unwrap().to_string(),
        state_path.to_str().unwrap().to_string(),
    )));

    {
        let mut manager = sensor_manager.lock().unwrap();
        manager.load_sensors(&["777A1234".to_string()]).unwrap();
    }

    // 5. Instantiate AvailabilityMonitor with 1 second check interval
    let (offline_tx, mut offline_rx) = mpsc::channel(10);
    let monitor = AvailabilityMonitor::new(
        Arc::clone(&sensor_manager),
        Duration::from_secs(1), // check interval
        offline_tx,
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let monitor_handle = tokio::spawn(async move {
        monitor.start(shutdown_rx).await;
    });

    // Wait 1.5 seconds. Sensor timeout is 3 seconds, so it should NOT be offline yet
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(offline_rx.try_recv().is_err());

    // Now simulate time passing by manually updating the sensor's last seen to 5 seconds ago
    {
        let mut manager = sensor_manager.lock().unwrap();
        if let Some(sensor) = manager.get_sensors_mut().get_mut("777A1234") {
            sensor.set_last_seen(now - 5);
        }
    }

    // Wait for monitor to sweep and detect timeout (should happen within 1-2 seconds)
    let mut received = None;
    tokio::select! {
        res = offline_rx.recv() => {
            received = res;
        }
        _ = tokio::time::sleep(Duration::from_secs(4)) => {
            // Timeout
        }
    }

    assert_eq!(received, Some("777A1234".to_string()));

    // Clean up monitor tasks
    let _ = shutdown_tx.send(());
    let _ = monitor_handle.await;

    // Clean up temp files
    let _ = fs::remove_file(config_path);
    let _ = fs::remove_file(state_path);
}
