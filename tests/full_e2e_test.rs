//! Full end-to-end integration tests using VirtualDongle + TestHarness.
//!
//! These tests exercise the complete system stack:
//! Web API → EnginesMap → Engine → VirtualDongle (protocol mock)
//!
//! No hardware required.

mod test_harness;

use test_harness::TestHarness;
use wyzesense2mqtt_rs::protocol::telemetry::{SensorType, TelemetryData};
use wyzesense2mqtt_rs::protocol::sensor::WyzeSensor;
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;

// ---------------------------------------------------------------------------
// Test 1: Single Dongle — Full Sensor Lifecycle
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_single_dongle_sensor_lifecycle() {
    let dongle = VirtualDongle::new("DONGLE_A", "V2.3.9");
    let mut harness = TestHarness::boot(vec![dongle.clone()]).await;

    // Verify initial state: 1 dongle, 0 sensors
    let body = harness.get_dongles().await;
    let dongles = body["dongles"].as_array().unwrap();
    assert_eq!(dongles.len(), 1);
    assert_eq!(dongles[0]["mac"], "DONGLE_A");
    assert_eq!(dongles[0]["sensor_count"], 0);

    // Enable scan
    let resp = harness.set_scan("DONGLE_A", true).await;
    assert_eq!(resp.status(), 200);

    // Inject scan event — simulates sensor in pairing mode
    dongle.inject_scan_event("SENSOR01", SensorType::ContactV2, 25);

    // Wait for the scanned event
    let event = harness.expect_event(3000).await
        .expect("Should receive scan event");
    assert_eq!(event.mac, "SENSOR01");
    assert_eq!(event.sensor_type, SensorType::ContactV2);
    assert_eq!(event.data, TelemetryData::Scanned { version: 25 });

    // Disable scan
    let resp = harness.set_scan("DONGLE_A", false).await;
    assert_eq!(resp.status(), 200);

    // Inject alarm event — door opens
    dongle.inject_alarm_event("SENSOR01", SensorType::ContactV2, 1, 0x5A, 60);

    let event = harness.expect_event(3000).await
        .expect("Should receive alarm event");
    assert_eq!(event.mac, "SENSOR01");
    match &event.data {
        TelemetryData::Alarm { state, rssi, .. } => {
            assert_eq!(*state, 1, "Door should be open");
            assert_eq!(*rssi, -60);
        }
        other => panic!("Expected Alarm, got {:?}", other),
    }

    // Verify sensor state through WyzeSensor
    let mut sensor = WyzeSensor::new("SENSOR01".to_string(), SensorType::ContactV2, "Test".to_string());
    sensor.update_from_event(&event).unwrap();
    assert_eq!(sensor.get_state_payload()["state"], "open");

    // Delete sensor via API
    let resp = harness.delete_sensor("SENSOR01").await;
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------------------
// Test 2: Multi-Dongle — Sensor Isolation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multi_dongle_sensor_isolation() {
    let dongle_a = VirtualDongle::new("DONGLE_A", "V2.3.9")
        .with_sensors(vec!["SENSOR_A".to_string()]);
    let dongle_b = VirtualDongle::new("DONGLE_B", "V1.0.0")
        .with_sensors(vec!["SENSOR_B".to_string()]);

    let mut harness = TestHarness::boot(vec![dongle_a.clone(), dongle_b.clone()]).await;

    // Drain any sensor-list-item events that come from with_sensors
    harness.drain_events().await;

    // Verify 2 dongles registered
    let body = harness.get_dongles().await;
    let dongles = body["dongles"].as_array().unwrap();
    assert_eq!(dongles.len(), 2);

    // Find each dongle's sensors
    let a = dongles.iter().find(|d| d["mac"] == "DONGLE_A").unwrap();
    let b = dongles.iter().find(|d| d["mac"] == "DONGLE_B").unwrap();

    // Each dongle should have exactly 1 sensor
    assert_eq!(a["sensor_count"], 1, "DONGLE_A should have 1 sensor");
    assert_eq!(b["sensor_count"], 1, "DONGLE_B should have 1 sensor");

    // Inject alarm on DONGLE_A's sensor — verify it comes through
    dongle_a.inject_alarm_event("SENSOR_A", SensorType::ContactV2, 1, 0x5A, 50);
    let event = harness.expect_event(3000).await
        .expect("Should receive alarm from DONGLE_A");
    assert_eq!(event.mac, "SENSOR_A");

    // Inject heartbeat on DONGLE_B's sensor — verify isolation
    dongle_b.inject_heartbeat_event("SENSOR_B", SensorType::MotionV2, 0x64, 40);
    let event = harness.expect_event(3000).await
        .expect("Should receive heartbeat from DONGLE_B");
    assert_eq!(event.mac, "SENSOR_B");
}

// ---------------------------------------------------------------------------
// Test 3: Exclusive Scan Enforcement
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_exclusive_scan_enforcement() {
    let dongle_a = VirtualDongle::new("DONGLE_A", "V2.0.0");
    let dongle_b = VirtualDongle::new("DONGLE_B", "V2.0.0");
    dongle_b.register_extra_scan_responses(1, 1);

    let harness = TestHarness::boot(vec![dongle_a.clone(), dongle_b.clone()]).await;

    // Start scan on DONGLE_A
    let resp = harness.set_scan("DONGLE_A", true).await;
    assert_eq!(resp.status(), 200);

    // Attempt scan on DONGLE_B while DONGLE_A is scanning → 409 CONFLICT
    let resp = harness.set_scan("DONGLE_B", true).await;
    assert_eq!(resp.status(), 409, "Should reject concurrent scan");

    // Stop scan on DONGLE_A
    let resp = harness.set_scan("DONGLE_A", false).await;
    assert_eq!(resp.status(), 200);

    // Now DONGLE_B should be allowed to scan
    let resp = harness.set_scan("DONGLE_B", true).await;
    assert_eq!(resp.status(), 200);

    // Cleanup: stop scan on B
    let resp = harness.set_scan("DONGLE_B", false).await;
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------------------
// Test 4: ScanRequest API Contract Validation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_scan_api_contract() {
    let dongle = VirtualDongle::new("DONGLE_A", "V2.0.0");
    let harness = TestHarness::boot(vec![dongle]).await;

    // Missing dongle_mac → 422 Unprocessable Entity (JSON parse failure)
    let resp = harness.client
        .post(&format!("{}/api/scan", harness.base_url))
        .json(&serde_json::json!({ "enable": true }))
        .send().await.unwrap();
    assert!(
        resp.status() == 422 || resp.status() == 400,
        "Missing dongle_mac should fail: got {}",
        resp.status()
    );

    // Non-existent dongle_mac → 404
    let resp = harness.set_scan("NONEXISTENT", true).await;
    assert_eq!(resp.status(), 404, "Unknown dongle should return 404");

    // Valid request → 200
    let resp = harness.set_scan("DONGLE_A", true).await;
    assert_eq!(resp.status(), 200, "Valid request should succeed");
}

// ---------------------------------------------------------------------------
// Test 5: Raw Packet Round-Trip
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_raw_packet_roundtrip() {
    let dongle = VirtualDongle::new("DONGLE_A", "V2.0.0");

    // Register an extra GetMAC response for the raw packet test
    use wyzesense2mqtt_rs::protocol::packet::{commands, Packet};
    let mac_resp = Packet::new_sync(0x05, b"DONGLE_A".to_vec());
    dongle.replay_transport().register_response(commands::CMD_GET_MAC, mac_resp.to_bytes());

    let harness = TestHarness::boot(vec![dongle]).await;

    // Build a GetMAC command packet
    let get_mac_pkt = Packet::new_sync(0x04, vec![]);
    let raw_bytes: Vec<u8> = get_mac_pkt.to_bytes();

    let resp = harness.send_raw("DONGLE_A", raw_bytes).await;
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let response_bytes: Vec<u8> = serde_json::from_value(body["response_bytes"].clone()).unwrap();

    // Parse the response to verify it contains our MAC
    let (parsed, _) = Packet::parse(&response_bytes).unwrap();
    let payload = parsed.payload_bytes().unwrap();
    let mac_str = String::from_utf8(payload.to_vec()).unwrap();
    assert_eq!(mac_str, "DONGLE_A");
}

// ---------------------------------------------------------------------------
// Test 6: Pre-Paired Sensors Warm-Up
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pre_paired_sensor_warmup() {
    let dongle = VirtualDongle::new("DONGLE_A", "V2.3.9")
        .with_sensors(vec!["S0000001".to_string(), "S0000002".to_string(), "S0000003".to_string()]);

    let mut harness = TestHarness::boot(vec![dongle]).await;
    harness.drain_events().await;

    // All 3 sensors should be assigned to DONGLE_A
    let body = harness.get_dongles().await;
    let dongles = body["dongles"].as_array().unwrap();
    assert_eq!(dongles.len(), 1);
    let dongle_info = &dongles[0];
    assert_eq!(dongle_info["mac"], "DONGLE_A");
    assert_eq!(dongle_info["sensor_count"], 3, "Should have 3 pre-paired sensors");

    // Verify each sensor is assigned in SensorManager
    let manager = harness.sensor_manager.lock().unwrap();
    for mac in &["S0000001", "S0000002", "S0000003"] {
        let sensor = manager.get_sensors().get(*mac)
            .unwrap_or_else(|| panic!("Sensor {} should exist", mac));
        assert_eq!(sensor.dongle_mac.as_deref(), Some("DONGLE_A"),
            "Sensor {} should be assigned to DONGLE_A", mac);
    }
}

// ---------------------------------------------------------------------------
// Test 7: Alarm Event State Change
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_alarm_state_change_open_close() {
    let dongle = VirtualDongle::new("DONGLE_A", "V2.3.9");
    let mut harness = TestHarness::boot(vec![dongle.clone()]).await;

    // Inject alarm: door opens
    dongle.inject_alarm_event("SENSOR01", SensorType::ContactV2, 1, 0x5A, 60);
    let open_event = harness.expect_event(3000).await.expect("open event");
    let mut sensor = WyzeSensor::new("SENSOR01".to_string(), SensorType::ContactV2, "Door".to_string());
    sensor.update_from_event(&open_event).unwrap();
    assert_eq!(sensor.get_state_payload()["state"], "open");
    assert!(sensor.battery_pct.is_some(), "Battery should be computed");
    assert_eq!(sensor.rssi_dbm, -60);

    // Inject heartbeat: state should NOT change
    dongle.inject_heartbeat_event("SENSOR01", SensorType::ContactV2, 0x5A, 60);
    let hb_event = harness.expect_event(3000).await.expect("heartbeat event");
    sensor.update_from_event(&hb_event).unwrap();
    assert_eq!(sensor.get_state_payload()["state"], "open", "Heartbeat should not change state");

    // Inject alarm: door closes
    dongle.inject_alarm_event("SENSOR01", SensorType::ContactV2, 0, 0x5A, 60);
    let close_event = harness.expect_event(3000).await.expect("close event");
    sensor.update_from_event(&close_event).unwrap();
    assert_eq!(sensor.get_state_payload()["state"], "closed");
}

// ---------------------------------------------------------------------------
// Test 8: Climate & Leak Sensor Event Types
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_climate_and_leak_events() {
    let dongle = VirtualDongle::new("DONGLE_A", "V2.3.9");
    let mut harness = TestHarness::boot(vec![dongle.clone()]).await;

    // --- Climate Event ---
    dongle.inject_climate_event("CLIMATE1", 23.5, 55, 0x64, 50);
    let event = harness.expect_event(3000).await.expect("climate event");
    assert_eq!(event.mac, "CLIMATE1");

    match &event.data {
        TelemetryData::Climate { temperature, humidity, battery, rssi, .. } => {
            assert!((*temperature - 23.5).abs() < 0.1,
                "Temperature should be ~23.5, got {}", temperature);
            assert_eq!(*humidity, 55);
            assert_eq!(*battery, 0x64);
            assert_eq!(*rssi, -50);
        }
        other => panic!("Expected Climate, got {:?}", other),
    }

    // --- Leak Event ---
    dongle.inject_leak_event("LEAK0001", 1, true, 0, 0x5A, 45);
    let event = harness.expect_event(3000).await.expect("leak event");
    assert_eq!(event.mac, "LEAK0001");

    match &event.data {
        TelemetryData::Leak { state, probe_available, probe_state, battery, rssi } => {
            assert_eq!(*state, 1, "Should be wet");
            assert!(*probe_available, "Probe should be available");
            assert_eq!(*probe_state, 0, "Probe should be dry");
            assert_eq!(*battery, 0x5A);
            assert_eq!(*rssi, -45);
        }
        other => panic!("Expected Leak, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Test 9: Scan + Verify + Assign Registers Sensor Under Correct Dongle
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_auto_pairing_dongle_association() {
    let dongle_a = VirtualDongle::new("DONGLE_A", "V2.3.9");
    let dongle_b = VirtualDongle::new("DONGLE_B", "V2.3.9");
    dongle_a.register_extra_scan_responses(1, 1);
    dongle_a.register_extra_verify_responses(1);

    let mut harness = TestHarness::boot(vec![dongle_a.clone(), dongle_b.clone()]).await;

    // Enable scan on DONGLE_A
    let resp = harness.set_scan("DONGLE_A", true).await;
    assert_eq!(resp.status(), 200);

    // Inject scan event — simulates sensor arriving during pairing
    dongle_a.inject_scan_event("NEWSENS1", SensorType::ContactV2, 25);

    // Wait for scan event
    let event = harness.expect_event(3000).await.expect("scan event");
    assert_eq!(event.mac, "NEWSENS1");
    assert_eq!(event.data, TelemetryData::Scanned { version: 25 });

    // Verify the sensor via API (like the UI would)
    let resp = harness.verify_sensor("DONGLE_A", "NEWSENS1", "contact_v2").await;
    assert_eq!(resp.status(), 200);

    // Register sensor in SensorManager (as the real system does after verify)
    {
        let mut manager = harness.sensor_manager.lock().unwrap();
        manager.assign_dongle("DONGLE_A", &["NEWSENS1".to_string()]);
    }

    // Disable scan
    let resp = harness.set_scan("DONGLE_A", false).await;
    assert_eq!(resp.status(), 200);

    // Verify sensor is assigned to DONGLE_A in SensorManager
    let manager = harness.sensor_manager.lock().unwrap();
    let sensor = manager.get_sensors().get("NEWSENS1")
        .expect("Sensor NEWSENS1 should exist");
    assert_eq!(sensor.dongle_mac.as_deref(), Some("DONGLE_A"),
        "NEWSENS1 should be assigned to DONGLE_A");
    drop(manager);

    // Verify via API
    let body = harness.get_dongles().await;
    let dongles = body["dongles"].as_array().unwrap();
    let a = dongles.iter().find(|d| d["mac"] == "DONGLE_A").unwrap();
    assert!(a["sensor_count"].as_u64().unwrap() >= 1,
        "DONGLE_A should have at least 1 sensor");
}

// ---------------------------------------------------------------------------
// Test 10: Dongle Disconnect Removes From Engine Map
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_dongle_disconnect_cleanup() {
    let dongle_a = VirtualDongle::new("DONGLE_A", "V2.3.9")
        .with_sensors(vec!["SENSORA1".to_string()]);
    let dongle_b = VirtualDongle::new("DONGLE_B", "V2.3.9")
        .with_sensors(vec!["SENSORB1".to_string()]);

    let mut harness = TestHarness::boot(vec![dongle_a.clone(), dongle_b.clone()]).await;
    harness.drain_events().await;

    // Verify: 2 dongles initially
    let body = harness.get_dongles().await;
    assert_eq!(body["dongles"].as_array().unwrap().len(), 2);

    // Disconnect DONGLE_A
    dongle_a.disconnect();
    let disconnected = harness.await_dongle_disconnect("DONGLE_A", 3000).await;
    assert!(disconnected, "DONGLE_A should have disconnected");

    // Verify: only DONGLE_B remains
    let body = harness.get_dongles().await;
    let dongles = body["dongles"].as_array().unwrap();
    assert_eq!(dongles.len(), 1, "Only 1 dongle should remain");
    assert_eq!(dongles[0]["mac"], "DONGLE_B");

    // Verify SENSORA1 is no longer associated with DONGLE_A
    let manager = harness.sensor_manager.lock().unwrap();
    if let Some(sensor) = manager.get_sensors().get("SENSORA1") {
        assert_ne!(sensor.dongle_mac.as_deref(), Some("DONGLE_A"),
            "SENSORA1 should no longer be assigned to DONGLE_A");
    }
}

// ---------------------------------------------------------------------------
// Test 11: Sensor Re-Pairing Across Dongles After Disconnect
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_sensor_repairing_after_disconnect() {
    let dongle_a = VirtualDongle::new("DONGLE_A", "V2.3.9")
        .with_sensors(vec!["SHARED01".to_string()]);
    let dongle_b = VirtualDongle::new("DONGLE_B", "V2.3.9");
    dongle_b.register_extra_scan_responses(1, 1);
    dongle_b.register_extra_verify_responses(1);

    let mut harness = TestHarness::boot(vec![dongle_a.clone(), dongle_b.clone()]).await;
    harness.drain_events().await;

    // Verify SHARED01 starts on DONGLE_A
    {
        let manager = harness.sensor_manager.lock().unwrap();
        let sensor = manager.get_sensors().get("SHARED01").expect("SHARED01 should exist");
        assert_eq!(sensor.dongle_mac.as_deref(), Some("DONGLE_A"));
    }

    // Disconnect DONGLE_A
    dongle_a.disconnect();
    let disconnected = harness.await_dongle_disconnect("DONGLE_A", 3000).await;
    assert!(disconnected, "DONGLE_A should have disconnected");

    // After disconnect, SHARED01 should be unassigned (dongle_mac = None)
    {
        let manager = harness.sensor_manager.lock().unwrap();
        let sensor = manager.get_sensors().get("SHARED01").expect("SHARED01 should still exist");
        assert_eq!(sensor.dongle_mac, None,
            "SHARED01 should be unassigned after dongle disconnect");
    }

    // Re-pair SHARED01 onto DONGLE_B via scan + verify + assign
    let resp = harness.set_scan("DONGLE_B", true).await;
    assert_eq!(resp.status(), 200);

    dongle_b.inject_scan_event("SHARED01", SensorType::ContactV2, 25);
    let event = harness.expect_event(3000).await.expect("scan event for SHARED01");
    assert_eq!(event.mac, "SHARED01");

    let resp = harness.verify_sensor("DONGLE_B", "SHARED01", "contact_v2").await;
    assert_eq!(resp.status(), 200);

    // Manually register in SensorManager (as the real system does)
    {
        let mut manager = harness.sensor_manager.lock().unwrap();
        manager.assign_dongle("DONGLE_B", &["SHARED01".to_string()]);
    }

    harness.drain_events().await;

    // Verify SHARED01 is now on DONGLE_B
    let manager = harness.sensor_manager.lock().unwrap();
    let sensor = manager.get_sensors().get("SHARED01").expect("SHARED01 should still exist");
    assert_eq!(sensor.dongle_mac.as_deref(), Some("DONGLE_B"),
        "SHARED01 should now be assigned to DONGLE_B");
}

// ---------------------------------------------------------------------------
// Test 12: Leak Event State Transitions (Wet/Dry)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_leak_event_state_transitions() {
    let dongle = VirtualDongle::new("DONGLE_A", "V2.3.9");
    let mut harness = TestHarness::boot(vec![dongle.clone()]).await;

    // Inject leak: WET
    dongle.inject_leak_event("LEAK0001", 1, false, 0, 0x52, 40);
    let event = harness.expect_event(3000).await.expect("wet leak event");
    assert_eq!(event.mac, "LEAK0001");
    match &event.data {
        TelemetryData::Leak { state, probe_available, battery, rssi, .. } => {
            assert_eq!(*state, 1, "Should be wet");
            assert!(!*probe_available);
            assert_eq!(*battery, 0x52);
            assert_eq!(*rssi, -40);
        }
        other => panic!("Expected Leak, got {:?}", other),
    }

    // Update WyzeSensor
    let mut sensor = WyzeSensor::new("LEAK0001".to_string(), SensorType::LeakV2, "Leak".to_string());
    sensor.update_from_event(&event).unwrap();

    // Inject leak: DRY
    dongle.inject_leak_event("LEAK0001", 0, false, 0, 0x52, 40);
    let event = harness.expect_event(3000).await.expect("dry leak event");
    match &event.data {
        TelemetryData::Leak { state, .. } => {
            assert_eq!(*state, 0, "Should be dry");
        }
        other => panic!("Expected Leak, got {:?}", other),
    }
    sensor.update_from_event(&event).unwrap();
}

// ---------------------------------------------------------------------------
// Test 13: Battery Level Changes Through Alarm Events
// ---------------------------------------------------------------------------
// ContactV2 uses Alkaline1V5SingleAAA chemistry (curve: 50=100%, 42=50%, 32=0%).
// We use raw values within the curve range to verify differentiation.
#[tokio::test]
async fn test_battery_level_propagation() {
    let dongle = VirtualDongle::new("DONGLE_A", "V2.3.9");
    let mut harness = TestHarness::boot(vec![dongle.clone()]).await;

    // Inject alarm with high battery (raw=48, near top of 1.5V AAA curve)
    dongle.inject_alarm_event("BATSENS1", SensorType::ContactV2, 0, 48, 50);
    let event1 = harness.expect_event(3000).await.expect("high battery event");
    match &event1.data {
        TelemetryData::Alarm { battery, .. } => assert_eq!(*battery, 48),
        other => panic!("Expected Alarm, got {:?}", other),
    }

    let mut sensor = WyzeSensor::new("BATSENS1".to_string(), SensorType::ContactV2, "Bat".to_string());
    sensor.update_from_event(&event1).unwrap();
    let battery_high = sensor.battery_pct.expect("Battery should be computed");

    // Inject alarm with lower battery (raw=35, near bottom of 1.5V AAA curve = ~10%)
    dongle.inject_alarm_event("BATSENS1", SensorType::ContactV2, 0, 35, 50);
    let event2 = harness.expect_event(3000).await.expect("low battery event");
    match &event2.data {
        TelemetryData::Alarm { battery, .. } => assert_eq!(*battery, 35),
        other => panic!("Expected Alarm, got {:?}", other),
    }
    sensor.update_from_event(&event2).unwrap();
    let battery_low = sensor.battery_pct.expect("Battery should still be computed");

    assert!(battery_low < battery_high,
        "Lower raw battery (35) should produce lower pct ({}% vs {}%)",
        battery_low, battery_high);
}

// ---------------------------------------------------------------------------
// Test 14: Three Dongle Full Lifecycle
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_three_dongle_lifecycle() {
    let dongle_a = VirtualDongle::new("DONGLE_A", "V2.3.9")
        .with_sensors(vec!["SENSORA1".to_string(), "SENSORA2".to_string(), "SENSORA3".to_string()]);
    let dongle_b = VirtualDongle::new("DONGLE_B", "V2.3.9")
        .with_sensors(vec!["SENSORB1".to_string(), "SENSORB2".to_string()]);
    let dongle_c = VirtualDongle::new("DONGLE_C", "V2.3.9")
        .with_sensors(vec!["SENSORC1".to_string(), "SENSORC2".to_string(), "SENSORC3".to_string()]);

    dongle_a.register_extra_scan_responses(1, 1);
    dongle_a.register_extra_verify_responses(1);

    let mut harness = TestHarness::boot(vec![
        dongle_a.clone(), dongle_b.clone(), dongle_c.clone()
    ]).await;
    harness.drain_events().await;

    // Verify initial state: 3 dongles, 8 sensors total
    let body = harness.get_dongles().await;
    let dongles = body["dongles"].as_array().unwrap();
    assert_eq!(dongles.len(), 3, "Should have 3 dongles");
    let total: u64 = dongles.iter().map(|d| d["sensor_count"].as_u64().unwrap()).sum();
    assert_eq!(total, 8, "Should have 8 total sensors");

    // Disconnect DONGLE_B
    dongle_b.disconnect();
    let ok = harness.await_dongle_disconnect("DONGLE_B", 3000).await;
    assert!(ok, "DONGLE_B should disconnect");

    // Verify: 2 dongles remain
    let body = harness.get_dongles().await;
    let dongles = body["dongles"].as_array().unwrap();
    assert_eq!(dongles.len(), 2, "Should have 2 dongles after disconnect");
    assert!(dongles.iter().all(|d| d["mac"] != "DONGLE_B"),
        "DONGLE_B should be gone");

    // Re-pair SENSORB1 (from dead DONGLE_B) onto DONGLE_A via scan + verify + assign
    let resp = harness.set_scan("DONGLE_A", true).await;
    assert_eq!(resp.status(), 200);

    dongle_a.inject_scan_event("SENSORB1", SensorType::ContactV2, 25);
    let event = harness.expect_event(3000).await.expect("scan event for SENSORB1");
    assert_eq!(event.mac, "SENSORB1");

    let resp = harness.verify_sensor("DONGLE_A", "SENSORB1", "contact_v2").await;
    assert_eq!(resp.status(), 200);

    // Register in SensorManager
    {
        let mut manager = harness.sensor_manager.lock().unwrap();
        manager.assign_dongle("DONGLE_A", &["SENSORB1".to_string()]);
    }

    let resp = harness.set_scan("DONGLE_A", false).await;
    assert_eq!(resp.status(), 200);
    harness.drain_events().await;

    // Verify: SENSORB1 now assigned to DONGLE_A
    let manager = harness.sensor_manager.lock().unwrap();
    let sensor = manager.get_sensors().get("SENSORB1").expect("SENSORB1 should exist");
    assert_eq!(sensor.dongle_mac.as_deref(), Some("DONGLE_A"),
        "SENSORB1 should now be on DONGLE_A");
    drop(manager);

    // Verify: DONGLE_A now has 4 sensors (3 original + SENSORB1)
    let body = harness.get_dongles().await;
    let dongles = body["dongles"].as_array().unwrap();
    let a = dongles.iter().find(|d| d["mac"] == "DONGLE_A").unwrap();
    assert_eq!(a["sensor_count"].as_u64().unwrap(), 4,
        "DONGLE_A should now have 4 sensors");
}

