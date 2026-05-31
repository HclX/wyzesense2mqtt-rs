use wyzesense2mqtt_rs::engine::Engine;
use wyzesense2mqtt_rs::transport::replay::ReplayTransport;
use wyzesense2mqtt_rs::protocol::telemetry::{DongleEvent, SensorType, TelemetryData};
use wyzesense2mqtt_rs::protocol::packet::commands;
use wyzesense2mqtt_rs::protocol::sensor::WyzeSensor;

use tokio::sync::mpsc;
use tracing::info;

#[tokio::test]
async fn test_sensor_lifecycle_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let replay_transport = ReplayTransport::new();

    // --- 1. Register Expected Command Responses (to avoid race conditions) ---
    
    // Handshake 1: Inquiry (0x4327) -> InquiryResponse (0x4328)
    replay_transport.register_response(commands::CMD_INQUIRY, vec![0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F]);
    
    // Handshake 2: Get ENR (0x4302) -> GetEnrResponse (0x4303)
    let mut enr_resp = vec![0x55, 0xAA, 0x43, 0x13, 0x03];
    enr_resp.extend_from_slice(&[0x31; 16]);
    enr_resp.extend_from_slice(&[0x04, 0x68]);
    replay_transport.register_response(commands::CMD_GET_ENR, enr_resp);
    
    // Handshake 3: Get MAC (0x4304) -> GetMacResponse (0x4305)
    replay_transport.register_response(commands::CMD_GET_MAC, vec![0x55, 0xAA, 0x43, 0x0B, 0x05, 77, 65, 67, 65, 68, 68, 82, 49, 0x03, 0x6F]);
    
    // Handshake 4: Get Version (0x5316) -> GetVersionResponse (0x5317)
    replay_transport.register_response(commands::CMD_GET_VERSION, vec![0x55, 0xAA, 0x53, 0x09, 0x17, 86, 49, 46, 48, 46, 48, 0x02, 0xB5]);
    
    // Handshake 5: Finish Auth (0x5314) -> FinishAuthResponse (0x5315)
    replay_transport.register_response(commands::CMD_FINISH_AUTH, vec![0x55, 0xAA, 0x53, 0x03, 0x15, 0x01, 0x6A]);

    // Scan Enable (0x531C) -> ScanResponse (0x531D) - First call (Corrected checksum to 0x01, 0x74)
    replay_transport.register_response(commands::CMD_SET_SCAN, vec![0x55, 0xAA, 0x53, 0x04, 0x1D, 0x01, 0x01, 0x74]);

    // Verify Sensor (0x5323) -> VerifyResponse (0x5324) (Corrected checksum to 0x01, 0x7B)
    replay_transport.register_response(commands::CMD_VERIFY_SENSOR, vec![0x55, 0xAA, 0x53, 0x04, 0x24, 0x01, 0x01, 0x7B]);

    // Scan Disable (0x531C) -> ScanResponse (0x531D) - Second call (Corrected checksum to 0x01, 0x73)
    replay_transport.register_response(commands::CMD_SET_SCAN, vec![0x55, 0xAA, 0x53, 0x04, 0x1D, 0x00, 0x01, 0x73]);

    // Delete Sensor (0x5325) -> DeleteResponse (0x5326) (Corrected checksum to 0x01, 0x7D)
    replay_transport.register_response(commands::CMD_DELETE_SENSOR, vec![0x55, 0xAA, 0x53, 0x04, 0x26, 0x01, 0x01, 0x7D]);


    // --- Setup Engine ---
    let (event_tx, mut event_rx) = mpsc::channel::<DongleEvent>(32);
    let mut engine = Engine::new(replay_transport.clone(), event_tx, None);
    let exit_tx = engine.start();

    // 1. Execute Handshake
    info!("Executing handshake...");
    engine.initialize_handshake().await.unwrap();
    assert_eq!(engine.dongle_mac(), Some("MACADDR1"));
    assert_eq!(engine.dongle_version(), Some("V1.0.0"));

    // 2. Enable Scan
    info!("Enabling scan...");
    engine.set_scan(true).await.unwrap();

    // Inject Scanned Event (spontaneous)
    // MAC: "SENSO001" (83, 69, 78, 83, 79, 48, 48, 49), Type: ContactV1 (0x01), Version: 23
    let scan_payload = vec![
        0x55, 0xAA, 0x53, 0x0E, 0x20,
        0xA3, // EVENT_TYPE
        83, 69, 78, 83, 79, 48, 48, 49, // "SENSO001"
        0x01, // ContactV1
        0x17, // Version: 23
        0x04, 0x54 // Checksum
    ];
    info!("Injecting scanned event...");
    replay_transport.enqueue_read(&scan_payload);

    // 3. Wait for Scanned Event
    info!("Waiting for scanned event...");
    let scan_event = event_rx.recv().await.unwrap();
    assert_eq!(scan_event.mac, "SENSO001");
    assert_eq!(scan_event.sensor_type, SensorType::ContactV1);
    assert_eq!(scan_event.data, TelemetryData::Scanned { version: 23 });

    // 4. Verify Sensor
    info!("Verifying sensor...");
    engine.verify_sensor("SENSO001", SensorType::ContactV1).await.unwrap();

    // 5. Disable Scan
    info!("Disabling scan...");
    engine.set_scan(false).await.unwrap();

    // Inject Alarm Event (spontaneous)
    // Alarm data format: >BBBBBHB = DataType, Battery, Unk, Unk, State, Seq(u16), RSSI
    let mut alarm_payload = vec![0x55, 0xAA, 0x53, 0x1D, 0x19];
    let alarm_data = vec![
        0x00, 0x00, 0x01, 0x8F, 0xAB, 0x8E, 0x8C, 0x00, // Timestamp
        0xA2, // EVENT_TYPE_ALARM
        83, 69, 78, 83, 79, 48, 48, 49, // "SENSO001"
        0x01, // ContactV1
        0x01, // remaining[0]: data_type
        0x5A, // remaining[1]: battery: 90%
        0x00, // remaining[2]: unknown
        0x00, // remaining[3]: unknown
        0x01, // remaining[4]: state: 1 (open) ← correct offset!
        0x00, 0x0A, // remaining[5..7]: sequence (16-bit BE)
        0x3C, // remaining[7]: RSSI: 60
    ];
    alarm_payload.extend_from_slice(&alarm_data);
    // Recalculate checksum: sum of all bytes before checksum & 0xFFFF
    let cs: u16 = alarm_payload.iter().map(|&b| b as u16).sum::<u16>() & 0xFFFF;
    alarm_payload.push((cs >> 8) as u8);
    alarm_payload.push((cs & 0xFF) as u8);
    info!("Injecting alarm event...");
    replay_transport.enqueue_read(&alarm_payload);

    // 6. Wait for Alarm Event (Open)
    info!("Waiting for alarm event...");
    let alarm_event = event_rx.recv().await.unwrap();
    assert_eq!(alarm_event.mac, "SENSO001");
    assert_eq!(alarm_event.sensor_type, SensorType::ContactV1);
    
    let mut sensor = WyzeSensor::new(
        alarm_event.mac.clone(),
        alarm_event.sensor_type,
        "Contact Sensor".to_string(),
    );
    sensor.update_from_event(&alarm_event).unwrap();

    assert_eq!(sensor.battery_pct, Some(90));
    assert_eq!(sensor.rssi_dbm, -60);
    assert_eq!(sensor.get_state_payload()["state"], "open");

    // Inject Heartbeat Event (spontaneous)
    // Heartbeat uses same >BBBBBHB format but state field is not semantically used
    let mut hb_payload = vec![0x55, 0xAA, 0x53, 0x1D, 0x19];
    let hb_data = vec![
        0x00, 0x00, 0x01, 0x8F, 0xAB, 0x8E, 0x8C, 0x00, // Timestamp
        0xA1, // EVENT_TYPE_HEARTBEAT
        83, 69, 78, 83, 79, 48, 48, 49, // "SENSO001"
        0x01, // ContactV1
        0x02, // remaining[0]: data_type
        0x5A, // remaining[1]: battery: 90%
        0x00, // remaining[2]: unknown
        0x00, // remaining[3]: unknown
        0x00, // remaining[4]: state (not used for heartbeat)
        0x00, 0x0B, // remaining[5..7]: sequence
        0x3C, // remaining[7]: RSSI: 60
    ];
    hb_payload.extend_from_slice(&hb_data);
    let cs: u16 = hb_payload.iter().map(|&b| b as u16).sum::<u16>() & 0xFFFF;
    hb_payload.push((cs >> 8) as u8);
    hb_payload.push((cs & 0xFF) as u8);
    info!("Injecting heartbeat event...");
    replay_transport.enqueue_read(&hb_payload);

    // 7. Wait for Heartbeat Event
    info!("Waiting for heartbeat event...");
    let hb_event = event_rx.recv().await.unwrap();
    assert_eq!(hb_event.mac, "SENSO001");
    
    sensor.update_from_event(&hb_event).unwrap();
    assert_eq!(sensor.get_state_payload()["state"], "open");

    // NOTE: Availability monitor / offline timeout is tested separately
    // in config_test.rs::test_availability_monitor which works correctly.
    // The e2e test focuses on protocol-level correctness.

    // 8. Delete/Unpair Sensor
    info!("Deleting sensor...");
    engine.delete_sensor("SENSO001").await.unwrap();

    // Verify that transport is fully consumed
    assert!(!replay_transport.has_unread(), "ReplayTransport has unread bytes left!");

    // Shutdown
    let _ = exit_tx.send(());
}
