use wyzesense2mqtt_rs::engine::Engine;
use wyzesense2mqtt_rs::transport::replay::ReplayTransport;
use wyzesense2mqtt_rs::protocol::telemetry::DongleEvent;
use wyzesense2mqtt_rs::protocol::packet::commands;
use wyzesense2mqtt_rs::web::start_web_server;

use std::time::Duration;
use tokio::sync::mpsc;
use serde_json::Value;

#[tokio::test]
async fn test_web_endpoints_integration() {
    // 1. Setup ReplayTransport with handshake transactions
    let replay_transport = ReplayTransport::new();
    
    // Inquiry
    replay_transport.register_response(
        commands::CMD_INQUIRY,
        vec![0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F]
    );
    // GetENR
    let mut enr_resp = vec![0x55, 0xAA, 0x43, 0x13, 0x03];
    enr_resp.extend_from_slice(&[0x31; 16]);
    enr_resp.extend_from_slice(&[0x04, 0x68]);
    replay_transport.register_response(commands::CMD_GET_ENR, enr_resp);
    
    // GetMAC
    replay_transport.register_response(
        commands::CMD_GET_MAC,
        vec![0x55, 0xAA, 0x43, 0x0B, 0x05, 77, 65, 67, 65, 68, 68, 82, 49, 0x03, 0x6F]
    );
    // GetVersion
    replay_transport.register_response(
        commands::CMD_GET_VERSION,
        vec![0x55, 0xAA, 0x53, 0x09, 0x17, 86, 49, 46, 48, 46, 48, 0x02, 0xB5]
    );
    // FinishAuth
    replay_transport.register_response(
        commands::CMD_FINISH_AUTH,
        vec![0x55, 0xAA, 0x53, 0x03, 0x15, 0x01, 0x6A]
    );

    // 2. Initialize engine and handshake
    let (event_tx, mut _event_rx) = mpsc::channel::<DongleEvent>(32);
    let mut engine = Engine::new(replay_transport.clone(), event_tx, None);
    let _exit_tx = engine.start();
    engine.initialize_handshake().await.unwrap();

    // 3. Find free port dynamically and spawn Axum Web Server in background
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let port = local_addr.port();
    
    // Re-bind or pass the listener... wait, start_web_server binds to address internally.
    // So we extract the port, drop the listener, and let start_web_server bind to it!
    drop(listener);

    let server_engine = engine; // Transfer ownership
    let sensor_manager = std::sync::Arc::new(std::sync::Mutex::new(
        wyzesense2mqtt_rs::protocol::sensor::SensorManager::new(
            "config/sensors.yaml".to_string(),
            "config/state.yaml".to_string(),
        )
    ));
    tokio::spawn(async move {
        if let Err(e) = start_web_server(server_engine, sensor_manager, port).await {
            panic!("Web server failed to run: {}", e);
        }
    });

    // Give the server a brief moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{}", port);

    // --- TEST 1: GET /api/dongle ---
    let resp = client.get(&format!("{}/api/dongle", base_url)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["connected"], true);
    assert_eq!(body["mac"], "MACADDR1");
    assert_eq!(body["version"], "V1.0.0");

    // --- TEST 2: POST /api/raw (Diagnostic HEX Packet) ---
    // We want to send a raw packet representing the GetMAC command.
    // Command bytes: [170, 85, 67, 3, 4, 1, 73] (checksum 0x0149)
    // Expected response enqueued for 0x4304 in replay transport
    replay_transport.register_response(
        commands::CMD_GET_MAC,
        vec![0x55, 0xAA, 0x43, 0x0B, 0x05, 77, 65, 67, 65, 68, 68, 82, 49, 0x03, 0x6F]
    );

    let raw_req = serde_json::json!({
        "bytes": vec![170, 85, 67, 3, 4, 1, 73]
    });
    let resp = client.post(&format!("{}/api/raw", base_url))
        .json(&raw_req)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let expected_resp_bytes = vec![170, 85, 67, 11, 5, 77, 65, 67, 65, 68, 68, 82, 49, 3, 111];
    let response_bytes: Vec<u8> = serde_json::from_value(body["response_bytes"].clone()).unwrap();
    assert_eq!(response_bytes, expected_resp_bytes);
}
