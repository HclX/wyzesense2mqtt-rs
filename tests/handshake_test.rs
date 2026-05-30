use wyzesense2mqtt_rs::engine::Engine;
use wyzesense2mqtt_rs::transport::replay::ReplayTransport;
use wyzesense2mqtt_rs::protocol::telemetry::DongleEvent;
use wyzesense2mqtt_rs::protocol::packet::commands;

use tokio::sync::mpsc;
use tracing::info;

#[tokio::test]
async fn test_dongle_handshake_integration() {
    // Initialize structural logging
    let _ = tracing_subscriber::fmt::try_init();

    let replay_transport = ReplayTransport::new();

    // 1. Load ReplayTransport with mock replies in correct sequence
    
    // Register interactive responses to avoid race conditions in background reader loop
    
    // Command 0x4327 (Inquiry) -> Reply 1 (0x4328)
    replay_transport.register_response(commands::CMD_INQUIRY, vec![0x55, 0xAA, 0x43, 0x04, 0x28, 0x01, 0x01, 0x6F]);

    // Command 0x4302 (Get ENR) -> Reply 2 (0x4303)
    let mut enr_resp = vec![0x55, 0xAA, 0x43, 0x13, 0x03];
    enr_resp.extend_from_slice(&[0x31; 16]);
    enr_resp.extend_from_slice(&[0x04, 0x68]);
    replay_transport.register_response(commands::CMD_GET_ENR, enr_resp);

    // Command 0x4304 (Get MAC) -> Reply 3 (0x4305)
    replay_transport.register_response(commands::CMD_GET_MAC, vec![0x55, 0xAA, 0x43, 0x0B, 0x05, 77, 65, 67, 65, 68, 68, 82, 49, 0x03, 0x6F]);

    // Command 0x5316 (Get Version) -> Reply 4 (0x5317)
    replay_transport.register_response(commands::CMD_GET_VERSION, vec![0x55, 0xAA, 0x53, 0x09, 0x17, 86, 49, 46, 48, 46, 48, 0x02, 0xB5]);

    // Command 0x5314 (Finish Auth) -> Reply 5 (0x5315)
    replay_transport.register_response(commands::CMD_FINISH_AUTH, vec![0x55, 0xAA, 0x53, 0x03, 0x15, 0x01, 0x6A]);

    // 2. Set up event queue channel
    let (event_tx, _event_rx) = mpsc::channel::<DongleEvent>(32);

    // 3. Initialize the engine
    let mut engine = Engine::new(replay_transport.clone(), event_tx, None);

    // Start the listener thread loop
    let exit_tx = engine.start();

    info!("Triggering handshake...");
    let handshake_res = engine.initialize_handshake().await;
    assert!(handshake_res.is_ok(), "Handshake failed: {:?}", handshake_res.err());

    // Validate Engine collected properties
    assert_eq!(engine.dongle_mac(), Some("MACADDR1"));
    assert_eq!(engine.dongle_version(), Some("V1.0.0"));

    // Verify that the transport read queue is fully consumed
    assert!(!replay_transport.has_unread(), "ReplayTransport has unread handshake response bytes left!");

    // Shutdown the background actor loop safely
    let _ = exit_tx.send(());
}
