//! A sensor joining is paired only by the gateway the user put into pairing
//! mode: gateways sharing a network identity all report the join.

use std::time::Duration;

use tokio::sync::mpsc;
use wyzesense2mqtt_rs::engine::Engine;
use wyzesense2mqtt_rs::protocol::packet::{commands, Packet};
use wyzesense2mqtt_rs::protocol::telemetry::{DongleEvent, SensorType, TelemetryData};
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;

const S: &str = "777B42DF";

async fn engine(mac: &str) -> (VirtualDongle, Engine, mpsc::Receiver<DongleEvent>) {
    let v = VirtualDongle::new(mac, "OWS").with_sensors(vec!["77A8C793".into()]);
    // The sensor's answer to the R1 token exchange.
    let mut r1 = S.as_bytes().to_vec();
    r1.extend_from_slice(&[0u8; 16]);
    v.replay_transport().register_response(commands::CMD_GET_R1, Packet::new_async(0x22, r1).to_bytes());
    let (tx, rx) = mpsc::channel(64);
    let mut e = Engine::new(v.transport(), tx);
    std::mem::forget(e.start());
    e.initialize_handshake().await.unwrap();
    e.set_auto_verify(true);
    (v, e, rx)
}

async fn paired(rx: &mut mpsc::Receiver<DongleEvent>, within: Duration) -> bool {
    let end = tokio::time::Instant::now() + within;
    while let Ok(Some(e)) = tokio::time::timeout_at(end, rx.recv()).await {
        if e.mac == S && matches!(e.data, TelemetryData::Paired { .. }) {
            return true;
        }
    }
    false
}

#[tokio::test]
async fn only_the_pairing_gateway_pairs_a_joining_sensor() {
    let (scanning, mut se, mut srx) = engine("77A85A36").await;
    let (other, _oe, mut orx) = engine("7788CCF6").await;
    scanning.register_extra_verify_responses(1);
    se.set_scan(true).await.unwrap();

    // Both radios hear the join.
    scanning.inject_scan_event(S, SensorType::ContactV1, 16);
    other.inject_scan_event(S, SensorType::ContactV1, 16);

    assert!(paired(&mut srx, Duration::from_secs(3)).await, "the pairing gateway pairs it");
    assert!(!paired(&mut orx, Duration::from_millis(500)).await, "the other gateway leaves it alone");
}
