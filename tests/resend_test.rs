//! The dongle re-sending a reply whose ACK it hadn't seen yet: the copy is
//! neither handled nor ACKed again. Sensor reports are always passed on.

use std::time::Duration;

use tokio::sync::mpsc;
use wyzesense2mqtt_rs::engine::{Engine, RESEND_WINDOW};
use wyzesense2mqtt_rs::protocol::packet::Packet;
use wyzesense2mqtt_rs::protocol::telemetry::SensorType;
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;

const SENSOR: &str = "77BD514D";

fn count(written: &[u8], pkt: &Packet) -> usize {
    let b = pkt.to_bytes();
    written.windows(b.len()).filter(|w| *w == b.as_slice()).count()
}

async fn engine() -> (VirtualDongle, Engine, mpsc::Receiver<wyzesense2mqtt_rs::protocol::telemetry::DongleEvent>) {
    let dongle = VirtualDongle::new("77A85A36", "0.0.0.47 V1.8 Gateway GW3U").with_sensors(vec![SENSOR.into()]);
    let (event_tx, mut events) = mpsc::channel(32);
    let mut engine = Engine::new(dongle.transport(), event_tx);
    let exit = engine.start();
    std::mem::forget(exit);
    engine.initialize_handshake().await.unwrap();
    while tokio::time::timeout(Duration::from_millis(100), events.recv()).await.is_ok() {}
    dongle.replay_transport().clear_written();
    (dongle, engine, events)
}

#[tokio::test]
async fn a_resent_reply_is_not_acked_twice() {
    let (dongle, _engine, _events) = engine().await;
    // An OpenWyzeSense flags reply, then its resend.
    let reply = Packet::new_async(0xF1, vec![0x01, 0, 0, 0, 0, 0, 0, 0, 0x1F]);
    dongle.replay_transport().enqueue_read(&reply.to_bytes());
    dongle.replay_transport().enqueue_read(&reply.to_bytes());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(count(&dongle.replay_transport().get_written(), &Packet::new_ack(0x53F1)), 1);

    // The same bytes after the window are a new packet.
    tokio::time::sleep(RESEND_WINDOW).await;
    dongle.replay_transport().enqueue_read(&reply.to_bytes());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(count(&dongle.replay_transport().get_written(), &Packet::new_ack(0x53F1)), 2);
}

#[tokio::test]
async fn identical_sensor_reports_are_all_passed_on() {
    let (dongle, _engine, mut events) = engine().await;
    // A sensor's retries can be byte-identical; roaming counts every copy.
    dongle.inject_report(SENSOR, SensorType::ContactV2, 1, 7, 60);
    dongle.inject_report(SENSOR, SensorType::ContactV2, 1, 7, 60);
    for _ in 0..2 {
        let e = tokio::time::timeout(Duration::from_millis(500), events.recv()).await.unwrap().unwrap();
        assert_eq!(e.mac, SENSOR);
    }
    assert_eq!(count(&dongle.replay_transport().get_written(), &Packet::new_ack(0x5319)), 2);
}
