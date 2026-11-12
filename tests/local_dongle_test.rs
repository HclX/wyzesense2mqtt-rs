//! Local USB dongle sessions: attach, unplug, plug back in.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use wyzesense2mqtt_rs::engine::EnginesMap;
use wyzesense2mqtt_rs::gateway::mqtt::DongleStatus;
use wyzesense2mqtt_rs::local_dongle::{LocalDongles, SessionEnd};
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;
use wyzesense2mqtt_rs::protocol::telemetry::DongleEvent;
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;

const DONGLE: &str = "77A85A36";
const SENSOR: &str = "77BD514D";

struct Ctx {
    local: LocalDongles,
    engines: EnginesMap,
    sensor_manager: Arc<Mutex<SensorManager>>,
    status_rx: mpsc::Receiver<DongleStatus>,
    _event_rx: mpsc::Receiver<DongleEvent>,
}

fn ctx() -> Ctx {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let (event_tx, event_rx) = mpsc::channel(128);
    let (status_tx, status_rx) = mpsc::channel(32);
    let (broadcast_tx, _) = tokio::sync::broadcast::channel(16);
    let engines: EnginesMap = Arc::new(tokio::sync::Mutex::new(Default::default()));
    let sensor_manager = Arc::new(Mutex::new(SensorManager::new(
        format!("/tmp/local_dongle_{}_config.yaml", id),
        format!("/tmp/local_dongle_{}_state.yaml", id),
    )));
    let local = LocalDongles::new(
        Arc::clone(&engines),
        Arc::clone(&sensor_manager),
        event_tx,
        status_tx,
        broadcast_tx,
    );
    Ctx { local, engines, sensor_manager, status_rx, _event_rx: event_rx }
}

fn dongle() -> VirtualDongle {
    VirtualDongle::new(DONGLE, "0.0.0.47 V1.8 Gateway GW3U").with_sensors(vec![SENSOR.to_string()])
}

async fn wait_registered(engines: &EnginesMap, mac: &str) {
    for _ in 0..100 {
        if engines.lock().await.contains_key(mac) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("dongle {} never registered", mac);
}

fn dongle_of(sm: &Arc<Mutex<SensorManager>>, sensor: &str) -> Option<String> {
    sm.lock().unwrap().get_sensors().get(sensor).and_then(|s| s.dongle_mac.clone())
}

#[tokio::test]
async fn unplug_and_replug_reattaches_the_dongle() {
    let mut c = ctx();

    // Plugged in.
    let first = dongle();
    let local = c.local.clone();
    let t = first.transport();
    let session = tokio::spawn(async move { local.run_session("/dev/hidraw7", t).await });
    wait_registered(&c.engines, DONGLE).await;
    assert_eq!(dongle_of(&c.sensor_manager, SENSOR).as_deref(), Some(DONGLE));
    let online = c.status_rx.recv().await.unwrap();
    assert_eq!((online.mac.as_str(), online.online), (DONGLE, true));

    // Unplugged: the session ends and cleans up.
    first.disconnect();
    let end = tokio::time::timeout(Duration::from_secs(5), session).await.unwrap().unwrap();
    assert_eq!(end, SessionEnd::Disconnected { mac: DONGLE.to_string() });
    assert!(!c.engines.lock().await.contains_key(DONGLE));
    assert_eq!(dongle_of(&c.sensor_manager, SENSOR), None);
    let offline = c.status_rx.recv().await.unwrap();
    assert_eq!((offline.mac.as_str(), offline.online), (DONGLE, false));

    // Live state that only exists in memory survives the replug.
    c.sensor_manager.lock().unwrap().get_sensors_mut().get_mut(SENSOR).unwrap().rssi_dbm = -42;

    // Plugged back in (a new device node, as after a real replug).
    let second = dongle();
    let local = c.local.clone();
    let t = second.transport();
    let _session = tokio::spawn(async move { local.run_session("/dev/hidraw8", t).await });
    wait_registered(&c.engines, DONGLE).await;
    assert_eq!(dongle_of(&c.sensor_manager, SENSOR).as_deref(), Some(DONGLE));
    assert_eq!(c.sensor_manager.lock().unwrap().get_sensors()[SENSOR].rssi_dbm, -42);
    let engine_path = c.engines.lock().await[DONGLE].device_path.clone();
    assert_eq!(engine_path.as_deref(), Some("/dev/hidraw8"));
    assert!(c.status_rx.recv().await.unwrap().online);
}

#[tokio::test]
async fn an_old_session_ending_late_leaves_the_new_one_alone() {
    let mut c = ctx();

    let old = dongle();
    let local = c.local.clone();
    let t = old.transport();
    let old_session = tokio::spawn(async move { local.run_session("/dev/hidraw0", t).await });
    wait_registered(&c.engines, DONGLE).await;
    assert!(c.status_rx.recv().await.unwrap().online);

    // The same dongle registers on a new node before the old one notices.
    let new = dongle();
    let local = c.local.clone();
    let t = new.transport();
    let _new_session = tokio::spawn(async move { local.run_session("/dev/hidraw1", t).await });
    for _ in 0..100 {
        if c.engines.lock().await[DONGLE].device_path.as_deref() == Some("/dev/hidraw1") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(c.status_rx.recv().await.unwrap().online);

    old.disconnect();
    tokio::time::timeout(Duration::from_secs(5), old_session).await.unwrap().unwrap();
    assert_eq!(c.engines.lock().await[DONGLE].device_path.as_deref(), Some("/dev/hidraw1"));
    assert_eq!(dongle_of(&c.sensor_manager, SENSOR).as_deref(), Some(DONGLE));
    assert!(c.status_rx.try_recv().is_err(), "no offline for a dongle still attached");
}

#[tokio::test]
async fn replugged_dongle_purges_sensor_repaired_elsewhere_while_unplugged() {
    use wyzesense2mqtt_rs::protocol::packet::{commands, Packet, PacketPayload};
    use wyzesense2mqtt_rs::protocol::telemetry::{SensorType, TelemetryData};

    let mut c = ctx();
    const DONGLE_B: &str = "77B96B47";
    const KEEP_ON_A: &str = "77AA0001";

    let first_a = VirtualDongle::new(DONGLE, "0.0.0.47")
        .with_sensors(vec![SENSOR.to_string(), KEEP_ON_A.to_string()]);
    let dongle_b = VirtualDongle::new(DONGLE_B, "0.0.0.47").with_sensors(vec![]);

    let local_a = c.local.clone();
    let t_a = first_a.transport();
    let session_a = tokio::spawn(async move { local_a.run_session("/dev/hidraw0", t_a).await });

    let local_b = c.local.clone();
    let t_b = dongle_b.transport();
    let _session_b = tokio::spawn(async move { local_b.run_session("/dev/hidraw1", t_b).await });

    wait_registered(&c.engines, DONGLE).await;
    wait_registered(&c.engines, DONGLE_B).await;
    assert_eq!(dongle_of(&c.sensor_manager, SENSOR).as_deref(), Some(DONGLE));

    // Unplug DONGLE_A, then pair SENSOR onto DONGLE_B while DONGLE_A is offline.
    first_a.disconnect();
    let end = tokio::time::timeout(Duration::from_secs(5), session_a).await.unwrap().unwrap();
    assert_eq!(end, SessionEnd::Disconnected { mac: DONGLE.to_string() });
    assert_eq!(dongle_of(&c.sensor_manager, SENSOR), None);

    c.engines.lock().await.get_mut(DONGLE_B).unwrap().clone().set_scan(true).await.unwrap();
    dongle_b.inject_scan_event(SENSOR, SensorType::ContactV2, 25);
    loop {
        let evt = tokio::time::timeout(Duration::from_secs(3), c._event_rx.recv())
            .await
            .expect("timed out waiting for Paired event")
            .unwrap();
        c.sensor_manager.lock().unwrap().dispatch_event(&evt);
        if evt.mac == SENSOR && matches!(evt.data, TelemetryData::Paired { .. }) {
            break;
        }
    }
    assert_eq!(dongle_of(&c.sensor_manager, SENSOR).as_deref(), Some(DONGLE_B));

    // Replug DONGLE_A still reporting SENSOR + KEEP_ON_A in its NVRAM.
    let replugged_a = VirtualDongle::new(DONGLE, "0.0.0.47")
        .with_sensors(vec![SENSOR.to_string(), KEEP_ON_A.to_string()]);
    let local_a2 = c.local.clone();
    let t_a2 = replugged_a.transport();
    let _session_a2 = tokio::spawn(async move { local_a2.run_session("/dev/hidraw2", t_a2).await });
    wait_registered(&c.engines, DONGLE).await;

    // DONGLE_A must delete stale SENSOR from its NVRAM and leave SENSOR on DONGLE_B.
    assert_eq!(dongle_of(&c.sensor_manager, SENSOR).as_deref(), Some(DONGLE_B));
    assert_eq!(dongle_of(&c.sensor_manager, KEEP_ON_A).as_deref(), Some(DONGLE));

    let written = replugged_a.replay_transport().get_written();
    let mut slice = written.as_slice();
    let mut deleted_sensor = 0;
    let mut deleted_keep = 0;
    while let Ok((pkt, n)) = Packet::parse(slice) {
        if !matches!(pkt.payload, PacketPayload::Ack(_))
            && pkt.cmd() == commands::CMD_DELETE_SENSOR
        {
            if matches!(&pkt.payload, PacketPayload::Bytes(b) if b == SENSOR.as_bytes()) {
                deleted_sensor += 1;
            }
            if matches!(&pkt.payload, PacketPayload::Bytes(b) if b == KEEP_ON_A.as_bytes()) {
                deleted_keep += 1;
            }
        }
        slice = &slice[n..];
    }
    assert_eq!(deleted_sensor, 1, "replugged DONGLE_A should delete stale SENSOR once");
    assert_eq!(deleted_keep, 0, "replugged DONGLE_A must keep its own sensor");
}
