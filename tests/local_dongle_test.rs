//! Local USB dongle sessions: attach, unplug, plug back in.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use wyzesense2mqtt_rs::engine::EnginesMap;
use wyzesense2mqtt_rs::gateway::mqtt::DongleStatus;
use wyzesense2mqtt_rs::local_dongle::{LocalDongles, RadioWatch, SessionEnd};
use wyzesense2mqtt_rs::protocol::ows::{NetworkIdentity, NvSensor};
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

/// The same dongle running OpenWyzeSense (only those are watched for hangs).
fn ows_dongle() -> VirtualDongle {
    let net = NetworkIdentity { pan: 0x65FE, short: 0x0000, ext: 0x00124B00E2E0000A };
    dongle().with_ows(net, vec![NvSensor { mac: SENSOR.into(), short: 0x0001, sensor_type: 0x0E }])
}

/// Short timings, so a hang shows within a second.
fn fast_watch() -> RadioWatch {
    RadioWatch {
        check_every: Duration::from_millis(50),
        probe_after: Duration::from_millis(100),
        probe_timeout: Duration::from_millis(100),
        hung_after: Duration::from_millis(400),
        ..RadioWatch::default()
    }
}

/// Get Version probes (0x5316) the host sent.
fn probes(d: &VirtualDongle) -> usize {
    d.replay_transport().get_written().windows(5).filter(|w| *w == [0xAA, 0x55, 0x53, 0x03, 0x16]).count()
}

#[tokio::test]
async fn a_dongle_whose_radio_chip_hangs_ends_its_session_as_hung() {
    let mut c = ctx();
    let d = ows_dongle();
    let local = c.local.clone().with_radio_watch(fast_watch());
    let t = d.transport();
    let session = tokio::spawn(async move { local.run_session("/dev/hidraw7", t).await });
    wait_registered(&c.engines, DONGLE).await;
    assert!(c.status_rx.recv().await.unwrap().online);

    // The watcher checks that it runs OpenWyzeSense first.
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The USB chip keeps answering and the device stays plugged in: only the
    // radio chip's silence shows the hang.
    d.radio_hang();
    let end = tokio::time::timeout(Duration::from_secs(5), session).await.expect("the hang was noticed").unwrap();
    assert_eq!(end, SessionEnd::Hung { mac: DONGLE.to_string() });
    assert!(!c.engines.lock().await.contains_key(DONGLE));
    let offline = c.status_rx.recv().await.unwrap();
    assert_eq!((offline.mac.as_str(), offline.online), (DONGLE, false));
    assert!(probes(&d) >= 2, "it was probed before being given up on");
}

#[tokio::test]
async fn a_quiet_dongle_that_answers_its_probes_is_not_hung() {
    let c = ctx();
    let d = ows_dongle();
    let local = c.local.clone().with_radio_watch(fast_watch());
    let t = d.transport();
    let session = tokio::spawn(async move { local.run_session("/dev/hidraw7", t).await });
    wait_registered(&c.engines, DONGLE).await;

    // No sensor traffic for several times the hang threshold.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!session.is_finished(), "a quiet but answering dongle stays attached");
    assert!(c.engines.lock().await.contains_key(DONGLE));
    assert!(probes(&d) >= 3, "probed while quiet: {}", probes(&d));
}

#[tokio::test]
async fn no_probe_while_a_sensor_update_runs() {
    let c = ctx();
    let d = ows_dongle();
    let local = c.local.clone().with_radio_watch(fast_watch());
    let t = d.transport();
    let session = tokio::spawn(async move { local.run_session("/dev/hidraw7", t).await });
    wait_registered(&c.engines, DONGLE).await;

    tokio::time::sleep(Duration::from_millis(300)).await; // identified as OpenWyzeSense
    let _ota = c.engines.lock().await[DONGLE].subscribe_ota();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = probes(&d);
    d.radio_hang();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(probes(&d), before, "no probe during the update");
    assert!(!session.is_finished());

    // Once the update is over, a silent radio chip is noticed.
    c.engines.lock().await[DONGLE].unsubscribe_ota();
    let end = tokio::time::timeout(Duration::from_secs(5), session).await.unwrap().unwrap();
    assert_eq!(end, SessionEnd::Hung { mac: DONGLE.to_string() });
}

#[tokio::test]
async fn a_stock_dongle_is_not_watched() {
    let c = ctx();
    let d = dongle();
    let local = c.local.clone().with_radio_watch(fast_watch());
    let t = d.transport();
    let session = tokio::spawn(async move { local.run_session("/dev/hidraw7", t).await });
    wait_registered(&c.engines, DONGLE).await;
    let before = probes(&d);
    d.radio_hang();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!session.is_finished(), "stock firmware isn't watched");
    assert_eq!(probes(&d), before, "no Get Version probes: stock firmware blinks its LED on each");
}
