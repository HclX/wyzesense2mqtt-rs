//! Roaming across two emulated OpenWyzeSense gateways sharing one network
//! identity: real `Engine`s and the `Roaming` coordinator, with `VirtualDongle`
//! answering 0x53F0 like our gateway firmware does.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use wyzesense2mqtt_rs::engine::{Engine, EnginesMap};
use wyzesense2mqtt_rs::protocol::ows::{self, NetworkIdentity, NvSensor};
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;
use wyzesense2mqtt_rs::protocol::telemetry::{DongleEvent, SensorType, TelemetryData};
use wyzesense2mqtt_rs::roaming::{Roaming, RoamingConfig};
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;

const NET: NetworkIdentity = NetworkIdentity { pan: 0x65FE, short: 0x0000, ext: 0x00124B00E2E0000A };
const S: &str = "77BD514D";
const S_SHORT: u16 = 0x0005;

/// Roaming turned on for every gateway (it is off unless configured).
fn roam() -> RoamingConfig {
    RoamingConfig { default_flags: ows::FLAG_ROAM, ..Default::default() }
}

/// Roaming, with reports judged after 100 ms instead of seconds.
fn fast() -> RoamingConfig {
    RoamingConfig { settle: Duration::from_millis(100), tick: Duration::from_millis(50), ..roam() }
}

fn entry(mac: &str, short: u16) -> NvSensor {
    NvSensor { mac: mac.into(), short, sensor_type: SensorType::ContactV2.to_u8() }
}

struct World {
    engines: EnginesMap,
    sensors: Arc<Mutex<SensorManager>>,
    roaming: Arc<Roaming>,
    raw_tx: mpsc::Sender<DongleEvent>,
    events: mpsc::Receiver<DongleEvent>,
    exits: Vec<tokio::sync::oneshot::Sender<()>>,
}

impl World {
    async fn new(cfg: RoamingConfig) -> Self {
        let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).try_init();
        let id = format!("{}_{:?}", std::process::id(), std::time::SystemTime::now());
        let id: String = id.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
        let sensors = Arc::new(Mutex::new(SensorManager::new(
            format!("/tmp/roam_{}_config.yaml", id),
            format!("/tmp/roam_{}_state.yaml", id),
        )));
        let engines: EnginesMap = Arc::new(tokio::sync::Mutex::new(Default::default()));
        let (broadcast_tx, _) = tokio::sync::broadcast::channel(16);
        let roaming = Roaming::new(cfg, Arc::clone(&engines), Arc::clone(&sensors), broadcast_tx);
        let (raw_tx, raw_rx) = mpsc::channel(128);
        let (event_tx, events) = mpsc::channel(128);
        roaming.spawn(raw_rx, event_tx);
        World { engines, sensors, roaming, raw_tx, events, exits: Vec::new() }
    }

    /// Connects a dongle the way the daemon does: handshake, sensor list,
    /// registry, then the coordinator.
    async fn connect(&mut self, dongle: &VirtualDongle) {
        let mut engine = Engine::new(dongle.transport(), self.raw_tx.clone());
        self.exits.push(engine.start());
        engine.initialize_handshake().await.unwrap();
        let list = engine.get_sensor_list().await.unwrap();
        self.sensors.lock().unwrap().assign_dongle(dongle.mac(), &list);
        self.engines.lock().await.insert(dongle.mac().to_string(), engine);
        self.roaming.gateway_connected(dongle.mac()).await;
    }

    async fn disconnect(&mut self, dongle: &VirtualDongle) {
        self.engines.lock().await.remove(dongle.mac());
        self.roaming.gateway_disconnected(dongle.mac());
    }

    async fn next_event(&mut self) -> Option<DongleEvent> {
        tokio::time::timeout(Duration::from_millis(500), self.events.recv()).await.ok().flatten()
    }

    fn owner(&self, mac: &str) -> Option<String> {
        self.sensors.lock().unwrap().get_sensors().get(mac).and_then(|s| s.dongle_mac.clone())
    }
}

async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..100 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {}", what);
}

fn gateways(a_table: Vec<NvSensor>, b_table: Vec<NvSensor>) -> (VirtualDongle, VirtualDongle) {
    (
        VirtualDongle::new("GWAAAAAA", "OWS").with_ows(NET, a_table),
        VirtualDongle::new("GWBBBBBB", "OWS").with_ows(NET, b_table),
    )
}

#[tokio::test]
async fn a_single_gateway_gets_no_flag_unless_configured() {
    let a = VirtualDongle::new("GWAAAAAA", "OWS").with_ows(NET, vec![entry(S, S_SHORT)]);
    let mut w = World::new(RoamingConfig::default()).await;
    w.connect(&a).await;
    assert_eq!(a.ows().flags, 0);
    let info = w.roaming.gateway_json("GWAAAAAA").expect("detected as ours");
    assert_eq!(info["flags_supported"], "00000017");
    assert_eq!(info["roaming"], false);
}

#[tokio::test]
async fn gateways_sharing_a_network_roam_automatically() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(RoamingConfig::default()).await;
    w.connect(&a).await;
    assert_eq!(a.ows().flags, 0, "alone on its network: no roaming");
    w.connect(&b).await;
    // The second gateway on the network turns roaming on for both.
    assert_eq!((a.ows().flags, b.ows().flags), (ows::FLAG_ROAM, ows::FLAG_ROAM));
    let status = w.roaming.status_json();
    assert!(status["gateways"].as_array().unwrap().iter().all(|g| g["roaming"] == true));
}

#[tokio::test]
async fn roam_false_keeps_a_gateway_out() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let cfg = RoamingConfig { roam_gateways: [("GWBBBBBB".to_string(), false)].into(), ..Default::default() };
    let mut w = World::new(cfg).await;
    w.connect(&a).await;
    w.connect(&b).await;
    assert_eq!((a.ows().flags, b.ows().flags), (0, 0), "one of two opted out: no roaming group");
    // overheard reports are not merged without roaming
    b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 1, 7, 80);
    assert!(w.next_event().await.is_none());
}

#[tokio::test]
async fn configured_flags_are_applied_per_gateway() {
    let c = VirtualDongle::new("GWCCCCCC", "OWS").with_ows(NetworkIdentity { pan: 0x1234, ..NET }, vec![]);
    let (a, _) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let cfg = RoamingConfig {
        default_flags: ows::FLAG_ROAM | ows::FLAG_DEDUP | ows::FLAG_ANNOUNCE,
        gateway_flags: [("GWCCCCCC".to_string(), ows::FLAG_FIX_ZERO_MAC)].into(),
        ..Default::default()
    };
    let mut w = World::new(cfg).await;
    w.connect(&a).await;
    w.connect(&c).await;
    // ANNOUNCE is configured but the emulated firmware does not support it;
    // DEDUP is never set with ROAM (the host must see resends)
    assert_eq!(a.ows().flags, ows::FLAG_ROAM);
    assert_eq!(c.ows().flags, ows::FLAG_FIX_ZERO_MAC);
}

#[tokio::test]
async fn flags_lost_to_a_gateway_restart_are_set_again() {
    let (a, _) = gateways(vec![], vec![]);
    let cfg = RoamingConfig {
        default_flags: ows::FLAG_DEDUP,
        flag_check_interval: Duration::from_millis(100),
        tick: Duration::from_millis(50),
        ..Default::default()
    };
    let mut w = World::new(cfg).await;
    w.connect(&a).await;
    assert_eq!(a.ows().flags, ows::FLAG_DEDUP);
    a.ows_restart();
    wait_for("flags set again", || a.ows().flags == ows::FLAG_DEDUP).await;
}

#[tokio::test]
async fn a_gateway_flashed_back_to_stock_is_forgotten() {
    let ours = VirtualDongle::new("GWAAAAAA", "OWS").with_ows(NET, vec![entry(S, S_SHORT)]);
    let mut w = World::new(RoamingConfig { probe_timeout: Duration::from_millis(200), ..Default::default() }).await;
    w.connect(&ours).await;
    assert!(w.roaming.gateway_json("GWAAAAAA").is_some());
    w.disconnect(&ours).await;
    // Stock restored: the same MAC comes back without our extensions.
    let stock = VirtualDongle::new("GWAAAAAA", "0.0.0.47").with_sensors(vec![S.into()]);
    w.connect(&stock).await;
    assert!(w.roaming.gateway_json("GWAAAAAA").is_none());
}

#[tokio::test]
async fn stock_dongle_is_left_alone() {
    let stock = VirtualDongle::new("STOCK001", "0.0.0.30").with_sensors(vec![S.into()]);
    let mut w = World::new(RoamingConfig { probe_timeout: Duration::from_millis(200), ..Default::default() }).await;
    w.connect(&stock).await;
    let written = stock.replay_transport().get_written();
    // one probe (0x53F0 [0x01]) and nothing after it
    let probe = wyzesense2mqtt_rs::protocol::packet::Packet::new_async(0xF0, vec![0x01]).to_bytes();
    assert_eq!(written.windows(probe.len()).filter(|w| *w == probe.as_slice()).count(), 1);
    // its reports pass through untouched
    stock.inject_report(S, SensorType::ContactV2, 1, 1, 60);
    let e = w.next_event().await.expect("report");
    assert_eq!((e.mac.as_str(), e.dongle_mac.as_deref()), (S, Some("STOCK001")));
}

#[tokio::test]
async fn owner_report_and_overheard_copy_make_one_event() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(roam()).await;
    w.connect(&a).await;
    w.connect(&b).await;

    b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 1, 7, 80);
    a.inject_report(S, SensorType::ContactV2, 1, 7, 60);
    let e = w.next_event().await.expect("one event");
    assert_eq!(e.mac, S);
    assert_eq!(e.dongle_mac.as_deref(), Some("GWAAAAAA"), "named after the owner, whichever copy came first");
    assert!(matches!(e.data, TelemetryData::Alarm { state: 1, .. }));
    assert!(w.next_event().await.is_none(), "the second copy is dropped");

    // the next report (new counter) gets through
    a.inject_report(S, SensorType::ContactV2, 0, 8, 60);
    assert!(w.next_event().await.is_some());
}

#[tokio::test]
async fn zero_mac_overheard_report_is_named_by_short_address() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(roam()).await;
    w.connect(&a).await;
    w.connect(&b).await;

    b.inject_overheard(S_SHORT, "\0\0\0\0\0\0\0\0", SensorType::ContactV1, 1, 3, 70);
    let e = w.next_event().await.expect("event");
    assert_eq!(e.mac, S);

    // a sensor no gateway of ours owns is ignored
    b.inject_overheard(0x0999, "\0\0\0\0\0\0\0\0", SensorType::ContactV1, 1, 4, 70);
    b.inject_overheard(0x0998, "7FFFFFFF", SensorType::ContactV2, 1, 4, 70);
    assert!(w.next_event().await.is_none());
}

#[tokio::test]
async fn stronger_gateway_takes_the_sensor_over() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(roam()).await;
    w.connect(&a).await;
    w.connect(&b).await;

    for seq in 1..=6 {
        a.inject_report(S, SensorType::ContactV2, 1, seq, 85); // -85 dBm at the owner
        b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 1, seq, 60); // -60 dBm at B
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    wait_for("handover to B", || b.ows().table.iter().any(|s| s.mac == S)).await;
    assert!(a.ows().table.is_empty(), "dropped at the old owner");
    assert_eq!(a.ows().log, vec![format!("delete {}", S)]);
    assert_eq!(b.ows().log, vec![format!("add {} {:04X}", S, S_SHORT)], "same short address");
    assert_eq!(b.ows().table[0].sensor_type, SensorType::ContactV2.to_u8(), "type kept");
    wait_for("owner recorded", || w.owner(S).as_deref() == Some("GWBBBBBB")).await;

    // B's own reports now name B; A's overheard copies are merged into them
    while w.next_event().await.is_some() {}
    a.inject_overheard(S_SHORT, S, SensorType::ContactV2, 0, 9, 85);
    b.inject_report(S, SensorType::ContactV2, 0, 9, 60);
    let e = w.next_event().await.expect("event");
    assert_eq!(e.dongle_mac.as_deref(), Some("GWBBBBBB"));
    assert!(w.next_event().await.is_none());
}

#[tokio::test]
async fn small_signal_differences_do_not_move_a_sensor() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(roam()).await;
    w.connect(&a).await;
    w.connect(&b).await;
    for seq in 1..=6 {
        a.inject_report(S, SensorType::ContactV2, 1, seq, 70);
        b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 1, seq, 64); // 6 dB better: under the hysteresis
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(a.ows().log.is_empty() && b.ows().log.is_empty());
}

#[tokio::test]
async fn an_owner_cut_off_from_the_host_keeps_its_sensor_until_resends_show_its_radio_is_gone() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(fast()).await;
    w.connect(&a).await;
    w.connect(&b).await;
    a.inject_report(S, SensorType::ContactV2, 1, 1, 60);
    assert!(w.next_event().await.is_some());

    // A's bridge drops; its radio keeps ACKing (a relay buffers the reports).
    // B's overheard copies keep the events coming, and the sensor stays on A.
    w.disconnect(&a).await;
    for seq in 2..=8 {
        b.inject_overheard(S_SHORT, S, SensorType::ContactV2, (seq % 2) as u8, seq, 50);
        let e = w.next_event().await.expect("event through B");
        assert_eq!(e.dongle_mac.as_deref(), Some("GWAAAAAA"), "still A's");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(b.ows().log.is_empty(), "no handover while nothing says A's radio is gone");

    // Up to 3 copies (lost ACKs) are what a healthy stock sensor sends anyway.
    for seq in 9..=13 {
        for _ in 0..3 {
            b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 1, seq, 80);
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(b.ows().log.is_empty(), "no handover for lost ACKs");

    // Now nobody ACKs: the stock sensor's 6-copy bursts, B missing some.
    for (seq, copies) in (14..).zip([6, 2, 5, 6]) {
        for _ in 0..copies {
            b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 0, seq, 80);
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    wait_for("handover to B", || b.ows().table.iter().any(|s| s.mac == S)).await;
    assert!(a.ows().log.is_empty(), "the disconnected owner was not touched");
    while w.next_event().await.is_some() {}

    // A comes back (a new connection) still holding the sensor, and its relay
    // sends on what it buffered: A forgets the sensor, the replays are dropped.
    let a = VirtualDongle::new("GWAAAAAA", "OWS").with_ows(NET, a.ows().table);
    w.connect(&a).await;
    assert_eq!(a.ows().log, vec![format!("delete {}", S)]);
    assert!(b.ows().table.iter().any(|s| s.mac == S));
    assert_eq!(w.owner(S).as_deref(), Some("GWBBBBBB"));
    for seq in 2..=8 {
        a.inject_report(S, SensorType::ContactV2, (seq % 2) as u8, seq, 60);
    }
    assert!(w.next_event().await.is_none(), "stale replays dropped");
    b.inject_report(S, SensorType::ContactV2, 0, 20, 60);
    assert_eq!(w.next_event().await.expect("new event").dongle_mac.as_deref(), Some("GWBBBBBB"));
}

#[tokio::test]
async fn a_connected_owner_whose_radio_misses_reports_loses_the_sensor() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(fast()).await;
    w.connect(&a).await;
    w.connect(&b).await;

    // B overhears reports A never delivers, though A is connected; A delivers
    // one in between, which breaks the streak.
    for seq in [1, 2] {
        b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 1, seq, 70);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    a.inject_report(S, SensorType::ContactV2, 1, 3, 70);
    b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 1, 3, 70);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(a.ows().log.is_empty());

    for seq in 4..=6 {
        b.inject_overheard(S_SHORT, S, SensorType::ContactV2, (seq % 2) as u8, seq, 70);
        let e = w.next_event().await.expect("delivered from B's copy meanwhile");
        assert_eq!(e.dongle_mac.as_deref(), Some("GWAAAAAA"));
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    wait_for("handover to B", || b.ows().table.iter().any(|s| s.mac == S)).await;
    assert_eq!(a.ows().log, vec![format!("delete {}", S)], "deleted at the connected owner first");
}

#[tokio::test]
async fn an_owner_that_lost_power_loses_the_sensor_though_its_connection_looks_alive() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(fast()).await;
    w.connect(&a).await;
    w.connect(&b).await;

    // A loses power; its connection has not timed out yet, so it still looks
    // connected, and the delete there will time out. B hears the sensor's
    // retry bursts (seen on hardware: 2, 6, 4, 4 copies).
    a.ows_power_off();
    for (seq, copies) in (1..).zip([2, 6, 4, 4]) {
        for _ in 0..copies {
            b.inject_overheard(S_SHORT, S, SensorType::ContactV2, (seq % 2) as u8, seq, 40);
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    // Each delete at the unpowered owner waits out its 5 s timeout.
    for _ in 0..1000 {
        if b.ows().table.iter().any(|s| s.mac == S) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(b.ows().table.iter().any(|s| s.mac == S), "handed over despite the unreachable owner");
    assert_eq!(w.owner(S).as_deref(), Some("GWBBBBBB"));
}

#[tokio::test]
async fn pairing_gets_a_range_above_every_short_address_in_use() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![entry("77AAAAAA", 0x4002)]);
    let mut w = World::new(roam()).await;
    w.connect(&a).await;
    w.connect(&b).await;

    w.roaming.prepare_pairing("GWAAAAAA").await.unwrap();
    assert_eq!(a.ows().range, (0x4003, ows::ADDR_LAST));
    assert_eq!(b.ows().range, (ows::ADDR_FIRST, ows::ADDR_LAST), "the other gateway is not changed");

    // pairing stops on the other gateway of the network
    b.register_extra_scan_responses(1, 1);
    let mut eb = w.engines.lock().await.get("GWBBBBBB").cloned().unwrap();
    eb.set_scan(true).await.unwrap();
    w.roaming.prepare_pairing("GWAAAAAA").await.unwrap();
    assert!(!eb.is_scanning());
}

#[tokio::test]
async fn a_report_first_overheard_far_away_carries_the_owners_signal() {
    let (a, b) = gateways(vec![entry(S, S_SHORT)], vec![]);
    let mut w = World::new(roam()).await;
    w.connect(&a).await;
    w.connect(&b).await;
    let rssi = |e: &DongleEvent| match e.data {
        TelemetryData::Alarm { rssi, .. } => rssi,
        _ => panic!("not an alarm: {:?}", e.data),
    };

    // The owner hears it well.
    a.inject_report(S, SensorType::ContactV2, 1, 7, 50);
    let near = rssi(&w.next_event().await.expect("first report"));

    // Next report: the far gateway's copy arrives first.
    b.inject_overheard(S_SHORT, S, SensorType::ContactV2, 0, 8, 90);
    let e = w.next_event().await.expect("second report");
    assert_eq!(e.dongle_mac.as_deref(), Some("GWAAAAAA"));
    assert_eq!(rssi(&e), near, "the owner's signal, not the far copy's");
    a.inject_report(S, SensorType::ContactV2, 0, 8, 50);
    assert!(w.next_event().await.is_none(), "the owner's copy is the duplicate");
}
