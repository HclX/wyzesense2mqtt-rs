//! Sensor updates over the air through an OpenWyzeSense gateway, against the
//! virtual dongle's emulation of the gateway's update messages.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use wyzesense2mqtt_rs::engine::{Engine, EnginesMap};
use wyzesense2mqtt_rs::firmware::{Firmware, SensorJob, SensorState};
use wyzesense2mqtt_rs::local_dongle::LocalDongles;
use wyzesense2mqtt_rs::protocol::ows::{self, NetworkIdentity, NvSensor};
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;
use wyzesense2mqtt_rs::roaming::{Roaming, RoamingConfig};
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;

const GW: &str = "GWAAAAAA";
const S: &str = "777B42DF";
const NET: NetworkIdentity = NetworkIdentity { pan: 0x65FE, short: 0, ext: 0x00124B00E2E0000A };

/// A slot image as tools/slot_image.py makes it: header, then code linked for the slot.
fn slot_image(slot: u32, board: u32, version: u32, code_len: usize) -> Vec<u8> {
    let base = [0x01000u32, 0x0F000][slot as usize];
    let mut code: Vec<u8> = (0..code_len as u32).map(|i| (i * 31 + slot) as u8).collect();
    code[0..4].copy_from_slice(&0x2000_5000u32.to_le_bytes());
    code[4..8].copy_from_slice(&((base + 0x100 + 0x41) | 1).to_le_bytes());
    let mut img = vec![0xFFu8; 0x100];
    img[0..4].copy_from_slice(b"OWSI");
    img[4..8].copy_from_slice(&board.to_le_bytes());
    img[8..12].copy_from_slice(&version.to_le_bytes());
    img[12..16].copy_from_slice(&(code.len() as u32).to_le_bytes());
    img[16..20].copy_from_slice(&crc32fast::hash(&code).to_le_bytes());
    img[20..24].copy_from_slice(&slot.to_le_bytes());
    img.extend_from_slice(&code);
    img
}

fn release_dir(root: &Path, a: &[u8], b: &[u8]) -> PathBuf {
    let dir = root.join("release");
    std::fs::create_dir_all(&dir).unwrap();
    let mut files = Vec::new();
    for (name, img) in [("ows-sensor-contact-v1-slot-a-v0.2.2.img", a), ("ows-sensor-contact-v1-slot-b-v0.2.2.img", b)] {
        std::fs::write(dir.join(name), img).unwrap();
        files.push(serde_json::json!({
            "file": name, "device": "Wyze v1 contact", "what": "sensor", "board": "wyze_contact_v1",
            "product": "sensor-contact-v1", "size": img.len(), "sha256": ows_host::manifest::sha256_hex(img),
        }));
    }
    let manifest = serde_json::json!({ "format": "openwyzesense-release", "version": "v0.2.2", "files": files });
    std::fs::write(dir.join("manifest.json"), manifest.to_string()).unwrap();
    dir
}

struct Ctx {
    firmware: Arc<Firmware>,
    sm: Arc<Mutex<SensorManager>>,
    gw: VirtualDongle,
    image_a: Vec<u8>,
    dir: PathBuf,
    _exit: tokio::sync::oneshot::Sender<()>,
}

impl Drop for Ctx {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn ctx(tag: &str, ours: bool) -> Ctx {
    let id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("w2m_sota_{}_{}", tag, id));
    std::fs::create_dir_all(&dir).unwrap();
    let (a, b) = (slot_image(0, 0x1234_5678, 6, 1000), slot_image(1, 0x1234_5678, 6, 1000));
    let source = release_dir(&dir, &a, &b);

    let (event_tx, event_rx) = mpsc::channel(256);
    std::mem::forget(event_rx);
    let (status_tx, status_rx) = mpsc::channel(64);
    std::mem::forget(status_rx);
    let (broadcast_tx, _) = tokio::sync::broadcast::channel(1024);
    let engines: EnginesMap = Arc::new(tokio::sync::Mutex::new(Default::default()));
    let sm = Arc::new(Mutex::new(SensorManager::new(
        dir.join("sensors.yaml").to_string_lossy().to_string(),
        dir.join("state.yaml").to_string_lossy().to_string(),
    )));
    let roaming = Roaming::new(
        RoamingConfig { probe_timeout: Duration::from_millis(300), ..Default::default() },
        Arc::clone(&engines),
        Arc::clone(&sm),
        broadcast_tx.clone(),
    );

    let gw = if ours {
        VirtualDongle::new(GW, "OWS").with_ows(NET, vec![NvSensor { mac: S.into(), short: 0x0003, sensor_type: 0x01 }])
    } else {
        VirtualDongle::new(GW, "0.0.0.47").with_sensors(vec![S.into()])
    };
    let mut engine = Engine::new(gw.transport(), event_tx.clone());
    let exit = engine.start();
    engine.initialize_handshake().await.unwrap();
    let list = engine.get_sensor_list().await.unwrap();
    sm.lock().unwrap().assign_dongle(GW, &list);
    engines.lock().await.insert(GW.into(), engine);
    roaming.gateway_connected(GW).await;

    let local = LocalDongles::new(Arc::clone(&engines), Arc::clone(&sm), event_tx, status_tx, broadcast_tx.clone());
    let firmware =
        Firmware::new(local, engines, Some(roaming), broadcast_tx, &dir.join("state"), Some(source.to_string_lossy().to_string()));
    Ctx { firmware, sm, gw, image_a: a, dir, _exit: exit }
}

async fn wait(f: &Firmware, what: &str, cond: impl Fn(&SensorJob) -> bool) -> SensorJob {
    for _ in 0..200 {
        if let Some(j) = f.sensor_job() {
            if cond(&j) {
                return j;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {}: {:?}", what, f.sensor_job());
}

#[tokio::test]
async fn a_sensor_takes_its_update_block_by_block() {
    let c = ctx("ok", true).await;
    c.firmware.start_sensor_update(GW, vec![S.into()], "contact-v1").await.unwrap();
    let staged = wait(&c.firmware, "the offer", |j| j.sensors[0].state == SensorState::PressButton).await;
    assert_eq!(staged.target, "v0.2.2");
    assert_eq!(c.gw.ows().ota_offers, vec![S.to_string()]);
    assert_ne!(c.gw.ows().flags & ows::FLAG_OTA, 0, "the update flag is on while it runs");

    // The sensor (button pressed) asks for slot A block by block.
    let mut off = 0u32;
    while (off as usize) < c.image_a.len() {
        let before = c.gw.ows().ota_data.len();
        c.gw.inject_ota_need(S, 0, off, 96);
        for _ in 0..100 {
            if c.gw.ows().ota_data.len() > before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        off += 96;
    }
    let j = wait(&c.firmware, "progress", |j| j.sensors[0].state == SensorState::Updating).await;
    assert!(j.sensors[0].total > 0);
    c.gw.inject_ota_done(S, 0);
    let done = wait(&c.firmware, "the end", |j| !j.active()).await;
    assert_eq!(done.sensors[0].state, SensorState::Done, "{:?}", done);

    // What reached the gateway is slot A, whole.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let sim = c.gw.ows();
    let mut got = vec![0u8; c.image_a.len()];
    for (o, d) in &sim.ota_data {
        let o = *o as usize;
        got[o..o + d.len()].copy_from_slice(d);
    }
    assert_eq!(got, c.image_a);
    // Afterwards: offers withdrawn, the update flag as it was.
    assert!(sim.ota_cancels >= 1);
    assert_eq!(sim.flags & ows::FLAG_OTA, 0);
}

#[tokio::test]
async fn a_sensor_that_turns_it_down_is_reported() {
    let c = ctx("refused", true).await;
    c.firmware.start_sensor_update(GW, vec![S.into()], "contact-v1").await.unwrap();
    wait(&c.firmware, "the offer", |j| j.sensors[0].state == SensorState::PressButton).await;
    c.gw.inject_ota_done(S, 2); // refused: e.g. already on this version
    let done = wait(&c.firmware, "the end", |j| !j.active()).await;
    assert_eq!(done.sensors[0].state, SensorState::Refused);
    assert!(done.sensors[0].message.as_deref().unwrap().contains("already runs this version"));
}

#[tokio::test]
async fn stopping_withdraws_the_offers_and_restores_the_flag() {
    let c = ctx("stop", true).await;
    c.firmware.start_sensor_update(GW, vec![S.into()], "contact-v1").await.unwrap();
    wait(&c.firmware, "the offer", |j| j.sensors[0].state == SensorState::PressButton).await;
    c.firmware.cancel_sensor_update().unwrap();
    let j = wait(&c.firmware, "the stop", |j| !j.active()).await;
    assert_eq!(format!("{:?}", j.step), "Cancelled");
    assert_eq!(j.sensors[0].state, SensorState::Stopped);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let sim = c.gw.ows();
    assert!(sim.ota_offers.is_empty() && sim.ota_cancels >= 1);
    assert_eq!(sim.flags & ows::FLAG_OTA, 0);
}

#[tokio::test]
async fn only_through_our_gateway() {
    let c = ctx("stock", false).await;
    let err = c.firmware.start_sensor_update(GW, vec![S.into()], "contact-v1").await.unwrap_err();
    assert!(err.contains("OpenWyzeSense"), "{}", err);
}

#[tokio::test]
async fn a_sensor_heard_far_better_elsewhere_is_sent_there() {
    let c = ctx("where", true).await;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    {
        let mut sm = c.sm.lock().unwrap();
        let s = sm.get_sensors_mut().get_mut(S).unwrap();
        s.heard.insert("77A85A36".into(), (-48, now));
        s.heard.insert(GW.into(), (-94, now));
    }
    let err = c.firmware.start_sensor_update(GW, vec![S.into()], "contact-v1").await.unwrap_err();
    assert!(err.contains("talks to gateway 77A85A36"), "{}", err);

    // Heard about as well here: allowed.
    c.sm.lock().unwrap().get_sensors_mut().get_mut(S).unwrap().heard.insert(GW.into(), (-52, now));
    c.firmware.start_sensor_update(GW, vec![S.into()], "contact-v1").await.unwrap();
}
