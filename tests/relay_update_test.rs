//! Updating a bridged relay's own firmware, against a fake relay web server.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::mpsc;
use wyzesense2mqtt_rs::engine::{Engine, EnginesMap};
use wyzesense2mqtt_rs::firmware::{Firmware, Job, JobKind, Step};
use wyzesense2mqtt_rs::local_dongle::LocalDongles;
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;

const RELAY: &str = "7788CCF6";
const IMAGE_FILE: &str = "ows-relay-esp32-ota-v0.2.2.bin";

#[derive(Default)]
struct FakeRelay {
    build: String,
    password: Option<String>,
    /// What /ota received (the whole multipart body).
    uploads: Vec<Vec<u8>>,
    /// The build it restarts into after an upload.
    next_build: String,
    /// When it last started.
    started: Option<std::time::Instant>,
}

type Shared = Arc<Mutex<FakeRelay>>;

async fn status(State(r): State<Shared>) -> Json<serde_json::Value> {
    let r = r.lock().unwrap();
    Json(serde_json::json!({
        "name": "OWS-relay-9A78", "chip": "ESP32", "build": r.build, "auth": r.password.is_some(),
        "uptime": r.started.map_or(3600, |t| t.elapsed().as_secs()),
    }))
}

async fn ota(State(r): State<Shared>, headers: HeaderMap, body: Bytes) -> (StatusCode, &'static str) {
    let mut r = r.lock().unwrap();
    if let Some(p) = &r.password {
        let want = format!("Bearer {}", p);
        if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(want.as_str()) {
            return (StatusCode::UNAUTHORIZED, "authentication required");
        }
    }
    r.uploads.push(body.to_vec());
    r.build = r.next_build.clone();
    r.started = Some(std::time::Instant::now());
    (StatusCode::OK, "new firmware written; restarting")
}

struct Ctx {
    firmware: Arc<Firmware>,
    relay: Shared,
    dir: PathBuf,
    image: Vec<u8>,
}

impl Drop for Ctx {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn release_dir(root: &Path, image: &[u8]) -> PathBuf {
    let dir = root.join("release");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(IMAGE_FILE), image).unwrap();
    let manifest = serde_json::json!({
        "format": "openwyzesense-release", "version": "v0.2.2",
        "files": [{
            "file": IMAGE_FILE, "device": "classic ESP32 (LOLIN32)", "what": "relay: update",
            "product": "relay-esp32-ota", "size": image.len(),
            "sha256": ows_host::manifest::sha256_hex(image),
        }],
    });
    std::fs::write(dir.join("manifest.json"), manifest.to_string()).unwrap();
    dir
}

async fn ctx(tag: &str, password: Option<&str>, token: Option<&str>, next_build: &str) -> Ctx {
    let id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("w2m_relay_{}_{}", tag, id));
    std::fs::create_dir_all(&dir).unwrap();
    let mut image: Vec<u8> = (0..20_000u32).map(|i| (i * 13 + 5) as u8).collect();
    image.extend_from_slice(b"OWS-RELAY-VERSION:v0.2.2\0");
    let source = release_dir(&dir, &image);

    // The fake relay's web server.
    let relay: Shared = Arc::new(Mutex::new(FakeRelay {
        build: "c642c0c-dirty (ota_1)".into(),
        password: password.map(str::to_string),
        next_build: next_build.into(),
        ..Default::default()
    }));
    let app = Router::new().route("/api/status", get(status)).route("/ota", post(ota)).with_state(Arc::clone(&relay));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // The relay's bridge connection.
    let (event_tx, event_rx) = mpsc::channel(128);
    std::mem::forget(event_rx);
    let (status_tx, status_rx) = mpsc::channel(64);
    std::mem::forget(status_rx);
    let (broadcast_tx, _) = tokio::sync::broadcast::channel(256);
    let engines: EnginesMap = Arc::new(tokio::sync::Mutex::new(Default::default()));
    let v = VirtualDongle::new(RELAY, "0.0.0.47 V1.8 Gateway GW3U");
    let mut engine = Engine::new(v.transport(), event_tx.clone());
    engine.transport_label = "bridge".into();
    engine.remote_addr = Some("127.0.0.1:50123".into());
    std::mem::forget(v);
    engines.lock().await.insert(RELAY.into(), engine);

    let sm = Arc::new(Mutex::new(SensorManager::new(
        dir.join("sensors.yaml").to_string_lossy().to_string(),
        dir.join("state.yaml").to_string_lossy().to_string(),
    )));
    let local = LocalDongles::new(Arc::clone(&engines), sm, event_tx, status_tx, broadcast_tx.clone());
    let firmware = Firmware::new(local, engines, None, broadcast_tx, &dir.join("state"), Some(source.to_string_lossy().to_string()))
        .with_bridge_token(token.map(str::to_string))
        .with_relay_port(port);
    Ctx { firmware, relay, dir, image }
}

async fn wait_end(f: &Firmware) -> Job {
    for _ in 0..400 {
        if let Some(j) = f.job() {
            if matches!(j.step, Step::Done | Step::Failed) {
                return j;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the relay job never finished: {:?}", f.job());
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

#[tokio::test]
async fn a_relay_takes_its_own_firmware_and_comes_back_on_it() {
    let c = ctx("ok", None, None, "v0.2.2 (ota_0)").await;
    let status = c.firmware.status().await;
    assert_eq!(status["dongles"][RELAY]["relay"]["chip"], "ESP32");
    assert_eq!(status["dongles"][RELAY]["relay"]["product"], "relay-esp32-ota");

    let job = c.firmware.start_relay_update(RELAY, None).await.unwrap();
    assert_eq!(job.kind, JobKind::Relay);
    let done = wait_end(&c.firmware).await;
    assert_eq!(done.step, Step::Done, "{:?}", done.message);
    assert_eq!(done.current.as_deref(), Some("ESP32 v0.2.2 (ota_0)"));
    let uploads = &c.relay.lock().unwrap().uploads;
    assert_eq!(uploads.len(), 1);
    assert!(contains(&uploads[0], b"name=\"image\""), "a multipart field named image");
    assert!(contains(&uploads[0], &c.image), "the release image, whole");
}

#[tokio::test]
async fn a_relay_with_a_password_needs_it() {
    let c = ctx("auth", Some("s3cret"), None, "v0.2.2 (ota_0)").await;
    let err = c.firmware.start_relay_update(RELAY, None).await.unwrap_err();
    assert!(err.contains("password"), "{}", err);

    c.firmware.start_relay_update(RELAY, Some("wrong".into())).await.unwrap();
    let failed = wait_end(&c.firmware).await;
    assert_eq!(failed.step, Step::Failed);
    assert!(failed.message.unwrap().contains("refused the password"));

    c.firmware.start_relay_update(RELAY, Some("s3cret".into())).await.unwrap();
    assert_eq!(wait_end(&c.firmware).await.step, Step::Done);
}

#[tokio::test]
async fn the_bridge_token_serves_as_the_password() {
    let c = ctx("token", Some("bridge-token"), Some("bridge-token"), "v0.2.2 (ota_0)").await;
    c.firmware.start_relay_update(RELAY, None).await.unwrap();
    assert_eq!(wait_end(&c.firmware).await.step, Step::Done);
}

#[tokio::test]
async fn a_usb_flash_is_refused_for_a_relay() {
    let c = ctx("guard", None, None, "v0.2.2 (ota_0)").await;
    assert!(c.firmware.start_install(RELAY).await.unwrap_err().contains("relay"));
}

#[tokio::test]
async fn a_build_without_a_version_counts_by_its_new_slot() {
    let c = ctx("unknown", None, None, "unknown (ota_0)").await;
    c.firmware.start_relay_update(RELAY, None).await.unwrap();
    let done = wait_end(&c.firmware).await;
    assert_eq!(done.step, Step::Done, "{:?}", done.message);
    assert_eq!(done.current.as_deref(), Some("ESP32 unknown (ota_0)"));
}

#[tokio::test]
async fn a_rollback_to_the_old_build_fails_at_once() {
    let c = ctx("rollback", None, None, "c642c0c-dirty (ota_1)").await;
    c.firmware.start_relay_update(RELAY, None).await.unwrap();
    let failed = wait_end(&c.firmware).await;
    assert_eq!(failed.step, Step::Failed);
    assert!(failed.message.unwrap().contains("rolled back"));
}
