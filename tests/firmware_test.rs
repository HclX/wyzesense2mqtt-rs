//! OpenWyzeSense firmware install, update and restore on a local USB dongle,
//! against `ows-host`'s simulated dongle.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ows_host::sim::{self, SimDongle};
use ows_host::HidTransport;
use tokio::sync::mpsc;
use wyzesense2mqtt_rs::engine::EnginesMap;
use wyzesense2mqtt_rs::firmware::{Firmware, Job, JobKind, Opener, Step};
use wyzesense2mqtt_rs::local_dongle::LocalDongles;
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;

const DONGLE: &str = "77A85A36";
const NODE: &str = "/dev/hidraw-sim";

/// The simulated chip behind every handle the firmware opens.
struct Shared(Arc<Mutex<SimDongle>>);

impl HidTransport for Shared {
    fn write(&mut self, report: &[u8]) -> std::io::Result<()> {
        self.0.lock().unwrap().write(report)
    }
    fn read(&mut self, buf: &mut [u8], timeout: Duration) -> std::io::Result<usize> {
        self.0.lock().unwrap().read(buf, timeout)
    }
}

struct Ctx {
    firmware: Arc<Firmware>,
    local: LocalDongles,
    engines: EnginesMap,
    chip: Arc<Mutex<SimDongle>>,
    dir: PathBuf,
}

impl Drop for Ctx {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn tmp_dir(tag: &str) -> PathBuf {
    let id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("w2m_fw_{}_{}", tag, id));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A release directory: manifest.json and the dongle image.
fn release_dir(root: &Path, image: &[u8], tamper: bool) -> PathBuf {
    let dir = root.join("release");
    std::fs::create_dir_all(&dir).unwrap();
    let file = "ows-gateway-dongle-v0.2.3.bin";
    std::fs::write(dir.join(file), image).unwrap();
    let mut sha = ows_host::manifest::sha256_hex(image);
    if tamper {
        sha = sha.replace(&sha[..1], if &sha[..1] == "0" { "1" } else { "0" });
    }
    let manifest = serde_json::json!({
        "format": "openwyzesense-release",
        "version": "v0.2.3",
        "files": [{
            "file": file, "device": "Wyze Sense USB dongle", "what": "gateway firmware",
            "board": "wyze_dongle", "product": "gateway-dongle", "size": image.len(), "sha256": sha,
        }],
    });
    std::fs::write(dir.join("manifest.json"), manifest.to_string()).unwrap();
    dir
}

async fn ctx(tag: &str, chip: SimDongle, tamper: bool) -> Ctx {
    let dir = tmp_dir(tag);
    let (event_tx, _event_rx) = mpsc::channel(128);
    std::mem::forget(_event_rx);
    let (status_tx, status_rx) = mpsc::channel(64);
    std::mem::forget(status_rx);
    let (broadcast_tx, _) = tokio::sync::broadcast::channel(256);
    let engines: EnginesMap = Arc::new(tokio::sync::Mutex::new(Default::default()));
    let sm = Arc::new(Mutex::new(SensorManager::new(
        dir.join("sensors.yaml").to_string_lossy().to_string(),
        dir.join("state.yaml").to_string_lossy().to_string(),
    )));
    let local = LocalDongles::new(Arc::clone(&engines), sm, event_tx, status_tx, broadcast_tx.clone());
    let source = release_dir(&dir, &sim::gateway_image("v0.2.3"), tamper);
    let chip = Arc::new(Mutex::new(chip));
    let shared = Arc::clone(&chip);
    let opener: Opener = Arc::new(move |_p: &Path| Ok(Box::new(Shared(Arc::clone(&shared))) as Box<dyn HidTransport + Send>));
    let firmware = Firmware::new(
        local.clone(),
        Arc::clone(&engines),
        None,
        broadcast_tx,
        &dir.join("state"),
        Some(source.to_string_lossy().to_string()),
    )
    .with_opener(opener);
    let c = Ctx { firmware, local, engines, chip, dir };
    attach(&c).await;
    c
}

/// The dongle's normal-mode session (a virtual dongle stands in for the protocol).
async fn attach(c: &Ctx) {
    let local = c.local.clone();
    let v = VirtualDongle::new(DONGLE, "0.0.0.47 V1.8 Gateway GW3U").with_sensors(vec!["77BD514D".to_string()]);
    tokio::spawn(async move {
        let _keep = v.mac().to_string();
        local.run_session(NODE, v.transport()).await
    });
    for _ in 0..100 {
        if c.engines.lock().await.contains_key(DONGLE) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the dongle never attached");
}

async fn wait_step(f: &Firmware, want: &[Step]) -> Job {
    for _ in 0..600 {
        if let Some(j) = f.job() {
            if want.contains(&j.step) {
                return j;
            }
            assert!(
                !(j.step == Step::Failed && !want.contains(&Step::Failed)),
                "job failed: {:?}",
                j.message
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("job never reached {:?}: {:?}", want, f.job());
}

#[tokio::test]
async fn install_on_a_stock_dongle_then_restore_stock() {
    let c = ctx("install", SimDongle::stock(), false).await;
    let stock_flash = c.chip.lock().unwrap().flash.clone();

    // Install: download, backup, review.
    let job = c.firmware.start_install(DONGLE).await.unwrap();
    assert_eq!(job.kind, JobKind::Install);
    let review = wait_step(&c.firmware, &[Step::Review]).await;
    assert_eq!(review.current.as_deref(), Some("stock"));
    assert_eq!(review.target.as_deref(), Some("v0.2.3"));
    let carried = review.carried.clone().unwrap();
    assert_eq!(carried.sensors.len(), 2);
    assert!(carried.network);
    let repair: Vec<_> = review.dropped.iter().filter_map(|d| d.mac.clone()).collect();
    assert_eq!(repair.len(), 1, "one sensor shares a short address: {:?}", review.dropped);
    // Nothing written yet; the backup is saved.
    assert!(!c.chip.lock().unwrap().runs_ows());
    assert_eq!(c.chip.lock().unwrap().flash, stock_flash);
    let backups = c.firmware.backups(DONGLE);
    assert_eq!(backups.len(), 1);
    assert_eq!(backups[0].firmware, "stock");
    // The dongle was taken from its session for the backup.
    assert!(!c.engines.lock().await.contains_key(DONGLE));

    // Confirm: flash and migrate.
    attach(&c).await;
    c.firmware.confirm().await.unwrap();
    let done = wait_step(&c.firmware, &[Step::Done]).await;
    assert_eq!(done.dropped.iter().filter(|d| d.mac.is_some()).count(), 1);
    {
        let chip = c.chip.lock().unwrap();
        assert_eq!(chip.ows_version().as_deref(), Some("v0.2.3"));
        assert!(ows_host::nv::is_ows(chip.nv_page()));
        assert_eq!(chip.flash[0x1F000..], stock_flash[0x1F000..], "page 31 untouched");
    }
    let backup_dir = c.dir.join("state/backups").join(done.backup.as_deref().unwrap());
    assert!(backup_dir.join("nv.json").exists());
    assert!(backup_dir.join("nv_before.bin").exists());

    // Restore stock from that backup.
    attach(&c).await;
    c.firmware.start_restore(DONGLE).await.unwrap();
    let restored = wait_step(&c.firmware, &[Step::Done]).await;
    assert_eq!(restored.kind, JobKind::Restore);
    assert_eq!(c.chip.lock().unwrap().flash, stock_flash, "stock again, sensor table included");
}

#[tokio::test]
async fn update_an_ows_dongle_keeps_its_sensors() {
    let c = ctx("update", SimDongle::ows("v0.0.9"), false).await;
    c.firmware.start_install(DONGLE).await.unwrap();
    let review = wait_step(&c.firmware, &[Step::Review]).await;
    assert_eq!(review.current.as_deref(), Some("OpenWyzeSense v0.0.9"));
    assert_eq!(review.carried.as_ref().unwrap().sensors.len(), 2);
    assert!(review.dropped.iter().all(|d| d.mac.is_none()), "nothing to re-pair: {:?}", review.dropped);

    c.firmware.confirm().await.unwrap();
    wait_step(&c.firmware, &[Step::Done]).await;
    assert_eq!(c.chip.lock().unwrap().ows_version().as_deref(), Some("v0.2.3"));

    // Its only backup ran our firmware: no stock to restore.
    attach(&c).await;
    let err = c.firmware.start_restore(DONGLE).await.unwrap_err();
    assert!(err.contains("stock"), "{}", err);
}

#[tokio::test]
async fn a_dongle_that_stays_silent_asks_for_a_replug() {
    let mut chip = SimDongle::stock();
    chip.needs_replug = true;
    let c = ctx("replug", chip, false).await;
    c.firmware.start_install(DONGLE).await.unwrap();
    wait_step(&c.firmware, &[Step::Review]).await;
    // After the backup's reset the simulated chip stays silent until replugged.
    c.chip.lock().unwrap().replug();
    c.firmware.confirm().await.unwrap();
    let job = wait_step(&c.firmware, &[Step::Replug]).await;
    assert!(job.message.unwrap().contains("plug"));
    assert!(c.chip.lock().unwrap().runs_ows());

    // Replugged: the dongle attaches again and the job finishes.
    c.chip.lock().unwrap().replug();
    attach(&c).await;
    wait_step(&c.firmware, &[Step::Done]).await;
}

#[tokio::test]
async fn a_write_that_does_not_verify_fails_without_a_reset() {
    let c = ctx("verify", SimDongle::stock(), false).await;
    c.firmware.start_install(DONGLE).await.unwrap();
    wait_step(&c.firmware, &[Step::Review]).await;
    let resets = c.chip.lock().unwrap().resets;
    c.chip.lock().unwrap().corrupt_at = Some(0x2000);
    c.firmware.confirm().await.unwrap();
    let job = wait_step(&c.firmware, &[Step::Failed]).await;
    assert!(job.message.unwrap().contains("verify"));
    assert_eq!(c.chip.lock().unwrap().resets, resets, "no reset after a failed verify");
    // Another job may start: the failed one no longer holds the dongle.
    assert!(!c.local.is_reserved(NODE));
}

#[tokio::test]
async fn a_tampered_release_is_refused_before_touching_the_dongle() {
    let c = ctx("tamper", SimDongle::stock(), true).await;
    c.firmware.start_install(DONGLE).await.unwrap();
    let job = wait_step(&c.firmware, &[Step::Failed]).await;
    assert!(job.message.unwrap().to_lowercase().contains("sha"), "{:?}", c.firmware.job());
    assert!(c.chip.lock().unwrap().log.is_empty(), "nothing sent to the dongle");
    assert!(c.engines.lock().await.contains_key(DONGLE), "the dongle stayed attached");
}

#[tokio::test]
async fn one_job_at_a_time_and_only_known_dongles() {
    let c = ctx("guards", SimDongle::stock(), false).await;
    assert!(c.firmware.start_install("77000000").await.unwrap_err().contains("not connected"));
    c.firmware.start_install(DONGLE).await.unwrap();
    wait_step(&c.firmware, &[Step::Review]).await;
    attach(&c).await;
    assert!(c.firmware.start_install(DONGLE).await.unwrap_err().contains("already running"));
    c.firmware.cancel().unwrap();
    assert_eq!(c.firmware.job().unwrap().step, Step::Cancelled);
    assert!(c.firmware.confirm().await.is_err());
    // A new job may start after a cancel.
    c.firmware.start_install(DONGLE).await.unwrap();
}
