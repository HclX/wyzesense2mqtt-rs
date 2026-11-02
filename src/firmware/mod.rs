//! Installing, updating and removing OpenWyzeSense firmware on local USB
//! dongles, through `ows-host` (the dongle's update mode and bootloader).
//!
//! One job at a time:
//!
//! 1. **Install / update** starts with a backup: the firmware is downloaded
//!    and checked against the release manifest, the dongle is taken from its
//!    session, the whole chip is read (the chip resets after, and the dongle
//!    comes back), and the backup is saved under `backups_dir`. The job then
//!    waits in `review`, showing which sensors carry over and which need
//!    pairing again.
//! 2. **Confirm** flashes the firmware with that backup and converts the
//!    sensor table. The dongle normally comes back by itself; if it doesn't
//!    answer, the job asks for a replug and finishes when it is back.
//! 3. **Restore stock** writes the newest backup taken while the dongle ran
//!    stock firmware back.
//!
//! 4. **Relay update** (a bridged OpenWyzeSense relay, `relay`): the relay's
//!    own update image for its chip is uploaded to it; it restarts, brings its
//!    contact-board radio up to the gateway firmware it carries, and
//!    reconnects.
//!
//! USB flashing is for local dongles only; relays update themselves.

mod relay;
mod sensors;
pub use relay::RelayInfo;
pub use sensors::{SensorJob, SensorRow, SensorState};

use crate::engine::EnginesMap;
use crate::local_dongle::LocalDongles;
use crate::roaming::Roaming;
use ows_host::dongle::{self, Backup, FlashOptions, Phase, Progress};
use ows_host::manifest::Manifest;
use ows_host::nv::{self, Dropped, NvChoice};
use ows_host::HidTransport;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

/// The OpenWyzeSense release this build installs (the `ows-host` tag).
pub const RELEASE_VERSION: &str = "v0.2.0";
/// Where releases are downloaded from; `{version}` is replaced.
pub const DEFAULT_SOURCE: &str = "https://github.com/HclX/openwyzesense/releases/download/{version}";
/// The release file for the USB dongle.
pub const PRODUCT: &str = "gateway-dongle";
/// How long a session gets to let go of its dongle.
const RESERVE_TIMEOUT: Duration = Duration::from_secs(20);
/// How long a replugged dongle is waited for.
const REPLUG_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Opens a dongle's device node for the bootloader.
pub type Opener = Arc<dyn Fn(&Path) -> std::io::Result<Box<dyn HidTransport + Send>> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Install,
    Restore,
    /// A bridged relay's own firmware, over Wi-Fi.
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    /// Downloading and checking the release.
    Download,
    /// Reading the whole chip.
    Backup,
    /// Waiting for the user to confirm (install only).
    Review,
    /// Writing the firmware (install) or the backup (restore).
    Flash,
    /// Written and verified, but the dongle doesn't answer: unplug it and plug it back in.
    Replug,
    /// A relay restarting into its new firmware, until it reconnects.
    Restart,
    Done,
    Failed,
    Cancelled,
}

impl Step {
    /// Whether the job still holds the dongle or waits for the user.
    pub fn active(self) -> bool {
        matches!(self, Step::Download | Step::Backup | Step::Review | Step::Flash | Step::Replug | Step::Restart)
    }
}

/// What carries over to the new firmware.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Carried {
    pub sensors: Vec<String>,
    pub network: bool,
    pub summary: String,
}

/// The current (or last) job, as the dashboard shows it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Job {
    pub id: u64,
    pub kind: JobKind,
    pub dongle: String,
    pub device_path: String,
    pub step: Step,
    /// The bootloader phase while backing up or flashing.
    pub phase: Option<String>,
    pub done: usize,
    pub total: usize,
    pub message: Option<String>,
    /// What the dongle ran when it was backed up: "stock" or "OpenWyzeSense <version>".
    pub current: Option<String>,
    /// The version being installed.
    pub target: Option<String>,
    /// The backup directory's name.
    pub backup: Option<String>,
    pub carried: Option<Carried>,
    pub dropped: Vec<Dropped>,
}

/// A saved backup, as listed for a dongle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupMeta {
    pub dongle: String,
    /// "stock" or "OpenWyzeSense <version>".
    pub firmware: String,
    pub time: String,
    #[serde(default)]
    pub dir: String,
}

/// The release image, downloaded and checked.
#[derive(Clone)]
struct Release {
    version: String,
    image: Arc<Vec<u8>>,
}

pub struct Firmware {
    local: LocalDongles,
    engines: EnginesMap,
    roaming: Option<Arc<Roaming>>,
    broadcast_tx: tokio::sync::broadcast::Sender<()>,
    backups_dir: PathBuf,
    cache_dir: PathBuf,
    source: String,
    opener: Opener,
    job: Arc<Mutex<Option<Job>>>,
    /// Sent as the relay's password when the dashboard gives none (a relay
    /// takes its bridge token too).
    bridge_token: Option<String>,
    /// The relays' web port (80; tests change it).
    relay_port: u16,
    /// Relay status by address, fetched at most every few seconds.
    relay_cache: Mutex<std::collections::HashMap<String, (std::time::Instant, Option<RelayInfo>)>>,
    /// The release fetched for the job in review.
    release: Mutex<Option<Release>>,
    next_id: Mutex<u64>,
    /// The current (or last) sensor update.
    sensor_job: Arc<Mutex<Option<SensorJob>>>,
    /// Set to stop the running sensor update.
    sensor_cancel: Arc<tokio::sync::Notify>,
}

fn default_opener() -> Opener {
    Arc::new(|p: &Path| {
        ows_host::hidraw::Hidraw::open(p).map(|h| Box::new(h) as Box<dyn HidTransport + Send>)
    })
}

fn phase_name(p: Phase) -> &'static str {
    match p {
        Phase::Enter => "enter",
        Phase::Backup => "backup",
        Phase::Check => "check",
        Phase::Erase => "erase",
        Phase::Write => "write",
        Phase::Verify => "verify",
        Phase::Nv => "nv",
        Phase::Reset => "reset",
        Phase::Done => "done",
    }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// "stock", or "OpenWyzeSense <version>" when the image carries our build info.
fn firmware_of(flash: &[u8]) -> String {
    match ows_host::image::build_info(&flash[..ows_host::image::APP_END.min(flash.len())]) {
        Some(bi) => format!("OpenWyzeSense {}", bi.version),
        None => "stock".to_string(),
    }
}

impl Firmware {
    pub fn new(
        local: LocalDongles,
        engines: EnginesMap,
        roaming: Option<Arc<Roaming>>,
        broadcast_tx: tokio::sync::broadcast::Sender<()>,
        state_dir: &Path,
        source: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            local,
            engines,
            roaming,
            broadcast_tx,
            backups_dir: state_dir.join("backups"),
            cache_dir: state_dir.join("firmware"),
            source: source.unwrap_or_else(|| DEFAULT_SOURCE.to_string()),
            opener: default_opener(),
            job: Arc::new(Mutex::new(None)),
            bridge_token: None,
            relay_port: 80,
            relay_cache: Mutex::new(Default::default()),
            release: Mutex::new(None),
            next_id: Mutex::new(1),
            sensor_job: Arc::new(Mutex::new(None)),
            sensor_cancel: Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// For tests: open dongles some other way.
    pub fn with_opener(self: Arc<Self>, opener: Opener) -> Arc<Self> {
        let mut me = Arc::try_unwrap(self).ok().expect("with_opener before the firmware is shared");
        me.opener = opener;
        Arc::new(me)
    }

    /// The bridge token, tried as a relay's password.
    pub fn with_bridge_token(self: Arc<Self>, token: Option<String>) -> Arc<Self> {
        let mut me = Arc::try_unwrap(self).ok().expect("with_bridge_token before the firmware is shared");
        me.bridge_token = token;
        Arc::new(me)
    }

    /// For tests: relays' web port.
    pub fn with_relay_port(self: Arc<Self>, port: u16) -> Arc<Self> {
        let mut me = Arc::try_unwrap(self).ok().expect("with_relay_port before the firmware is shared");
        me.relay_port = port;
        Arc::new(me)
    }

    pub fn job(&self) -> Option<Job> {
        self.job.lock().unwrap().clone()
    }

    fn update(&self, f: impl FnOnce(&mut Job)) {
        if let Some(job) = self.job.lock().unwrap().as_mut() {
            f(job);
        }
        let _ = self.broadcast_tx.send(());
    }

    fn fail(&self, message: String) {
        error!("Firmware job failed: {}", message);
        self.update(|j| {
            j.step = Step::Failed;
            j.message = Some(message);
        });
    }

    /// Progress from the bootloader, throttled to whole percents and phase changes.
    fn progress_sink(job: Arc<Mutex<Option<Job>>>, tx: tokio::sync::broadcast::Sender<()>) -> impl FnMut(Progress) {
        let mut last = (String::new(), usize::MAX);
        move |p: Progress| {
            let name = phase_name(p.phase).to_string();
            let pct = if p.total > 0 { p.done * 100 / p.total } else { 0 };
            if let Some(j) = job.lock().unwrap().as_mut() {
                j.phase = Some(name.clone());
                j.done = p.done;
                j.total = p.total;
            }
            if (name.clone(), pct) != last {
                last = (name, pct);
                let _ = tx.send(());
            }
        }
    }

    /// The dashboard's view: the release, the job, and each local dongle's firmware and backups.
    pub async fn status(&self) -> serde_json::Value {
        let (dongles, bridged): (Vec<(String, Option<String>)>, Vec<(String, Option<String>)>) = {
            let map = self.engines.lock().await;
            let local = map
                .iter()
                .filter(|(_, e)| e.transport_label == "local")
                .map(|(mac, e)| (mac.clone(), e.device_path.clone()))
                .collect();
            let bridged = map
                .iter()
                .filter(|(_, e)| e.transport_label == "bridge")
                .map(|(mac, e)| (mac.clone(), e.remote_addr.clone()))
                .collect();
            (local, bridged)
        };
        let mut out = serde_json::Map::new();
        for (mac, addr) in bridged {
            if let Some(info) = self.relay_info(addr.as_deref()).await {
                out.insert(mac, serde_json::json!({ "relay": info }));
            }
        }
        for (mac, path) in dongles {
            let firmware = match self.roaming.as_ref().map(|r| r.gateway_json(&mac)) {
                Some(Some(g)) => format!("OpenWyzeSense {}", g["version"].as_str().unwrap_or("")).trim().to_string(),
                Some(None) => "stock".to_string(),
                None => "unknown".to_string(),
            };
            let backups = self.backups(&mac);
            out.insert(
                mac,
                serde_json::json!({
                    "device_path": path,
                    "firmware": firmware,
                    "backups": backups,
                    "can_restore_stock": backups.iter().any(|b| b.firmware == "stock"),
                }),
            );
        }
        // Sensor updates: gateways running OpenWyzeSense that can update sensors,
        // and their sensors of a board our sensor firmware runs on.
        let gateways: Vec<String> = self.engines.lock().await.keys().cloned().collect();
        for mac in gateways {
            let supported = self
                .roaming
                .as_ref()
                .and_then(|r| r.gateway_json(&mac))
                .and_then(|g| g["flags_supported"].as_str().and_then(|h| u32::from_str_radix(h, 16).ok()));
            let Some(supported) = supported else { continue };
            let sensors: Vec<serde_json::Value> = {
                let manager = self.local.sensor_manager.lock().unwrap();
                let mut list: Vec<_> = manager
                    .get_sensors()
                    .values()
                    .filter(|s| s.dongle_mac.as_deref() == Some(mac.as_str()))
                    .filter_map(|s| sensors::board_of(s.sensor_type).map(|b| serde_json::json!({ "mac": s.mac, "board": b })))
                    .collect();
                list.sort_by(|a, b| a["mac"].as_str().cmp(&b["mac"].as_str()));
                list
            };
            let entry = out.entry(mac).or_insert_with(|| serde_json::json!({}));
            entry["ota"] = serde_json::json!({
                "supported": supported & ows_host::host::FLAG_OTA != 0,
                "sensors": sensors,
            });
        }
        serde_json::json!({
            "release": RELEASE_VERSION,
            "job": self.job(),
            "sensor_job": self.sensor_job(),
            "dongles": out,
        })
    }

    /// Backups saved for a dongle, newest first.
    pub fn backups(&self, mac: &str) -> Vec<BackupMeta> {
        let mut list: Vec<(u64, BackupMeta)> = std::fs::read_dir(&self.backups_dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                let stamp: u64 = name.strip_prefix(&format!("{}-", mac))?.parse().ok()?;
                let text = std::fs::read_to_string(e.path().join("w2m.json")).ok()?;
                let mut meta: BackupMeta = serde_json::from_str(&text).ok()?;
                meta.dir = name;
                Some((stamp, meta))
            })
            .collect();
        list.sort_by(|a, b| b.0.cmp(&a.0));
        list.into_iter().map(|(_, m)| m).collect()
    }

    /// Starts a job on a local dongle, if none is running.
    async fn begin(&self, mac: &str, kind: JobKind) -> Result<Job, String> {
        let path = {
            let map = self.engines.lock().await;
            let engine = map.get(mac).ok_or_else(|| format!("dongle {} is not connected", mac))?;
            match (kind, engine.transport_label.as_str()) {
                (JobKind::Relay, "bridge") => engine.remote_addr.clone().ok_or("the relay's address is unknown")?,
                (JobKind::Relay, _) => return Err("only a bridged relay updates over Wi-Fi".into()),
                (_, "local") => engine.device_path.clone().ok_or("the dongle has no device path")?,
                _ => {
                    return Err("only local USB dongles can be flashed here; a relay is updated with its own firmware".into())
                }
            }
        };
        if let Some(j) = self.sensor_job().filter(|j| j.active()) {
            return Err(format!("a sensor update is running on gateway {}", j.gateway));
        }
        let mut slot = self.job.lock().unwrap();
        if let Some(j) = slot.as_ref().filter(|j| j.step.active()) {
            return Err(format!("a firmware job is already running on dongle {}", j.dongle));
        }
        let id = {
            let mut n = self.next_id.lock().unwrap();
            *n += 1;
            *n
        };
        let job = Job {
            id,
            kind,
            dongle: mac.to_string(),
            device_path: path,
            step: if kind == JobKind::Restore { Step::Flash } else { Step::Download },
            phase: None,
            done: 0,
            total: 0,
            message: None,
            current: None,
            target: None,
            backup: None,
            carried: None,
            dropped: Vec::new(),
        };
        *slot = Some(job.clone());
        drop(slot);
        let _ = self.broadcast_tx.send(());
        Ok(job)
    }

    /// Install or update: download, back up, then wait for [`Firmware::confirm`].
    pub async fn start_install(self: &Arc<Self>, mac: &str) -> Result<Job, String> {
        let job = self.begin(mac, JobKind::Install).await?;
        let me = Arc::clone(self);
        let task = job.clone();
        tokio::spawn(async move {
            if let Err(e) = me.prepare(&task).await {
                me.fail(e);
            }
        });
        Ok(job)
    }

    async fn prepare(&self, job: &Job) -> Result<(), String> {
        let release = self.fetch_release().await?;
        self.update(|j| j.target = Some(release.version.clone()));
        *self.release.lock().unwrap() = Some(release);

        self.update(|j| j.step = Step::Backup);
        let reservation = self.local.reserve(&job.device_path, RESERVE_TIMEOUT).await?;
        let opener = Arc::clone(&self.opener);
        let path = PathBuf::from(&job.device_path);
        let mut progress = Self::progress_sink(Arc::clone(&self.job), self.broadcast_tx.clone());
        let backup = tokio::task::spawn_blocking(move || -> Result<Backup, String> {
            let mut t = opener(&path).map_err(|e| format!("open {}: {}", path.display(), e))?;
            dongle::backup(&mut *t, &mut progress).map_err(|e| explain(&e, "backup"))
        })
        .await
        .map_err(|e| e.to_string())?;
        // The backup ends with a reset: the dongle comes back by itself.
        drop(reservation);
        let backup = backup?;

        let current = firmware_of(&backup.flash);
        let dir_name = format!("{}-{}", job.dongle, unix_now());
        let dir = self.backups_dir.join(&dir_name);
        backup.save(&dir).map_err(|e| format!("saving the backup: {}", e))?;
        let meta = BackupMeta {
            dongle: job.dongle.clone(),
            firmware: current.clone(),
            time: backup.info["time"].as_str().unwrap_or("").to_string(),
            dir: String::new(),
        };
        std::fs::write(dir.join("w2m.json"), serde_json::to_string_pretty(&meta).unwrap() + "\n")
            .map_err(|e| format!("saving the backup: {}", e))?;
        info!("Backed up dongle {} ({}) to {}", job.dongle, current, dir.display());

        let doc = nv::export(backup.nv_page());
        self.update(|j| {
            j.step = Step::Review;
            j.phase = None;
            j.current = Some(current);
            j.backup = Some(dir_name);
            j.carried = Some(Carried {
                sensors: doc.sensors.iter().map(|s| s.mac.clone()).collect(),
                network: doc.network.is_some(),
                summary: doc.summary(),
            });
            j.dropped = doc.dropped.clone();
        });
        Ok(())
    }

    /// Flashes the firmware for the job in review.
    pub async fn confirm(self: &Arc<Self>) -> Result<Job, String> {
        let job = {
            let mut slot = self.job.lock().unwrap();
            let job = slot.as_mut().filter(|j| j.step == Step::Review).ok_or("no firmware job is waiting for confirmation")?;
            job.step = Step::Flash;
            job.clone()
        };
        let _ = self.broadcast_tx.send(());
        let release = self.release.lock().unwrap().take();
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let res = match release {
                Some(r) => me.write(&job, Some(r)).await,
                None => Err("the downloaded firmware is gone; start again".to_string()),
            };
            if let Err(e) = res {
                me.fail(e);
            }
        });
        Ok(self.job().unwrap())
    }

    /// Drops the job in review (the backup stays).
    pub fn cancel(&self) -> Result<(), String> {
        let mut slot = self.job.lock().unwrap();
        let job = slot.as_mut().filter(|j| j.step == Step::Review).ok_or("no firmware job is waiting for confirmation")?;
        job.step = Step::Cancelled;
        drop(slot);
        self.release.lock().unwrap().take();
        let _ = self.broadcast_tx.send(());
        Ok(())
    }

    /// Restore stock firmware from the newest stock backup of this dongle.
    pub async fn start_restore(self: &Arc<Self>, mac: &str) -> Result<Job, String> {
        let stock = self
            .backups(mac)
            .into_iter()
            .find(|b| b.firmware == "stock")
            .ok_or_else(|| format!("no backup of dongle {} running stock firmware", mac))?;
        let job = self.begin(mac, JobKind::Restore).await?;
        self.update(|j| {
            j.backup = Some(stock.dir.clone());
            j.target = Some("stock".into());
        });
        let job = self.job().unwrap_or(job);
        let me = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = me.write(&job, None).await {
                me.fail(e);
            }
        });
        Ok(self.job().unwrap())
    }

    /// Writes the release (install) or the job's backup (restore), then checks the dongle came back.
    async fn write(&self, job: &Job, release: Option<Release>) -> Result<(), String> {
        let dir = self.backups_dir.join(job.backup.as_deref().ok_or("the job has no backup")?);
        let backup = Backup::load(&dir).map_err(|e| format!("backup {}: {}", dir.display(), e))?;

        // The node may have changed since the backup (a replug).
        let path = {
            let map = self.engines.lock().await;
            map.get(&job.dongle).and_then(|e| e.device_path.clone()).unwrap_or_else(|| job.device_path.clone())
        };
        self.update(|j| j.device_path = path.clone());
        let reservation = self.local.reserve(&path, RESERVE_TIMEOUT).await?;
        let opener = Arc::clone(&self.opener);
        let node = PathBuf::from(&path);
        let mut progress = Self::progress_sink(Arc::clone(&self.job), self.broadcast_tx.clone());
        let outcome = tokio::task::spawn_blocking(move || -> Result<(dongle::FlashReport, bool), String> {
            let mut t = opener(&node).map_err(|e| format!("open {}: {}", node.display(), e))?;
            let report = match &release {
                Some(r) => dongle::flash(&mut *t, &r.image, &backup, &FlashOptions { fast: false, nv: NvChoice::Migrate }, &mut progress),
                None => dongle::restore(&mut *t, &backup, false, &mut progress),
            }
            .map_err(|e| e.to_string())?;
            drop(t);
            // A fresh handle, as after a replug, tells whether it is back in normal mode.
            std::thread::sleep(Duration::from_millis(500));
            let back = match opener(&node) {
                Ok(mut t) => dongle::inquiry(&mut *t).unwrap_or(false),
                Err(_) => false,
            };
            Ok((report, back))
        })
        .await
        .map_err(|e| e.to_string())?;
        drop(reservation);
        let (report, back) = outcome.map_err(|e| {
            format!("{} (the dongle may still be in update mode: try again, or unplug it and plug it back in)", e)
        })?;

        if let Some(doc) = &report.nv_doc {
            let _ = std::fs::write(dir.join("nv.json"), doc.to_json());
            let _ = std::fs::write(dir.join("nv_before.bin"), &report.nv_before);
        }
        info!(
            "Dongle {}: {} written in {:.1} s{}",
            job.dongle,
            job.target.as_deref().unwrap_or("firmware"),
            report.seconds,
            if back { "" } else { "; it needs a replug" }
        );
        let dropped = report.nv_doc.as_ref().map(|d| d.dropped.clone());
        self.update(|j| {
            j.phase = None;
            if let Some(d) = dropped {
                j.dropped = d;
            }
            if back {
                j.step = Step::Done;
            } else {
                j.step = Step::Replug;
                j.message = Some("Unplug the dongle and plug it back in.".into());
            }
        });
        if !back {
            self.await_replug(job.dongle.clone());
        }
        Ok(())
    }

    /// Finishes a job in `replug` once the dongle is attached again.
    fn await_replug(&self, mac: String) {
        let engines = Arc::clone(&self.engines);
        let job = Arc::clone(&self.job);
        let tx = self.broadcast_tx.clone();
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + REPLUG_TIMEOUT;
            // The old session's engine may still be listed for a moment.
            tokio::time::sleep(Duration::from_secs(3)).await;
            while tokio::time::Instant::now() < deadline {
                if engines.lock().await.contains_key(&mac) {
                    if let Some(j) = job.lock().unwrap().as_mut().filter(|j| j.dongle == mac && j.step == Step::Replug) {
                        j.step = Step::Done;
                        j.message = None;
                    }
                    let _ = tx.send(());
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            warn!("Dongle {} did not come back after its firmware job", mac);
        });
    }

    /// The dongle image of [`RELEASE_VERSION`], from the cache, a local
    /// directory, or the release page; checked against its manifest.
    async fn fetch_release(&self) -> Result<Release, String> {
        let (version, file, image) = self.fetch_file(PRODUCT).await?;
        let (_, bi) = ows_host::image::check_product(&image, PRODUCT).map_err(|e| e.to_string())?;
        info!("OpenWyzeSense release {} ready: {} ({})", version, file, bi.version);
        Ok(Release { version, image: Arc::new(image) })
    }

    /// One file of [`RELEASE_VERSION`] by product: (release version, file name, bytes), checked against the manifest.
    async fn fetch_file(&self, product: &str) -> Result<(String, String, Vec<u8>), String> {
        self.fetch_matching(product, |f| f.product.as_deref() == Some(product)).await
    }

    /// The first file of [`RELEASE_VERSION`] that `pick` accepts (`what` names it in errors).
    async fn fetch_matching(
        &self,
        what: &str,
        pick: impl Fn(&ows_host::manifest::ManifestFile) -> bool,
    ) -> Result<(String, String, Vec<u8>), String> {
        let base = self.source.replace("{version}", RELEASE_VERSION);
        let cache = self.cache_dir.join(RELEASE_VERSION);
        let remote = base.starts_with("http://") || base.starts_with("https://");

        let read = |dir: &Path, name: &str| std::fs::read(dir.join(name));
        let manifest_text = match read(&cache, "manifest.json") {
            Ok(b) if remote => String::from_utf8_lossy(&b).to_string(),
            _ if remote => {
                let text = download(&format!("{}/manifest.json", base)).await?;
                String::from_utf8_lossy(&text).to_string()
            }
            _ => String::from_utf8_lossy(&read(Path::new(&base), "manifest.json").map_err(|e| format!("{}/manifest.json: {}", base, e))?)
                .to_string(),
        };
        let manifest = Manifest::from_json(&manifest_text).map_err(|e| e.to_string())?;
        let file = manifest
            .files
            .iter()
            .find(|f| pick(f))
            .ok_or_else(|| format!("release {} has no {} image", manifest.version, what))?
            .clone();
        let image = match read(&cache, &file.file) {
            Ok(b) if remote && file.verify(&b).is_ok() => b,
            _ if remote => download(&format!("{}/{}", base, file.file)).await?,
            _ => read(Path::new(&base), &file.file).map_err(|e| format!("{}/{}: {}", base, file.file, e))?,
        };
        file.verify(&image).map_err(|e| e.to_string())?;
        if remote {
            let _ = std::fs::create_dir_all(&cache);
            let _ = std::fs::write(cache.join("manifest.json"), &manifest_text);
            let _ = std::fs::write(cache.join(&file.file), &image);
        }
        Ok((manifest.version, file.file, image))
    }
}

/// A bootloader error in words a user can act on.
fn explain(e: &ows_host::Error, step: &str) -> String {
    let text = e.to_string();
    if text.contains("no bootloader") {
        return "this dongle's bootloader can't be reached (it is turned off in the chip's configuration), \
                so it can't be flashed over USB. Nothing was changed, and the dongle keeps working as before."
            .to_string();
    }
    format!("{}: {}", step, text)
}

async fn download(url: &str) -> Result<Vec<u8>, String> {
    info!("Downloading {}", url);
    let resp = reqwest::Client::new()
        .get(url)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .map_err(|e| format!("{}: {}", url, e))?;
    if !resp.status().is_success() {
        return Err(format!("{}: HTTP {}", url, resp.status()));
    }
    resp.bytes().await.map(|b| b.to_vec()).map_err(|e| format!("{}: {}", url, e))
}
