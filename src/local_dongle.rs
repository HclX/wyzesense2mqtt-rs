//! Local USB dongles: attach each one as it appears, detach it when it goes.
//!
//! A supervisor polls for dongle device nodes (`/dev/hidrawN` with the Wyze
//! VID:PID, or the one configured path). Each new node gets a session: open,
//! handshake, register the engine, then wait for the transport to fail (the
//! dongle was unplugged) and clean up. A dongle plugged back in, or plugged in
//! after start-up, is picked up on the next poll.
//!
//! A node whose handshake fails (e.g. a dongle left in its firmware update
//! mode) is retried when it reappears, or after `retry_after`.

use crate::engine::{Engine, EnginesMap, HANDSHAKE_TIMEOUT};
use crate::gateway::mqtt::DongleStatus;
use crate::protocol::sensor::SensorManager;
use crate::roaming::Roaming;
use crate::protocol::telemetry::{DongleEvent, TelemetryData};
use crate::transport::hidraw::{discover_all_dongle_devices, HidrawTransport};
use crate::transport::GatewayTransport;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Notify};
use tracing::{debug, error, info, warn};

/// How often the supervisor looks for dongles.
pub const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// How long a node whose handshake failed is left alone (unless replugged).
pub const RETRY_AFTER: Duration = Duration::from_secs(60);

/// How a USB session tells that the dongle's radio chip hung. Its USB chip
/// keeps answering the sync commands and the device stays plugged in, so
/// only the radio chip's own silence shows it (OpenWyzeSense #85).
#[derive(Debug, Clone, Copy)]
pub struct RadioWatch {
    /// How often the session checks.
    pub check_every: Duration,
    /// Silence before a check sends a probe (Get Version).
    pub probe_after: Duration,
    /// How long one probe waits for its answer.
    pub probe_timeout: Duration,
    /// Silence, with every probe in it unanswered, after which the radio
    /// chip counts as hung.
    pub hung_after: Duration,
    /// The first wait between resets of a dongle that keeps hanging; it
    /// doubles up to `max_backoff`, and starts over after a quiet hour.
    pub backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RadioWatch {
    fn default() -> Self {
        Self {
            check_every: Duration::from_secs(10),
            probe_after: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(5),
            hung_after: Duration::from_secs(60),
            backoff: Duration::from_secs(2 * 60),
            max_backoff: Duration::from_secs(30 * 60),
        }
    }
}

/// Resets a dongle on a device path; true if it worked.
pub type ResetFn = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Where local dongles come from.
#[derive(Debug, Clone, PartialEq)]
pub enum DongleSource {
    /// Every node with the Wyze VID:PID (`usb.dongle: auto`).
    Auto,
    /// One configured device path.
    Path(String),
}

impl DongleSource {
    fn present(&self) -> Vec<String> {
        match self {
            DongleSource::Auto => discover_all_dongle_devices().unwrap_or_default(),
            DongleSource::Path(p) if std::path::Path::new(p).exists() => vec![p.clone()],
            DongleSource::Path(_) => Vec::new(),
        }
    }
}

/// What a local dongle session needs from the daemon.
#[derive(Clone)]
pub struct LocalDongles {
    pub engines: EnginesMap,
    pub sensor_manager: Arc<Mutex<SensorManager>>,
    pub event_tx: mpsc::Sender<DongleEvent>,
    pub dongle_status_tx: mpsc::Sender<DongleStatus>,
    pub broadcast_tx: broadcast::Sender<()>,
    /// OpenWyzeSense detection, feature flags and roaming, told about each
    /// dongle that comes and goes.
    pub roaming: Option<Arc<Roaming>>,
    /// Dongle MACs attached at least once since start-up.
    seen: Arc<Mutex<HashSet<String>>>,
    /// Device nodes with a session running, and how to stop each.
    sessions: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    /// Device nodes taken for maintenance (e.g. flashing): no session starts on them.
    reserved: Arc<Mutex<HashSet<String>>>,
    radio_watch: RadioWatch,
    reset: ResetFn,
    /// Dongles that answered as OpenWyzeSense in some session: still watched
    /// if they hang before answering again.
    ows_dongles: Arc<Mutex<HashSet<String>>>,
}

/// A device node taken away from its session; given back when dropped.
pub struct Reservation {
    reserved: Arc<Mutex<HashSet<String>>>,
    path: String,
}

impl Reservation {
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.reserved.lock().unwrap().remove(&self.path);
        info!("{} given back to the dongle supervisor", self.path);
    }
}

/// Why a session ended.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionEnd {
    /// The dongle answered the handshake, ran, and then went away.
    Disconnected { mac: String },
    /// It never got through the handshake.
    HandshakeFailed(String),
    /// Still plugged in, but its radio chip stopped answering.
    Hung { mac: String },
}

impl LocalDongles {
    pub fn new(
        engines: EnginesMap,
        sensor_manager: Arc<Mutex<SensorManager>>,
        event_tx: mpsc::Sender<DongleEvent>,
        dongle_status_tx: mpsc::Sender<DongleStatus>,
        broadcast_tx: broadcast::Sender<()>,
    ) -> Self {
        Self {
            engines,
            sensor_manager,
            event_tx,
            dongle_status_tx,
            broadcast_tx,
            roaming: None,
            seen: Arc::new(Mutex::new(HashSet::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            reserved: Arc::new(Mutex::new(HashSet::new())),
            radio_watch: RadioWatch::default(),
            reset: Arc::new(leave_update_mode),
            ows_dongles: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn with_radio_watch(mut self, watch: RadioWatch) -> Self {
        self.radio_watch = watch;
        self
    }

    /// How a hung dongle is reset (tests stand in for the bootloader).
    pub fn with_reset(mut self, reset: ResetFn) -> Self {
        self.reset = reset;
        self
    }

    /// Stops the session on `path` (if any) and keeps new ones off it until
    /// the returned reservation is dropped. Fails if the session doesn't end
    /// within `timeout`.
    pub async fn reserve(&self, path: &str, timeout: Duration) -> Result<Reservation, String> {
        if !self.reserved.lock().unwrap().insert(path.to_string()) {
            return Err(format!("{} is already in use for maintenance", path));
        }
        let reservation = Reservation { reserved: Arc::clone(&self.reserved), path: path.to_string() };
        if let Some(stop) = self.sessions.lock().unwrap().get(path) {
            stop.notify_one();
        }
        let deadline = Instant::now() + timeout;
        while self.sessions.lock().unwrap().contains_key(path) {
            if Instant::now() > deadline {
                return Err(format!("the session on {} did not stop", path));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        info!("{} taken from the dongle supervisor", path);
        Ok(reservation)
    }

    pub fn is_reserved(&self, path: &str) -> bool {
        self.reserved.lock().unwrap().contains(path)
    }

    pub fn with_roaming(mut self, roaming: Option<Arc<Roaming>>) -> Self {
        self.roaming = roaming;
        self
    }

    /// Runs one dongle from handshake to disconnect.
    pub async fn run_session(&self, device_path: &str, transport: GatewayTransport) -> SessionEnd {
        let stop = Arc::new(Notify::new());
        self.sessions.lock().unwrap().insert(device_path.to_string(), Arc::clone(&stop));
        let end = self.session(device_path, transport, &stop).await;
        self.sessions.lock().unwrap().remove(device_path);
        end
    }

    async fn session(&self, device_path: &str, transport: GatewayTransport, stop: &Notify) -> SessionEnd {
        let mut engine = Engine::new(transport, self.event_tx.clone());
        engine.transport_label = "local".to_string();
        engine.device_path = Some(device_path.to_string());
        // Held for the life of the session; dropping it stops the engine worker.
        let exit_tx = engine.start();

        match tokio::time::timeout(HANDSHAKE_TIMEOUT, engine.initialize_handshake()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return SessionEnd::HandshakeFailed(e.to_string()),
            Err(_) => return SessionEnd::HandshakeFailed("timed out".to_string()),
        }
        info!("Dongle on {} successfully unlocked and authenticated!", device_path);
        engine.set_auto_verify(true);

        let mac = engine.dongle_mac().unwrap_or("unknown").to_string();
        let version = engine.dongle_version().map(|s| s.to_string());
        info!("Registering dongle {} ({}) in engines registry", mac, device_path);

        info!("Warming up paired sensors cache from NVRAM for dongle {}...", mac);
        match engine.get_sensor_list().await {
            Ok(sensors) => self.load_sensors(&mac, &sensors),
            Err(e) => warn!("Failed to warm up sensors cache for dongle {}: {}", mac, e),
        }

        let disconnect_notify = Arc::clone(&engine.disconnect_notify);
        let watched = engine.clone();
        self.engines.lock().await.insert(mac.clone(), engine);
        if let Some(r) = &self.roaming {
            r.gateway_connected(&mac).await;
        }
        let _ = self.dongle_status_tx.try_send(DongleStatus { mac: mac.clone(), online: true, version });
        let _ = self.broadcast_tx.send(());

        let mut hung = false;
        tokio::select! {
            _ = disconnect_notify.notified() => {
                warn!("Local USB dongle {} ({}) disconnected", mac, device_path);
            }
            _ = stop.notified() => {
                info!("Local USB dongle {} ({}) detached for maintenance", mac, device_path);
            }
            quiet = watch_radio(watched, self.radio_watch, Arc::clone(&self.ows_dongles)) => {
                hung = true;
                warn!(
                    "Local USB dongle {} ({}) is plugged in, but its radio chip hasn't answered for {} s: \
                     its firmware hung. Detaching it to reset it",
                    mac, device_path, quiet.as_secs()
                );
            }
        }
        drop(exit_tx);

        // The same dongle may already be back on another node: leave its new
        // session alone.
        let still_ours = {
            let mut map = self.engines.lock().await;
            let ours = map.get(&mac).is_some_and(|e| Arc::ptr_eq(&e.disconnect_notify, &disconnect_notify));
            if ours {
                map.remove(&mac);
            }
            ours
        };
        if still_ours {
            if let Some(r) = &self.roaming {
                r.gateway_disconnected(&mac);
            }
            self.sensor_manager.lock().unwrap().unassign_dongle(&mac);
            let _ = self.dongle_status_tx.try_send(DongleStatus { mac: mac.clone(), online: false, version: None });
            let _ = self.broadcast_tx.send(());
        } else {
            info!("Dongle {} already re-registered by a newer session", mac);
        }
        if hung {
            SessionEnd::Hung { mac }
        } else {
            SessionEnd::Disconnected { mac }
        }
    }

    /// Merges the dongle's sensor list into the sensor manager and triggers
    /// MQTT discovery for its sensors.
    fn load_sensors(&self, mac: &str, sensors: &[String]) {
        let first_time = self.seen.lock().unwrap().insert(mac.to_string());
        let mut manager = self.sensor_manager.lock().unwrap();
        // The first time: from the files on disk, as at start-up. After a
        // replug, the sensors already known keep their live state.
        let to_load: Vec<String> = if first_time {
            sensors.to_vec()
        } else {
            sensors.iter().filter(|s| !manager.get_sensors().contains_key(*s)).cloned().collect()
        };
        if !to_load.is_empty() {
            if let Err(e) = manager.load_sensors(&to_load) {
                error!("Failed to load/bootstrap sensors for dongle {}: {}", mac, e);
            }
        }
        manager.assign_dongle(mac, sensors);
        info!("Sensors memory cache for dongle {} successfully warmed up ({} sensors)", mac, sensors.len());

        for sensor_mac in sensors {
            if let Some(sensor) = manager.get_sensors().get(sensor_mac) {
                let _ = self.event_tx.try_send(DongleEvent {
                    mac: sensor.mac.clone(),
                    timestamp: std::time::SystemTime::now(),
                    sensor_type: sensor.sensor_type,
                    event_type: 0xFF,
                    data: TelemetryData::UnknownEvent(Vec::new()),
                    dongle_mac: None,
                    overheard_short: None,
                });
            }
        }
    }

    /// Attaches dongles as they appear, forever.
    pub async fn supervise(self, source: DongleSource, poll: Duration, retry_after: Duration) {
        // Nodes with a session running.
        let attached: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        // Nodes whose handshake failed, and when.
        let failed: Arc<Mutex<HashMap<String, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
        // Nodes already reset out of update mode once since they appeared.
        let reset_tried: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        // Hung dongles reset: when last, and how many times in a row.
        let hang_resets: Arc<Mutex<HashMap<String, (Instant, u32)>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut warned_none = false;

        loop {
            let present = source.present();
            if present.is_empty() && !warned_none {
                warn!("No Wyze Sense USB dongle found ({:?}); waiting for one to be plugged in", source);
            }
            warned_none = present.is_empty();

            // A node that went away and came back gets a fresh try.
            failed.lock().unwrap().retain(|p, at| present.contains(p) && at.elapsed() < retry_after);
            reset_tried.lock().unwrap().retain(|p| present.contains(p));

            for path in present {
                if self.is_reserved(&path)
                    || failed.lock().unwrap().contains_key(&path)
                    || !attached.lock().unwrap().insert(path.clone()) {
                    continue;
                }
                info!("USB dongle found on {}", path);
                let me = self.clone();
                let attached = Arc::clone(&attached);
                let failed = Arc::clone(&failed);
                let reset_tried = Arc::clone(&reset_tried);
                let hang_resets = Arc::clone(&hang_resets);
                tokio::spawn(async move {
                    let end = match HidrawTransport::open(&path).await {
                        Ok(t) => me.run_session(&path, GatewayTransport::Hidraw(t)).await,
                        Err(e) => SessionEnd::HandshakeFailed(format!("open failed: {}", e)),
                    };
                    if let SessionEnd::HandshakeFailed(reason) = &end {
                        // A firmware job cut short (the daemon stopped mid-backup,
                        // say) leaves the dongle in its update mode, where it
                        // half-answers. Its bootloader's reset brings it back.
                        if !reason.starts_with("open failed")
                            && !me.is_reserved(&path)
                            && reset_tried.lock().unwrap().insert(path.clone())
                        {
                            let node = path.clone();
                            let reset_fn = Arc::clone(&me.reset);
                            let reset = tokio::task::spawn_blocking(move || reset_fn(&node)).await.unwrap_or(false);
                            if reset {
                                warn!("USB dongle on {} was left in its update mode; reset it, trying again", path);
                                attached.lock().unwrap().remove(&path);
                                return;
                            }
                        }
                        error!(
                            "USB dongle on {} did not answer: {}. Retrying when it is replugged or in {} s",
                            path, reason, retry_after.as_secs()
                        );
                        failed.lock().unwrap().insert(path.clone(), Instant::now());
                    }
                    if let SessionEnd::Hung { mac } = &end {
                        me.reset_hung(&path, mac, &hang_resets, &failed, retry_after).await;
                    }
                    if matches!(end, SessionEnd::Disconnected { .. } | SessionEnd::Hung { .. }) {
                        reset_tried.lock().unwrap().remove(&path);
                    }
                    debug!("Session on {} ended: {:?}", path, end);
                    attached.lock().unwrap().remove(&path);
                });
            }
            tokio::time::sleep(poll).await;
        }
    }
}

impl LocalDongles {
    /// Resets a dongle whose radio chip hung, through its bootloader, unless
    /// it was reset too recently (the wait doubles while it keeps hanging).
    async fn reset_hung(
        &self,
        path: &str,
        mac: &str,
        resets: &Mutex<HashMap<String, (Instant, u32)>>,
        failed: &Mutex<HashMap<String, Instant>>,
        retry_after: Duration,
    ) {
        if self.is_reserved(path) {
            return; // a firmware job has it
        }
        let w = self.radio_watch;
        let count = {
            let mut map = resets.lock().unwrap();
            let entry = map.entry(path.to_string()).or_insert((Instant::now(), 0));
            // A dongle that ran for a quiet hour starts over.
            if entry.1 > 0 && entry.0.elapsed() > Duration::from_secs(3600) + w.max_backoff {
                entry.1 = 0;
            }
            let wait = w.backoff.saturating_mul(1 << entry.1.saturating_sub(1).min(16)).min(w.max_backoff);
            if entry.1 > 0 && entry.0.elapsed() < wait {
                let left = wait - entry.0.elapsed();
                warn!(
                    "USB dongle {} on {} hung again {} s after its last reset; the next reset in {} s",
                    mac, path, entry.0.elapsed().as_secs(), left.as_secs()
                );
                // Kept detached until then; the supervisor attaches it after that.
                let until = (Instant::now() + left).checked_sub(retry_after).unwrap_or_else(Instant::now);
                failed.lock().unwrap().insert(path.to_string(), until);
                return;
            }
            *entry = (Instant::now(), entry.1 + 1);
            entry.1
        };
        let node = path.to_string();
        let reset_fn = Arc::clone(&self.reset);
        let ok = tokio::task::spawn_blocking(move || reset_fn(&node)).await.unwrap_or(false);
        if ok {
            warn!("Reset USB dongle {} on {} through its bootloader (reset {} in a row); attaching it again", mac, path, count);
        } else {
            error!("Could not reset USB dongle {} on {} through its bootloader: unplug it and plug it back in", mac, path);
        }
    }
}

/// Returns once the dongle's radio chip has been silent for `w.hung_after`,
/// with every probe in that time unanswered: how long it was silent. Never
/// probes while a sensor update uses the gateway.
///
/// Only OpenWyzeSense gateways are watched: they are the ones seen to hang
/// (#85), and stock firmware blinks its LED on every Get Version. Stock
/// firmware doesn't answer the 0x53F0 flags query, which tells them apart.
async fn watch_radio(mut engine: Engine, w: RadioWatch, known_ows: Arc<Mutex<HashSet<String>>>) -> Duration {
    let mac = engine.dongle_mac().unwrap_or_default().to_string();
    let mut ows = known_ows.lock().unwrap().contains(&mac);
    for _ in 0..3 {
        if ows {
            break;
        }
        tokio::time::sleep(w.check_every).await;
        if let Ok(Some(_)) = engine.ows_probe(w.probe_timeout.max(Duration::from_secs(1))).await {
            ows = true;
            known_ows.lock().unwrap().insert(mac.clone());
        }
    }
    if !ows {
        debug!("Not watching {:?} for hangs: stock firmware", engine.dongle_mac());
        return std::future::pending().await;
    }
    loop {
        tokio::time::sleep(w.check_every).await;
        if engine.ota_active() || engine.radio_quiet_for() < w.probe_after {
            continue;
        }
        if engine.probe_radio(w.probe_timeout).await {
            continue;
        }
        let quiet = engine.radio_quiet_for();
        debug!("Radio chip of {:?} silent for {} s", engine.dongle_mac(), quiet.as_secs());
        if quiet >= w.hung_after {
            return quiet;
        }
    }
}

/// If the dongle on `path` is in (or can enter) its update mode, resets it
/// through the bootloader so that it comes back in normal mode. False if its
/// bootloader doesn't answer.
fn leave_update_mode(path: &str) -> bool {
    let Ok(mut t) = ows_host::hidraw::Hidraw::open(std::path::Path::new(path)) else { return false };
    match ows_host::sbl::Sbl::enter(&mut t) {
        Ok(mut s) => s.reset().is_ok(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn a_dongle_that_keeps_hanging_is_reset_less_and_less_often() {
        let (event_tx, _event_rx) = mpsc::channel(8);
        let (status_tx, _status_rx) = mpsc::channel(8);
        let (broadcast_tx, _) = broadcast::channel(8);
        let resets = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&resets);
        let local = LocalDongles::new(
            Arc::new(tokio::sync::Mutex::new(Default::default())),
            Arc::new(Mutex::new(SensorManager::new("/nonexistent/s.yaml".into(), "/nonexistent/t.yaml".into()))),
            event_tx,
            status_tx,
            broadcast_tx,
        )
        .with_radio_watch(RadioWatch {
            backoff: Duration::from_millis(200),
            max_backoff: Duration::from_millis(400),
            ..RadioWatch::default()
        })
        .with_reset(Arc::new(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            true
        }));
        let history = Mutex::new(HashMap::new());
        let failed = Mutex::new(HashMap::new());
        let retry = Duration::from_millis(50);
        let hang = || local.reset_hung("/dev/hidraw1", "778B5F63", &history, &failed, retry);

        hang().await;
        assert_eq!(resets.load(Ordering::SeqCst), 1, "the first hang is reset at once");
        hang().await;
        assert_eq!(resets.load(Ordering::SeqCst), 1, "hung again right away: wait first");
        assert!(failed.lock().unwrap().contains_key("/dev/hidraw1"), "kept detached meanwhile");

        tokio::time::sleep(Duration::from_millis(220)).await;
        hang().await;
        assert_eq!(resets.load(Ordering::SeqCst), 2, "reset once the first wait is over");
        // The wait doubled (400 ms): 220 ms later it is still too soon.
        tokio::time::sleep(Duration::from_millis(220)).await;
        hang().await;
        assert_eq!(resets.load(Ordering::SeqCst), 2);
        tokio::time::sleep(Duration::from_millis(220)).await;
        hang().await;
        assert_eq!(resets.load(Ordering::SeqCst), 3);

        // A reserved dongle (a firmware job) is never reset.
        let _job = local.reserve("/dev/hidraw2", Duration::from_secs(1)).await.unwrap();
        local.reset_hung("/dev/hidraw2", "77A85A36", &history, &failed, retry).await;
        assert_eq!(resets.load(Ordering::SeqCst), 3);
    }
}
