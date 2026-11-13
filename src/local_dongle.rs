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
use crate::protocol::telemetry::{DongleEvent, TelemetryData};
use crate::transport::hidraw::{discover_all_dongle_devices, HidrawTransport};
use crate::transport::GatewayTransport;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

/// How often the supervisor looks for dongles.
pub const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// How long a node whose handshake failed is left alone (unless replugged).
pub const RETRY_AFTER: Duration = Duration::from_secs(60);

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
    /// Dongle MACs attached at least once since start-up.
    seen: Arc<Mutex<HashSet<String>>>,
}

/// Why a session ended.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionEnd {
    /// The dongle answered the handshake, ran, and then went away.
    Disconnected { mac: String },
    /// It never got through the handshake.
    HandshakeFailed(String),
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
            seen: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Runs one dongle from handshake to disconnect.
    pub async fn run_session(&self, device_path: &str, transport: GatewayTransport) -> SessionEnd {
        let mut engine = Engine::new(transport, self.event_tx.clone());
        engine.transport_label = "local".to_string();
        engine.device_path = Some(device_path.to_string());
        engine.attach_peer_context(&self.engines, &self.sensor_manager);
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
            Ok(mut sensors) => {
                crate::engine::purge_sensors_claimed_by_online_dongles(
                    &self.engines,
                    &self.sensor_manager,
                    &mac,
                    &mut engine,
                    &mut sensors,
                )
                .await;
                self.load_sensors(&mac, &sensors);
            }
            Err(e) => warn!("Failed to warm up sensors cache for dongle {}: {}", mac, e),
        }

        let disconnect_notify = Arc::clone(&engine.disconnect_notify);
        self.engines.lock().await.insert(mac.clone(), engine);
        let _ = self.dongle_status_tx.try_send(DongleStatus { mac: mac.clone(), online: true, version });
        let _ = self.broadcast_tx.send(());

        disconnect_notify.notified().await;
        warn!("Local USB dongle {} ({}) disconnected", mac, device_path);
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
            self.sensor_manager.lock().unwrap().unassign_dongle(&mac);
            let _ = self.dongle_status_tx.try_send(DongleStatus { mac: mac.clone(), online: false, version: None });
            let _ = self.broadcast_tx.send(());
        } else {
            info!("Dongle {} already re-registered by a newer session", mac);
        }
        SessionEnd::Disconnected { mac }
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
        let mut warned_none = false;

        loop {
            let present = source.present();
            if present.is_empty() && !warned_none {
                warn!("No Wyze Sense USB dongle found ({:?}); waiting for one to be plugged in", source);
            }
            warned_none = present.is_empty();

            // A node that went away and came back gets a fresh try.
            failed.lock().unwrap().retain(|p, at| present.contains(p) && at.elapsed() < retry_after);

            for path in present {
                if failed.lock().unwrap().contains_key(&path) || !attached.lock().unwrap().insert(path.clone()) {
                    continue;
                }
                info!("USB dongle found on {}", path);
                let me = self.clone();
                let attached = Arc::clone(&attached);
                let failed = Arc::clone(&failed);
                tokio::spawn(async move {
                    let end = match HidrawTransport::open(&path).await {
                        Ok(t) => me.run_session(&path, GatewayTransport::Hidraw(t)).await,
                        Err(e) => SessionEnd::HandshakeFailed(format!("open failed: {}", e)),
                    };
                    if let SessionEnd::HandshakeFailed(reason) = &end {
                        error!(
                            "USB dongle on {} did not answer: {}. Retrying when it is replugged or in {} s",
                            path, reason, retry_after.as_secs()
                        );
                        failed.lock().unwrap().insert(path.clone(), Instant::now());
                    }
                    debug!("Session on {} ended: {:?}", path, end);
                    attached.lock().unwrap().remove(&path);
                });
            }
            tokio::time::sleep(poll).await;
        }
    }
}
