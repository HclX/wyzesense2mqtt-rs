//! Updating sensors that run OpenWyzeSense firmware, over the air, through
//! an OpenWyzeSense gateway (`ows-host`'s `ota`).
//!
//! The gateway's update flag goes on for the run (and back as it was after),
//! the release's slot A and slot B images for the sensors' board are offered
//! to the chosen sensors, and each sensor takes its update within 5 minutes
//! of a short press of its button (under a second: a long press pairs). The
//! gateway asks for blocks and w2m sends them; one sensor at a time receives,
//! up to 8 wait with an offer. Stock sensors never take an offer.

use super::{Firmware, RELEASE_VERSION};
use crate::protocol::packet::Packet;
use ows_host::host;
use ows_host::ota::{self, Event, Ota, Outcome, Targets};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// How long the update waits while nothing advances (the sensors' button window is 5 minutes).
const IDLE_TIMEOUT: Duration = Duration::from_secs(360);
const OWS_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SensorState {
    /// Waiting for an offer slot (more than 8 sensors).
    Queued,
    /// Offered: press the sensor's button briefly.
    PressButton,
    /// Receiving its update.
    Updating,
    Done,
    /// It turned the update down (a different board, already on this version, or a weak battery).
    Refused,
    Failed,
    /// Its button wasn't pressed in time, or it doesn't run OpenWyzeSense.
    TimedOut,
    /// The update was stopped before it finished.
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SensorRow {
    pub mac: String,
    pub state: SensorState,
    pub done: usize,
    pub total: usize,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SensorStep {
    Download,
    Running,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SensorJob {
    pub id: u64,
    pub gateway: String,
    /// "contact-v1" or "motion-v1".
    pub board: String,
    pub target: String,
    pub step: SensorStep,
    pub message: Option<String>,
    pub sensors: Vec<SensorRow>,
}

impl SensorJob {
    pub fn active(&self) -> bool {
        matches!(self.step, SensorStep::Download | SensorStep::Running)
    }

    fn row(&mut self, mac: &[u8; 8]) -> Option<&mut SensorRow> {
        let mac = String::from_utf8_lossy(mac).to_string();
        self.sensors.iter_mut().find(|r| r.mac == mac)
    }
}

/// The boards our sensor firmware runs on, by the sensor type they report.
pub fn board_of(sensor_type: crate::protocol::telemetry::SensorType) -> Option<&'static str> {
    use crate::protocol::telemetry::SensorType;
    match sensor_type {
        SensorType::ContactV1 => Some("contact-v1"),
        SensorType::MotionV1 => Some("motion-v1"),
        _ => None,
    }
}

/// How long a gateway's reading of a sensor counts.
const HEARD_FOR: Duration = Duration::from_secs(3600);
/// How much better another gateway must hear a sensor before the update is
/// refused here (the sensor talks to the one that hears it best).
const BETTER_DB: i16 = 10;

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// How strongly `gateway` heard the sensor within the last hour.
pub fn recent(s: &crate::protocol::sensor::WyzeSensor, gateway: &str) -> Option<i8> {
    let (rssi, at) = s.heard.get(gateway)?;
    (now_secs().saturating_sub(*at) <= HEARD_FOR.as_secs()).then_some(*rssi)
}

/// The gateway that heard the sensor best within the last hour.
pub fn best_gateway(s: &crate::protocol::sensor::WyzeSensor) -> Option<(String, i8)> {
    s.heard
        .keys()
        .filter_map(|g| recent(s, g).map(|r| (g.clone(), r)))
        .max_by_key(|(_, r)| *r)
}

fn mac_bytes(mac: &str) -> Result<[u8; 8], String> {
    let up = mac.to_ascii_uppercase();
    up.as_bytes().try_into().map_err(|_| format!("{} is not a sensor MAC", mac))
}

impl Firmware {
    pub fn sensor_job(&self) -> Option<SensorJob> {
        self.sensor_job.lock().unwrap().clone()
    }

    fn update_sensors(&self, f: impl FnOnce(&mut SensorJob)) {
        if let Some(j) = self.sensor_job.lock().unwrap().as_mut() {
            f(j);
        }
        let _ = self.broadcast_tx.send(());
    }

    /// Updates these sensors (all of one board) through `gateway`.
    pub async fn start_sensor_update(self: &Arc<Self>, gateway: &str, macs: Vec<String>, board: &str) -> Result<SensorJob, String> {
        if !matches!(board, "contact-v1" | "motion-v1") {
            return Err(format!("no sensor firmware for board {}", board));
        }
        let targets: Vec<[u8; 8]> = macs.iter().map(|m| mac_bytes(m)).collect::<Result<_, _>>()?;
        if targets.is_empty() {
            return Err("choose at least one sensor".into());
        }
        let engine = self
            .engines
            .lock()
            .await
            .get(gateway)
            .cloned()
            .ok_or_else(|| format!("gateway {} is not connected", gateway))?;
        let ours = self.roaming.as_ref().and_then(|r| r.gateway_json(gateway));
        let supported = ours
            .as_ref()
            .and_then(|g| g["flags_supported"].as_str().and_then(|h| u32::from_str_radix(h, 16).ok()))
            .ok_or("sensors update only through a gateway running OpenWyzeSense")?;
        if supported & host::FLAG_OTA == 0 {
            return Err("this gateway's firmware can't update sensors; update the gateway first".into());
        }
        if let Some(j) = self.job().filter(|j| j.step.active()) {
            return Err(format!("a firmware job is running on dongle {}", j.dongle));
        }
        // The sensor fetches its update from the gateway it talks to: the one
        // that hears it best. Offered elsewhere, it would never see the offer.
        {
            let manager = self.local.sensor_manager.lock().unwrap();
            for mac in &macs {
                let Some(s) = manager.get_sensors().get(&mac.to_ascii_uppercase()) else { continue };
                let (Some((best, best_rssi)), here) = (best_gateway(s), recent(s, gateway)) else { continue };
                if best != gateway && here.is_none_or(|h| best_rssi as i16 >= h as i16 + BETTER_DB) {
                    return Err(format!(
                        "{} talks to gateway {} ({} dBm there, {} here): update it from that gateway",
                        s.mac,
                        best,
                        best_rssi,
                        here.map_or("not heard".to_string(), |h| format!("{} dBm", h))
                    ));
                }
            }
        }
        let job = {
            let mut slot = self.sensor_job.lock().unwrap();
            if let Some(j) = slot.as_ref().filter(|j| j.active()) {
                return Err(format!("a sensor update is already running on gateway {}", j.gateway));
            }
            let id = {
                let mut n = self.next_id.lock().unwrap();
                *n += 1;
                *n
            };
            let job = SensorJob {
                id,
                gateway: gateway.to_string(),
                board: board.to_string(),
                target: RELEASE_VERSION.to_string(),
                step: SensorStep::Download,
                message: None,
                sensors: macs
                    .iter()
                    .map(|m| SensorRow { mac: m.to_ascii_uppercase(), state: SensorState::Queued, done: 0, total: 0, message: None })
                    .collect(),
            };
            *slot = Some(job.clone());
            job
        };
        let _ = self.broadcast_tx.send(());
        let me = Arc::clone(self);
        let board = board.to_string();
        tokio::spawn(async move {
            let res = me.run_sensor_update(engine, &board, targets).await;
            me.update_sensors(|j| match res {
                Ok(()) if j.step == SensorStep::Running => j.step = SensorStep::Done,
                Ok(()) => {}
                Err(e) => {
                    warn!("Sensor update failed: {}", e);
                    j.step = SensorStep::Failed;
                    j.message = Some(e);
                }
            });
        });
        Ok(job)
    }

    /// Stops the running sensor update (offers withdrawn, the gateway's flags put back).
    pub fn cancel_sensor_update(&self) -> Result<(), String> {
        if !self.sensor_job().is_some_and(|j| j.active()) {
            return Err("no sensor update is running".into());
        }
        self.sensor_cancel.notify_one();
        Ok(())
    }

    async fn run_sensor_update(&self, mut engine: crate::engine::Engine, board: &str, targets: Vec<[u8; 8]>) -> Result<(), String> {
        let product = format!("sensor-{}", board);
        let slot = |s: &'static str| {
            let product = product.clone();
            move |f: &ows_host::manifest::ManifestFile| f.product.as_deref() == Some(product.as_str()) && f.file.contains(s)
        };
        let (version, _, a) = self.fetch_matching(&format!("{} slot A", product), slot("-slot-a-")).await?;
        let (_, _, b) = self.fetch_matching(&format!("{} slot B", product), slot("-slot-b-")).await?;
        let mut ota = Ota::new([a, b], Targets::Macs(targets), IDLE_TIMEOUT, Instant::now()).map_err(|e| e.to_string())?;
        let (board_id, build) = ota.build();
        info!("Sensor update {} ({:08X}) build {} through {}", version, board_id, build, engine.dongle_mac().unwrap_or("?"));

        let before = engine
            .ows_probe(OWS_TIMEOUT)
            .await
            .map_err(|e| format!("reading the gateway's flags: {}", e))?
            .ok_or("the gateway doesn't answer as OpenWyzeSense")?;
        let mut rx = engine.subscribe_ota();
        let res = async {
            engine
                .ows_set_flags(before.enabled | host::FLAG_OTA)
                .await
                .map_err(|e| format!("turning the gateway's update flag on: {}", e))?;
            self.update_sensors(|j| {
                j.target = version.clone();
                j.step = SensorStep::Running;
            });
            for p in ota.start() {
                engine.write_raw(&p).await.map_err(|e| e.to_string())?;
            }
            loop {
                if ota.finished(Instant::now()) {
                    break;
                }
                let pkt = tokio::select! {
                    p = rx.recv() => p,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => continue,
                    _ = self.sensor_cancel.notified() => {
                        self.update_sensors(|j| { j.step = SensorStep::Cancelled; });
                        break;
                    }
                };
                let Some(pkt) = pkt else { return Err("the gateway disconnected".to_string()) };
                let Some(hp) = host::parse(&to_host_bytes(&pkt)) else { continue };
                let step = ota.handle(&hp, Instant::now());
                for p in &step.send {
                    engine.write_raw(p).await.map_err(|e| e.to_string())?;
                }
                if !step.events.is_empty() {
                    self.update_sensors(|j| step.events.iter().for_each(|e| apply(j, e)));
                }
            }
            if ota.refused() {
                return Err("the gateway refused the offer".to_string());
            }
            Ok(())
        }
        .await;

        // Whatever happened: offers withdrawn, the update flag as it was.
        engine.unsubscribe_ota();
        let _ = engine.write_raw(&Ota::cancel()).await;
        if let Ok(Some(now)) = engine.ows_probe(OWS_TIMEOUT).await {
            let restored = (now.enabled & !host::FLAG_OTA) | (before.enabled & host::FLAG_OTA);
            if restored != now.enabled {
                if let Err(e) = engine.ows_set_flags(restored).await {
                    warn!("Putting the gateway's flags back failed: {}", e);
                }
            }
        }
        let results = ota.results();
        self.update_sensors(|j| {
            let stopped = j.step == SensorStep::Cancelled;
            for (mac, outcome) in &results {
                if let Some(r) = j.row(mac) {
                    match outcome {
                        Outcome::Done(0) => r.state = SensorState::Done,
                        Outcome::Done(st) => {
                            r.state = state_of(*st);
                            r.message = Some(status_message(*st));
                        }
                        Outcome::Refused => r.state = SensorState::Refused,
                        Outcome::TimedOut if !matches!(r.state, SensorState::Done | SensorState::Refused | SensorState::Failed) => {
                            r.state = if stopped { SensorState::Stopped } else { SensorState::TimedOut }
                        }
                        Outcome::TimedOut => {}
                    }
                }
            }
        });
        res
    }
}

/// w2m's parsed packet back to the bytes `ows-host` parses.
fn to_host_bytes(pkt: &Packet) -> Vec<u8> {
    pkt.to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses() {
        assert_eq!(state_of(0), SensorState::Done);
        assert_eq!(state_of(2), SensorState::Refused);
        assert_eq!(state_of(1), SensorState::Failed);
        assert_eq!(mac_bytes("777b42df").unwrap(), *b"777B42DF");
        assert!(mac_bytes("777B42").is_err());
    }
}

/// A sensor's reported status in words.
fn status_message(status: u8) -> String {
    match status {
        2 => "it already runs this version or newer, is another board, or its battery is under 2.5 V".into(),
        _ => ota::status_text(status).to_string(),
    }
}

/// A sensor's reported status: 0 ok, 2 it turned the update down, else it failed.
fn state_of(status: u8) -> SensorState {
    match status {
        0 => SensorState::Done,
        2 => SensorState::Refused,
        _ => SensorState::Failed,
    }
}

fn apply(j: &mut SensorJob, e: &Event) {
    match e {
        Event::Staged(mac) => {
            if let Some(r) = j.row(mac) {
                r.state = SensorState::PressButton;
            }
        }
        Event::Refused(mac) => {
            if let Some(r) = j.row(mac) {
                r.state = SensorState::Refused;
                r.message = Some("the gateway didn't stage the offer".into());
            }
        }
        Event::Progress { mac, done, total, .. } => {
            if let Some(r) = j.row(mac) {
                r.state = SensorState::Updating;
                r.done = *done;
                r.total = *total;
            }
        }
        Event::Done { mac, status } => {
            if let Some(r) = j.row(mac) {
                if *status == 0 {
                    r.state = SensorState::Done;
                    r.message = None;
                } else {
                    r.state = state_of(*status);
                    r.message = Some(status_message(*status));
                }
            }
        }
    }
}
