//! Roaming across OpenWyzeSense gateways that share one network identity
//! (OpenWyzeSense `docs/roaming.md`, host side: https://github.com/HclX/openwyzesense/issues/41).
//!
//! Several of our gateways can carry the same coordinator identity, so a
//! sensor is heard by every one of them in range. With `GW_FLAG_ROAM` on, only
//! the gateway whose table holds a sensor (its *owner*) ACKs it and reports it
//! as usual (`0x5319`); the others forward what they hear as *overheard*
//! reports (`0x53F1` sub `0x20`). This module:
//!
//! - finds our gateways (a stock dongle ignores `0x53F0`) and applies the
//!   feature flags configured for each, re-checking them periodically since a
//!   gateway restart clears them unannounced;
//! - merges every gateway's reports into one stream (dropping the copies of
//!   one report, and stale replays, by its counter; naming zero-MAC v1 reports
//!   by short address). While an owner's link to the host is down, its
//!   sensors' events still arrive through the others' overheard copies;
//! - decides who owns each sensor and hands sensors over: `0x5325` delete at
//!   the old owner first, then `ADD_SENSOR` (same short address and type) at
//!   the new one, so two gateways never own a sensor at once. A sensor moves
//!   only on evidence about the radio: the others hear it *resending* (nobody
//!   ACKs it), or one of them hears it much stronger over many reports. An
//!   owner merely disconnected from the host (a relay's WebSocket down) keeps
//!   ACKing its sensors over the air, so that alone never moves them;
//! - before a gateway pairs, sets its address range above every short address
//!   in use, and stops any other gateway of the network from pairing.

use crate::engine::{Engine, EnginesMap};
use crate::protocol::ows::{self, NetworkIdentity, NvSensor};
use crate::protocol::sensor::SensorManager;
use crate::protocol::telemetry::{DongleEvent, TelemetryData};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Tuning for the ownership policy and the gateway checks.
#[derive(Debug, Clone)]
pub struct RoamingConfig {
    /// A non-owner must be this much stronger (dB, smoothed) to take a sensor.
    pub hysteresis_db: f32,
    /// Reports both gateways must have heard (since the last handover) before
    /// signal strengths are compared.
    pub min_samples: u32,
    /// No handover for signal strength within this long of the last one.
    pub cooldown: Duration,
    /// Owner connected to the host: reports in a row that other gateways
    /// overheard but the owner never delivered (its radio missed them),
    /// after which the sensor moves.
    pub missed_reports: u32,
    /// Owner cut off from the host (its radio may still be fine): bursts
    /// among the last `burst_window` reports after which the sensor moves.
    /// Not "in a row": a far-away listener misses some copies of a burst.
    pub burst_reports: u32,
    pub burst_window: u32,
    /// The fewest copies of one report that make a burst; raised per sensor
    /// to one more than it ever sent while its owner was answering. Healthy
    /// stock sensors send up to 3 copies (lost ACKs); one nobody answers
    /// sends 6 (measured on a stock v2 contact).
    pub burst_copies: u32,
    /// How long after its first copy a report is judged (all copies and the
    /// owner's delivery in).
    pub settle: Duration,
    /// The least time between two handovers of one sensor, whatever the reason.
    pub min_interval: Duration,
    /// A gateway that has not heard a sensor for this long is no candidate.
    pub stale: Duration,
    /// Copies of one report arriving within this window are dropped; so is an
    /// event whose counter is not newer than the last one delivered.
    pub dedup_window: Duration,
    /// How often each gateway with configured flags has them re-read.
    pub flag_check_interval: Duration,
    /// How often the periodic checks run.
    pub tick: Duration,
    /// How long the first 0x53F0 waits for an answer before calling the
    /// gateway stock.
    pub probe_timeout: Duration,
    /// Feature flags for gateways not named in `gateway_flags` (`ows::FLAG_*`).
    pub default_flags: u32,
    /// Feature flags per gateway MAC (upper case).
    pub gateway_flags: HashMap<String, u32>,
    /// `roam` as configured for all gateways: `None` means automatic (on
    /// once two of our gateways share a network identity).
    pub roam_default: Option<bool>,
    /// `roam` as configured per gateway MAC (upper case), where set.
    pub roam_gateways: HashMap<String, bool>,
    /// Where the owner each sensor was last given is kept across restarts
    /// (sensor MAC → gateway MAC), so that when two gateways both hold a
    /// sensor the rightful one keeps it. `None`: in memory only.
    pub owners_path: Option<std::path::PathBuf>,
}

impl RoamingConfig {
    /// Roaming as configured for a gateway: on, off, or automatic (`None`).
    pub fn roam_setting(&self, gateway: &str) -> Option<bool> {
        self.roam_gateways.get(&gateway.to_ascii_uppercase()).copied().or(self.roam_default)
    }

    /// The flags configured for a gateway.
    pub fn configured_flags(&self, gateway: &str) -> u32 {
        self.gateway_flags.get(&gateway.to_ascii_uppercase()).copied().unwrap_or(self.default_flags)
    }

    /// The flags a gateway gets: what is configured for it, within what it
    /// supports and what this host manages. A roaming gateway never gets
    /// DEDUP: the host needs every copy of a report to tell a retry burst
    /// from a single send (and drops the copies itself).
    pub fn wanted_flags(&self, gateway: &str, supported: u32) -> u32 {
        let wanted = self.configured_flags(gateway) & supported & ows::MANAGED_FLAGS;
        if wanted & ows::FLAG_ROAM != 0 {
            wanted & !ows::FLAG_DEDUP
        } else {
            wanted
        }
    }
}

impl Default for RoamingConfig {
    fn default() -> Self {
        Self {
            hysteresis_db: 10.0,
            min_samples: 5,
            cooldown: Duration::from_secs(1800),
            missed_reports: 3,
            burst_reports: 3,
            burst_window: 5,
            burst_copies: 4,
            settle: Duration::from_secs(5),
            min_interval: Duration::from_secs(60),
            stale: Duration::from_secs(900),
            dedup_window: Duration::from_secs(30),
            flag_check_interval: Duration::from_secs(45),
            tick: Duration::from_secs(2),
            probe_timeout: Duration::from_millis(1500),
            default_flags: 0,
            gateway_flags: HashMap::new(),
            roam_default: None,
            roam_gateways: HashMap::new(),
            owners_path: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Pure bookkeeping: what each gateway hears of each sensor
// ---------------------------------------------------------------------------

const EWMA_ALPHA: f32 = 0.3;

/// How a handover failure starts when the old owner could not be reached.
const OLD_OWNER_UNREACHABLE: &str = "delete at old owner";

/// How long the last delivered counter is trusted to tell a stale replay
/// (a relay's buffer sent on after an outage) from a new event.
const COUNTER_MEMORY: Duration = Duration::from_secs(24 * 3600);

/// What one gateway hears of one sensor.
#[derive(Debug, Clone)]
pub struct Link {
    /// Smoothed RSSI, dBm (higher is stronger).
    pub rssi: f32,
    /// Reports heard since the last handover of the sensor.
    pub samples: u32,
    pub last: Instant,
}

/// Identifies one report: sensor, event type, and the sensor's event counter
/// (or the contents, for a motion history, which has none).
pub type ReportKey = (String, u8, u64);

/// One report while its copies come in.
#[derive(Debug, Clone)]
struct Pending {
    first: Instant,
    owner: String,
    /// Whether the owner was connected to the host when the report came.
    owner_online: bool,
    owner_delivered: bool,
    copies: HashMap<String, u32>,
}

/// What a sensor's recent reports say about its owner's radio.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Evidence {
    /// Reports in a row others overheard and the connected owner never delivered.
    pub missed: u32,
    /// Bursts among the recent reports others heard while the owner was cut off.
    pub bursts: u32,
    /// Those recent reports, oldest first: burst or not.
    pub recent: Vec<bool>,
    /// The most copies of one report seen while the owner was answering.
    pub learned_copies: u32,
}

#[derive(Default)]
pub struct Tracker {
    links: HashMap<String, HashMap<String, Link>>,
    /// When each report was first seen.
    seen: HashMap<ReportKey, Instant>,
    /// The last counter delivered per sensor and event type.
    counters: HashMap<(String, u8), (u16, Instant)>,
    pending: HashMap<ReportKey, Pending>,
    evidence: HashMap<String, Evidence>,
}

impl Tracker {
    /// Records that `gateway` heard `sensor` at `rssi`.
    pub fn record(&mut self, sensor: &str, gateway: &str, rssi: i8, now: Instant) {
        let link = self
            .links
            .entry(sensor.to_string())
            .or_default()
            .entry(gateway.to_string())
            .or_insert(Link { rssi: rssi as f32, samples: 0, last: now });
        if link.samples > 0 {
            link.rssi += EWMA_ALPHA * (rssi as f32 - link.rssi);
        } else {
            link.rssi = rssi as f32;
        }
        link.samples += 1;
        link.last = now;
    }

    /// Counts one copy of a report: from its owner (`own`, a `0x5319`) or
    /// overheard by another gateway.
    pub fn note_copy(&mut self, key: &ReportKey, gateway: &str, owner: &str, owner_online: bool, own: bool, now: Instant) {
        let p = self.pending.entry(key.clone()).or_insert_with(|| Pending {
            first: now,
            owner: owner.to_string(),
            owner_online,
            owner_delivered: false,
            copies: HashMap::new(),
        });
        *p.copies.entry(gateway.to_string()).or_insert(0) += 1;
        if own {
            p.owner_delivered = true;
        }
    }

    /// Judges the reports whose copies are all in (`settle` after the first);
    /// returns the sensors whose evidence changed.
    pub fn settle(&mut self, now: Instant, settle: Duration, burst_copies: u32, burst_window: u32) -> Vec<String> {
        let mut due: Vec<(ReportKey, Pending)> =
            self.pending.iter().filter(|(_, p)| now.duration_since(p.first) >= settle).map(|(k, p)| (k.clone(), p.clone())).collect();
        due.sort_by_key(|(_, p)| p.first);
        let mut changed = Vec::new();
        for (key, p) in due {
            self.pending.remove(&key);
            let ev = self.evidence.entry(key.0.clone()).or_default();
            let overheard = p.copies.iter().filter(|(g, _)| **g != p.owner).map(|(_, n)| *n).max().unwrap_or(0);
            if p.owner_delivered {
                // The owner's path works (copies counted while it answers
                // calibrate the burst bar).
                if p.owner_online {
                    let most = p.copies.values().copied().max().unwrap_or(0);
                    ev.learned_copies = ev.learned_copies.max(most);
                }
                ev.missed = 0;
                ev.recent.clear();
                ev.bursts = 0;
            } else if overheard > 0 {
                // Missed reports count only while the owner is known to be
                // connected; bursts count either way, since "connected" can
                // be stale (a gateway that lost power leaves a half-open
                // connection until the heartbeat notices).
                if p.owner_online {
                    ev.missed += 1;
                } else {
                    ev.missed = 0;
                }
                let bar = burst_copies.max(ev.learned_copies + 1);
                ev.recent.push(overheard >= bar);
                let excess = ev.recent.len().saturating_sub(burst_window as usize);
                ev.recent.drain(..excess);
                ev.bursts = ev.recent.iter().filter(|b| **b).count() as u32;
            }
            changed.push(key.0.clone());
        }
        changed.dedup();
        changed
    }

    /// True if this report was already delivered: a copy within `window`, or
    /// an event whose counter is not newer than the last one delivered (a
    /// stale replay). `counter` is the event counter, for event types where
    /// it moves with every event (not heartbeats, which repeat it).
    pub fn is_duplicate(&mut self, key: ReportKey, counter: Option<u16>, now: Instant, window: Duration) -> bool {
        if matches!(self.seen.get(&key), Some(t) if now.duration_since(*t) <= window) {
            return true;
        }
        if let Some(c) = counter {
            let slot = (key.0.clone(), key.1);
            if let Some((last, t)) = self.counters.get(&slot) {
                // Modular: newer means ahead by less than half the range.
                if now.duration_since(*t) <= COUNTER_MEMORY && c.wrapping_sub(*last) as i16 <= 0 {
                    return true;
                }
            }
            self.counters.insert(slot, (c, now));
        }
        self.seen.insert(key, now);
        false
    }

    /// Forgets a sensor's counters (it was paired again and may start over).
    pub fn forget_counters(&mut self, sensor: &str) {
        self.counters.retain(|(mac, _), _| mac != sensor);
    }

    pub fn prune(&mut self, now: Instant, window: Duration) {
        self.seen.retain(|_, t| now.duration_since(*t) <= window);
        self.counters.retain(|_, (_, t)| now.duration_since(*t) <= COUNTER_MEMORY);
    }

    /// After a handover, everything is measured afresh (what the sensor
    /// normally sends is kept).
    pub fn reset(&mut self, sensor: &str) {
        if let Some(links) = self.links.get_mut(sensor) {
            for l in links.values_mut() {
                l.samples = 0;
            }
        }
        if let Some(ev) = self.evidence.get_mut(sensor) {
            ev.missed = 0;
            ev.bursts = 0;
            ev.recent.clear();
        }
        self.pending.retain(|k, _| k.0 != sensor);
    }

    pub fn links(&self, sensor: &str) -> Option<&HashMap<String, Link>> {
        self.links.get(sensor)
    }

    pub fn evidence(&self, sensor: &str) -> Evidence {
        self.evidence.get(sensor).cloned().unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// The connected owner keeps missing reports others hear.
    OwnerMissing,
    /// The owner is cut off and others keep hearing the sensor retry: nobody ACKs.
    NotAcked,
    StrongerSignal,
}

impl std::fmt::Display for Reason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Reason::OwnerMissing => "its owner keeps missing its reports",
            Reason::NotAcked => "its owner no longer ACKs it",
            Reason::StrongerSignal => "stronger signal",
        })
    }
}

/// Whether a sensor should move from `owner`, and to which gateway.
///
/// `eligible` says which other gateways may take it (online, same network,
/// roaming). Being cut off from the host does not by itself count against the
/// owner: its radio may well be working.
pub fn decide(
    owner: &str,
    links: &HashMap<String, Link>,
    evidence: &Evidence,
    eligible: impl Fn(&str) -> bool,
    last_handover: Option<Instant>,
    now: Instant,
    cfg: &RoamingConfig,
) -> Option<(String, Reason)> {
    let since_last = last_handover.map(|t| now.duration_since(t));
    if since_last.is_some_and(|d| d < cfg.min_interval) {
        return None;
    }
    let best = links
        .iter()
        .filter(|(g, l)| g.as_str() != owner && eligible(g) && now.duration_since(l.last) <= cfg.stale)
        .max_by(|a, b| a.1.rssi.total_cmp(&b.1.rssi));
    let (best, link) = best?;

    // Bursts first: they also say the old owner is not answering, which
    // lets the handover go on if the old owner cannot be reached.
    if evidence.bursts >= cfg.burst_reports {
        return Some((best.clone(), Reason::NotAcked));
    }
    if evidence.missed >= cfg.missed_reports {
        return Some((best.clone(), Reason::OwnerMissing));
    }
    if since_last.is_some_and(|d| d < cfg.cooldown) {
        return None;
    }
    let own = links.get(owner).filter(|o| now.duration_since(o.last) <= cfg.stale && o.samples >= cfg.min_samples)?;
    (link.samples >= cfg.min_samples && link.rssi >= own.rssi + cfg.hysteresis_db).then(|| (best.clone(), Reason::StrongerSignal))
}

/// The signal strength a report carries, if any.
fn report_rssi(evt: &DongleEvent) -> Option<i8> {
    match evt.data {
        TelemetryData::Heartbeat { rssi, .. }
        | TelemetryData::Alarm { rssi, .. }
        | TelemetryData::Climate { rssi, .. }
        | TelemetryData::AlarmData { rssi, .. }
        | TelemetryData::Leak { rssi, .. } => Some(rssi),
        _ => None,
    }
}

/// Replaces the signal strength a report carries.
fn set_report_rssi(evt: &mut DongleEvent, to: i8) {
    match &mut evt.data {
        TelemetryData::Heartbeat { rssi, .. }
        | TelemetryData::Alarm { rssi, .. }
        | TelemetryData::Climate { rssi, .. }
        | TelemetryData::AlarmData { rssi, .. }
        | TelemetryData::Leak { rssi, .. } => *rssi = to,
        _ => {}
    }
}

/// What tells one report from the next, and the event counter when it moves
/// with every report of this type (heartbeats repeat the last event's).
fn report_key(evt: &DongleEvent) -> Option<(ReportKey, Option<u16>)> {
    let (id, counter) = match &evt.data {
        TelemetryData::Heartbeat { event_sequence, .. } => (*event_sequence as u64, None),
        TelemetryData::Alarm { event_sequence, .. } | TelemetryData::Climate { event_sequence, .. } => {
            (*event_sequence as u64, Some(*event_sequence))
        }
        TelemetryData::AlarmData { ring_buffer, .. } => {
            (ring_buffer.iter().fold(0xCBF2_9CE4_8422_2325u64, |h, b| (h ^ *b as u64).wrapping_mul(0x100_0000_01B3)), None)
        }
        _ => return None,
    };
    Some(((evt.mac.clone(), evt.event_type, id), counter))
}

// ---------------------------------------------------------------------------
// The coordinator
// ---------------------------------------------------------------------------

struct Gateway {
    identity: NetworkIdentity,
    build: Option<ows::BuildInfo>,
    supported: u32,
    enabled: u32,
    /// The flags configured for it (within what it supports).
    wanted: u32,
    table: Vec<NvSensor>,
    online: bool,
    offline_since: Option<Instant>,
    last_flag_check: Instant,
}

impl Gateway {
    /// Roaming is configured for this gateway, and its firmware has it.
    fn can_roam(&self) -> bool {
        self.wanted & ows::FLAG_ROAM != 0
    }
}

#[derive(Default)]
struct State {
    gateways: HashMap<String, Gateway>,
    tracker: Tracker,
    last_handover: HashMap<String, Instant>,
    /// Sensors being handed over right now.
    busy: HashSet<String>,
    /// The owner each sensor was last given (see `RoamingConfig::owners_path`).
    owners: HashMap<String, String>,
    /// Sensors a gateway answers without holding them in its table: on a
    /// short address another sensor holds (sensor -> (short, the sensor
    /// holding it, if known; the gateway answering it), when).
    conflicts: HashMap<String, ((Option<u16>, Option<String>, String), Instant)>,
}

impl State {
    /// Automatic roaming: once two of our gateways that support it share a
    /// network identity, roaming goes on for each whose `roam` isn't set
    /// (they would otherwise all answer every sensor of the network, and all
    /// report every join). It stays on. Returns the gateways turned on now.
    fn auto_roam(&mut self, identity: NetworkIdentity, cfg: &RoamingConfig) -> Vec<String> {
        let group: Vec<String> = self
            .gateways
            .iter()
            .filter(|(m, g)| g.identity == identity && g.supported & ows::FLAG_ROAM != 0 && cfg.roam_setting(m) != Some(false))
            .map(|(m, _)| m.clone())
            .collect();
        if group.len() < 2 {
            return Vec::new();
        }
        let mut turned_on = Vec::new();
        for m in &group {
            let g = self.gateways.get_mut(m).unwrap();
            if cfg.roam_setting(m).is_none() && g.wanted & ows::FLAG_ROAM == 0 {
                g.wanted = (g.wanted | ows::FLAG_ROAM) & !ows::FLAG_DEDUP;
                turned_on.push(m.clone());
            }
        }
        if !turned_on.is_empty() {
            info!(
                "Roaming: gateways {} share network {}; roaming on for {}",
                group.join(", "),
                identity,
                turned_on.join(", ")
            );
        }
        turned_on
    }

    /// Gateways that can roam, sharing `identity`.
    fn members(&self, identity: NetworkIdentity) -> impl Iterator<Item = (&String, &Gateway)> {
        self.gateways.iter().filter(move |(_, g)| g.identity == identity && g.can_roam())
    }

    /// Roaming is on for a network once two of our gateways carry it.
    fn is_roaming(&self, identity: NetworkIdentity) -> bool {
        self.members(identity).count() >= 2
    }

    fn roaming_identity(&self, gateway: &str) -> Option<NetworkIdentity> {
        let g = self.gateways.get(gateway)?;
        (g.can_roam() && self.is_roaming(g.identity)).then_some(g.identity)
    }

    /// The gateway whose table holds `mac`, preferring an online one.
    fn owner_of(&self, mac: &str, identity: NetworkIdentity) -> Option<String> {
        let mut owners: Vec<_> = self.members(identity).filter(|(_, g)| g.table.iter().any(|s| s.mac == mac)).collect();
        owners.sort_by_key(|(_, g)| !g.online);
        owners.first().map(|(m, _)| m.to_string())
    }

    fn entry(&self, mac: &str, identity: NetworkIdentity) -> Option<NvSensor> {
        self.members(identity).flat_map(|(_, g)| g.table.iter()).find(|s| s.mac == mac).cloned()
    }

    fn mac_of_short(&self, short: u16, identity: NetworkIdentity) -> Option<String> {
        self.members(identity).flat_map(|(_, g)| g.table.iter()).find(|s| s.short == short).map(|s| s.mac.clone())
    }

    fn eligible(&self, gateway: &str, identity: NetworkIdentity) -> bool {
        self.gateways.get(gateway).is_some_and(|g| g.online && g.identity == identity && g.can_roam())
    }

    /// Checks whether `sensor` should move; if so marks it busy and says where.
    fn consider(&mut self, sensor: &str, identity: NetworkIdentity, now: Instant, cfg: &RoamingConfig) -> Option<Handover> {
        if self.busy.contains(sensor) {
            return None;
        }
        let owner = self.owner_of(sensor, identity)?;
        let links = self.tracker.links(sensor)?;
        let evidence = self.tracker.evidence(sensor);
        let (to, reason) = decide(
            &owner,
            links,
            &evidence,
            |g| self.eligible(g, identity),
            self.last_handover.get(sensor).copied(),
            now,
            cfg,
        )?;
        self.busy.insert(sensor.to_string());
        Some(Handover { sensor: sensor.to_string(), from: owner, to, reason, identity })
    }

    /// Judges the reports whose copies are all in, and checks the sensors
    /// whose evidence changed.
    fn settle(&mut self, now: Instant, cfg: &RoamingConfig) -> Vec<Handover> {
        let mut changed = self.tracker.settle(now, cfg.settle, cfg.burst_copies, cfg.burst_window);
        // Also those with evidence pending (e.g. judged while a handover
        // that then failed was under way).
        for (sensor, ev) in &self.tracker.evidence {
            if (ev.missed > 0 || ev.bursts > 0) && !changed.contains(sensor) {
                changed.push(sensor.clone());
            }
        }
        changed
            .into_iter()
            .filter_map(|sensor| {
                let identity = self.gateways.values().find(|g| g.can_roam() && g.table.iter().any(|s| s.mac == sensor))?.identity;
                self.consider(&sensor, identity, now, cfg)
            })
            .collect()
    }
}

#[derive(Debug)]
struct Handover {
    sensor: String,
    from: String,
    to: String,
    reason: Reason,
    identity: NetworkIdentity,
}

enum Action {
    Handover(Handover),
    RefreshTable(String),
}

pub struct Roaming {
    cfg: RoamingConfig,
    engines: EnginesMap,
    sensors: Arc<Mutex<SensorManager>>,
    broadcast_tx: tokio::sync::broadcast::Sender<()>,
    state: Mutex<State>,
}

impl Roaming {
    pub fn new(
        cfg: RoamingConfig,
        engines: EnginesMap,
        sensors: Arc<Mutex<SensorManager>>,
        broadcast_tx: tokio::sync::broadcast::Sender<()>,
    ) -> Arc<Self> {
        let owners: HashMap<String, String> = cfg
            .owners_path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|text| serde_yaml::from_str(&text).ok())
            .unwrap_or_default();
        let state = State { owners, ..State::default() };
        Arc::new(Self { cfg, engines, sensors, broadcast_tx, state: Mutex::new(state) })
    }

    /// Starts merging events from `raw_rx` into `event_tx`, and the periodic
    /// checks.
    pub fn spawn(self: &Arc<Self>, mut raw_rx: mpsc::Receiver<DongleEvent>, event_tx: mpsc::Sender<DongleEvent>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(evt) = raw_rx.recv().await {
                let (evt, actions) = me.process(evt, Instant::now());
                for action in actions {
                    let me = Arc::clone(&me);
                    tokio::spawn(async move { me.run(action).await });
                }
                if let Some(evt) = evt {
                    if event_tx.send(evt).await.is_err() {
                        break;
                    }
                }
            }
        });
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(me.cfg.tick);
            loop {
                interval.tick().await;
                me.tick().await;
            }
        });
    }

    async fn engine(&self, gateway: &str) -> Option<Engine> {
        self.engines.lock().await.get(gateway).cloned()
    }

    /// Call once a gateway is registered in the engines map (handshake done).
    pub async fn gateway_connected(&self, gateway: &str) {
        let Some(mut engine) = self.engine(gateway).await else { return };
        let flags = match engine.ows_probe(self.cfg.probe_timeout).await {
            Ok(Some(f)) => f,
            Ok(None) => {
                info!("Gateway {} does not answer 0x53F0: stock firmware", gateway);
                // It may have run our firmware before (stock restored).
                if self.state.lock().unwrap().gateways.remove(gateway).is_some() {
                    info!("Gateway {} no longer runs OpenWyzeSense; forgotten", gateway);
                    let _ = self.broadcast_tx.send(());
                }
                return;
            }
            Err(e) => {
                warn!("Roaming: probing gateway {} failed: {}", gateway, e);
                return;
            }
        };
        let build = match engine.ows_build_info().await {
            Ok(b) => Some(b),
            Err(e) => {
                warn!("Reading the build info of gateway {} failed: {}", gateway, e);
                None
            }
        };
        let identity = match engine.ows_network().await {
            Ok(id) => id,
            Err(e) => {
                warn!("Roaming: reading the network identity of gateway {} failed: {}", gateway, e);
                return;
            }
        };
        let table = match engine.ows_sensor_table().await {
            Ok(t) => t,
            Err(e) => {
                warn!("Roaming: reading the sensor table of gateway {} failed: {}", gateway, e);
                return;
            }
        };
        let wanted = self.cfg.wanted_flags(gateway, flags.supported);
        info!(
            "Gateway {}: OpenWyzeSense {} on {} (flags supported {:08X}, enabled {:08X}, configured {:08X}; {} sensors)",
            gateway,
            build.as_ref().map_or("(unknown version)", |b| b.version.as_str()),
            identity,
            flags.supported,
            flags.enabled,
            wanted,
            table.len()
        );
        let configured = self.cfg.gateway_flags.get(&gateway.to_ascii_uppercase()).copied().unwrap_or(self.cfg.default_flags);
        if configured & ows::MANAGED_FLAGS & !flags.supported != 0 {
            warn!(
                "Gateway {} firmware lacks configured flags {:08X}; update it to use them",
                gateway,
                configured & ows::MANAGED_FLAGS & !flags.supported
            );
        }

        // Sensors another online gateway of the network already owns stay
        // there: the newcomer (back from an outage, typically, after its
        // sensors were handed over) forgets them.
        // (sensor, gateway that keeps it, gateway that forgets it)
        let auto_roam;
        let duplicates: Vec<(String, String, String)> = {
            let mut st = self.state.lock().unwrap();
            st.gateways.insert(
                gateway.to_string(),
                Gateway {
                    identity,
                    build,
                    supported: flags.supported,
                    enabled: flags.enabled,
                    wanted,
                    table: table.clone(),
                    online: true,
                    offline_since: None,
                    last_flag_check: Instant::now(),
                },
            );
            auto_roam = st.auto_roam(identity, &self.cfg);
            let mut dups = Vec::new();
            for s in &table {
                for (other, g) in st.members(identity) {
                    if other == gateway || !g.online {
                        continue;
                    }
                    if g.table.iter().any(|o| o.mac == s.mac) {
                        // The owner last decided keeps it; failing that, the
                        // gateway that stayed up (the newcomer is typically
                        // back from an outage during which it was handed over).
                        if st.owners.get(&s.mac).map(String::as_str) == Some(gateway) {
                            dups.push((s.mac.clone(), gateway.to_string(), other.clone()));
                        } else {
                            dups.push((s.mac.clone(), other.clone(), gateway.to_string()));
                        }
                    } else if let Some(o) = g.table.iter().find(|o| o.short == s.short) {
                        error!(
                            "Roaming: {} on gateway {} and {} on gateway {} share short address {:04X}; re-pair one of them",
                            s.mac, gateway, o.mac, other, s.short
                        );
                    }
                }
            }
            dups
        };
        for (mac, keep, drop_at) in duplicates {
            info!("Roaming: {} is on gateways {} and {}; {} keeps it, removing it from {}", mac, keep, drop_at, keep, drop_at);
            let Some(mut e) = self.engine(&drop_at).await else { continue };
            match e.delete_sensor(&mac).await {
                Ok(()) => {
                    if let Some(g) = self.state.lock().unwrap().gateways.get_mut(&drop_at) {
                        g.table.retain(|s| s.mac != mac);
                    }
                    self.set_owner(&mac, &keep);
                }
                Err(e) => warn!("Roaming: removing {} from gateway {} failed: {}", mac, drop_at, e),
            }
        }

        // Gateways already connected that roaming just turned on for.
        for other in auto_roam.iter().filter(|g| g.as_str() != gateway) {
            self.ensure_flags(other).await;
        }
        self.ensure_flags(gateway).await;
    }

    /// On a roaming network: the gateway that owns `sensor` (the one that
    /// answers it), found from any gateway of that network. `None` without
    /// roaming, or when no online gateway holds it.
    pub fn roaming_owner(&self, any_gateway: &str, sensor: &str) -> Option<String> {
        let st = self.state.lock().unwrap();
        let identity = st.roaming_identity(any_gateway)?;
        let owner = st.owner_of(sensor, identity)?;
        st.gateways.get(&owner).is_some_and(|g| g.online).then_some(owner)
    }

    /// Call when a gateway's connection is gone.
    pub fn gateway_disconnected(&self, gateway: &str) {
        let mut st = self.state.lock().unwrap();
        if let Some(g) = st.gateways.get_mut(gateway) {
            g.online = false;
            g.offline_since = Some(Instant::now());
            // Its sensors stay: its radio may still be ACKing them (a relay
            // buffers their reports), and the other gateways' overheard copies
            // keep their events coming meanwhile.
            info!("Gateway {} offline", gateway);
        }
    }

    /// Sets the configured flags on a gateway if it does not have them (the
    /// flags this host does not manage are left as they are).
    async fn ensure_flags(&self, gateway: &str) {
        let flags = {
            let st = self.state.lock().unwrap();
            let Some(g) = st.gateways.get(gateway) else { return };
            if g.enabled & ows::MANAGED_FLAGS == g.wanted {
                return;
            }
            (g.enabled & !ows::MANAGED_FLAGS) | g.wanted
        };
        let Some(mut engine) = self.engine(gateway).await else { return };
        match engine.ows_set_flags(flags).await {
            Ok(f) => {
                info!("Gateway {}: flags set to {:08X}", gateway, f.enabled);
                if let Some(g) = self.state.lock().unwrap().gateways.get_mut(gateway) {
                    g.enabled = f.enabled;
                    g.last_flag_check = Instant::now();
                }
            }
            Err(e) => warn!("Setting flags on gateway {} failed: {}", gateway, e),
        }
    }

    /// Call right before turning pairing on at `gateway`. On a roaming network
    /// it stops any other gateway pairing and gives this one an address range
    /// above every short address in use.
    pub async fn prepare_pairing(&self, gateway: &str) -> Result<(), String> {
        let (others, range) = {
            let st = self.state.lock().unwrap();
            let Some(identity) = st.roaming_identity(gateway) else { return Ok(()) };
            let others: Vec<String> = st.members(identity).filter(|(m, g)| *m != gateway && g.online).map(|(m, _)| m.clone()).collect();
            let shorts: Vec<u16> = st.members(identity).flat_map(|(_, g)| g.table.iter().map(|s| s.short)).collect();
            (others, ows::pairing_range(&shorts).ok_or("no short addresses left on this network")?)
        };
        for other in others {
            if let Some(mut e) = self.engine(&other).await {
                if e.is_scanning() {
                    info!("Roaming: gateway {} stops pairing, gateway {} pairs now", other, gateway);
                    e.set_scan(false).await.map_err(|e| format!("stopping pairing on {}: {}", other, e))?;
                }
            }
        }
        let mut engine = self.engine(gateway).await.ok_or_else(|| format!("gateway {} is not connected", gateway))?;
        engine.ows_set_addr_range(range.0, range.1).await.map_err(|e| e.to_string())?;
        info!("Roaming: gateway {} pairs new sensors at {:04X}-{:04X}", gateway, range.0, range.1);
        Ok(())
    }

    /// Merges one event into the stream: `None` drops it (a copy, or a sensor
    /// of no gateway of ours). May also ask for a handover or a table reload.
    fn process(&self, mut evt: DongleEvent, now: Instant) -> (Option<DongleEvent>, Vec<Action>) {
        let Some(gateway) = evt.dongle_mac.clone() else { return (Some(evt), vec![]) };
        // The sensors this host knows, taken before the roaming state: never
        // hold both locks (the dashboard takes the sensors, then asks roaming).
        let known: HashSet<String> = if evt.overheard_short.is_some() {
            self.sensors.lock().unwrap().get_sensors().keys().cloned().collect()
        } else {
            HashSet::new()
        };
        let mut st = self.state.lock().unwrap();
        let Some(identity) = st.roaming_identity(&gateway) else {
            // Not a roaming gateway (stock, or alone on its network).
            let mut actions = vec![];
            if matches!(evt.data, TelemetryData::Paired { .. }) && st.gateways.contains_key(&gateway) {
                actions.push(Action::RefreshTable(gateway.clone()));
            }
            return (evt.overheard_short.is_none().then_some(evt), actions);
        };
        if matches!(evt.data, TelemetryData::Paired { .. }) {
            st.tracker.forget_counters(&evt.mac);
            return (Some(evt), vec![Action::RefreshTable(gateway)]);
        }

        let owner = match evt.overheard_short {
            None => {
                // The gateway answered and reported it, so it takes the sensor
                // for one of its own; if its table doesn't hold this MAC, the
                // sensor shares a short address with one that it does.
                let unlisted = st.gateways.get(&gateway).is_some_and(|g| !g.table.iter().any(|s| s.mac == evt.mac));
                if unlisted && !ows::is_zero_mac(&evt.mac) {
                    let fresh = st.conflicts.get(&evt.mac).is_none_or(|(_, t)| now.duration_since(*t) > Duration::from_secs(3600));
                    if fresh {
                        warn!(
                            "Roaming: gateway {} answers {} without holding it in its table: it shares a short address with \
                             one of that gateway's sensors and can't roam. Pair {} again.",
                            gateway, evt.mac, evt.mac
                        );
                    }
                    let known = st.conflicts.get(&evt.mac).map(|((s, o, _), _)| (*s, o.clone())).unwrap_or((None, None));
                    st.conflicts.insert(evt.mac.clone(), ((known.0, known.1, gateway.clone()), now));
                } else if !unlisted {
                    // In its table now (paired again): no longer a conflict.
                    st.conflicts.remove(&evt.mac);
                }
                gateway.clone()
            }
            Some(short) => {
                if ows::is_zero_mac(&evt.mac) {
                    match st.mac_of_short(short, identity) {
                        Some(mac) => evt.mac = mac,
                        None => {
                            debug!("Roaming: gateway {} overheard unknown short address {:04X}", gateway, short);
                            return (None, vec![]);
                        }
                    }
                }
                let owner = st.owner_of(&evt.mac, identity);
                match owner {
                    Some(o) if known.contains(&evt.mac) => o,
                    _ => {
                        // A sensor no table holds, on a short address another
                        // sensor holds: the two share an address (a migration
                        // dropped one, say), so the gateway holding the address
                        // answers this sensor as the other one, and roaming can't
                        // move it (by address it would move the other).
                        let holder = st.mac_of_short(short, identity).filter(|m| *m != evt.mac);
                        match holder {
                            Some(other) => {
                                let at = st.owner_of(&other, identity).unwrap_or_default();
                                let fresh = st.conflicts.get(&evt.mac).is_none_or(|(_, t)| now.duration_since(*t) > Duration::from_secs(3600));
                                if fresh {
                                    warn!(
                                        "Roaming: {} uses short address {:04X}, which {} holds on gateway {}: they share an address, \
                                         so {} answers it as {} and it can't roam. Pair {} again.",
                                        evt.mac, short, other, at, at, other, evt.mac
                                    );
                                }
                                st.conflicts.insert(evt.mac.clone(), ((Some(short), Some(other), at), now));
                            }
                            None => debug!("Roaming: gateway {} overheard {}, which no gateway of ours owns", gateway, evt.mac),
                        }
                        return (None, vec![]);
                    }
                }
            }
        };

        let mut actions: Vec<Action> = st.settle(now, &self.cfg).into_iter().map(Action::Handover).collect();
        let mut duplicate = false;
        if let Some(rssi) = report_rssi(&evt) {
            st.tracker.record(&evt.mac, &gateway, rssi, now);
            if let Some((key, counter)) = report_key(&evt) {
                let owner_online = st.gateways.get(&owner).is_some_and(|g| g.online);
                let own = evt.overheard_short.is_none();
                st.tracker.note_copy(&key, &gateway, &owner, owner_online, own, now);
                duplicate = st.tracker.is_duplicate(key, counter, now, self.cfg.dedup_window);
            }
            // Signal strength can be judged at once.
            actions.extend(st.consider(&evt.mac, identity, now, &self.cfg).map(Action::Handover));
        }
        if duplicate {
            return (None, actions);
        }
        // The first copy may come from another gateway (an overheard copy
        // over a faster link): the event speaks for the owner, so it carries
        // the owner's signal, not the far gateway's.
        if gateway != owner {
            if let Some(link) = st.tracker.links(&evt.mac).and_then(|l| l.get(&owner)) {
                if now.duration_since(link.last) <= self.cfg.stale {
                    set_report_rssi(&mut evt, link.rssi.round() as i8);
                }
            }
        }
        evt.dongle_mac = Some(owner);
        (Some(evt), actions)
    }

    async fn run(&self, action: Action) {
        match action {
            Action::Handover(h) => self.handover(h).await,
            Action::RefreshTable(gateway) => self.refresh_table(&gateway).await,
        }
    }

    async fn refresh_table(&self, gateway: &str) {
        let Some(mut engine) = self.engine(gateway).await else { return };
        match engine.ows_sensor_table().await {
            Ok(table) => {
                debug!("Roaming: gateway {} table reloaded ({} sensors)", gateway, table.len());
                if let Some(g) = self.state.lock().unwrap().gateways.get_mut(gateway) {
                    g.table = table;
                }
            }
            Err(e) => warn!("Roaming: reloading the sensor table of gateway {} failed: {}", gateway, e),
        }
    }

    /// Moves a sensor: delete at the old owner first (if it is reachable),
    /// then add at the new one with the same short address and type.
    async fn handover(&self, h: Handover) {
        let result = self.do_handover(&h).await;
        let mut st = self.state.lock().unwrap();
        st.busy.remove(&h.sensor);
        match result {
            Ok(()) => {
                let entry = st.gateways.get(&h.from).and_then(|g| g.table.iter().find(|s| s.mac == h.sensor).cloned());
                if let Some(g) = st.gateways.get_mut(&h.from) {
                    g.table.retain(|s| s.mac != h.sensor);
                }
                if let (Some(g), Some(e)) = (st.gateways.get_mut(&h.to), entry) {
                    g.table.retain(|s| s.mac != h.sensor);
                    g.table.push(e);
                }
                st.last_handover.insert(h.sensor.clone(), Instant::now());
                st.tracker.reset(&h.sensor);
                drop(st);
                info!("Roaming: {} handed over from gateway {} to gateway {} ({})", h.sensor, h.from, h.to, h.reason);
                self.set_owner(&h.sensor, &h.to);
            }
            Err(e) => {
                // Do not retry at once: wait for the cooldown. Except when the
                // old owner could not be reached: the next report may show
                // the sensor unanswered, which lets the handover go on without
                // it (see `do_handover`).
                if !e.starts_with(OLD_OWNER_UNREACHABLE) {
                    st.last_handover.insert(h.sensor.clone(), Instant::now());
                }
                warn!("Roaming: handing {} over from gateway {} to gateway {} failed: {}", h.sensor, h.from, h.to, e);
            }
        }
    }

    async fn do_handover(&self, h: &Handover) -> Result<(), String> {
        let (entry, from_online) = {
            let st = self.state.lock().unwrap();
            let entry = st.entry(&h.sensor, h.identity).ok_or("not in any sensor table")?;
            (entry, st.gateways.get(&h.from).is_some_and(|g| g.online))
        };
        let sensor_type = match entry.sensor_type {
            0 => self.sensors.lock().unwrap().get_sensors().get(&h.sensor).map_or(0, |s| s.sensor_type.to_u8()),
            t => t,
        };
        let mut to = self.engine(&h.to).await.ok_or("new owner not connected")?;
        let mut from = if from_online { self.engine(&h.from).await } else { None };
        if let Some(mut f) = from.clone() {
            if let Err(e) = f.delete_sensor(&h.sensor).await {
                if h.reason != Reason::NotAcked {
                    return Err(format!("{}: {}", OLD_OWNER_UNREACHABLE, e));
                }
                // Nobody answers the sensor, so the old owner is not acting as
                // its owner, whatever its connection looks like (typically a
                // gateway that lost power, its connection not yet timed out).
                // Go on; when it comes back, the recorded owner wins.
                warn!("Roaming: old owner {} of {} unreachable ({}); handing over anyway", h.from, h.sensor, e);
                from = None;
            }
        }
        if let Err(e) = to.ows_add_sensor(&h.sensor, entry.short, sensor_type).await {
            if let Some(mut from) = from {
                // Put it back where it was rather than leave it ownerless.
                if let Err(e2) = from.ows_add_sensor(&h.sensor, entry.short, sensor_type).await {
                    error!("Roaming: {} is now on no gateway (restoring it failed: {})", h.sensor, e2);
                }
            }
            return Err(format!("add at new owner: {}", e));
        }
        Ok(())
    }

    fn set_owner(&self, sensor: &str, gateway: &str) {
        let owners = {
            let mut st = self.state.lock().unwrap();
            st.owners.insert(sensor.to_string(), gateway.to_string());
            st.owners.clone()
        };
        if let Some(path) = &self.cfg.owners_path {
            let tmp = path.with_extension("yaml.tmp");
            let saved = serde_yaml::to_string(&owners)
                .map_err(|e| e.to_string())
                .and_then(|text| std::fs::write(&tmp, text).map_err(|e| e.to_string()))
                .and_then(|_| std::fs::rename(&tmp, path).map_err(|e| e.to_string()));
            if let Err(e) = saved {
                warn!("Roaming: saving sensor owners to {:?} failed: {}", path, e);
            }
        }
        let mut mgr = self.sensors.lock().unwrap();
        if let Some(s) = mgr.get_sensors_mut().get_mut(sensor) {
            s.dongle_mac = Some(gateway.to_string());
        }
        if let Err(e) = mgr.save_state_to_disk() {
            warn!("Roaming: saving state failed: {}", e);
        }
        drop(mgr);
        let _ = self.broadcast_tx.send(());
    }

    /// Periodic: re-apply flags a gateway lost (it restarted unannounced), and
    /// judge reports whose copies are all in.
    async fn tick(&self) {
        let now = Instant::now();
        let to_check: Vec<String> = {
            let mut st = self.state.lock().unwrap();
            st.tracker.prune(now, self.cfg.dedup_window);
            // Only gateways with flags to lose: a restart resets them to 0.
            st.gateways
                .iter()
                .filter(|(_, g)| g.online && g.wanted != 0 && now.duration_since(g.last_flag_check) >= self.cfg.flag_check_interval)
                .map(|(m, _)| m.clone())
                .collect()
        };
        for gateway in to_check {
            self.check_flags(&gateway).await;
        }
        let handovers = self.state.lock().unwrap().settle(Instant::now(), &self.cfg);
        for h in handovers {
            self.handover(h).await;
        }
    }

    async fn check_flags(&self, gateway: &str) {
        let Some(mut engine) = self.engine(gateway).await else { return };
        let flags = match engine.ows_probe(self.cfg.probe_timeout).await {
            Ok(Some(f)) => f,
            Ok(None) => {
                warn!("Gateway {} did not answer the flag check", gateway);
                return;
            }
            Err(e) => {
                warn!("Flag check on gateway {} failed: {}", gateway, e);
                return;
            }
        };
        let (lost, roams) = {
            let mut st = self.state.lock().unwrap();
            let Some(g) = st.gateways.get_mut(gateway) else { return };
            g.enabled = flags.enabled;
            g.last_flag_check = Instant::now();
            (flags.enabled & ows::MANAGED_FLAGS != g.wanted, g.can_roam())
        };
        if lost {
            warn!("Gateway {} lost its flags (it restarted?); setting them again", gateway);
            self.ensure_flags(gateway).await;
            if roams {
                self.refresh_table(gateway).await;
            }
        }
    }

    /// What is known about one gateway's OpenWyzeSense firmware, for the
    /// dongle list; `None` for a stock dongle (or one not probed yet).
    pub fn gateway_json(&self, gateway: &str) -> Option<serde_json::Value> {
        let st = self.state.lock().unwrap();
        let g = st.gateways.get(gateway)?;
        Some(serde_json::json!({
            "version": g.build.as_ref().map(|b| b.version.clone()),
            "build": g.build,
            "flags_supported": format!("{:08X}", g.supported),
            "flags_enabled": format!("{:08X}", g.enabled),
            "flags_configured": format!("{:08X}", g.wanted),
            "roaming": g.can_roam() && st.is_roaming(g.identity),
            "network": g.identity.to_string(),
        }))
    }

    /// For the dashboard / API.
    pub fn status_json(&self) -> serde_json::Value {
        let st = self.state.lock().unwrap();
        let now = Instant::now();
        let gateways: Vec<_> = st
            .gateways
            .iter()
            .map(|(mac, g)| {
                serde_json::json!({
                    "mac": mac,
                    "online": g.online,
                    "network": g.identity.to_string(),
                    "roaming": g.can_roam() && st.is_roaming(g.identity),
                    "build": g.build,
                    "flags": format!("{:08X}", g.enabled),
                    "configured": format!("{:08X}", g.wanted),
                    "supported": format!("{:08X}", g.supported),
                    "sensors": g.table.iter().map(|s| serde_json::json!({
                        "mac": s.mac, "short": format!("{:04X}", s.short), "type": s.sensor_type,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        let links: serde_json::Map<String, serde_json::Value> = st
            .tracker
            .links
            .iter()
            .map(|(sensor, links)| {
                let heard: serde_json::Map<String, serde_json::Value> = links
                    .iter()
                    .map(|(gw, l)| {
                        (gw.clone(), serde_json::json!({
                            "rssi": (l.rssi * 10.0).round() / 10.0,
                            "samples": l.samples,
                            "age_s": now.duration_since(l.last).as_secs(),
                        }))
                    })
                    .collect();
                (sensor.clone(), serde_json::Value::Object(heard))
            })
            .collect();
        let evidence: serde_json::Map<String, serde_json::Value> = st
            .tracker
            .evidence
            .iter()
            .map(|(sensor, e)| {
                (sensor.clone(), serde_json::json!({
                    "missed_in_a_row": e.missed,
                    "bursts_recently": format!("{} of last {}", e.bursts, e.recent.len()),
                    "most_copies_when_answered": e.learned_copies,
                }))
            })
            .collect();
        let conflicts: serde_json::Map<String, serde_json::Value> = st
            .conflicts
            .iter()
            .filter(|(_, (_, t))| now.duration_since(*t) < Duration::from_secs(24 * 3600))
            .map(|(sensor, ((short, other, gw), _))| {
                (
                    sensor.clone(),
                    serde_json::json!({ "short": short.map(|s| format!("{:04X}", s)), "held_by": other, "gateway": gw }),
                )
            })
            .collect();
        serde_json::json!({ "gateways": gateways, "heard": links, "evidence": evidence, "address_conflicts": conflicts })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RoamingConfig {
        RoamingConfig::default()
    }

    fn link(rssi: f32, samples: u32, age: Duration, now: Instant) -> Link {
        Link { rssi, samples, last: now - age }
    }

    fn links(entries: &[(&str, Link)]) -> HashMap<String, Link> {
        entries.iter().map(|(g, l)| (g.to_string(), l.clone())).collect()
    }

    fn ev(missed: u32, bursts: u32) -> Evidence {
        Evidence { missed, bursts, learned_copies: 0, recent: vec![] }
    }

    // `Instant` cannot go below its origin; leave room to subtract ages.
    fn later() -> Instant {
        Instant::now() + Duration::from_secs(100_000)
    }

    const S: Duration = Duration::from_secs(5);

    #[test]
    fn stronger_gateway_takes_the_sensor_only_past_the_hysteresis() {
        let now = later();
        let l = links(&[("A", link(-80.0, 5, S, now)), ("B", link(-65.0, 5, S, now))]);
        assert_eq!(decide("A", &l, &ev(0, 0), |_| true, None, now, &cfg()), Some(("B".into(), Reason::StrongerSignal)));
        let l = links(&[("A", link(-80.0, 5, S, now)), ("B", link(-72.0, 5, S, now))]);
        assert_eq!(decide("A", &l, &ev(0, 0), |_| true, None, now, &cfg()), None);
    }

    #[test]
    fn signal_needs_sustained_reports_and_rests_after_a_handover() {
        let now = later();
        let l = links(&[("A", link(-80.0, 5, S, now)), ("B", link(-50.0, 4, S, now))]);
        assert_eq!(decide("A", &l, &ev(0, 0), |_| true, None, now, &cfg()), None, "too few reports at B");
        let l = links(&[("A", link(-80.0, 4, S, now)), ("B", link(-50.0, 5, S, now))]);
        assert_eq!(decide("A", &l, &ev(0, 0), |_| true, None, now, &cfg()), None, "too few at the owner");
        let l = links(&[("A", link(-80.0, 5, S, now)), ("B", link(-50.0, 5, S, now))]);
        let recent = Some(now - Duration::from_secs(600));
        assert_eq!(decide("A", &l, &ev(0, 0), |_| true, recent, now, &cfg()), None, "within the cooldown");
        assert_eq!(decide("A", &l, &ev(0, 0), |g| g != "B", None, now, &cfg()), None, "B not eligible");
    }

    #[test]
    fn evidence_moves_the_sensor_to_the_best_candidate() {
        let now = later();
        let l = links(&[("A", link(-60.0, 5, S, now)), ("B", link(-85.0, 3, S, now)), ("C", link(-70.0, 3, S, now))]);
        assert_eq!(decide("A", &l, &ev(3, 0), |_| true, None, now, &cfg()), Some(("C".into(), Reason::OwnerMissing)));
        assert_eq!(decide("A", &l, &ev(2, 0), |_| true, None, now, &cfg()), None);
        assert_eq!(decide("A", &l, &ev(0, 3), |_| true, None, now, &cfg()), Some(("C".into(), Reason::NotAcked)));
        assert_eq!(decide("A", &l, &ev(0, 2), |_| true, None, now, &cfg()), None);
        // not right after another handover, but well within the signal cooldown
        assert_eq!(decide("A", &l, &ev(3, 0), |_| true, Some(now - Duration::from_secs(10)), now, &cfg()), None);
        assert!(decide("A", &l, &ev(3, 0), |_| true, Some(now - Duration::from_secs(120)), now, &cfg()).is_some());
    }

    fn key(seq: u64) -> ReportKey {
        ("S".to_string(), 0xA2, seq)
    }

    /// One report: `owner_copies` from the owner, `other_copies` overheard by B.
    fn report(t: &mut Tracker, seq: u64, owner_online: bool, owner_copies: u32, other_copies: u32, now: Instant) -> Evidence {
        for _ in 0..owner_copies {
            t.note_copy(&key(seq), "A", "A", owner_online, true, now);
        }
        for _ in 0..other_copies {
            t.note_copy(&key(seq), "B", "A", owner_online, false, now);
        }
        t.settle(now + Duration::from_secs(10), Duration::from_secs(5), 4, 5);
        t.evidence("S")
    }

    #[test]
    fn a_connected_owner_missing_reports_others_hear() {
        let now = later();
        let mut t = Tracker::default();
        assert_eq!(report(&mut t, 1, true, 0, 1, now).missed, 1);
        assert_eq!(report(&mut t, 2, true, 0, 3, now).missed, 2);
        assert_eq!(report(&mut t, 3, true, 1, 1, now).missed, 0, "delivered: the streak ends");
        assert_eq!(report(&mut t, 4, true, 0, 1, now).missed, 1);
        // the owner's copy may come after the overheard one: still delivered
        t.note_copy(&key(5), "B", "A", true, false, now);
        t.note_copy(&key(5), "A", "A", true, true, now);
        t.settle(now + Duration::from_secs(10), Duration::from_secs(5), 4, 5);
        assert_eq!(t.evidence("S").missed, 0);
    }

    #[test]
    fn reports_are_judged_only_once_their_copies_are_in() {
        let now = later();
        let mut t = Tracker::default();
        t.note_copy(&key(1), "B", "A", true, false, now);
        assert!(t.settle(now + Duration::from_secs(1), Duration::from_secs(5), 4, 5).is_empty());
        t.note_copy(&key(1), "A", "A", true, true, now + Duration::from_secs(1));
        assert_eq!(t.settle(now + Duration::from_secs(6), Duration::from_secs(5), 4, 5), vec!["S".to_string()]);
        assert_eq!(t.evidence("S").missed, 0);
    }

    #[test]
    fn a_cut_off_owner_loses_the_sensor_only_to_repeated_bursts() {
        let now = later();
        let mut t = Tracker::default();
        // While A answered, the sensor sometimes sent 3 copies (lost ACKs).
        report(&mut t, 1, true, 3, 3, now);
        assert_eq!(t.evidence("S").learned_copies, 3);
        // A cut off from the host: single copies (A's radio ACKs) are no bursts,
        // nor are 3 copies, which this sensor sends when answered
        for seq in 2..=10 {
            assert_eq!(report(&mut t, seq, false, 0, 1 + seq as u32 % 3, now).bursts, 0);
        }
        // Nobody answers: what B heard of a stock v2 contact's bursts on
        // hardware, B far away and missing some copies (6, 6, 4, 3, 2, 5)
        let heard = [6, 6, 4, 3, 2, 5];
        let bursts: Vec<u32> = heard.iter().enumerate().map(|(i, n)| report(&mut t, 11 + i as u64, false, 0, *n, now).bursts).collect();
        assert_eq!(bursts, vec![1, 2, 3, 3, 3, 3]);
        assert_eq!(t.evidence("S").recent.len(), 5, "a window of the last 5");
    }

    #[test]
    fn the_burst_bar_rises_above_what_the_sensor_sends_when_answered() {
        let now = later();
        let mut t = Tracker::default();
        assert_eq!(report(&mut t, 1, false, 0, 4, now).bursts, 1, "4 copies with nothing learned yet");
        let mut t = Tracker::default();
        report(&mut t, 1, true, 5, 5, now);
        assert_eq!(report(&mut t, 2, false, 0, 5, now).bursts, 0, "5 copies are normal for this sensor");
        assert_eq!(report(&mut t, 3, false, 0, 6, now).bursts, 1);
    }

    #[test]
    fn bursts_count_even_while_the_owner_looks_connected() {
        let now = later();
        let mut t = Tracker::default();
        let ev = report(&mut t, 1, true, 0, 6, now);
        assert_eq!((ev.missed, ev.bursts), (1, 1));
        report(&mut t, 2, true, 0, 4, now);
        let ev = report(&mut t, 3, true, 0, 5, now);
        assert_eq!((ev.missed, ev.bursts), (3, 3));
        // both rules hold: bursts win, so an unreachable old owner is no obstacle
        let l = links(&[("A", link(-60.0, 5, S, now)), ("B", link(-40.0, 3, S, now))]);
        assert_eq!(decide("A", &l, &ev, |_| true, None, now, &cfg()), Some(("B".into(), Reason::NotAcked)));
        // the owner delivering a report clears both
        let ev = report(&mut t, 4, true, 1, 1, now);
        assert_eq!((ev.missed, ev.bursts), (0, 0));
    }

    #[test]
    fn tracker_smooths_rssi() {
        let now = later();
        let mut t = Tracker::default();
        t.record("S", "A", -60, now);
        assert_eq!(t.links("S").unwrap()["A"].rssi, -60.0);
        t.record("S", "A", -70, now);
        assert!((t.links("S").unwrap()["A"].rssi - -63.0).abs() < 0.01);
        t.reset("S");
        assert_eq!(t.links("S").unwrap()["A"].samples, 0);
    }

    #[test]
    fn copies_inside_the_window_are_duplicates() {
        let now = later();
        let w = Duration::from_secs(30);
        let mut t = Tracker::default();
        let key = ("S".to_string(), 0xA1, 7);
        assert!(!t.is_duplicate(key.clone(), None, now, w));
        assert!(t.is_duplicate(key.clone(), None, now + Duration::from_secs(1), w));
        // a heartbeat repeats the counter: a later one is a new report
        assert!(!t.is_duplicate(key, None, now + Duration::from_secs(60), w));
    }

    #[test]
    fn stale_replays_are_dropped_by_counter() {
        let now = later();
        let w = Duration::from_secs(30);
        let mut t = Tracker::default();
        let k = |seq: u16| ("S".to_string(), 0xA2u8, seq as u64);
        assert!(!t.is_duplicate(k(5), Some(5), now, w));
        assert!(!t.is_duplicate(k(6), Some(6), now, w));
        // an hour later the old owner's relay sends on its buffer
        let later = now + Duration::from_secs(3600);
        assert!(t.is_duplicate(k(5), Some(5), later, w));
        assert!(t.is_duplicate(k(6), Some(6), later, w));
        assert!(!t.is_duplicate(k(7), Some(7), later, w));
        // the counter wraps
        let mut t = Tracker::default();
        assert!(!t.is_duplicate(k(0xFFFF), Some(0xFFFF), now, w));
        assert!(!t.is_duplicate(k(0), Some(0), now, w));
        // re-paired: counters start over
        t.forget_counters("S");
        assert!(!t.is_duplicate(k(1), Some(1), later, w));
    }
}
