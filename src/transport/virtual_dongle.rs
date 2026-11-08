//! Virtual dongle for integration testing.
//!
//! A `VirtualDongle` wraps a `ReplayTransport` and operates at the **protocol level**
//! rather than the byte level. It maintains internal state (MAC, version, NVRAM sensor list)
//! and automatically pre-registers handshake and operational command responses, so tests
//! never need to hand-craft hex packets.
//!
//! # Example
//! ```no_run
//! use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;
//!
//! let dongle = VirtualDongle::new("DONGLE_A", "V2.3.9")
//!     .with_sensors(vec!["SENSOR01".into(), "SENSOR02".into()]);
//! let transport = dongle.transport();
//! // ... plug into Engine
//! ```

use super::replay::ReplayTransport;
use super::GatewayTransport;
use crate::protocol::packet::{commands, Packet};
use crate::protocol::ows::{self, NetworkIdentity, NvSensor};
use crate::protocol::telemetry::{DongleEvent, SensorType};
use std::sync::{Arc, Mutex};

/// State of an emulated OpenWyzeSense gateway (see [`VirtualDongle::with_ows`]).
#[derive(Debug, Clone)]
pub struct OwsSim {
    pub flags: u32,
    pub supported: u32,
    pub range: (u16, u16),
    pub network: NetworkIdentity,
    pub table: Vec<NvSensor>,
    /// Table changes in order: "delete <mac>", "add <mac> <short>".
    pub log: Vec<String>,
    /// Lost power: answers nothing (the connection may look alive).
    pub dead: bool,
    /// Sensor-update offers staged (MACs), in order; cleared by a cancel.
    pub ota_offers: Vec<String>,
    /// Sensor-update blocks received: (offset, bytes).
    pub ota_data: Vec<(u32, Vec<u8>)>,
    /// Cancels received (an `********` offer with an empty body).
    pub ota_cancels: usize,
}

/// A virtual Wyze Sense dongle for integration testing.
///
/// Provides a protocol-level mock that automatically responds to handshake,
/// scan, verify, delete, and sensor list commands.
#[derive(Clone)]
pub struct VirtualDongle {
    transport: ReplayTransport,
    mac: String,
    #[allow(dead_code)]
    version: String,
    nvram_sensors: Vec<String>,
    ows: Option<Arc<Mutex<OwsSim>>>,
}

impl VirtualDongle {
    /// Create a new virtual dongle with the given MAC address and firmware version.
    ///
    /// Automatically registers responses for:
    /// - Handshake (Inquiry, ENR, MAC, Version, FinishAuth)
    /// - Scan enable/disable (2 responses pre-registered)
    /// - Verify sensor
    /// - Delete sensor
    /// - Play chime
    pub fn new(mac: &str, version: &str) -> Self {
        let transport = ReplayTransport::new();

        // --- Handshake responses ---

        // 1. Inquiry → InquiryResponse (0x4328): payload = [0x01] (success)
        let inquiry_resp = Packet::new_sync(0x28, vec![0x01]);
        transport.register_response(commands::CMD_INQUIRY, inquiry_resp.to_bytes());

        // 2. GetENR → ENRResponse (0x4303): payload = 16 bytes of ENR token
        let enr_resp = Packet::new_sync(0x03, vec![0x31; 16]);
        transport.register_response(commands::CMD_GET_ENR, enr_resp.to_bytes());

        // 3. GetMAC → MACResponse (0x4305): payload = MAC as ASCII bytes
        let mac_bytes = mac.as_bytes().to_vec();
        let mac_resp = Packet::new_sync(0x05, mac_bytes);
        transport.register_response(commands::CMD_GET_MAC, mac_resp.to_bytes());

        // 4. GetVersion → VersionResponse (0x5317): payload = version string bytes
        let ver_bytes = version.as_bytes().to_vec();
        let ver_resp = Packet::new_async(0x17, ver_bytes);
        // Every time: it is also how a session checks the radio chip is alive.
        transport.register_persistent_response(commands::CMD_GET_VERSION, ver_resp.to_bytes());

        // 5. FinishAuth → FinishAuthResponse (0x5315): payload = [0x01] (success)
        let auth_resp = Packet::new_async(0x15, vec![]);
        transport.register_response(commands::CMD_FINISH_AUTH, auth_resp.to_bytes());

        // --- Operational responses ---

        // Scan enable (first call)
        let scan_on = Packet::new_async(0x1D, vec![0x01]);
        transport.register_response(commands::CMD_SET_SCAN, scan_on.to_bytes());

        // Scan disable (second and third calls, e.g. auto/manual verify + explicit disable)
        let scan_off = Packet::new_async(0x1D, vec![0x00]);
        transport.register_response(commands::CMD_SET_SCAN, scan_off.to_bytes());
        transport.register_response(commands::CMD_SET_SCAN, scan_off.to_bytes());

        // Get sensor R1 response (success)
        let r1_resp = Packet::new_async(0x22, vec![0x42; 16]);
        transport.register_response(commands::CMD_GET_R1, r1_resp.to_bytes());
        transport.register_response(commands::CMD_GET_R1, r1_resp.to_bytes());

        // Verify sensor response (success)
        let verify_resp = Packet::new_async(0x24, vec![0x01]);
        transport.register_response(commands::CMD_VERIFY_SENSOR, verify_resp.to_bytes());
        transport.register_response(commands::CMD_VERIFY_SENSOR, verify_resp.to_bytes());

        // Delete sensor response (success)
        let delete_resp = Packet::new_async(0x26, vec![0x01]);
        transport.register_response(commands::CMD_DELETE_SENSOR, delete_resp.to_bytes());

        // Play chime response (success)
        let chime_resp = Packet::new_async(0x71, vec![0x01]);
        transport.register_response(commands::CMD_PLAY_CHIME, chime_resp.to_bytes());

        Self {
            transport,
            mac: mac.to_string(),
            version: version.to_string(),
            nvram_sensors: Vec::new(),
            ows: None,
        }
    }

    /// Makes this dongle answer like an OpenWyzeSense gateway with roaming
    /// support: flags, network identity, sensor table (get / add, and the
    /// stock delete), address range. The table also feeds the stock sensor list.
    pub fn with_ows(self, network: NetworkIdentity, table: Vec<NvSensor>) -> Self {
        let mut me = self.with_sensors(table.iter().map(|s| s.mac.clone()).collect());
        let sim = Arc::new(Mutex::new(OwsSim {
            flags: 0,
            supported: ows::FLAG_DEDUP | ows::FLAG_FIX_ZERO_MAC | ows::FLAG_OTA | ows::FLAG_ROAM,
            range: (ows::ADDR_FIRST, ows::ADDR_LAST),
            network,
            table,
            log: Vec::new(),
            dead: false,
            ota_offers: Vec::new(),
            ota_data: Vec::new(),
            ota_cancels: 0,
        }));
        let state = Arc::clone(&sim);
        me.transport.set_handler(Box::new(move |pkt| Self::ows_answer(&mut state.lock().unwrap(), pkt)));
        me.ows = Some(sim);
        me
    }

    /// The emulated gateway restarts without the host noticing: flags and
    /// address range go back to their defaults, the table (flash) stays.
    pub fn ows_restart(&self) {
        let mut sim = self.ows.as_ref().expect("not an OWS dongle").lock().unwrap();
        sim.flags = 0;
        sim.range = (ows::ADDR_FIRST, ows::ADDR_LAST);
    }

    /// A snapshot of the emulated gateway's state.
    pub fn ows(&self) -> OwsSim {
        self.ows.as_ref().expect("not an OWS dongle").lock().unwrap().clone()
    }

    /// The radio chip hangs: async (0x53xx) commands go unanswered, while the
    /// USB chip keeps answering the sync ones and the device stays plugged in
    /// (OpenWyzeSense #85).
    pub fn radio_hang(&self) {
        self.transport.set_async_silent(true);
    }

    /// The radio chip runs again (after a reset).
    pub fn radio_recover(&self) {
        self.transport.set_async_silent(false);
    }

    /// The emulated gateway loses power but its connection stays up (as a
    /// relay's half-open WebSocket does until the heartbeat notices).
    pub fn ows_power_off(&self) {
        self.ows.as_ref().expect("not an OWS dongle").lock().unwrap().dead = true;
    }

    fn ows_answer(sim: &mut OwsSim, pkt: &Packet) -> Option<Vec<u8>> {
        if sim.dead {
            return Some(Vec::new());
        }
        let p = pkt.payload_bytes()?;
        if pkt.cmd() == commands::CMD_DELETE_SENSOR {
            let mac = String::from_utf8_lossy(p).to_string();
            sim.table.retain(|s| s.mac != mac);
            sim.log.push(format!("delete {}", mac));
            return Some(Packet::new_async(0x26, vec![0x01]).to_bytes());
        }
        if pkt.cmd() != ows::CMD_OWS || p.is_empty() {
            return None;
        }
        let count = sim.table.len() as u8;
        let reply = match (p[0], p.len()) {
            (ows::SUB_FLAGS, n) => {
                if n == 5 {
                    sim.flags = u32::from_be_bytes([p[1], p[2], p[3], p[4]]) & sim.supported;
                }
                let mut r = vec![ows::SUB_FLAGS];
                r.extend_from_slice(&sim.flags.to_be_bytes());
                r.extend_from_slice(&sim.supported.to_be_bytes());
                r
            }
            (ows::SUB_NETWORK, 1) => {
                let mut r = vec![ows::SUB_NETWORK, ows::STATUS_OK];
                r.extend_from_slice(&sim.network.pan.to_le_bytes());
                r.extend_from_slice(&sim.network.short.to_le_bytes());
                r.extend_from_slice(&sim.network.ext.to_le_bytes());
                r
            }
            (ows::SUB_NV, 3) if p[1] == ows::NV_GET_SENSOR => match sim.table.get(p[2] as usize) {
                Some(s) => {
                    let mut r = vec![ows::SUB_NV, ows::NV_GET_SENSOR, ows::STATUS_OK, count, p[2], p[2], 0];
                    r.extend_from_slice(s.mac.as_bytes());
                    r.extend_from_slice(&s.short.to_le_bytes());
                    r.push(s.sensor_type);
                    r
                }
                None => vec![ows::SUB_NV, ows::NV_GET_SENSOR, 0xE0, count, p[2]],
            },
            (ows::SUB_NV, 13) if p[1] == ows::NV_ADD_SENSOR => {
                let mac = String::from_utf8_lossy(&p[2..10]).to_string();
                let short = u16::from_le_bytes([p[10], p[11]]);
                if sim.table.iter().any(|s| s.short == short && s.mac != mac) {
                    vec![ows::SUB_NV, ows::NV_ADD_SENSOR, 0xE0, count]
                } else {
                    sim.table.retain(|s| s.mac != mac);
                    sim.table.push(NvSensor { mac: mac.clone(), short, sensor_type: p[12] });
                    sim.log.push(format!("add {} {:04X}", mac, short));
                    vec![ows::SUB_NV, ows::NV_ADD_SENSOR, ows::STATUS_OK, sim.table.len() as u8]
                }
            }
            (ows::SUB_ADDR_RANGE, n) => {
                let mut status = ows::STATUS_OK;
                if n == 5 {
                    let (first, last) = (u16::from_le_bytes([p[1], p[2]]), u16::from_le_bytes([p[3], p[4]]));
                    if (ows::ADDR_FIRST..=ows::ADDR_LAST).contains(&first) && first <= last && last <= ows::ADDR_LAST {
                        sim.range = (first, last);
                    } else {
                        status = 0xE0;
                    }
                }
                let mut r = vec![ows::SUB_ADDR_RANGE, status];
                r.extend_from_slice(&sim.range.0.to_le_bytes());
                r.extend_from_slice(&sim.range.1.to_le_bytes());
                r
            }
            (ows::SUB_OTA_OFFER, 33) => {
                let mac = String::from_utf8_lossy(&p[1..9]).to_string();
                if mac == "********" && p[9..].iter().all(|&b| b == 0) {
                    sim.ota_offers.clear();
                    sim.ota_cancels += 1;
                    vec![ows::SUB_OTA_OFFER, ows::STATUS_OK]
                } else if sim.flags & ows::FLAG_OTA == 0 {
                    vec![ows::SUB_OTA_OFFER, 0xE0]
                } else {
                    sim.ota_offers.push(mac);
                    vec![ows::SUB_OTA_OFFER, ows::STATUS_OK]
                }
            }
            (ows::SUB_OTA_DATA, n) if n > 5 => {
                sim.ota_data.push((u32::from_be_bytes([p[1], p[2], p[3], p[4]]), p[5..].to_vec()));
                return Some(Vec::new());
            }
            _ => return None,
        };
        Some(Packet::new_async((ows::CMD_OWS_RESPONSE & 0xFF) as u8, reply).to_bytes())
    }

    /// A sensor (that took an update offer) asks for a block (`0x53F1` sub `0x11`).
    pub fn inject_ota_need(&self, mac: &str, slot: u8, offset: u32, len: u8) {
        let mut p = vec![ows::SUB_OTA_NEED];
        p.extend_from_slice(mac.as_bytes());
        p.push(slot);
        p.extend_from_slice(&offset.to_be_bytes());
        p.push(len);
        self.transport.enqueue_read(&Packet::new_async((ows::CMD_OWS_RESPONSE & 0xFF) as u8, p).to_bytes());
    }

    /// A sensor reports how its update ended (`0x53F1` sub `0x13`; 0 ok).
    pub fn inject_ota_done(&self, mac: &str, status: u8) {
        let mut p = vec![ows::SUB_OTA_DONE];
        p.extend_from_slice(mac.as_bytes());
        p.push(status);
        p.extend_from_slice(&[0; 4]);
        self.transport.enqueue_read(&Packet::new_async((ows::CMD_OWS_RESPONSE & 0xFF) as u8, p).to_bytes());
    }

    /// Inject the gateway's own report (`0x5319`) with an event counter.
    pub fn inject_report(&self, mac: &str, sensor_type: SensorType, state: u8, seq: u16, rssi: u8) {
        let payload = self.report_payload(mac, sensor_type, state, seq, rssi);
        self.transport.enqueue_read(&Packet::new_async(0x19, payload).to_bytes());
    }

    /// Inject a report this gateway overheard (`0x53F1` sub `0x20`).
    pub fn inject_overheard(&self, short: u16, mac: &str, sensor_type: SensorType, state: u8, seq: u16, rssi: u8) {
        let mut payload = vec![ows::SUB_OVERHEARD];
        payload.extend_from_slice(&short.to_le_bytes());
        payload.extend_from_slice(&self.report_payload(mac, sensor_type, state, seq, rssi));
        self.transport.enqueue_read(&Packet::new_async((ows::CMD_OWS_RESPONSE & 0xFF) as u8, payload).to_bytes());
    }

    fn report_payload(&self, mac: &str, sensor_type: SensorType, state: u8, seq: u16, rssi: u8) -> Vec<u8> {
        let [hi, lo] = seq.to_be_bytes();
        self.build_alarm1_payload(
            DongleEvent::EVENT_TYPE_ALARM,
            mac,
            sensor_type,
            &[0x01, 0x5A, 0x00, 0x01, state, hi, lo, rssi],
        )
    }

    /// Pre-populate the dongle's NVRAM with paired sensor MACs.
    /// These will be returned via the two-phase get_sensor_count + get_sensor_list protocol.
    pub fn with_sensors(mut self, sensors: Vec<String>) -> Self {
        let count = sensors.len() as u8;

        // Phase 1: Sensor count response (0x532F): payload = [count]
        let count_resp = Packet::new_async(0x2F, vec![count]);
        self.transport.register_response(commands::CMD_GET_SENSOR_COUNT, count_resp.to_bytes());

        // Phase 2: Build a COMBINED response for CMD_GET_SENSOR_LIST.
        //
        // When the engine sends 0x5330, the transport fires the auto-response.
        // We concatenate the ACK packet + all 0x5331 sensor list item packets
        // into a single byte sequence. The engine's reader loop will parse them
        // sequentially from the read queue.
        //
        // This avoids the race condition where items enqueued directly get
        // consumed before get_sensor_list() starts.
        let list_ack = Packet::new_async(0x30, vec![count]);
        let mut combined = list_ack.to_bytes();

        for mac_str in &sensors {
            // Sensor list item payload is just the MAC as ASCII bytes.
            // The engine's reader loop does `String::from_utf8(bytes)` on the
            // full payload, so we must NOT include type/version suffix.
            let mac_bytes = mac_str.as_bytes();
            let mut payload = Vec::new();
            payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
            // Pad to 8 bytes if MAC is shorter
            while payload.len() < 8 {
                payload.push(b'0');
            }

            let item_pkt = Packet::new_async(0x31, payload);
            combined.extend_from_slice(&item_pkt.to_bytes());
        }

        self.transport.register_response(commands::CMD_GET_SENSOR_LIST, combined);

        self.nvram_sensors = sensors;
        self
    }

    /// Get a `GatewayTransport::Replay` wrapping this dongle's transport.
    pub fn transport(&self) -> GatewayTransport {
        GatewayTransport::Replay(self.transport.clone())
    }

    /// Get a reference to the underlying `ReplayTransport` for advanced operations.
    pub fn replay_transport(&self) -> &ReplayTransport {
        &self.transport
    }

    /// Get the dongle MAC address.
    pub fn mac(&self) -> &str {
        &self.mac
    }

    /// Simulate a transport disconnect (broken pipe).
    pub fn disconnect(&self) {
        self.transport.disconnect();
    }

    /// Register additional scan enable/disable responses (needed if tests toggle scan
    /// more than once beyond the initial pair).
    pub fn register_extra_scan_responses(&self, enable_count: usize, disable_count: usize) {
        for _ in 0..enable_count {
            let resp = Packet::new_async(0x1D, vec![0x01]);
            self.transport.register_response(commands::CMD_SET_SCAN, resp.to_bytes());
        }
        for _ in 0..disable_count {
            let resp = Packet::new_async(0x1D, vec![0x00]);
            self.transport.register_response(commands::CMD_SET_SCAN, resp.to_bytes());
        }
    }

    /// Register additional verify sensor responses.
    pub fn register_extra_verify_responses(&self, count: usize) {
        for _ in 0..count {
            let resp = Packet::new_async(0x24, vec![0x01]);
            self.transport.register_response(commands::CMD_VERIFY_SENSOR, resp.to_bytes());
        }
    }

    /// Register additional delete sensor responses.
    pub fn register_extra_delete_responses(&self, count: usize) {
        for _ in 0..count {
            let resp = Packet::new_async(0x26, vec![0x01]);
            self.transport.register_response(commands::CMD_DELETE_SENSOR, resp.to_bytes());
        }
    }

    // -----------------------------------------------------------------------
    // Event injection — simulate spontaneous sensor events
    // -----------------------------------------------------------------------

    /// Inject a sensor scan event (simulates a sensor in pairing mode).
    /// This injects a CMD_SENSOR_SCAN (0x5320) packet.
    pub fn inject_scan_event(&self, mac: &str, sensor_type: SensorType, version: u8) {
        let mut payload = Vec::new();
        payload.push(0xA3); // scan event type

        let mac_bytes = mac.as_bytes();
        payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
        while payload.len() < 9 { // 1 byte event_type + 8 bytes MAC
            payload.push(b'0');
        }
        payload.push(sensor_type.to_u8());
        payload.push(version);

        let pkt = Packet::new_async(0x20, payload);
        self.transport.enqueue_read(&pkt.to_bytes());
    }

    /// Inject an alarm event (door open/close, motion detected).
    /// This injects a CMD_ALARM1 (0x5319) packet.
    ///
    /// * `state` — 0 = inactive/closed, 1 = active/open
    /// * `battery` — raw battery byte (voltage = battery/32.0)
    /// * `rssi` — positive value, will be negated by parser to dBm
    pub fn inject_alarm_event(
        &self, mac: &str, sensor_type: SensorType,
        state: u8, battery: u8, rssi: u8,
    ) {
        let payload = self.build_alarm1_payload(
            DongleEvent::EVENT_TYPE_ALARM,
            mac, sensor_type,
            &[0x01, battery, 0x00, 0x00, state, 0x00, 0x0A, rssi],
        );
        let pkt = Packet::new_async(0x19, payload);
        self.transport.enqueue_read(&pkt.to_bytes());
    }

    /// Inject a heartbeat event.
    pub fn inject_heartbeat_event(
        &self, mac: &str, sensor_type: SensorType,
        battery: u8, rssi: u8,
    ) {
        let payload = self.build_alarm1_payload(
            DongleEvent::EVENT_TYPE_HEARTBEAT,
            mac, sensor_type,
            &[0x02, battery, 0x00, 0x00, 0x00, 0x00, 0x0B, rssi],
        );
        let pkt = Packet::new_async(0x19, payload);
        self.transport.enqueue_read(&pkt.to_bytes());
    }

    /// Inject a climate event (temperature + humidity).
    pub fn inject_climate_event(
        &self, mac: &str, temperature: f32, humidity: u8,
        battery: u8, rssi: u8,
    ) {
        let temp_hi = temperature.trunc() as i8;
        let temp_lo = ((temperature.fract()) * 100.0) as u8;

        let remaining = vec![
            0x01,       // die temperature
            battery,
            0x00,       // marker
            0x03,       // marker for climate
            temp_hi as u8,
            temp_lo,
            humidity,
            0x00, 0x0C, // event sequence
            rssi,
        ];

        let payload = self.build_alarm1_payload(
            DongleEvent::EVENT_TYPE_CLIMATE,
            mac, SensorType::ClimateV2,
            &remaining,
        );
        let pkt = Packet::new_async(0x19, payload);
        self.transport.enqueue_read(&pkt.to_bytes());
    }

    /// Inject a leak event.
    /// Uses CMD_ALARM2 (0x5355) format.
    ///
    /// * `state` — 0 = dry, 1 = wet
    /// * `probe_available` — whether the external probe is connected
    /// * `probe_state` — 0 = dry, 1 = wet (only meaningful if probe_available)
    pub fn inject_leak_event(
        &self, mac: &str, state: u8, probe_available: bool,
        probe_state: u8, battery: u8, rssi: u8,
    ) {
        let mut payload = Vec::new();
        payload.push(DongleEvent::EVENT_TYPE_LEAK);

        let mac_bytes = mac.as_bytes();
        payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
        while payload.len() < 9 { // 1 byte event_type + 8 bytes MAC
            payload.push(b'0');
        }
        payload.push(SensorType::LeakV2.to_u8());

        // remaining[0..1] = unk
        payload.push(0x00);
        payload.push(0x00);
        // remaining[2] = battery
        payload.push(battery);
        // remaining[3..4] = unk
        payload.push(0x00);
        payload.push(0x00);
        // remaining[5] = state
        payload.push(state);
        // remaining[6] = probe_state
        payload.push(probe_state);
        // remaining[7] = probe_available
        payload.push(if probe_available { 1 } else { 0 });
        // remaining[8..9] = unk
        payload.push(0x00);
        payload.push(0x00);
        // remaining[10] = rssi
        payload.push(rssi);

        let pkt = Packet::new_async(0x55, payload);
        self.transport.enqueue_read(&pkt.to_bytes());
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Build the alarm1 (0x5319) payload format:
    /// [timestamp_8B][event_type][mac_8B][sensor_type][remaining...]
    fn build_alarm1_payload(
        &self, event_type: u8, mac: &str, sensor_type: SensorType,
        remaining: &[u8],
    ) -> Vec<u8> {
        let mut payload = Vec::new();

        // Timestamp: 8 bytes BE (use a fixed fake timestamp for determinism)
        let fake_ts: u64 = 0x0000018FAB8E8C00;
        payload.extend_from_slice(&fake_ts.to_be_bytes());

        // Event type
        payload.push(event_type);

        // MAC as 8 ASCII bytes
        let mac_bytes = mac.as_bytes();
        payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
        while payload.len() < 17 { // 8 bytes ts + 1 event_type + 8 MAC
            payload.push(b'0');
        }

        // Sensor type
        payload.push(sensor_type.to_u8());

        // Remaining bytes (die_temp, battery, flags, state, seq, rssi)
        payload.extend_from_slice(remaining);

        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::protocol::telemetry::{DongleEvent, TelemetryData};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_virtual_dongle_handshake() {
        let dongle = VirtualDongle::new("TESTMAC1", "V3.0.0");
        let (event_tx, _event_rx) = mpsc::channel::<DongleEvent>(32);
        let mut engine = Engine::new(dongle.transport(), event_tx);
        let _exit_tx = engine.start();

        engine.initialize_handshake().await.unwrap();
        assert_eq!(engine.dongle_mac(), Some("TESTMAC1"));
        assert_eq!(engine.dongle_version(), Some("V3.0.0"));
    }

    #[tokio::test]
    async fn test_virtual_dongle_scan_event() {
        let dongle = VirtualDongle::new("TESTMAC1", "V3.0.0");
        let (event_tx, mut event_rx) = mpsc::channel::<DongleEvent>(32);
        let mut engine = Engine::new(dongle.transport(), event_tx);
        let _exit_tx = engine.start();

        engine.initialize_handshake().await.unwrap();
        engine.set_scan(true).await.unwrap();

        dongle.inject_scan_event("SENSOR01", SensorType::ContactV2, 25);

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            event_rx.recv(),
        ).await.unwrap().unwrap();

        assert_eq!(event.mac, "SENSOR01");
        assert_eq!(event.sensor_type, SensorType::ContactV2);
        assert_eq!(event.data, TelemetryData::Scanned { version: 25 });
    }

    #[tokio::test]
    async fn test_virtual_dongle_alarm_event() {
        let dongle = VirtualDongle::new("TESTMAC1", "V3.0.0");
        let (event_tx, mut event_rx) = mpsc::channel::<DongleEvent>(32);
        let mut engine = Engine::new(dongle.transport(), event_tx);
        let _exit_tx = engine.start();

        engine.initialize_handshake().await.unwrap();

        // Inject alarm: door open, battery=0x5A, rssi=60
        dongle.inject_alarm_event("SENSOR01", SensorType::ContactV2, 1, 0x5A, 60);

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            event_rx.recv(),
        ).await.unwrap().unwrap();

        assert_eq!(event.mac, "SENSOR01");
        match &event.data {
            TelemetryData::Alarm { state, battery, rssi, .. } => {
                assert_eq!(*state, 1);
                assert_eq!(*battery, 0x5A);
                assert_eq!(*rssi, -60);
            }
            other => panic!("Expected Alarm, got {:?}", other),
        }
    }
}
