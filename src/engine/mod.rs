use crate::protocol::packet::{Packet, CommandType, PacketPayload, commands};
use crate::protocol::keypad::KeypadReply;
use crate::protocol::sensor::SensorManager;
use crate::protocol::telemetry::{DongleEvent, SensorType, TelemetryData};
use crate::transport::GatewayTransport;
use crate::transport::AsyncTransport;

use std::collections::{HashMap, HashSet};
use std::io::{Result, Error, ErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Central registry of all connected dongle engines, keyed by dongle MAC address.
/// Shared across the web server, gateway command router, and event router.
pub type EnginesMap = std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, Engine>>>;

type PeerContext = (
    Weak<tokio::sync::Mutex<HashMap<String, Engine>>>,
    Weak<Mutex<SensorManager>>,
);

/// Overall time allowed for `initialize_handshake`, Inquiry retries included.
pub const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// How many times the handshake sends the Inquiry before giving up. One lost
/// packet on a serial line should not cost the whole handshake.
const INQUIRY_ATTEMPTS: u32 = 3;

type PendingRequests = Arc<Mutex<HashMap<u16, oneshot::Sender<Packet>>>>;

pub struct Engine {
    transport: GatewayTransport,
    pending_requests: PendingRequests,
    // Serializes do_command() calls on this dongle. The protocol correlates a
    // response to a request only by response *command type*, not a per-request
    // ID, so two do_command calls racing for the same expected_response_cmd
    // would otherwise clobber each other's slot in `pending_requests` (the
    // loser's oneshot gets dropped, and the winner may receive a response
    // meant for the loser's request). Holding this lock for the full
    // send-then-await-response round trip makes that impossible.
    cmd_lock: Arc<tokio::sync::Mutex<()>>,
    event_tx: mpsc::Sender<DongleEvent>,
    dongle_mac: Option<String>,
    dongle_version: Option<String>,
    exit_tx: Option<oneshot::Sender<()>>,
    auto_verify: Arc<AtomicBool>,
    sensor_list_tx: Arc<Mutex<Option<mpsc::Sender<String>>>>,
    is_scanning: Arc<AtomicBool>,
    // Transport metadata for dashboard display
    pub transport_label: String,     // "local" or "bridge"
    pub device_path: Option<String>, // e.g. /dev/hidraw0
    pub remote_addr: Option<String>, // e.g. 192.168.1.5:43210
    /// How long each handshake Inquiry waits for its response before retrying.
    pub inquiry_timeout: std::time::Duration,
    /// Notified when the engine's transport encounters a fatal error.
    pub disconnect_notify: Arc<tokio::sync::Notify>,
    /// Shared dongle MAC for the reader loop to stamp onto DongleEvents.
    shared_dongle_mac: Arc<Mutex<Option<String>>>,
    /// Sensor MACs currently stored in this dongle's NVRAM.
    paired_sensors: Arc<Mutex<HashSet<String>>>,
    /// Set once `get_sensor_list` has completed for this session.
    sensor_list_loaded: Arc<AtomicBool>,
    /// Optional weak handles to peer engines + SensorManager so auto-verify
    /// can remove a newly paired sensor from any previous dongle once.
    peer_context: Arc<Mutex<Option<PeerContext>>>,
}

impl Engine {
    pub fn new(transport: GatewayTransport, event_tx: mpsc::Sender<DongleEvent>) -> Self {
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));
        let cmd_lock = Arc::new(tokio::sync::Mutex::new(()));
        let auto_verify = Arc::new(AtomicBool::new(false));
        let sensor_list_tx = Arc::new(Mutex::new(None));
        let is_scanning = Arc::new(AtomicBool::new(false));
        let disconnect_notify = Arc::new(tokio::sync::Notify::new());
        let shared_dongle_mac = Arc::new(Mutex::new(None));
        let paired_sensors = Arc::new(Mutex::new(HashSet::new()));
        let sensor_list_loaded = Arc::new(AtomicBool::new(false));
        let peer_context = Arc::new(Mutex::new(None));

        Self {
            transport: transport.clone(),
            pending_requests: Arc::clone(&pending_requests),
            cmd_lock: Arc::clone(&cmd_lock),
            event_tx: event_tx.clone(),
            dongle_mac: None,
            dongle_version: None,
            exit_tx: None,
            auto_verify: Arc::clone(&auto_verify),
            sensor_list_tx: Arc::clone(&sensor_list_tx),
            is_scanning: Arc::clone(&is_scanning),
            transport_label: "local".to_string(),
            device_path: None,
            remote_addr: None,
            inquiry_timeout: std::time::Duration::from_secs(2),
            disconnect_notify: Arc::clone(&disconnect_notify),
            shared_dongle_mac: Arc::clone(&shared_dongle_mac),
            paired_sensors,
            sensor_list_loaded,
            peer_context,
        }
    }

    pub fn dongle_mac(&self) -> Option<&str> {
        self.dongle_mac.as_deref()
    }

    pub fn dongle_version(&self) -> Option<&str> {
        self.dongle_version.as_deref()
    }

    /// Whether this dongle's NVRAM currently holds `mac`.
    pub fn has_paired_sensor(&self, mac: &str) -> bool {
        self.paired_sensors.lock().unwrap().contains(mac)
    }

    /// Whether `get_sensor_list` has populated `paired_sensors` in this session.
    pub fn sensor_list_loaded(&self) -> bool {
        self.sensor_list_loaded.load(Ordering::SeqCst)
    }

    /// Attaches weak handles to the shared `EnginesMap` and `SensorManager` so
    /// auto-pairing can clean up stale records on peer dongles without holding
    /// strong reference cycles.
    pub fn attach_peer_context(
        &self,
        engines: &EnginesMap,
        sensor_manager: &Arc<Mutex<SensorManager>>,
    ) {
        *self.peer_context.lock().unwrap() =
            Some((Arc::downgrade(engines), Arc::downgrade(sensor_manager)));
    }

    fn upgrade_peer_context(&self) -> Option<(EnginesMap, Arc<Mutex<SensorManager>>)> {
        let (engines_w, sm_w) = self.peer_context.lock().unwrap().clone()?;
        Some((engines_w.upgrade()?, sm_w.upgrade()?))
    }

    /// Enables or disables automatic verification for scanned sensors in the background.
    pub fn set_auto_verify(&self, enable: bool) {
        self.auto_verify.store(enable, Ordering::SeqCst);
    }

    /// Starts the background packet listener loop and returns an exit controller.
    pub fn start(&mut self) -> oneshot::Sender<()> {
        let (exit_tx, mut exit_rx) = oneshot::channel();
        self.exit_tx = Some(exit_tx);

        // The auto-verify worker lives as long as the reader loop below, which
        // holds the only sender: when the engine stops, it ends too (and drops
        // its transport clone).
        let (auto_verify_tx, mut auto_verify_rx) = mpsc::channel::<(String, SensorType, u8)>(32);
        let mut engine_clone = self.clone();
        engine_clone.transport_label = "worker".to_string();
        tokio::spawn(async move {
            while let Some((mac, s_type, version)) = auto_verify_rx.recv().await {
                if !engine_clone.auto_verify.load(Ordering::SeqCst) {
                    continue;
                }
                info!("Auto-Pairing: Scanned sensor {} detected (type={:?}, version={}). Exchanging R1 token...", mac, s_type, version);

                // Step 1: Exchange crypto token (R1) with the sensor
                match engine_clone.get_sensor_r1(&mac).await {
                    Ok(r1) => {
                        debug!("Auto-Pairing: GetSensorR1 returned {} bytes for {}", r1.len(), mac);
                    }
                    Err(e) => {
                        error!("Auto-Pairing: Failed R1 exchange for sensor {}: {}", mac, e);
                        continue;
                    }
                }

                // Step 2: Disable scan mode
                info!("Auto-Pairing: Turning off scan mode...");
                let _ = engine_clone.set_scan(false).await;
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                
                // Step 3: Verify and bind sensor to NVRAM
                info!("Auto-Pairing: Committing verification for sensor {} to NVRAM...", mac);
                if let Err(e) = engine_clone.verify_sensor(&mac, s_type).await {
                    error!("Auto-Pairing: Failed to verify sensor {}: {}", mac, e);
                } else {
                    info!("Auto-Pairing: Sensor {} successfully paired & committed to NVRAM!", mac);
                    let dongle_mac = engine_clone.shared_dongle_mac.lock().unwrap().clone();
                    if let (Some(new_dongle), Some((engines, sensor_manager))) =
                        (dongle_mac.as_deref(), engine_clone.upgrade_peer_context())
                    {
                        let _ = detach_sensor_from_previous_dongle(
                            &engines,
                            &sensor_manager,
                            &mac,
                            new_dongle,
                        )
                        .await;
                    }
                    // Emit a synthetic Paired event so SensorManager auto-discovers
                    // and associates this sensor with the correct dongle.
                    let paired_evt = DongleEvent {
                        mac: mac.clone(),
                        timestamp: std::time::SystemTime::now(),
                        sensor_type: s_type,
                        event_type: 0xFE, // synthetic
                        data: TelemetryData::Paired { version },
                        dongle_mac,
                    };
                    let _ = engine_clone.event_tx.send(paired_evt).await;
                }
            }
        });

        let mut transport = self.transport.clone();
        let pending_requests = Arc::clone(&self.pending_requests);
        let event_tx = self.event_tx.clone();
        let sensor_list_tx = Arc::clone(&self.sensor_list_tx);
        let shared_dongle_mac = Arc::clone(&self.shared_dongle_mac);
 
        // Duplicate self references for local callbacks
        let mut tx_transport = self.transport.clone();
        let disconnect_notify_clone = Arc::clone(&self.disconnect_notify);
 
        // Spawn read worker loop
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut read_buf = [0u8; 1024];

            loop {
                tokio::select! {
                    _ = &mut exit_rx => {
                        info!("Engine worker loop received exit signal. Stopping...");
                        break;
                    }
                    res = transport.read(&mut read_buf) => {
                        match res {
                            Ok(0) => {
                                // No data currently available; avoid tight loop
                                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                            }
                            Ok(n) => {
                                buffer.extend_from_slice(&read_buf[..n]);

                                // Process buffer, extracting as many packets as possible
                                while let Some(start_idx) = Self::find_magic_prefix(&buffer) {
                                    if start_idx > 0 {
                                        // Discard junk data before the magic prefix
                                        buffer.drain(..start_idx);
                                    }

                                    match Packet::parse(&buffer) {
                                        Ok((pkt, consumed)) => {
                                            buffer.drain(..consumed);
                                            Self::handle_packet(
                                                pkt,
                                                &pending_requests,
                                                &event_tx,
                                                &mut tx_transport,
                                                &auto_verify_tx,
                                                &sensor_list_tx,
                                                &shared_dongle_mac,
                                            ).await;
                                        }
                                        Err(e) => {
                                            if e.contains("too short") {
                                                // Need more bytes to complete the packet, wait for next read
                                                break;
                                            } else {
                                                // Corrupted packet/checksum, discard header prefix and search next
                                                debug!("Discarding invalid packet parsing fragment: {}", e);
                                                if buffer.len() >= 2 {
                                                    buffer.drain(..2); // discard magic prefix
                                                } else {
                                                    buffer.clear();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Transport read error, signaling disconnect: {}", e);
                                disconnect_notify_clone.notify_one();
                                break;
                            }
                        }
                    }
                }
            }
        });



        self.exit_tx.take().unwrap()
    }

    fn find_magic_prefix(buf: &[u8]) -> Option<usize> {
        for i in 0..buf.len().saturating_sub(1) {
            let val = ((buf[i] as u16) << 8) | (buf[i+1] as u16);
            if val == 0x55AA || val == 0xAA55 {
                return Some(i);
            }
        }
        None
    }

    async fn handle_packet(
        pkt: Packet,
        pending: &PendingRequests,
        event_tx: &mpsc::Sender<DongleEvent>,
        transport: &mut GatewayTransport,
        auto_verify_tx: &mpsc::Sender<(String, SensorType, u8)>,
        sensor_list_tx: &Arc<Mutex<Option<mpsc::Sender<String>>>>,
        shared_dongle_mac: &Arc<Mutex<Option<String>>>,
    ) {
        debug!("<=== Received packet: {}", pkt);

        // 1. Check if this completes a pending request
        let pkt_cmd = match &pkt.payload {
            PacketPayload::Ack(ack_cmd) => *ack_cmd,
            _ => pkt.cmd(),
        };
        {
            let mut pending_map = pending.lock().unwrap();
            if let Some(tx) = pending_map.remove(&pkt_cmd) {
                let _ = tx.send(pkt.clone());
            }
        }

        // 2. Auto-acknowledge ALL async packets (type is Async) except ASYNC_ACK itself.
        // The Python reference ACKs every async packet including responses to host-initiated
        // commands (GetVersion response, FinishAuth response, etc.), not just unsolicited notifications.
        if pkt.cmd_type == CommandType::Async && pkt.command_id != (commands::CMD_ASYNC_ACK & 0xFF) as u8 {
            debug!("Sending ACK packet for async command {:04X}", pkt_cmd);
            let ack_pkt = Packet::new_ack(pkt_cmd);
            if let Err(e) = transport.write(&ack_pkt.to_bytes()).await {
                error!("Failed to write ACK packet to transport: {}", e);
            }
        }

        // 3. Route async notification codes
        match pkt.cmd() {
            commands::CMD_TIME_SYNC => {
                // Time Sync Notification. Send SyncTimeAck
                debug!("Received time sync request (0x5332). Replying with timestamp.");
                let timestamp_ms = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                // Reply payload: NOTIFY_SYNC_TIME + 1 = 0x5333
                let ack_payload = timestamp_ms.to_be_bytes().to_vec();
                let reply_pkt = Packet::new_async((commands::CMD_TIME_SYNC_RESPONSE & 0xFF) as u8, ack_payload);
                if let Err(e) = transport.write(&reply_pkt.to_bytes()).await {
                    error!("Failed to write time sync reply: {}", e);
                }
            }
            commands::CMD_ALARM1 => {
                // Telemetry packet Alarm1
                if let PacketPayload::Bytes(bytes) = pkt.payload {
                    match DongleEvent::parse_alarm1(&bytes) {
                        Ok(mut evt) => {
                            evt.dongle_mac = shared_dongle_mac.lock().unwrap().clone();
                            let _ = event_tx.send(evt).await;
                        }
                        Err(e) => warn!("Failed to parse alarm1 telemetry event: {}", e),
                    }
                }
            }
            commands::CMD_ALARM2 => {
                // Telemetry packet Alarm2
                if let PacketPayload::Bytes(bytes) = pkt.payload {
                    match DongleEvent::parse_alarm2(&bytes) {
                        Ok(mut evt) => {
                            evt.dongle_mac = shared_dongle_mac.lock().unwrap().clone();
                            let _ = event_tx.send(evt).await;
                        }
                        Err(e) => warn!("Failed to parse alarm2 telemetry event: {}", e),
                    }
                }
            }
            commands::CMD_SENSOR_SCAN => {
                // Sensor Scan Notification
                if let PacketPayload::Bytes(bytes) = pkt.payload {
                    match DongleEvent::parse_scan(&bytes) {
                        Ok(mut evt) => {
                            evt.dongle_mac = shared_dongle_mac.lock().unwrap().clone();
                            let mac = evt.mac.clone();
                            let s_type = evt.sensor_type;
                            let version = if let TelemetryData::Scanned { version } = &evt.data { *version } else { 0 };
                            let _ = event_tx.send(evt).await;
                            let _ = auto_verify_tx.send((mac, s_type, version)).await;
                        }
                        Err(e) => warn!("Failed to parse scan telemetry event: {}", e),
                    }
                }
            }
            commands::CMD_SENSOR_LIST_ITEM => {
                // Paired sensor item streamed asynchronously
                if let PacketPayload::Bytes(bytes) = pkt.payload {
                    if let Ok(mac) = String::from_utf8(bytes) {
                        let list_tx_guard = sensor_list_tx.lock().unwrap();
                        if let Some(tx) = &*list_tx_guard {
                            let _ = tx.try_send(mac);
                        }
                    }
                }
            }
            commands::CMD_EVENT_LOG => {
                // Event Log Notification — informational, log and move on.
                if let PacketPayload::Bytes(bytes) = pkt.payload {
                    if bytes.len() >= 9 {
                        let timestamp_ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap_or([0u8; 8]));
                        let msg_len = bytes[8] as usize;
                        let msg = &bytes[9..];
                        let hex_str = msg.iter().map(|b| format!("{:02x}", b)).collect::<Vec<String>>().join(",");
                        info!("EVENT LOG: ts={}, msg_len={}, data=[{}]", timestamp_ms, msg_len, hex_str);
                    } else {
                        debug!("Short event log payload: {} bytes", bytes.len());
                    }
                }
            }
            _ => {
                // Unknown or unhandled async events
            }
        }
    }

    /// Sends a command and awaits the expected response packet asynchronously.
    pub async fn do_command(&mut self, pkt: Packet, expected_response_cmd: u16) -> Result<Packet> {
        self.do_command_with_timeout(pkt, expected_response_cmd, tokio::time::Duration::from_secs(5)).await
    }

    /// Like `do_command`, but waits at most `timeout` for the response.
    pub async fn do_command_with_timeout(
        &mut self,
        pkt: Packet,
        expected_response_cmd: u16,
        timeout: std::time::Duration,
    ) -> Result<Packet> {
        // Held for the whole round trip: see the comment on `cmd_lock`.
        let _cmd_guard = self.cmd_lock.lock().await;

        let (tx, rx) = oneshot::channel();
        
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(expected_response_cmd, tx);
        }

        debug!("===> Sending command packet: {}", pkt);
        self.transport.write(&pkt.to_bytes()).await?;

        // Wait for the worker thread to resolve the response
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(Error::new(ErrorKind::ConnectionAborted, "Oneshot channel closed")),
            Err(_) => {
                // Remove pending request on timeout
                let mut pending = self.pending_requests.lock().unwrap();
                pending.remove(&expected_response_cmd);
                Err(Error::new(ErrorKind::TimedOut, format!("Command {:04X} timed out waiting for response {:04X}", pkt.cmd(), expected_response_cmd)))
            }
        }
    }

    /// Executes the startup handshake sequence to unlock the dongle.
    pub async fn initialize_handshake(&mut self) -> Result<()> {
        info!("Starting dongle initialization handshake...");

        // 1. Inquiry (0x4327) -> expect InquiryResponse (0x4328)
        // Retried on timeout: a lost Inquiry or response is the common failure on
        // a serial link, and the dongle answers a repeated Inquiry the same way.
        let mut attempt = 1;
        let resp = loop {
            let inquiry = Packet::new_sync((commands::CMD_INQUIRY & 0xFF) as u8, vec![]);
            match self.do_command_with_timeout(inquiry, commands::CMD_INQUIRY_RESPONSE, self.inquiry_timeout).await {
                Err(e) if e.kind() == ErrorKind::TimedOut && attempt < INQUIRY_ATTEMPTS => {
                    warn!("Handshake Inquiry attempt {}/{} timed out, retrying", attempt, INQUIRY_ATTEMPTS);
                    attempt += 1;
                }
                res => break res?,
            }
        };
        if let PacketPayload::Bytes(bytes) = resp.payload {
            if bytes.get(0) != Some(&0x01) {
                return Err(Error::new(ErrorKind::InvalidData, "Inquiry verification failed"));
            }
        } else {
            return Err(Error::new(ErrorKind::InvalidData, "Inquiry response payload mismatch"));
        }
        debug!("Handshake 1/5: Inquiry verified");

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 2. Get ENR (0x4302) -> expect GetEnrResponse (0x4303)
        // Python passes 16 ASCII zeroes '0' as token
        let enr_token = vec![0x30; 16]; 
        let enr_cmd = Packet::new_sync((commands::CMD_GET_ENR & 0xFF) as u8, enr_token);
        let resp = self.do_command(enr_cmd, commands::CMD_ENR_RESPONSE).await?;
        debug!("Handshake 2/5: ENR Token retrieved: {}", resp);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 3. Get MAC (0x4304) -> expect GetMacResponse (0x4305)
        let mac_cmd = Packet::new_sync((commands::CMD_GET_MAC & 0xFF) as u8, vec![]);
        let resp = self.do_command(mac_cmd, commands::CMD_MAC_RESPONSE).await?;
        if let PacketPayload::Bytes(bytes) = resp.payload {
            let mac = String::from_utf8(bytes)
                .map_err(|_| Error::new(ErrorKind::InvalidData, "MAC has non-UTF8 bytes"))?;
            info!("Dongle MAC address: {}", mac);
            self.dongle_mac = Some(mac.clone());
            *self.shared_dongle_mac.lock().unwrap() = Some(mac);
        } else {
            return Err(Error::new(ErrorKind::InvalidData, "MAC response payload mismatch"));
        }
        debug!("Handshake 3/5: MAC Address retrieved");

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 4. Get Version (0x5316) -> expect GetVersionResponse (0x5317)
        let ver_cmd = Packet::new_async((commands::CMD_GET_VERSION & 0xFF) as u8, vec![]);
        let resp = self.do_command(ver_cmd, commands::CMD_VERSION_RESPONSE).await?;
        if let PacketPayload::Bytes(bytes) = resp.payload {
            let version = String::from_utf8(bytes)
                .map_err(|_| Error::new(ErrorKind::InvalidData, "Version string has non-UTF8 bytes"))?;
            info!("Dongle version: {}", version);
            self.dongle_version = Some(version);
        } else {
            return Err(Error::new(ErrorKind::InvalidData, "Version response payload mismatch"));
        }
        debug!("Handshake 4/5: Version string retrieved");

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 5. Finish Auth (0x5314) -> expect FinishAuthResponse (0x5315)
        let auth_cmd = Packet::new_async((commands::CMD_FINISH_AUTH & 0xFF) as u8, vec![0xFF]);
        let _resp = self.do_command(auth_cmd, commands::CMD_FINISH_AUTH_RESPONSE).await?;
        debug!("Handshake 5/5: Authentication completed");

        info!("Dongle handshake completed successfully! Dongle is unlocked.");
        Ok(())
    }

    /// Enables or disables sensor pairing/scanning mode.
    pub async fn set_scan(&mut self, enable: bool) -> Result<()> {
        info!("Setting scan mode to: {}", enable);
        let payload = vec![if enable { 0x01 } else { 0x00 }];
        let pkt = Packet::new_async((commands::CMD_SET_SCAN & 0xFF) as u8, payload);
        // Expect 0x531D response
        let _resp = self.do_command(pkt, commands::CMD_SET_SCAN_RESPONSE).await?;
        
        // Commit to local atomic status
        self.is_scanning.store(enable, Ordering::SeqCst);
        
        Ok(())
    }

    /// Exposes the thread-safe local scan state tracker.
    pub fn is_scanning(&self) -> bool {
        self.is_scanning.load(Ordering::SeqCst)
    }

    /// Exchanges a crypto token (R1) with a sensor during the pairing handshake.
    /// This must be called after scan detection and before verify_sensor.
    pub async fn get_sensor_r1(&mut self, mac: &str) -> Result<Vec<u8>> {
        info!("Exchanging R1 crypto token with sensor {}...", mac);
        if mac.len() != 8 {
            return Err(Error::new(ErrorKind::InvalidInput, "MAC address must be 8 characters"));
        }
        let mut payload = mac.as_bytes().to_vec();
        // Hardcoded R1 token from the Python reference implementation
        payload.extend_from_slice(b"Ok5HPNQ4lf77u754");
        let pkt = Packet::new_async((commands::CMD_GET_R1 & 0xFF) as u8, payload);
        // Expect 0x5322 response
        let resp = self.do_command(pkt, commands::CMD_R1_RESPONSE).await?;
        if let PacketPayload::Bytes(bytes) = resp.payload {
            debug!("GetSensorR1 returned {} bytes", bytes.len());
            Ok(bytes)
        } else {
            Err(Error::new(ErrorKind::InvalidData, "Unexpected R1 response type"))
        }
    }

    /// Verifies and binds a scanned sensor.
    pub async fn verify_sensor(&mut self, mac: &str, _sensor_type: crate::protocol::telemetry::SensorType) -> Result<()> {
        info!("Verifying sensor {}...", mac);
        if mac.len() != 8 {
            return Err(Error::new(ErrorKind::InvalidInput, "MAC address must be 8 characters"));
        }
        let mut payload = mac.as_bytes().to_vec();
        payload.push(0xFF);
        payload.push(0x04);
        
        let pkt = Packet::new_async((commands::CMD_VERIFY_SENSOR & 0xFF) as u8, payload);
        // Expect 0x5324 response
        let _resp = self.do_command(pkt, commands::CMD_VERIFY_SENSOR_RESPONSE).await?;
        self.paired_sensors.lock().unwrap().insert(mac.to_string());
        Ok(())
    }

    /// Deletes/unpairs a sensor by MAC address.
    pub async fn delete_sensor(&mut self, mac: &str) -> Result<()> {
        info!("Deleting sensor: {}", mac);
        if mac.len() != 8 {
            return Err(Error::new(ErrorKind::InvalidInput, "MAC address must be 8 characters"));
        }
        let payload = mac.as_bytes().to_vec();
        let pkt = Packet::new_async((commands::CMD_DELETE_SENSOR & 0xFF) as u8, payload);
        // Expect 0x5326 response
        let _resp = self.do_command(pkt, commands::CMD_DELETE_SENSOR_RESPONSE).await?;
        self.paired_sensors.lock().unwrap().remove(mac);

        // Note: SensorManager (the single source of truth for persisted sensor
        // state) is updated and saved by the caller via delete_and_persist_sensor.
        Ok(())
    }

    /// Retrieves the number of paired sensors.
    pub async fn get_sensor_count(&mut self) -> Result<u8> {
        info!("Retrieving sensor count...");
        let pkt = Packet::new_async((commands::CMD_GET_SENSOR_COUNT & 0xFF) as u8, vec![]);
        // Expect 0x532F response
        let resp = self.do_command(pkt, commands::CMD_SENSOR_COUNT_RESPONSE).await?;
        if let PacketPayload::Bytes(bytes) = resp.payload {
            if bytes.is_empty() {
                return Err(Error::new(ErrorKind::InvalidData, "Empty sensor count response"));
            }
            Ok(bytes[0])
        } else {
            Err(Error::new(ErrorKind::InvalidData, "Invalid sensor count response payload"))
        }
    }

    /// Retrieves the list of paired sensor MAC addresses.
    /// Uses the two-phase protocol from the Python reference:
    ///   1. Get sensor count (0x532E → 0x532F)
    ///   2. Request sensor list with count as payload (0x5330 → individual 0x5331 responses)
    pub async fn get_sensor_list(&mut self) -> Result<Vec<String>> {
        info!("Retrieving sensor list...");

        // Phase 1: Get sensor count
        let count = self.get_sensor_count().await?;
        info!("{} sensors reported by dongle", count);

        if count == 0 {
            info!("No sensors paired to dongle.");
            self.paired_sensors.lock().unwrap().clear();
            self.sensor_list_loaded.store(true, Ordering::SeqCst);
            return Ok(Vec::new());
        }
        
        // Phase 2: Register receiver channel for streamed 0x5331 responses
        let (tx, mut rx) = mpsc::channel::<String>(32);
        {
            let mut list_tx = self.sensor_list_tx.lock().unwrap();
            *list_tx = Some(tx);
        }

        // Dispatch CMD_GET_SENSOR_LIST (0x5330) with count as payload
        let pkt = Packet::new_async((commands::CMD_GET_SENSOR_LIST & 0xFF) as u8, vec![count]);
        // The dongle first ACKs 0x5330, then streams individual 0x5331 responses.
        // We wait for the ACK to complete the do_command, then collect from the channel.
        let _resp = self.do_command(pkt, commands::CMD_GET_SENSOR_LIST).await?;

        // Collect MAC addresses from the channel until we have all of them or timeout
        let mut sensors = Vec::new();
        let timeout_per_sensor = tokio::time::Duration::from_millis(1500);
        loop {
            match tokio::time::timeout(timeout_per_sensor, rx.recv()).await {
                Ok(Some(mac)) => {
                    if !sensors.contains(&mac) {
                        sensors.push(mac.clone());
                        info!("Sensor {}/{}: MAC={}", sensors.len(), count, mac);
                    }
                    // If we've collected all expected sensors, stop waiting
                    if sensors.len() >= count as usize {
                        break;
                    }
                }
                Ok(None) => {
                    break; // Channel closed
                }
                Err(_) => {
                    // Stream settled, no more sensors returned within timeout
                    if sensors.len() < count as usize {
                        warn!("Sensor list retrieval timed out: got {} of {} expected sensors",
                              sensors.len(), count);
                    }
                    break;
                }
            }
        }

        // Clean up
        {
            let mut list_tx = self.sensor_list_tx.lock().unwrap();
            *list_tx = None;
        }
        {
            let mut set = self.paired_sensors.lock().unwrap();
            *set = sensors.iter().cloned().collect();
            self.sensor_list_loaded.store(true, Ordering::SeqCst);
        }

        info!("Successfully retrieved {} paired sensors", sensors.len());
        Ok(sensors)
    }

    /// Requests chime/alarm execution on a device.
    pub async fn play_chime(&mut self, mac: &str) -> Result<()> {
        info!("Playing chime on: {}", mac);
        if mac.len() != 8 {
            return Err(Error::new(ErrorKind::InvalidInput, "MAC address must be 8 characters"));
        }
        let mut payload = mac.as_bytes().to_vec();
        payload.push(0x01); // Default Ring ID: 1
        payload.push(0x01); // Default Repeat Count: 1
        payload.push(0x09); // Default Volume: 9 (Max)
        let pkt = Packet::new_async((commands::CMD_PLAY_CHIME & 0xFF) as u8, payload);
        // Expect 0x5371 response
        let _resp = self.do_command(pkt, commands::CMD_PLAY_CHIME_RESPONSE).await?;
        Ok(())
    }

    /// Sends a reply to a keypad request (`0x5353`) and waits for the dongle's ACK.
    pub async fn send_keypad_reply(&mut self, keypad_mac: &str, reply: KeypadReply) -> Result<()> {
        let pkt = reply.to_packet(keypad_mac)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
        debug!("Keypad {}: sending reply {:?}", keypad_mac, reply);
        // The dongle answers 0x5353 with an ACK only, keyed by the acked command.
        self.do_command(pkt, commands::CMD_SEND_KEYPAD).await?;
        Ok(())
    }
}

impl Clone for Engine {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            pending_requests: Arc::clone(&self.pending_requests),
            cmd_lock: Arc::clone(&self.cmd_lock),
            event_tx: self.event_tx.clone(),
            dongle_mac: self.dongle_mac.clone(),
            dongle_version: self.dongle_version.clone(),
            exit_tx: None,
            auto_verify: Arc::clone(&self.auto_verify),
            sensor_list_tx: Arc::clone(&self.sensor_list_tx),
            is_scanning: Arc::clone(&self.is_scanning),
            transport_label: self.transport_label.clone(),
            device_path: self.device_path.clone(),
            remote_addr: self.remote_addr.clone(),
            inquiry_timeout: self.inquiry_timeout,
            disconnect_notify: Arc::clone(&self.disconnect_notify),
            shared_dongle_mac: Arc::clone(&self.shared_dongle_mac),
            paired_sensors: Arc::clone(&self.paired_sensors),
            sensor_list_loaded: Arc::clone(&self.sensor_list_loaded),
            peer_context: Arc::clone(&self.peer_context),
        }
    }
}

/// Removes stale records of `mac` from any other dongle after pairing finishes
/// on `new_dongle_mac`.
///
/// Checks each connected peer engine's NVRAM sensor set (falling back to
/// `SensorManager` if a peer engine has not yet loaded its sensor list).
/// Best-effort: an offline previous dongle or a failed delete is logged and
/// never fails the pairing itself; offline dongles are cleaned up when they
/// reconnect via [`purge_sensors_claimed_by_online_dongles`].
pub async fn detach_sensor_from_previous_dongle(
    engines: &EnginesMap,
    sensor_manager: &Arc<Mutex<SensorManager>>,
    mac: &str,
    new_dongle_mac: &str,
) -> Vec<String> {
    let recorded_previous = {
        let manager = sensor_manager.lock().unwrap();
        manager
            .get_sensors()
            .get(mac)
            .and_then(|s| s.dongle_mac.clone())
            .filter(|prev| prev != new_dongle_mac)
    };

    let (mut stale_engines, offline_previous) = {
        let map = engines.lock().await;
        let mut stale: Vec<(String, Engine)> = map
            .iter()
            .filter(|(dmac, _)| dmac.as_str() != new_dongle_mac)
            .filter(|(dmac, eng)| {
                eng.has_paired_sensor(mac)
                    || (!eng.sensor_list_loaded()
                        && recorded_previous.as_deref() == Some(dmac.as_str()))
            })
            .map(|(dmac, eng)| (dmac.clone(), eng.clone()))
            .collect();
        stale.sort_by(|a, b| a.0.cmp(&b.0));

        let offline = recorded_previous
            .as_ref()
            .filter(|prev| !map.contains_key(*prev))
            .cloned();
        (stale, offline)
    };

    if stale_engines.is_empty() && offline_previous.is_none() {
        return Vec::new();
    }

    if recorded_previous.is_some() {
        let mut manager = sensor_manager.lock().unwrap();
        if let Some(sensor) = manager
            .get_sensors_mut()
            .get_mut(mac)
            .filter(|s| s.dongle_mac.as_deref() != Some(new_dongle_mac))
        {
            sensor.dongle_mac = None;
        }
    }

    let mut previous_dongles = Vec::new();
    for (prev_mac, engine) in &mut stale_engines {
        match engine.delete_sensor(mac).await {
            Ok(()) => info!(
                "Removed sensor {} from previous dongle {} (now paired with {})",
                mac, prev_mac, new_dongle_mac
            ),
            Err(e) => warn!(
                "Failed to remove sensor {} from previous dongle {}: {}; \
                 it will be cleaned up when that dongle reconnects (now paired with {})",
                mac, prev_mac, e, new_dongle_mac
            ),
        }
        previous_dongles.push(prev_mac.clone());
    }

    if let Some(offline_mac) = offline_previous {
        info!(
            "Sensor {} was paired with offline dongle {}; \
             it will be removed from {} when that dongle reconnects (now paired with {})",
            mac, offline_mac, offline_mac, new_dongle_mac
        );
        previous_dongles.push(offline_mac);
    }

    previous_dongles
}

/// Removes stale sensor entries from a connecting dongle when another online
/// dongle already owns them.
///
/// If a sensor was re-paired onto dongle B while dongle A was unplugged or
/// offline, A still reports that sensor in its sensor list when it comes back.
/// Deleting it from A here (and dropping it from `nvram_sensors` before
/// `assign_dongle`) prevents A from keeping a duplicate pairing or stealing
/// the sensor's association back from B.
pub async fn purge_sensors_claimed_by_online_dongles(
    engines: &EnginesMap,
    sensor_manager: &Arc<Mutex<SensorManager>>,
    connecting_mac: &str,
    engine: &mut Engine,
    nvram_sensors: &mut Vec<String>,
) -> Vec<String> {
    if nvram_sensors.is_empty() {
        return Vec::new();
    }

    let recorded_owners: HashMap<String, String> = {
        let manager = sensor_manager.lock().unwrap();
        nvram_sensors
            .iter()
            .filter_map(|mac| {
                manager
                    .get_sensors()
                    .get(mac)
                    .and_then(|s| s.dongle_mac.as_ref())
                    .map(|owner| (mac.clone(), owner.clone()))
            })
            .collect()
    };

    let claimed: Vec<(String, String)> = {
        let map = engines.lock().await;
        nvram_sensors
            .iter()
            .filter_map(|sensor_mac| {
                let recorded_owner = recorded_owners.get(sensor_mac).map(String::as_str);
                let owner = map
                    .iter()
                    .filter(|(dmac, _)| dmac.as_str() != connecting_mac)
                    .find(|(dmac, eng)| {
                        eng.has_paired_sensor(sensor_mac)
                            || (!eng.sensor_list_loaded()
                                && recorded_owner == Some(dmac.as_str()))
                    })
                    .map(|(dmac, _)| dmac.clone());
                owner.map(|owner_mac| (sensor_mac.clone(), owner_mac))
            })
            .collect()
    };

    if claimed.is_empty() {
        return Vec::new();
    }

    let claimed_set: HashSet<String> = claimed.iter().map(|(s, _)| s.clone()).collect();
    nvram_sensors.retain(|s| !claimed_set.contains(s));

    let mut purged = Vec::new();
    for (sensor_mac, owner_mac) in claimed {
        match engine.delete_sensor(&sensor_mac).await {
            Ok(()) => {
                info!(
                    "Removed stale sensor {} from reconnecting dongle {} (already paired with online dongle {})",
                    sensor_mac, connecting_mac, owner_mac
                );
                purged.push(sensor_mac);
            }
            Err(e) => warn!(
                "Failed to remove stale sensor {} from reconnecting dongle {}: {} \
                 (kept associated with online dongle {})",
                sensor_mac, connecting_mac, e, owner_mac
            ),
        }
    }
    purged
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::replay::ReplayTransport;
    use std::time::Duration;

    fn count_deletes_for(written: &[u8], expected_mac: &[u8]) -> usize {
        let mut slice = written;
        let mut count = 0;
        while let Ok((pkt, n)) = Packet::parse(slice) {
            if !matches!(pkt.payload, PacketPayload::Ack(_))
                && pkt.cmd() == commands::CMD_DELETE_SENSOR
                && matches!(&pkt.payload, PacketPayload::Bytes(b) if b == expected_mac)
            {
                count += 1;
            }
            slice = &slice[n..];
        }
        count
    }

    #[tokio::test]
    async fn engine_releases_transport_and_tasks_when_stopped_or_disconnected() {
        let (event_tx, _event_rx) = mpsc::channel(16);

        // Case 1: stopping via exit_tx drop
        let replay = ReplayTransport::new();
        let mut engine = Engine::new(GatewayTransport::Replay(replay.clone()), event_tx.clone());
        let exit_tx = engine.start();
        drop(exit_tx);
        drop(engine);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            replay.clone_count(),
            1,
            "stopping the engine must release all internal transport handles (no leaked auto_verify task)"
        );

        // Case 2: transport disconnect
        let replay2 = ReplayTransport::new();
        let mut engine2 = Engine::new(GatewayTransport::Replay(replay2.clone()), event_tx);
        let _exit_tx2 = engine2.start();
        let notified = engine2.disconnect_notify.notified();
        replay2.disconnect();
        notified.await;
        drop(engine2);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            replay2.clone_count(),
            1,
            "transport disconnect must release all internal transport handles (no leaked auto_verify task)"
        );
    }

    #[tokio::test]
    async fn detach_removes_sensor_from_previous_dongle_only() {
        use crate::protocol::sensor::SensorManager;
        use crate::transport::virtual_dongle::VirtualDongle;

        let dir = std::env::temp_dir().join(format!("w2m_detach_unit_{}", std::process::id()));
        let (event_tx, _event_rx) = mpsc::channel(16);
        let dongle_a = VirtualDongle::new("DONGLE_A", "V2.3.9")
            .with_sensors(vec!["SHARED01".to_string()]);
        let mut engine_a = Engine::new(dongle_a.transport(), event_tx.clone());
        let exit_a = engine_a.start();
        engine_a.initialize_handshake().await.unwrap();
        let list_a = engine_a.get_sensor_list().await.unwrap();
        assert_eq!(list_a, vec!["SHARED01".to_string()]);
        assert!(engine_a.has_paired_sensor("SHARED01"));

        let dongle_b = VirtualDongle::new("DONGLE_B", "V2.3.9").with_sensors(vec![]);
        let mut engine_b = Engine::new(dongle_b.transport(), event_tx.clone());
        let exit_b = engine_b.start();
        engine_b.initialize_handshake().await.unwrap();
        assert!(engine_b.get_sensor_list().await.unwrap().is_empty());

        let engines: EnginesMap = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        {
            let mut map = engines.lock().await;
            map.insert("DONGLE_A".to_string(), engine_a);
            map.insert("DONGLE_B".to_string(), engine_b.clone());
        }

        let manager = Arc::new(Mutex::new(SensorManager::new(
            dir.join("sensors.yaml").to_string_lossy().to_string(),
            dir.join("state.yaml").to_string_lossy().to_string(),
        )));
        manager
            .lock()
            .unwrap()
            .assign_dongle("DONGLE_A", &["SHARED01".to_string()]);

        // Dispatching a Scanned broadcast from DONGLE_B before verification
        // must NOT overwrite SHARED01's association with DONGLE_A.
        let scan_evt = DongleEvent {
            mac: "SHARED01".to_string(),
            timestamp: SystemTime::now(),
            sensor_type: SensorType::ContactV2,
            event_type: 0xA3,
            data: TelemetryData::Scanned { version: 25 },
            dongle_mac: Some("DONGLE_B".to_string()),
        };
        manager.lock().unwrap().dispatch_event(&scan_evt);
        assert_eq!(
            manager.lock().unwrap().get_sensors()["SHARED01"]
                .dongle_mac
                .as_deref(),
            Some("DONGLE_A")
        );

        engine_b
            .verify_sensor("SHARED01", SensorType::ContactV2)
            .await
            .unwrap();
        assert!(engine_b.has_paired_sensor("SHARED01"));

        let previous =
            detach_sensor_from_previous_dongle(&engines, &manager, "SHARED01", "DONGLE_B")
                .await;
        assert_eq!(previous, vec!["DONGLE_A".to_string()]);

        assert_eq!(
            count_deletes_for(&dongle_a.replay_transport().get_written(), b"SHARED01"),
            1,
            "DONGLE_A should receive exactly one CMD_DELETE_SENSOR for SHARED01"
        );
        assert_eq!(
            count_deletes_for(&dongle_b.replay_transport().get_written(), b"SHARED01"),
            0,
            "DONGLE_B must not receive a delete for a sensor pairing onto it"
        );

        // Calling detach a second time (or re-pairing onto the same dongle / fresh sensor)
        // must be a complete no-op because DONGLE_A already dropped SHARED01.
        let repeat =
            detach_sensor_from_previous_dongle(&engines, &manager, "SHARED01", "DONGLE_B")
                .await;
        assert!(repeat.is_empty());
        assert_eq!(
            count_deletes_for(&dongle_a.replay_transport().get_written(), b"SHARED01"),
            1,
            "second detach call must not send a duplicate CMD_DELETE_SENSOR"
        );

        let same =
            detach_sensor_from_previous_dongle(&engines, &manager, "SHARED01", "DONGLE_B")
                .await;
        assert!(same.is_empty());
        let fresh =
            detach_sensor_from_previous_dongle(&engines, &manager, "NEWSENS1", "DONGLE_B")
                .await;
        assert!(fresh.is_empty());

        drop(exit_a);
        drop(exit_b);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn reconnecting_dongle_purges_sensor_repaired_while_offline() {
        use crate::protocol::sensor::SensorManager;
        use crate::transport::virtual_dongle::VirtualDongle;

        let dir = std::env::temp_dir().join(format!("w2m_purge_unit_{}", std::process::id()));
        let (event_tx, _event_rx) = mpsc::channel(16);

        // DONGLE_B is online and owns SHARED01 (re-paired while DONGLE_A was unplugged).
        let dongle_b = VirtualDongle::new("DONGLE_B", "V2.3.9")
            .with_sensors(vec!["SHARED01".to_string()]);
        let mut engine_b = Engine::new(dongle_b.transport(), event_tx.clone());
        let exit_b = engine_b.start();
        engine_b.initialize_handshake().await.unwrap();
        let list_b = engine_b.get_sensor_list().await.unwrap();

        let engines: EnginesMap = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        engines.lock().await.insert("DONGLE_B".to_string(), engine_b);

        let manager = Arc::new(Mutex::new(SensorManager::new(
            dir.join("sensors.yaml").to_string_lossy().to_string(),
            dir.join("state.yaml").to_string_lossy().to_string(),
        )));
        manager.lock().unwrap().assign_dongle("DONGLE_B", &list_b);

        // DONGLE_A comes back online still carrying stale SHARED01 plus its own ONLY_A01.
        let dongle_a = VirtualDongle::new("DONGLE_A", "V2.3.9")
            .with_sensors(vec!["SHARED01".to_string(), "ONLY_A01".to_string()]);
        let mut engine_a = Engine::new(dongle_a.transport(), event_tx);
        let exit_a = engine_a.start();
        engine_a.initialize_handshake().await.unwrap();
        let mut list_a = engine_a.get_sensor_list().await.unwrap();

        let purged = purge_sensors_claimed_by_online_dongles(
            &engines,
            &manager,
            "DONGLE_A",
            &mut engine_a,
            &mut list_a,
        )
        .await;
        assert_eq!(purged, vec!["SHARED01".to_string()]);
        assert_eq!(list_a, vec!["ONLY_A01".to_string()]);
        assert!(!engine_a.has_paired_sensor("SHARED01"));
        assert!(engine_a.has_paired_sensor("ONLY_A01"));

        manager.lock().unwrap().assign_dongle("DONGLE_A", &list_a);
        assert_eq!(
            manager.lock().unwrap().get_sensors()["SHARED01"]
                .dongle_mac
                .as_deref(),
            Some("DONGLE_B"),
            "reconnecting DONGLE_A must not steal SHARED01 back from online DONGLE_B"
        );
        assert_eq!(
            manager.lock().unwrap().get_sensors()["ONLY_A01"]
                .dongle_mac
                .as_deref(),
            Some("DONGLE_A")
        );
        assert_eq!(
            count_deletes_for(&dongle_a.replay_transport().get_written(), b"SHARED01"),
            1
        );
        assert_eq!(
            count_deletes_for(&dongle_a.replay_transport().get_written(), b"ONLY_A01"),
            0
        );

        drop(exit_a);
        drop(exit_b);
        let _ = std::fs::remove_dir_all(dir);
    }
}
