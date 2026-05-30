use crate::protocol::packet::{Packet, CommandType, PacketPayload, commands};
use crate::protocol::telemetry::{DongleEvent, SensorType, TelemetryData};
use crate::transport::AsyncTransport;

use std::collections::HashMap;
use std::io::{Result, Error, ErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, Duration, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

type PendingRequests = Arc<Mutex<HashMap<u16, oneshot::Sender<Packet>>>>;

pub struct Engine<T: AsyncTransport> {
    transport: T,
    pending_requests: PendingRequests,
    event_tx: mpsc::Sender<DongleEvent>,
    dongle_mac: Option<String>,
    dongle_version: Option<String>,
    exit_tx: Option<oneshot::Sender<()>>,
    // Refactored local memory cache to use persistent SensorState
    pub sensors: Arc<Mutex<HashMap<String, crate::config::state::SensorState>>>,
    auto_verify_tx: mpsc::Sender<(String, SensorType)>,
    auto_verify: Arc<AtomicBool>,
    sensor_list_tx: Arc<Mutex<Option<mpsc::Sender<String>>>>,
    is_scanning: Arc<AtomicBool>,
    state_path: Option<String>,
}

impl<T: AsyncTransport + Clone + 'static> Engine<T> {
    pub fn new(transport: T, event_tx: mpsc::Sender<DongleEvent>, state_path: Option<String>) -> Self {
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));
        let sensors = Arc::new(Mutex::new(HashMap::new()));
        let (auto_verify_tx, mut auto_verify_rx) = mpsc::channel::<(String, SensorType)>(32);
        let auto_verify = Arc::new(AtomicBool::new(false));
        let sensor_list_tx = Arc::new(Mutex::new(None));
        let is_scanning = Arc::new(AtomicBool::new(false));

        let engine = Self {
            transport: transport.clone(),
            pending_requests: Arc::clone(&pending_requests),
            event_tx: event_tx.clone(),
            dongle_mac: None,
            dongle_version: None,
            exit_tx: None,
            sensors: Arc::clone(&sensors),
            auto_verify_tx,
            auto_verify: Arc::clone(&auto_verify),
            sensor_list_tx: Arc::clone(&sensor_list_tx),
            is_scanning: Arc::clone(&is_scanning),
            state_path: state_path.clone(),
        };

        let mut engine_clone = Self {
            transport: transport.clone(),
            pending_requests: Arc::clone(&pending_requests),
            event_tx: event_tx.clone(),
            dongle_mac: None,
            dongle_version: None,
            exit_tx: None,
            sensors: Arc::clone(&sensors),
            auto_verify_tx: engine.auto_verify_tx.clone(),
            auto_verify: Arc::clone(&auto_verify),
            sensor_list_tx: Arc::clone(&sensor_list_tx),
            is_scanning: Arc::clone(&is_scanning),
            state_path: state_path.clone(),
        };

        tokio::spawn(async move {
            while let Some((mac, s_type)) = auto_verify_rx.recv().await {
                if !engine_clone.auto_verify.load(Ordering::SeqCst) {
                    continue;
                }
                info!("Auto-Pairing: Scanned sensor {} detected. Exchanging R1 token...", mac);

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
                }
            }
        });

        engine
    }

    pub fn dongle_mac(&self) -> Option<&str> {
        self.dongle_mac.as_deref()
    }

    pub fn dongle_version(&self) -> Option<&str> {
        self.dongle_version.as_deref()
    }

    /// Exposes the thread-safe local memory list of rich persistent sensor states.
    pub fn get_rich_sensors(&self) -> Vec<crate::config::state::SensorState> {
        let guard = self.sensors.lock().unwrap();
        guard.values().cloned().collect()
    }

    /// Manually registers a sensor locally in the RAM cache (used for boot warming).
    pub fn register_sensor_locally(&self, mac: &str, s_type: &str, version: &str) {
        let mut guard = self.sensors.lock().unwrap();
        guard.insert(mac.to_string(), crate::config::state::SensorState {
            mac: mac.to_string(),
            sensor_type: s_type.to_string(),
            version: version.to_string(),
            last_seen: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs(),
            battery: 100,
            signal: -60,
        });
    }

    /// Enables or disables automatic verification for scanned sensors in the background.
    pub fn set_auto_verify(&self, enable: bool) {
        self.auto_verify.store(enable, Ordering::SeqCst);
    }

    /// Starts the background packet listener loop and returns an exit controller.
    pub fn start(&mut self) -> oneshot::Sender<()> {
        let (exit_tx, mut exit_rx) = oneshot::channel();
        self.exit_tx = Some(exit_tx);

        let mut transport = self.transport.clone();
        let pending_requests = Arc::clone(&self.pending_requests);
        let event_tx = self.event_tx.clone();
        let auto_verify_tx = self.auto_verify_tx.clone();
        let sensor_list_tx = Arc::clone(&self.sensor_list_tx);
 
        // Duplicate self references for local callbacks
        let mut tx_transport = self.transport.clone();
        let sensors_worker = Arc::clone(&self.sensors);
        let state_path_worker = self.state_path.clone();
 
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
                                                &sensors_worker,
                                                &auto_verify_tx,
                                                &sensor_list_tx,
                                                &state_path_worker,
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
                                error!("Error reading from transport: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Spawn availability monitor loop
        let sensors_timeout = Arc::clone(&self.sensors);
        let event_tx_timeout = self.event_tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                
                let mut offline_sensors = Vec::new();
                {
                    if let Ok(sensors_map) = sensors_timeout.lock() {
                        let now = SystemTime::now();
                        for (mac, state) in sensors_map.iter() {
                            let last_seen_epoch = UNIX_EPOCH + Duration::from_secs(state.last_seen);
                            if let Ok(duration) = now.duration_since(last_seen_epoch) {
                                let timeout = Duration::from_secs(3600 * 2); // Default 2 hours timeout
                                if duration > timeout {
                                    let s_type = match state.sensor_type.as_str() {
                                        "ContactV1" => SensorType::ContactV1,
                                        "MotionV1" => SensorType::MotionV1,
                                        "ContactV2" => SensorType::ContactV2,
                                        "MotionV2" => SensorType::MotionV2,
                                        "LeakV2" => SensorType::LeakV2,
                                        "ClimateV2" => SensorType::ClimateV2,
                                        "Chime" => SensorType::Chime,
                                        _ => SensorType::Unknown(0),
                                    };
                                    offline_sensors.push((mac.clone(), s_type));
                                }
                            }
                        }
                    }
                }

                for (mac, sensor_type) in offline_sensors {
                    warn!("Sensor {} timed out! Emitting offline event.", mac);
                    let offline_evt = DongleEvent {
                        mac: mac.clone(),
                        timestamp: SystemTime::now(),
                        sensor_type,
                        event_type: 0x00,
                        data: TelemetryData::Offline,
                    };
                    if event_tx_timeout.send(offline_evt).await.is_err() {
                        break; // Receiver closed, exit loop
                    }
                    if let Ok(mut sensors_map) = sensors_timeout.lock() {
                        sensors_map.remove(&mac);
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
        transport: &mut T,
        _sensors: &Arc<Mutex<HashMap<String, crate::config::state::SensorState>>>,
        auto_verify_tx: &mpsc::Sender<(String, SensorType)>,
        sensor_list_tx: &Arc<Mutex<Option<mpsc::Sender<String>>>>,
        _state_path: &Option<String>,
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
                        Ok(evt) => {
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
                        Ok(evt) => {
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
                        Ok(evt) => {
                            let mac = evt.mac.clone();
                            let s_type = evt.sensor_type;
                            let _ = event_tx.send(evt).await;
                            let _ = auto_verify_tx.send((mac, s_type)).await;
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
        let (tx, rx) = oneshot::channel();
        
        {
            let mut pending = self.pending_requests.lock().unwrap();
            pending.insert(expected_response_cmd, tx);
        }

        debug!("===> Sending command packet: {}", pkt);
        self.transport.write(&pkt.to_bytes()).await?;

        // Wait for the worker thread to resolve the response
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), rx).await {
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
        let inquiry = Packet::new_sync((commands::CMD_INQUIRY & 0xFF) as u8, vec![]);
        let resp = self.do_command(inquiry, commands::CMD_INQUIRY_RESPONSE).await?;
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
            self.dongle_mac = Some(mac);
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
        
        // Evict from memory cache state
        {
            let mut sensors = self.sensors.lock().unwrap();
            sensors.remove(mac);
        }

        // Write updates to disk atomically
        if let Some(ref path) = self.state_path {
            let s_map = self.sensors.lock().unwrap().clone();
            let system_state = crate::config::state::SystemState { sensors: s_map };
            let _ = system_state.save_to_yaml_atomic(path);
        }
        
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

    /// Registers a sensor with a specific timeout. Useful for pre-registering or testing.
    pub fn register_sensor(&self, mac: &str, sensor_type: SensorType, _timeout: std::time::Duration) {
        if let Ok(mut s_map) = self.sensors.lock() {
            s_map.insert(mac.to_string(), crate::config::state::SensorState {
                mac: mac.to_string(),
                sensor_type: format!("{:?}", sensor_type),
                version: "1".to_string(),
                last_seen: SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
                battery: 100,
                signal: -60,
            });
        }
    }
}

impl<T: AsyncTransport + Clone> Clone for Engine<T> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            pending_requests: Arc::clone(&self.pending_requests),
            event_tx: self.event_tx.clone(),
            dongle_mac: self.dongle_mac.clone(),
            dongle_version: self.dongle_version.clone(),
            exit_tx: None,
            sensors: Arc::clone(&self.sensors),
            auto_verify_tx: self.auto_verify_tx.clone(),
            auto_verify: Arc::clone(&self.auto_verify),
            sensor_list_tx: Arc::clone(&self.sensor_list_tx),
            is_scanning: Arc::clone(&self.is_scanning),
            state_path: self.state_path.clone(),
        }
    }
}
