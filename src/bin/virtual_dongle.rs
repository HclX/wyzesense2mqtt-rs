// virtual_dongle: Simulated USB dongle that connects to the gateway via WebSocket
//
// Usage: virtual_dongle [--config path/to/config.yaml]
//
// This binary emulates a Wyze Sense USB dongle entirely in software. It connects
// to the gateway via WebSocket (just like dongle_bridge does with a real dongle) and
// responds to all protocol commands (handshake, scan, verify, delete, sensor list).
//
// Configuration is loaded from a YAML file (default: virtual_dongle.yaml in the
// current directory). Paired sensors are persisted across restarts — including
// their type, battery level, RSSI, and simulation behavior.
//
// This is useful for:
// - Manual integration testing of the web dashboard
// - Verifying MQTT discovery and state publishing
// - Developing without physical hardware

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use wyzesense2mqtt_rs::protocol::packet::{commands, Packet};
use wyzesense2mqtt_rs::protocol::telemetry::{DongleEvent, SensorType};

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Top-level config file structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualDongleConfig {
    /// Dongle identity settings.
    #[serde(default)]
    pub dongle: DongleConfig,

    /// Gateway connection settings.
    #[serde(default)]
    pub gateway: GatewayConfig,

    /// Logging settings.
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Paired sensors — persisted across restarts.
    #[serde(default)]
    pub sensors: Vec<SensorConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DongleConfig {
    /// Dongle MAC address (reported to the gateway during handshake).
    #[serde(default = "default_dongle_mac")]
    pub mac: String,

    /// Firmware version string (reported during handshake).
    #[serde(default = "default_dongle_version")]
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// WebSocket URL of the gateway (e.g. ws://127.0.0.1:8080/ws/bridge).
    #[serde(default = "default_gateway_url")]
    pub url: String,

    /// Optional authentication token for bridge connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,

    /// Whether to automatically reconnect on disconnect.
    #[serde(default = "default_true")]
    pub auto_reconnect: bool,

    /// Seconds to wait before reconnection attempt.
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "default_log_level")]
    pub level: String,
}

/// Per-sensor configuration and simulation state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorConfig {
    /// Sensor MAC address (8 alphanumeric characters, e.g. "ABCD1234").
    pub mac: String,

    /// Sensor type: contact, motion, climate, leak, chime.
    #[serde(default = "default_sensor_type")]
    pub sensor_type: String,

    /// Battery level (0-100, maps to raw voltage byte internally).
    #[serde(default = "default_battery")]
    pub battery: u8,

    /// Signal strength in positive dBm (internally negated, e.g. 60 → -60 dBm).
    #[serde(default = "default_rssi")]
    pub rssi: u8,

    // --- Type-specific simulation state ---

    /// For contact/motion sensors: current state (open/closed or active/inactive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,

    /// For climate sensors: current temperature in °C.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    /// For climate sensors: current humidity %.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub humidity: Option<u8>,

    /// For leak sensors: leak state (dry/wet).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leak_state: Option<String>,

    // --- Automation ---

    /// Auto-emit events at this interval (in seconds). Null = manual only.
    /// For motion sensors, auto-triggers motion→clear cycles.
    /// For contact sensors, auto-toggles open↔close.
    /// For climate sensors, auto-emits with current temp/humidity (with jitter).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_event_interval_secs: Option<u64>,
}

// --- Serde defaults ---
fn default_dongle_mac() -> String { "VIRTUAL1".into() }
fn default_dongle_version() -> String { "V2.3.9".into() }
fn default_gateway_url() -> String { "ws://127.0.0.1:8080/ws/bridge".into() }
fn default_true() -> bool { true }
fn default_reconnect_delay() -> u64 { 5 }
fn default_log_level() -> String { "info".into() }
fn default_sensor_type() -> String { "contact".into() }
fn default_battery() -> u8 { 90 }
fn default_rssi() -> u8 { 60 }

impl Default for DongleConfig {
    fn default() -> Self {
        Self { mac: default_dongle_mac(), version: default_dongle_version() }
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            url: default_gateway_url(),
            auth_token: None,
            auto_reconnect: true,
            reconnect_delay_secs: 5,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self { level: default_log_level() }
    }
}

impl Default for VirtualDongleConfig {
    fn default() -> Self {
        Self {
            dongle: DongleConfig::default(),
            gateway: GatewayConfig::default(),
            logging: LoggingConfig::default(),
            sensors: Vec::new(),
        }
    }
}

impl SensorConfig {
    fn state_byte(&self) -> u8 {
        match self.state.as_deref() {
            Some("open") | Some("active") | Some("1") => 1,
            _ => 0,
        }
    }

    fn leak_byte(&self) -> u8 {
        match self.leak_state.as_deref() {
            Some("wet") | Some("1") => 1,
            _ => 0,
        }
    }
}

impl VirtualDongleConfig {
    fn load(path: &PathBuf) -> Self {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                match serde_yaml::from_str(&contents) {
                    Ok(config) => {
                        info!("Loaded config from {}", path.display());
                        config
                    }
                    Err(e) => {
                        eprintln!("⚠ Error parsing config file {}: {}", path.display(), e);
                        eprintln!("  Using defaults.");
                        Self::default()
                    }
                }
            }
            Err(_) => {
                info!("No config file found at {}. Using defaults.", path.display());
                Self::default()
            }
        }
    }

    fn save(&self, path: &PathBuf) {
        match serde_yaml::to_string(self) {
            Ok(yaml) => {
                let header = "# Virtual Dongle Configuration\n\
                              # Auto-saved — edit while the dongle is stopped.\n\n";
                let content = format!("{}{}", header, yaml);
                if let Err(e) = std::fs::write(path, content) {
                    error!("Failed to save config to {}: {}", path.display(), e);
                }
            }
            Err(e) => {
                error!("Failed to serialize config: {}", e);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Runtime state
// ---------------------------------------------------------------------------

struct DongleState {
    config: VirtualDongleConfig,
    config_path: PathBuf,
    /// Runtime sensor lookup (MAC → index in config.sensors).
    sensor_index: HashMap<String, usize>,
    /// Whether scan mode is currently enabled.
    scanning: bool,
}

impl DongleState {
    fn new(config: VirtualDongleConfig, config_path: PathBuf) -> Self {
        let mut sensor_index = HashMap::new();
        for (i, s) in config.sensors.iter().enumerate() {
            sensor_index.insert(s.mac.clone(), i);
        }
        Self { config, config_path, sensor_index, scanning: false }
    }

    fn paired_macs(&self) -> Vec<String> {
        self.config.sensors.iter().map(|s| s.mac.clone()).collect()
    }

    fn add_sensor(&mut self, sensor: SensorConfig) {
        let mac = sensor.mac.clone();
        self.config.sensors.push(sensor);
        let idx = self.config.sensors.len() - 1;
        self.sensor_index.insert(mac, idx);
        self.save();
    }

    fn remove_sensor(&mut self, mac: &str) {
        if let Some(_idx) = self.sensor_index.remove(mac) {
            self.config.sensors.retain(|s| s.mac != mac);
            // Rebuild index
            self.sensor_index.clear();
            for (i, s) in self.config.sensors.iter().enumerate() {
                self.sensor_index.insert(s.mac.clone(), i);
            }
            self.save();
        }
    }

    fn get_sensor(&self, mac: &str) -> Option<&SensorConfig> {
        self.sensor_index.get(mac).and_then(|&i| self.config.sensors.get(i))
    }

    fn save(&self) {
        self.config.save(&self.config_path);
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Parse arguments
    let mut config_path = PathBuf::from("virtual_dongle.yaml");

    let args: Vec<String> = std::env::args().collect();
    let mut idx = 1;
    while idx < args.len() {
        match args[idx].as_str() {
            "--config" | "-c" => {
                if idx + 1 < args.len() {
                    config_path = PathBuf::from(&args[idx + 1]);
                    idx += 2;
                } else {
                    return Err("Missing argument for --config".into());
                }
            }
            "--help" | "-h" => {
                println!("virtual_dongle: Simulated Wyze Sense dongle for testing");
                println!();
                println!("USAGE:");
                println!("    virtual_dongle [OPTIONS]");
                println!();
                println!("OPTIONS:");
                println!("    -c, --config <PATH>    Config file path [default: virtual_dongle.yaml]");
                println!("    -h, --help             Print this help message");
                println!();
                println!("CONFIG FILE (YAML):");
                println!("    dongle:");
                println!("      mac: VIRTUAL1            # Dongle MAC address");
                println!("      version: V2.3.9          # Firmware version");
                println!("    gateway:");
                println!("      url: ws://127.0.0.1:8080/ws/bridge");
                println!("      auth_token: null          # Optional auth token");
                println!("      auto_reconnect: true      # Reconnect on disconnect");
                println!("      reconnect_delay_secs: 5");
                println!("    logging:");
                println!("      level: info               # trace/debug/info/warn/error");
                println!("    sensors:");
                println!("      - mac: ABCD1234");
                println!("        sensor_type: contact    # contact/motion/climate/leak");
                println!("        battery: 90             # 0-100%");
                println!("        rssi: 60                # signal strength (positive dBm)");
                println!("        state: closed           # contact: open/closed");
                println!("        auto_event_interval_secs: 30  # auto-emit events (optional)");
                println!();
                println!("INTERACTIVE COMMANDS (after connection):");
                println!("    alarm <MAC> <open|close>    Inject door/motion alarm event");
                println!("    heartbeat <MAC>             Inject heartbeat event");
                println!("    climate <MAC> <temp> <hum>  Inject climate event");
                println!("    leak <MAC> <dry|wet>        Inject leak event");
                println!("    sensors                     List paired sensors");
                println!("    help                        Show this help");
                println!("    quit                        Disconnect and exit");
                return Ok(());
            }
            other => {
                eprintln!("Unknown argument: {}. Use --help for usage.", other);
                return Err("Unknown argument".into());
            }
        }
    }

    // Load config (before setting up logging, since log level comes from config)
    let config = VirtualDongleConfig::load(&config_path);

    // Setup logging
    let log_level = match config.logging.level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "warn" | "warning" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Print banner
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║          Virtual Wyze Sense Dongle                  ║");
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  MAC:      {:8}                               ║", config.dongle.mac);
    println!("║  Version:  {:8}                               ║", config.dongle.version);
    println!("║  Gateway:  {:<40} ║", config.gateway.url);
    println!("║  Sensors:  {:<40} ║",
        format!("{} paired", config.sensors.len()));
    println!("║  Config:   {:<40} ║", config_path.display());
    println!("╚══════════════════════════════════════════════════════╝");
    println!();

    if !config.sensors.is_empty() {
        println!("  Loaded sensors:");
        for s in &config.sensors {
            println!("    • {} ({}, battery={}%, rssi={})",
                s.mac, s.sensor_type, s.battery, s.rssi);
        }
        println!();
    }

    // Save initial config (creates the file if it doesn't exist)
    config.save(&config_path);

    let auto_reconnect = config.gateway.auto_reconnect;
    let reconnect_delay = config.gateway.reconnect_delay_secs;

    // Connection loop (with optional auto-reconnect)
    loop {
        match run_session(&config, &config_path).await {
            Ok(()) => {
                info!("Session ended normally.");
            }
            Err(e) => {
                error!("Session error: {}", e);
            }
        }

        if !auto_reconnect {
            break;
        }

        info!("Reconnecting in {} seconds...", reconnect_delay);
        tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay)).await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Session — one WebSocket connection lifetime
// ---------------------------------------------------------------------------

async fn run_session(
    config: &VirtualDongleConfig,
    config_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Build WebSocket URL
    let mut ws_url = format!("{}?device=virtual-dongle", config.gateway.url);
    if let Some(ref token) = config.gateway.auth_token {
        ws_url.push_str(&format!("&token={}", token));
    }

    info!("Connecting to gateway: {}", ws_url);
    let (ws_stream, _response) = connect_async(&ws_url).await?;
    info!("WebSocket connection established!");

    let (ws_writer, ws_reader) = ws_stream.split();
    let ws_writer = Arc::new(Mutex::new(ws_writer));

    // Shared dongle state
    let state = Arc::new(Mutex::new(DongleState::new(config.clone(), config_path.clone())));

    // Notify when scan mode is enabled (wakes up the CLI prompt)
    let scan_notify = Arc::new(Notify::new());

    // Channel for CLI/auto-events → packet injection
    let (inject_tx, mut inject_rx) = mpsc::channel::<Vec<u8>>(64);

    // Task 1: Read from WebSocket, process commands, send responses
    let ws_writer_cmd = Arc::clone(&ws_writer);
    let state_cmd = Arc::clone(&state);
    let scan_notify_cmd = Arc::clone(&scan_notify);
    let inject_tx_scan = inject_tx.clone();

    let read_task = tokio::spawn(async move {
        let mut ws_reader = ws_reader;
        let mut buffer = Vec::new();

        while let Some(msg) = ws_reader.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    buffer.extend_from_slice(&data);

                    // Parse packets from buffer
                    while let Some(start_idx) = find_magic(&buffer) {
                        if start_idx > 0 {
                            buffer.drain(..start_idx);
                        }
                        match Packet::parse(&buffer) {
                            Ok((pkt, consumed)) => {
                                buffer.drain(..consumed);
                                handle_command(
                                    pkt,
                                    &ws_writer_cmd,
                                    &state_cmd,
                                    &scan_notify_cmd,
                                    &inject_tx_scan,
                                ).await;
                            }
                            Err(e) => {
                                if e.contains("too short") {
                                    break; // need more data
                                } else {
                                    warn!("Packet parse error: {}", e);
                                    if buffer.len() >= 2 { buffer.drain(..2); }
                                    else { buffer.clear(); }
                                }
                            }
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket closed by server");
                    break;
                }
                Ok(_) => continue,
                Err(e) => {
                    error!("WebSocket read error: {}", e);
                    break;
                }
            }
        }
    });

    // Task 2: Forward injected event packets to WebSocket
    let ws_writer_inject = Arc::clone(&ws_writer);
    let inject_task = tokio::spawn(async move {
        while let Some(data) = inject_rx.recv().await {
            let mut writer = ws_writer_inject.lock().await;
            if let Err(e) = writer.send(Message::Binary(data)).await {
                error!("Failed to inject event via WebSocket: {}", e);
                break;
            }
        }
    });

    // Task 3: Auto-event emitters for sensors with auto_event_interval_secs
    let state_auto = Arc::clone(&state);
    let inject_tx_auto = inject_tx.clone();
    let auto_task = tokio::spawn(async move {
        // Wait for handshake to finish before emitting
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let st = state_auto.lock().await;
        let dongle_mac = st.config.dongle.mac.clone();
        let sensors: Vec<SensorConfig> = st.config.sensors.clone();
        drop(st);

        let mut handles = Vec::new();
        for sensor in sensors {
            if let Some(interval) = sensor.auto_event_interval_secs {
                if interval == 0 { continue; }
                let tx = inject_tx_auto.clone();
                let mac = dongle_mac.clone();
                let s = sensor.clone();
                let handle = tokio::spawn(async move {
                    let mut event_seq: u8 = 0;
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                        event_seq = event_seq.wrapping_add(1);
                        let pkt = build_auto_event_packet(&mac, &s, event_seq);
                        info!("⏱ Auto-event for {} ({})", s.mac, s.sensor_type);
                        if tx.send(pkt).await.is_err() { break; }
                    }
                });
                handles.push(handle);
            }
        }

        if handles.is_empty() {
            // No auto-event sensors — sleep forever (cancelled when session ends)
            loop { tokio::time::sleep(std::time::Duration::from_secs(86400)).await; }
        }

        // Wait for all auto-emitters (they run forever until the session ends)
        for h in handles {
            let _ = h.await;
        }
    });

    // Task 4: Interactive CLI
    let state_cli = Arc::clone(&state);
    let inject_tx_cli = inject_tx.clone();
    let scan_notify_cli = Arc::clone(&scan_notify);

    let cli_task = tokio::spawn(async move {
        // Wait for handshake to complete
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        println!();
        println!("─── Virtual Dongle Interactive Console ───");
        println!("Type 'help' for available commands.");
        println!();

        let (line_tx, mut line_rx) = mpsc::channel::<String>(16);

        // Blocking stdin reader thread
        std::thread::spawn(move || {
            let stdin = std::io::stdin();
            loop {
                print!("> ");
                std::io::stdout().flush().ok();

                let mut line = String::new();
                match stdin.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let trimmed = line.trim().to_string();
                        if !trimmed.is_empty() {
                            if line_tx.blocking_send(trimmed).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        loop {
            tokio::select! {
                // Check for scan mode activation
                _ = scan_notify_cli.notified() => {
                    let scanning = {
                        let st = state_cli.lock().await;
                        st.scanning
                    };
                    if scanning {
                        println!();
                        println!("📡 SCAN MODE ENABLED — Enter sensor MAC to simulate pairing");
                        println!("   Format: <MAC> [type]  (type: contact, motion, climate, leak)");
                        println!("   Example: ABCD1234 motion");
                        print!("scan> ");
                        std::io::stdout().flush().ok();
                    } else {
                        println!("📡 Scan mode disabled.");
                    }
                }
                // Process CLI input
                Some(line) = line_rx.recv() => {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.is_empty() { continue; }

                    let cmd = parts[0].to_lowercase();

                    let is_scanning = {
                        let st = state_cli.lock().await;
                        st.scanning
                    };

                    // If scanning and user enters a MAC, emit a scan event
                    if is_scanning && !["stopscan", "help", "quit", "sensors", "exit"].contains(&cmd.as_str()) {
                        let sensor_mac = parts[0].to_uppercase();
                        if sensor_mac.len() != 8 || !sensor_mac.chars().all(|c| c.is_ascii_alphanumeric()) {
                            println!("  ⚠ Invalid MAC. Must be 8 alphanumeric characters (e.g. ABCD1234)");
                            print!("scan> ");
                            std::io::stdout().flush().ok();
                            continue;
                        }

                        // Parse optional sensor type (default: contact)
                        let type_str = if parts.len() > 1 { parts[1].to_lowercase() } else { "contact".into() };
                        let s_type = type_str.parse::<SensorType>().unwrap_or(SensorType::ContactV2);

                        println!("  📡 Emitting scan event for {} ({:?})", sensor_mac, s_type);

                        // Build scan packet
                        let mut payload = Vec::new();
                        payload.push(0xA3); // scan event sub-type
                        let mac_bytes = sensor_mac.as_bytes();
                        payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
                        while payload.len() < 9 { payload.push(b'0'); }
                        payload.push(s_type.to_u8());
                        payload.push(0x19); // version byte

                        let pkt = Packet::new_async(0x20, payload);
                        let _ = inject_tx_cli.send(pkt.to_bytes()).await;

                        // Add to config and persist
                        {
                            let mut st = state_cli.lock().await;
                            if st.get_sensor(&sensor_mac).is_none() {
                                st.add_sensor(SensorConfig {
                                    mac: sensor_mac.clone(),
                                    sensor_type: type_str,
                                    battery: default_battery(),
                                    rssi: default_rssi(),
                                    state: Some("closed".into()),
                                    temperature: None,
                                    humidity: None,
                                    leak_state: None,
                                    auto_event_interval_secs: None,
                                });
                            }
                        }
                        println!("  ✅ Sensor {} added and config saved", sensor_mac);
                        print!("scan> ");
                        std::io::stdout().flush().ok();
                        continue;
                    }

                    // Normal command processing
                    let dongle_mac = {
                        let st = state_cli.lock().await;
                        st.config.dongle.mac.clone()
                    };

                    match cmd.as_str() {
                        "stopscan" => {
                            println!("  (Gateway must send scan disable command)");
                        }
                        "alarm" => {
                            if parts.len() < 3 {
                                println!("  Usage: alarm <MAC> <open|close>");
                                continue;
                            }
                            let sensor_mac = parts[1].to_uppercase();
                            let state_val: u8 = match parts[2].to_lowercase().as_str() {
                                "open" | "on" | "1" | "active" => 1,
                                _ => 0,
                            };

                            // Use per-sensor config if available
                            let st = state_cli.lock().await;
                            let (battery, rssi) = st.get_sensor(&sensor_mac)
                                .map(|s| (s.battery, s.rssi))
                                .unwrap_or((default_battery(), default_rssi()));
                            drop(st);

                            let pkt = build_alarm_packet(&dongle_mac, &sensor_mac, state_val, battery, rssi);
                            let _ = inject_tx_cli.send(pkt).await;
                            let state_name = if state_val == 1 { "OPEN/ACTIVE" } else { "CLOSED/INACTIVE" };
                            println!("  🚨 Injected alarm: {} → {} (battery={}%, rssi=-{}dBm)",
                                sensor_mac, state_name, battery, rssi);
                        }
                        "heartbeat" | "hb" => {
                            if parts.len() < 2 {
                                println!("  Usage: heartbeat <MAC>");
                                continue;
                            }
                            let sensor_mac = parts[1].to_uppercase();
                            let st = state_cli.lock().await;
                            let (battery, rssi) = st.get_sensor(&sensor_mac)
                                .map(|s| (s.battery, s.rssi))
                                .unwrap_or((default_battery(), default_rssi()));
                            drop(st);

                            let pkt = build_heartbeat_packet(&dongle_mac, &sensor_mac, battery, rssi);
                            let _ = inject_tx_cli.send(pkt).await;
                            println!("  💓 Injected heartbeat for {} (battery={}%, rssi=-{}dBm)",
                                sensor_mac, battery, rssi);
                        }
                        "climate" => {
                            if parts.len() < 4 {
                                println!("  Usage: climate <MAC> <temp_c> <humidity_%>");
                                println!("  Example: climate ABCD1234 22.5 45");
                                continue;
                            }
                            let sensor_mac = parts[1].to_uppercase();
                            let temp: f32 = parts[2].parse().unwrap_or(22.0);
                            let humidity: u8 = parts[3].parse().unwrap_or(50);
                            let st = state_cli.lock().await;
                            let (battery, rssi) = st.get_sensor(&sensor_mac)
                                .map(|s| (s.battery, s.rssi))
                                .unwrap_or((default_battery(), default_rssi()));
                            drop(st);

                            let pkt = build_climate_packet(&dongle_mac, &sensor_mac, temp, humidity, battery, rssi);
                            let _ = inject_tx_cli.send(pkt).await;
                            println!("  🌡️  Injected climate: {} → {:.1}°C, {}%", sensor_mac, temp, humidity);
                        }
                        "leak" => {
                            if parts.len() < 3 {
                                println!("  Usage: leak <MAC> <dry|wet>");
                                continue;
                            }
                            let sensor_mac = parts[1].to_uppercase();
                            let state_val: u8 = match parts[2].to_lowercase().as_str() {
                                "wet" | "1" => 1,
                                _ => 0,
                            };
                            let st = state_cli.lock().await;
                            let (battery, rssi) = st.get_sensor(&sensor_mac)
                                .map(|s| (s.battery, s.rssi))
                                .unwrap_or((default_battery(), default_rssi()));
                            drop(st);

                            let pkt = build_leak_packet(&dongle_mac, &sensor_mac, state_val, battery, rssi);
                            let _ = inject_tx_cli.send(pkt).await;
                            let state_name = if state_val == 1 { "WET" } else { "DRY" };
                            println!("  💧 Injected leak: {} → {}", sensor_mac, state_name);
                        }
                        "sensors" | "list" => {
                            let st = state_cli.lock().await;
                            if st.config.sensors.is_empty() {
                                println!("  No sensors paired.");
                            } else {
                                println!("  Paired sensors ({}):", st.config.sensors.len());
                                for s in &st.config.sensors {
                                    let extra = match s.sensor_type.as_str() {
                                        "climate" => format!(", temp={:.1}°C, humidity={}%",
                                            s.temperature.unwrap_or(22.0),
                                            s.humidity.unwrap_or(50)),
                                        "leak" => format!(", state={}",
                                            s.leak_state.as_deref().unwrap_or("dry")),
                                        _ => format!(", state={}",
                                            s.state.as_deref().unwrap_or("closed")),
                                    };
                                    let auto = s.auto_event_interval_secs
                                        .map(|i| format!(", auto={}s", i))
                                        .unwrap_or_default();
                                    println!("    • {} ({}, battery={}%, rssi=-{}dBm{}{})",
                                        s.mac, s.sensor_type, s.battery, s.rssi, extra, auto);
                                }
                            }
                        }
                        "help" | "?" => {
                            println!("  Commands:");
                            println!("    alarm <MAC> <open|close>     Inject alarm event");
                            println!("    heartbeat <MAC>              Inject heartbeat");
                            println!("    climate <MAC> <temp> <hum>   Inject climate event");
                            println!("    leak <MAC> <dry|wet>         Inject leak event");
                            println!("    sensors                      List paired sensors");
                            println!("    help                         Show this help");
                            println!("    quit                         Exit");
                        }
                        "quit" | "exit" => {
                            println!("  Disconnecting...");
                            std::process::exit(0);
                        }
                        _ => {
                            println!("  Unknown command: '{}'. Type 'help' for usage.", cmd);
                        }
                    }
                }
            }
        }
    });

    // Wait for any task to finish (read_task ending = disconnected)
    tokio::select! {
        _ = read_task => { info!("WebSocket read task ended — disconnected"); }
        _ = inject_task => { error!("Inject task ended unexpectedly"); }
        _ = auto_task => { info!("Auto-event task ended"); }
        _ = cli_task => { info!("CLI task ended"); }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Packet protocol helpers
// ---------------------------------------------------------------------------

/// Find the magic prefix 0x55 0xAA or 0xAA 0x55 in a byte buffer.
/// The protocol uses both byte orders (sync vs async packet types).
fn find_magic(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(1) {
        let val = ((buf[i] as u16) << 8) | (buf[i + 1] as u16);
        if val == 0x55AA || val == 0xAA55 {
            return Some(i);
        }
    }
    None
}

/// Handle an incoming command packet from the gateway's Engine.
async fn handle_command(
    pkt: Packet,
    ws_writer: &Arc<Mutex<futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        Message,
    >>>,
    state: &Arc<Mutex<DongleState>>,
    scan_notify: &Arc<Notify>,
    _inject_tx: &mpsc::Sender<Vec<u8>>,
) {
    let cmd = pkt.cmd();
    debug!("<=== Received command: {:04X}", cmd);

    let st = state.lock().await;
    let mac = st.config.dongle.mac.clone();
    let version = st.config.dongle.version.clone();
    let paired = st.paired_macs();
    drop(st);

    let response_bytes = match cmd {
        // --- Handshake commands ---
        commands::CMD_INQUIRY => {
            info!("  Handshake 1/5: Inquiry");
            Some(Packet::new_sync(0x28, vec![0x01]).to_bytes())
        }
        commands::CMD_GET_ENR => {
            info!("  Handshake 2/5: ENR exchange");
            Some(Packet::new_sync(0x03, vec![0x42; 16]).to_bytes())
        }
        commands::CMD_GET_MAC => {
            info!("  Handshake 3/5: MAC → {}", mac);
            Some(Packet::new_sync(0x05, mac.as_bytes().to_vec()).to_bytes())
        }
        commands::CMD_GET_VERSION => {
            info!("  Handshake 4/5: Version → {}", version);
            Some(Packet::new_async(0x17, version.as_bytes().to_vec()).to_bytes())
        }
        commands::CMD_FINISH_AUTH => {
            info!("  Handshake 5/5: Auth complete ✅");
            Some(Packet::new_async(0x15, vec![]).to_bytes())
        }

        // --- Sensor list protocol ---
        commands::CMD_GET_SENSOR_COUNT => {
            let count = paired.len() as u8;
            info!("  Sensor count: {}", count);
            Some(Packet::new_async(0x2F, vec![count]).to_bytes())
        }
        commands::CMD_GET_SENSOR_LIST => {
            let count = paired.len() as u8;
            info!("  Sensor list: {} items", count);

            let mut combined = Packet::new_async(0x30, vec![count]).to_bytes();
            for sensor_mac in &paired {
                let mac_bytes = sensor_mac.as_bytes();
                let mut payload = Vec::new();
                payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
                while payload.len() < 8 { payload.push(b'0'); }
                let item_pkt = Packet::new_async(0x31, payload);
                combined.extend_from_slice(&item_pkt.to_bytes());
            }
            Some(combined)
        }

        // --- Scan mode ---
        commands::CMD_SET_SCAN => {
            let enable = pkt.payload_bytes().map(|b| b.first().copied().unwrap_or(0) == 1).unwrap_or(false);
            let resp_byte = if enable { 0x01 } else { 0x00 };
            info!("  Scan mode: {}", if enable { "ENABLED" } else { "DISABLED" });

            {
                let mut st = state.lock().await;
                st.scanning = enable;
            }
            scan_notify.notify_one();

            Some(Packet::new_async(0x1D, vec![resp_byte]).to_bytes())
        }

        // --- Pairing commands ---
        commands::CMD_GET_R1 => {
            debug!("  R1 exchange (crypto token)");
            Some(Packet::new_async(0x22, vec![0xAA; 16]).to_bytes())
        }
        commands::CMD_VERIFY_SENSOR => {
            info!("  Verify sensor → success");
            Some(Packet::new_async(0x24, vec![0x01]).to_bytes())
        }

        // --- Delete sensor ---
        commands::CMD_DELETE_SENSOR => {
            if let Some(payload) = pkt.payload_bytes() {
                if let Ok(sensor_mac) = String::from_utf8(payload.to_vec()) {
                    let trimmed = sensor_mac.trim_end_matches('\0').to_string();
                    info!("  Deleting sensor: {}", trimmed);
                    let mut st = state.lock().await;
                    st.remove_sensor(&trimmed);
                }
            }
            Some(Packet::new_async(0x26, vec![0x01]).to_bytes())
        }

        // --- Chime ---
        commands::CMD_PLAY_CHIME => {
            info!("  Play chime 🔔");
            Some(Packet::new_async(0x71, vec![0x01]).to_bytes())
        }

        // ACKs from the Engine — absorb silently
        commands::CMD_ASYNC_ACK => None,
        _ if cmd & 0xFF00 == 0x4300 => None,

        _ => {
            warn!("  Unknown command {:04X}, ignoring", cmd);
            None
        }
    };

    if let Some(data) = response_bytes {
        let mut writer = ws_writer.lock().await;
        if let Err(e) = writer.send(Message::Binary(data)).await {
            error!("Failed to send response: {}", e);
        }
    }
}

// ---------------------------------------------------------------------------
// Event packet builders
// ---------------------------------------------------------------------------

fn now_timestamp_bytes() -> [u8; 8] {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    now.to_be_bytes()
}

/// Build a CMD_ALARM1 (0x5319) packet for contact/motion alarm events.
fn build_alarm_packet(_dongle_mac: &str, sensor_mac: &str, state: u8, battery: u8, rssi: u8) -> Vec<u8> {
    let event_type = DongleEvent::EVENT_TYPE_ALARM;
    let mut payload = Vec::new();

    payload.extend_from_slice(&now_timestamp_bytes());
    payload.push(event_type);

    let mac_bytes = sensor_mac.as_bytes();
    payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
    while payload.len() < 17 { payload.push(b'0'); }

    payload.push(SensorType::ContactV2.to_u8());

    payload.push(0x01);    // die temperature
    payload.push(battery); // battery raw
    payload.push(0x00);
    payload.push(0x00);
    payload.push(state);   // 0=closed, 1=open
    payload.push(0x00);
    payload.push(0x0A);    // event sequence
    payload.push(rssi);    // RSSI

    Packet::new_async(0x19, payload).to_bytes()
}

/// Build a CMD_ALARM1 heartbeat packet.
fn build_heartbeat_packet(_dongle_mac: &str, sensor_mac: &str, battery: u8, rssi: u8) -> Vec<u8> {
    let event_type = DongleEvent::EVENT_TYPE_HEARTBEAT;
    let mut payload = Vec::new();

    payload.extend_from_slice(&now_timestamp_bytes());
    payload.push(event_type);

    let mac_bytes = sensor_mac.as_bytes();
    payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
    while payload.len() < 17 { payload.push(b'0'); }

    payload.push(SensorType::ContactV2.to_u8());

    payload.push(0x02);    // die temp
    payload.push(battery); // battery raw
    payload.push(0x00);
    payload.push(0x00);
    payload.push(0x00);    // state (inactive)
    payload.push(0x00);
    payload.push(0x0B);    // event sequence
    payload.push(rssi);    // RSSI

    Packet::new_async(0x19, payload).to_bytes()
}

/// Build a climate event packet.
fn build_climate_packet(_dongle_mac: &str, sensor_mac: &str, temp: f32, humidity: u8, battery: u8, rssi: u8) -> Vec<u8> {
    let event_type = DongleEvent::EVENT_TYPE_CLIMATE;
    let mut payload = Vec::new();

    payload.extend_from_slice(&now_timestamp_bytes());
    payload.push(event_type);

    let mac_bytes = sensor_mac.as_bytes();
    payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
    while payload.len() < 17 { payload.push(b'0'); }

    payload.push(SensorType::ClimateV2.to_u8());

    let temp_hi = temp.trunc() as i8;
    let temp_lo = ((temp.fract()) * 100.0) as u8;

    payload.push(0x01);           // die temperature
    payload.push(battery);        // battery
    payload.push(0x00);           // marker
    payload.push(0x03);           // marker for climate
    payload.push(temp_hi as u8);  // temperature integer part
    payload.push(temp_lo);        // temperature fractional part
    payload.push(humidity);       // humidity
    payload.push(0x00);
    payload.push(0x0C);           // event sequence
    payload.push(rssi);           // RSSI

    Packet::new_async(0x19, payload).to_bytes()
}

/// Build a leak event packet (CMD_ALARM2 = 0x5355).
fn build_leak_packet(_dongle_mac: &str, sensor_mac: &str, state: u8, battery: u8, rssi: u8) -> Vec<u8> {
    let event_type = DongleEvent::EVENT_TYPE_LEAK;
    let mut payload = Vec::new();

    payload.push(event_type);

    let mac_bytes = sensor_mac.as_bytes();
    payload.extend_from_slice(&mac_bytes[..std::cmp::min(mac_bytes.len(), 8)]);
    while payload.len() < 9 { payload.push(b'0'); }

    payload.push(SensorType::LeakV2.to_u8());

    payload.push(0x00);    // unk
    payload.push(0x00);    // unk
    payload.push(battery); // battery [2]
    payload.push(0x00);    // unk [3]
    payload.push(0x00);    // unk [4]
    payload.push(state);   // main leak state [5]
    payload.push(0x00);    // probe state [6]
    payload.push(0x00);    // probe available (0 = no probe) [7]
    payload.push(0x00);    // unk [8]
    payload.push(0x00);    // unk [9]
    payload.push(rssi);    // RSSI [10]

    Packet::new_async(0x55, payload).to_bytes()
}

/// Build an auto-event packet based on the sensor's config type and state.
fn build_auto_event_packet(dongle_mac: &str, sensor: &SensorConfig, _seq: u8) -> Vec<u8> {
    match sensor.sensor_type.as_str() {
        "motion" | "motionv2" => {
            build_alarm_packet(dongle_mac, &sensor.mac, 1, sensor.battery, sensor.rssi)
        }
        "climate" => {
            // Add slight jitter to temperature for realism
            let temp = sensor.temperature.unwrap_or(22.0);
            let humidity = sensor.humidity.unwrap_or(50);
            build_climate_packet(dongle_mac, &sensor.mac, temp, humidity, sensor.battery, sensor.rssi)
        }
        "leak" => {
            let state = sensor.leak_byte();
            build_leak_packet(dongle_mac, &sensor.mac, state, sensor.battery, sensor.rssi)
        }
        _ => {
            // contact — toggle
            let state = sensor.state_byte();
            build_alarm_packet(dongle_mac, &sensor.mac, state, sensor.battery, sensor.rssi)
        }
    }
}
