use wyzesense2mqtt_rs::engine::Engine;
use wyzesense2mqtt_rs::transport::hidraw::HidrawTransport;
use wyzesense2mqtt_rs::transport::AsyncTransport;
use wyzesense2mqtt_rs::protocol::telemetry::{TelemetryData, DongleEvent, SensorType};
use wyzesense2mqtt_rs::web::start_web_server;
use wyzesense2mqtt_rs::config::app_config::AppConfig;
use wyzesense2mqtt_rs::gateway::mqtt::{MqttGateway, GatewayCommand};
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;
use wyzesense2mqtt_rs::config::monitor::AvailabilityMonitor;
use std::sync::Mutex;

use std::time::Duration;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn, Level};
use tracing_subscriber::fmt;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Strip CLI arguments dynamically using a helper function
    let mut args: Vec<String> = std::env::args().collect();

    // Intercept help request immediately
    if args.iter().any(|x| x == "--help" || x == "-h" || x == "help") {
        print_help_manual();
        return Ok(());
    }

    // Intercept version request immediately
    if args.iter().any(|x| x == "--version" || x == "-V" || x == "version") {
        println!("Wyze Sense to MQTT Bridge (Rust) v{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let config_path = strip_arg_string(&mut args, "--config", "-c");
    let usb_dongle = strip_arg_string(&mut args, "--usb-dongle", "-d");
    let log_level = strip_arg_string(&mut args, "--log-level", "-l");
    let no_ansi = strip_arg_bool(&mut args, "--no-ansi");
    
    let web_enabled = strip_arg_string(&mut args, "--web-enabled", "--web-enabled")
        .and_then(|s| s.parse::<bool>().ok());
    let web_port = strip_arg_string(&mut args, "--web-port", "-p")
        .and_then(|s| s.parse::<u16>().ok());

    let mqtt_enabled = strip_arg_string(&mut args, "--mqtt-enabled", "--mqtt-enabled")
        .and_then(|s| s.parse::<bool>().ok());
    let mqtt_host = strip_arg_string(&mut args, "--mqtt-host", "--mqtt-host");
    let mqtt_port = strip_arg_string(&mut args, "--mqtt-port", "--mqtt-port")
        .and_then(|s| s.parse::<u16>().ok());
    let mqtt_username = strip_arg_string(&mut args, "--mqtt-user", "--mqtt-user");
    let mqtt_password = strip_arg_string(&mut args, "--mqtt-password", "--mqtt-password");
    let mqtt_self_topic = strip_arg_string(&mut args, "--mqtt-self-topic", "--mqtt-self-topic");
    let mqtt_hass_topic = strip_arg_string(&mut args, "--mqtt-hass-topic", "--mqtt-hass-topic");

    // 2. Load Application Configuration
    let mut config = AppConfig::load(config_path.as_deref());

    // 3. Apply CLI Precedence Overrides
    if let Some(val) = usb_dongle {
        config.usb.dongle = val;
    }
    if let Some(val) = log_level {
        config.logging.level = val;
    }
    if no_ansi {
        config.logging.no_ansi = true;
    }
    if let Some(val) = web_enabled {
        config.web.enabled = val;
    }
    if let Some(val) = web_port {
        config.web.port = val;
    }
    if let Some(val) = mqtt_enabled {
        config.mqtt.enabled = val;
    }
    if let Some(val) = mqtt_host {
        config.mqtt.host = Some(val);
    }
    if let Some(val) = mqtt_port {
        config.mqtt.port = val;
    }
    if let Some(val) = mqtt_username {
        config.mqtt.username = Some(val);
    }
    if let Some(val) = mqtt_password {
        config.mqtt.password = Some(val);
    }
    if let Some(val) = mqtt_self_topic {
        config.mqtt.self_topic_root = val;
    }
    if let Some(val) = mqtt_hass_topic {
        config.mqtt.hass_topic_root = val;
    }

    // Ensure that if host is overridden, MQTT is enabled
    if config.mqtt.host.is_some() {
        config.mqtt.enabled = true;
    }

    // 4. Initialize Logger Tracing dynamically with the resolved level
    let tracing_level = match config.logging.level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    // Set up logging: file-based with rotation, or stdout
    // The _guard must be held for the lifetime of the program to ensure logs are flushed.
    let _log_guard: Box<dyn std::any::Any> = if let Some(ref log_file) = config.logging.log_file {
        // Resolve relative paths against the config file's directory,
        // not CWD, so behavior is predictable regardless of launch method.
        let log_path = std::path::PathBuf::from(log_file);
        let log_path = if log_path.is_relative() {
            let config_dir = config_path.as_deref()
                .map(|p| std::path::Path::new(p).parent().unwrap_or(std::path::Path::new(".")))
                .unwrap_or(std::path::Path::new("."));
            config_dir.join(&log_path)
        } else {
            log_path
        };

        let log_dir = log_path.parent().unwrap_or(std::path::Path::new("."));
        let log_prefix = log_path.file_stem()
            .and_then(|f| f.to_str())
            .unwrap_or("wyzesense2mqtt-rs");
        let log_suffix = log_path.extension()
            .and_then(|f| f.to_str())
            .unwrap_or("log");

        // Create log directory if needed
        if let Err(e) = std::fs::create_dir_all(log_dir) {
            eprintln!("WARNING: Failed to create log directory {:?}: {}", log_dir, e);
        }

        let file_appender = tracing_appender::rolling::RollingFileAppender::builder()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix(log_prefix)
            .filename_suffix(log_suffix)
            .build(log_dir)
            .expect("Failed to create log file appender");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let subscriber = fmt::Subscriber::builder()
            .with_max_level(tracing_level)
            .with_ansi(false)  // No color codes in files
            .with_writer(non_blocking)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);

        info!("File logging enabled: dir={:?}, prefix={}, suffix={}, rotation=daily, max_files={}",
            log_dir, log_prefix, log_suffix, config.logging.max_log_files);

        // Clean up old rotated log files
        cleanup_old_logs(log_dir, log_prefix, log_suffix, config.logging.max_log_files);

        Box::new(guard)
    } else {
        let subscriber = fmt::Subscriber::builder()
            .with_max_level(tracing_level)
            .with_ansi(!config.logging.no_ansi)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
        Box::new(())  // No guard needed for stdout
    };

    info!("===============================================================");
    info!("        wyzesense2mqtt-rs v{} Starting up... ", env!("CARGO_PKG_VERSION"));
    info!("===============================================================");
    info!("Configuration loaded successfully. Log Level set to: {}", config.logging.level);

    // 5. Run Auto-Discovery for USB Dongle path if set to "auto"
    if config.usb.dongle.to_lowercase() == "auto" {
        info!("USB Dongle path set to 'auto'. Scanning Linux sysfs class...");
        match wyzesense2mqtt_rs::transport::hidraw::discover_dongle_device() {
            Ok(discovered_path) => {
                info!("Auto-Discovery: Found Wyze Sense Bridge on {}", discovered_path);
                config.usb.dongle = discovered_path;
            }
            Err(e) => {
                error!("Auto-Discovery failed: {}", e);
                return Err(e);
            }
        }
    }

    if args.len() > 1 {
        // A subcommand was passed! Run CLI Subcommand Client mode
        let cmd = args[1].to_lowercase();
        let daemon_url = format!("http://127.0.0.1:{}", config.web.port);
        
        info!("CLI Mode: Checking if local daemon is running on {}...", daemon_url);
        let client = reqwest::Client::new();
        
        let is_daemon_active = match tokio::time::timeout(Duration::from_millis(350), client.get(&format!("{}/api/dongle", daemon_url)).send()).await {
            Ok(Ok(res)) => res.status() == 200,
            _ => false,
        };

        if is_daemon_active {
            info!("Daemon is active! Executing command via REST APIs...");
            return run_cli_via_rest(&client, &daemon_url, &cmd, &args[2..]).await;
        } else {
            info!("Daemon is offline. Executing local direct HID fallback on {}...", config.usb.dongle);
            return run_cli_via_hid(&config.usb.dongle, &cmd, &args[2..]).await;
        }
    }

    // 5. Default Mode: Start the Unified Daemon
    info!("Initializing Wyze Sense to MQTT Bridge (Rust) Daemon mode...");
    info!("  USB Device: {}", config.usb.dongle);
    info!("  Web Console: http://localhost:{}", config.web.port);

    // Open Hidraw transport asynchronously
    let transport = match HidrawTransport::open(&config.usb.dongle).await {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to open USB hidraw device [{}]: {}", config.usb.dongle, e);
            return Err(e.into());
        }
    };

    return run_daemon(transport, config).await;
}

async fn run_daemon<T: wyzesense2mqtt_rs::transport::AsyncTransport + Clone + 'static>(
    transport: T,
    config: AppConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // Setup central events channel
    let (event_tx, event_rx) = mpsc::channel::<DongleEvent>(128);

    // Setup mpsc channels for MQTT gateway command callbacks
    let (gateway_cmd_tx, mut gateway_cmd_rx) = mpsc::channel::<GatewayCommand>(32);

    // Instantiate the core engine
    let state_path = "config/state.yaml";
    let config_path = "config/sensors.yaml";
    let mut engine = Engine::new(transport, event_tx.clone(), Some(state_path.to_string()));

    // Instantiate the SensorManager
    let sensor_manager = Arc::new(Mutex::new(SensorManager::new(
        config_path.to_string(),
        state_path.to_string(),
    )));

    // Start background worker loop
    let _exit_tx = engine.start();

    // Perform dongle unlock handshake
    match tokio::time::timeout(Duration::from_secs(8), engine.initialize_handshake()).await {
        Ok(Ok(_)) => {
            info!("Dongle successfully unlocked and authenticated!");
            engine.set_auto_verify(true);

            // Warm up paired sensors cache from NVRAM and merge with saved configs
            info!("Warming up paired sensors cache from NVRAM...");
            match engine.get_sensor_list().await {
                Ok(sensors_list) => {
                    let mut manager = sensor_manager.lock().unwrap();
                    if let Err(e) = manager.load_sensors(&sensors_list) {
                        error!("Failed to load/bootstrap sensors: {}", e);
                    } else {
                        info!("Sensors memory cache successfully warmed up!");
                    }
                }
                Err(e) => {
                    warn!("Failed to warm up sensors cache on startup: {}", e);
                }
            }
        }
        Ok(Err(e)) => {
            error!("Failed during dongle handshake: {}", e);
            return Err(e.into());
        }
        Err(_) => {
        error!("Dongle handshake timed out! Make sure USB device is connected.");
            return Err("Handshake timeout".into());
        }
    }

    // Start dynamic Availability Monitor in the background
    let (offline_tx, mut offline_rx) = mpsc::channel::<String>(32);
    let monitor = AvailabilityMonitor::new(
        Arc::clone(&sensor_manager),
        Duration::from_secs(30), // Run sweep check every 30 seconds
        offline_tx,
    );
    let (_monitor_shutdown_tx, monitor_shutdown_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        monitor.start(monitor_shutdown_rx).await;
    });

    // Forward offline events from AvailabilityMonitor to the central Event queue
    let event_tx_timeout = event_tx.clone();
    let sensor_manager_timeout = Arc::clone(&sensor_manager);
    tokio::spawn(async move {
        while let Some(mac) = offline_rx.recv().await {
            warn!("AvailabilityMonitor: Sensor {} timed out! Emitting offline telemetry event.", mac);
            let s_type = {
                let manager = sensor_manager_timeout.lock().unwrap();
                manager.get_sensors().get(&mac)
                    .map(|s| s.sensor_type)
                    .unwrap_or(SensorType::Unknown(0))
            };
            let offline_evt = DongleEvent {
                mac,
                timestamp: std::time::SystemTime::now(),
                sensor_type: s_type,
                event_type: 0x00,
                data: TelemetryData::Offline,
            };
            let _ = event_tx_timeout.send(offline_evt).await;
        }
    });

    // Setup Output Gateways Concurrently
    
    // Route A: MQTT Broker Gateway (if enabled)
    let mut mqtt_gateway_handle = None;
    if config.mqtt.enabled {
        if let Some(ref host) = config.mqtt.host {
            info!("MQTT Gateway is enabled. Connecting to broker at {}:{}...", host, config.mqtt.port);
            let mut mqtt_options = rumqttc::v5::MqttOptions::new(
                "wyzesensers_daemon",
                host,
                config.mqtt.port
            );
            if let (Some(user), Some(pass)) = (&config.mqtt.username, &config.mqtt.password) {
                mqtt_options.set_credentials(user, pass);
            }

            // Set Last Will and Testament for bridge status with a 10 minute Will Delay
            let status_topic = format!("{}/status", config.mqtt.self_topic_root);
            
            let last_will_props = rumqttc::v5::mqttbytes::v5::LastWillProperties {
                delay_interval: Some(600), // 10 minutes delay!
                payload_format_indicator: None,
                message_expiry_interval: None,
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: Vec::new(),
            };
            
            let last_will = rumqttc::v5::mqttbytes::v5::LastWill::new(
                status_topic,
                "offline",
                rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                true, // retain
                Some(last_will_props)
            );
            
            mqtt_options.set_last_will(last_will);

            let gateway = MqttGateway::new(
                mqtt_options,
                event_rx, // Consumes event receiver
                gateway_cmd_tx,
                config.mqtt.self_topic_root.clone(),
                Arc::clone(&sensor_manager),
            );

            // Spawn MQTT bridge in background
            let handle = tokio::spawn(async move {
                if let Err(e) = gateway.run().await {
                    error!("MQTT Gateway encountered an error: {}", e);
                }
            });
            mqtt_gateway_handle = Some(handle);
        }
    } else {
        info!("MQTT Gateway is disabled. Serving Web Control Panel only.");
        // Drain the event receiver task so it doesn't backpressure the engine
        let mut drain_rx = event_rx;
        tokio::spawn(async move {
            while let Some(evt) = drain_rx.recv().await {
                info!("STATE EVENT: MAC={}, Data={:?}", evt.mac, evt.data);
            }
        });
    }

    // Spawn gateway callback command listener task
    let mut engine_tx = engine.clone();
    let sensor_manager_cmd_clone = Arc::clone(&sensor_manager);
    tokio::spawn(async move {
        while let Some(cmd) = gateway_cmd_rx.recv().await {
            match cmd {
                GatewayCommand::Scan(enable) => {
                    let _ = engine_tx.set_scan(enable).await;
                }
                GatewayCommand::Delete(mac) => {
                    let _ = engine_tx.delete_sensor(&mac).await;
                    let mut manager = sensor_manager_cmd_clone.lock().unwrap();
                    let _ = manager.delete_and_persist_sensor(&mac);
                }
                GatewayCommand::Reload => {
                    info!("Reload command received via gateway.");
                }
            }
        }
    });

    // Route B: Web REST Control Server (if enabled)
    if config.web.enabled {
        start_web_server(engine, Arc::clone(&sensor_manager), config.web.port).await?;
    } else {
        info!("Web Panel is disabled. Running in headless daemon mode.");
        if let Some(handle) = mqtt_gateway_handle {
            let _ = handle.await;
        } else {
            warn!("Both Web Panel and MQTT are disabled! Nothing to run. Stopping.");
        }
    }

    Ok(())
}

/// Strips a specific flag-key and its trailing string value from the args list if present.
fn strip_arg_string(args: &mut Vec<String>, flag1: &str, flag2: &str) -> Option<String> {
    if let Some(idx) = args.iter().position(|x| x == flag1 || x == flag2) {
        if idx + 1 < args.len() {
            let val = args[idx + 1].clone();
            args.remove(idx + 1);
            args.remove(idx);
            Some(val)
        } else {
            None
        }
    } else {
        None
    }
}

/// Strips a specific parameter-less boolean flag from the args list if present.
fn strip_arg_bool(args: &mut Vec<String>, flag: &str) -> bool {
    if let Some(idx) = args.iter().position(|x| x == flag) {
        args.remove(idx);
        true
    } else {
        false
    }
}

// --- REST CLIENT CLI MODE ---
async fn run_cli_via_rest(
    client: &reqwest::Client,
    base_url: &str,
    cmd: &str,
    args: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match cmd {
        "list" => {
            let res = client.get(&format!("{}/api/sensors", base_url)).send().await?;
            let body: serde_json::Value = res.json().await?;
            let sensors: Vec<String> = serde_json::from_value(body["sensors"].clone())?;
            println!("{} sensors paired to bridge:", sensors.len());
            for mac in sensors {
                println!("\tSensor: {}", mac);
            }
        }
        "pair" => {
            println!("Enabling pair scan mode... Trigger reset on your sensor.");
            client.post(&format!("{}/api/scan", base_url))
                .json(&json!({ "enable": true }))
                .send()
                .await?;
            println!("Scan enabled successfully! Pair via the Web Console dashboard.");
        }
        "unpair" => {
            if args.is_empty() { return Err("Missing sensor MAC address argument".into()); }
            let mac = &args[0];
            let res = client.delete(&format!("{}/api/sensors/{}", base_url, mac)).send().await?;
            let body: serde_json::Value = res.json().await?;
            println!("Result: {}", body["message"]);
        }
        "chime" => {
            if args.is_empty() { return Err("Missing sensor MAC address argument".into()); }
            let mac = &args[0];
            let res = client.post(&format!("{}/api/chime/{}", base_url, mac)).send().await?;
            let body: serde_json::Value = res.json().await?;
            println!("Result: {}", body["message"]);
        }
        "fix" => {
            println!("Fixing ghost sensors...");
            let res = client.post(&format!("{}/api/fix", base_url)).send().await?;
            let body: serde_json::Value = res.json().await?;
            println!("Purged {} ghost sensors: {:?}", body["purged_count"], body["purged_macs"]);
        }
        "raw" => {
            if args.is_empty() { return Err("Missing comma-separated hex bytes string argument".into()); }
            let bytes: Vec<u8> = args[0].split(",")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| u8::from_str_radix(s, 16).unwrap())
                .collect();
            let res = client.post(&format!("{}/api/raw", base_url))
                .json(&json!({ "bytes": bytes }))
                .send()
                .await?;
            let body: serde_json::Value = res.json().await?;
            let resp_bytes: Vec<u8> = serde_json::from_value(body["response_bytes"].clone())?;
            let hex_str = resp_bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join(",");
            println!("Raw response bytes: [{}]", hex_str);
        }
        _ => {
            println!("Unknown command: {}", cmd);
            println!("Available commands: list, pair, unpair <mac>, chime <mac>, fix, raw <hex_bytes>");
        }
    }
    Ok(())
}

// --- OFFLINE DIRECT HID FALLBACK CLI MODE ---
async fn run_cli_via_hid(
    device_path: &str,
    cmd: &str,
    args: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Special case: RAW command bypasses the Engine loop completely
    if cmd == "raw" {
        if args.is_empty() { return Err("Missing comma-separated hex bytes argument".into()); }
        let raw_bytes: Vec<u8> = args[0].split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| u8::from_str_radix(s, 16).unwrap())
            .collect();
        println!("Opening device {} for raw transmission...", device_path);
        let mut transport = HidrawTransport::open(device_path).await?;
        println!("===> Writing {} bytes...", raw_bytes.len());
        transport.write(&raw_bytes).await?;
        println!("<=== Reading response (waiting up to 1s)...");
        let mut read_buf = [0u8; 1024];
        match tokio::time::timeout(Duration::from_secs(1), transport.read(&mut read_buf)).await {
            Ok(Ok(n)) if n > 0 => {
                let hex_str: String = read_buf[..n].iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(",");
                println!("Received {} bytes: [{}]", n, hex_str);
            }
            Ok(Ok(_)) => println!("No data returned (0 bytes read)"),
            Ok(Err(e)) => eprintln!("Read error: {}", e),
            Err(_) => println!("Timeout. No response received within 1s."),
        }
        return Ok(());
    }

    // Open transport and run engine command
    let transport = HidrawTransport::open(device_path).await?;
    run_hid_command(transport, cmd, args).await
}

async fn run_hid_command<T: AsyncTransport + Clone + 'static>(
    transport: T,
    cmd: &str,
    args: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (event_tx, mut event_rx) = mpsc::channel(32);
    let mut engine = Engine::new(transport, event_tx, None);
    let _exit_tx = engine.start();

    engine.initialize_handshake().await?;
    let dongle_mac = engine.dongle_mac().unwrap_or("Unknown").to_string();
    let dongle_ver = engine.dongle_version().unwrap_or("Unknown").to_string();
    info!("Dongle unlocked: MAC={}, Version={}", dongle_mac, dongle_ver);

    match cmd {
        "list" => {
            let sensors = engine.get_sensor_list().await?;
            println!("{} sensors paired to bridge:", sensors.len());
            for mac in sensors {
                println!("\tSensor: {}", mac);
            }
        }
        "pair" => {
            println!("Entering pairing mode. Press reset pin on your Wyze sensor...");
            println!("Waiting for sensor scan events (timeout in 30 seconds)...");
            engine.set_scan(true).await?;

            let pair_future = async {
                while let Some(evt) = event_rx.recv().await {
                    if let TelemetryData::Scanned { .. } = evt.data {
                        return Some(evt);
                    }
                }
                None
            };

            match tokio::time::timeout(Duration::from_secs(30), pair_future).await {
                Ok(Some(evt)) => {
                    println!("Found sensor! MAC: {}, Type: {:?}", evt.mac, evt.sensor_type);

                    // Exchange R1 crypto token
                    println!("Exchanging R1 crypto token...");
                    match engine.get_sensor_r1(&evt.mac).await {
                        Ok(r1) => println!("R1 exchange successful ({} bytes)", r1.len()),
                        Err(e) => eprintln!("R1 exchange failed: {}. Continuing...", e),
                    }

                    // Verify and bind sensor
                    println!("Verifying and binding sensor...");
                    match engine.verify_sensor(&evt.mac, evt.sensor_type).await {
                        Ok(_) => println!("SUCCESS! Sensor {} paired and verified!", evt.mac),
                        Err(e) => eprintln!("Failed to verify sensor {}: {}", evt.mac, e),
                    }
                }
                Ok(None) => println!("Event channel closed unexpectedly."),
                Err(_) => println!("Timeout: No sensor scanned within 30 seconds."),
            }

            println!("Disabling scanning mode...");
            let _ = engine.set_scan(false).await;
        }
        "unpair" => {
            if args.is_empty() { return Err("Missing MAC address".into()); }
            let mac = &args[0];
            engine.delete_sensor(mac).await?;
            println!("Successfully deleted sensor {}", mac);
        }
        "chime" => {
            if args.is_empty() { return Err("Missing MAC address".into()); }
            let mac = &args[0];
            engine.play_chime(mac).await?;
            println!("Chime triggered on {}", mac);
        }
        "fix" => {
            println!("Fixing invalid MAC sensors...");
            let sensors = engine.get_sensor_list().await?;
            let mut purged = 0;
            let invalid_ghosts = ["00000000", "\0\0\0\0\0\0\0\0"];
            for mac in sensors {
                let is_invalid = mac.chars().any(|c| !c.is_alphanumeric()) || invalid_ghosts.contains(&mac.as_str());
                if is_invalid {
                    if let Ok(_) = engine.delete_sensor(&mac).await {
                        println!("\tPurged invalid sensor: {}", mac);
                        purged += 1;
                    }
                }
            }
            println!("Ghost purge completed. Purged {} sensors.", purged);
        }
        _ => {
            println!("Unknown subcommand: {}", cmd);
            println!("Available commands: list, pair, unpair <mac>, chime <mac>, fix, raw <hex>");
        }
    }
    Ok(())
}
/// Remove old rotated log files beyond the configured max.
/// tracing-appender names files as `{prefix}.YYYY-MM-DD.{suffix}`, so sorting
/// by name gives chronological order.
fn cleanup_old_logs(log_dir: &std::path::Path, prefix: &str, suffix: &str, max_files: usize) {
    let entries = match std::fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    let mut log_files: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|f| f.to_str())
                .map(|f| f.starts_with(prefix) && f.ends_with(suffix) && f != format!("{}.{}", prefix, suffix).as_str())
                .unwrap_or(false)
        })
        .collect();

    log_files.sort();

    if log_files.len() > max_files {
        let to_remove = log_files.len() - max_files;
        for old_file in &log_files[..to_remove] {
            if let Err(e) = std::fs::remove_file(old_file) {
                warn!("Failed to remove old log file {:?}: {}", old_file, e);
            } else {
                info!("Removed old log file: {:?}", old_file);
            }
        }
    }
}

fn print_help_manual() {
    println!(r#"Wyze Sense to MQTT Bridge (Rust): High-Performance Asynchronous USB Gateway Controller

USAGE:
    wyzesense2mqtt-rs [SUBCOMMAND] [OPTIONS]

DAEMON OPTIONS (Default Mode):
    -c, --config <PATH>          Optional path to config.yaml configuration profile
    -d, --usb-dongle <PATH>      Override USB hidraw device path (e.g. /dev/hidraw0)
    --web-enabled <true/false>   Override Web dashboard panel execution
    -p, --web-port <PORT>        Override Web server binding port (defaults to 8080)
    --mqtt-enabled <true/false>  Override background Home Assistant MQTT bridge
    --mqtt-host <HOST>           Override MQTT broker IP/Hostname
    --mqtt-port <PORT>           Override MQTT broker port (defaults to 1883)
    --mqtt-user <USER>           Override MQTT connection username
    --mqtt-password <PASS>       Override MQTT connection password
    --mqtt-self-topic <ROOT>     Override telemetry payload state topic path
    --mqtt-hass-topic <ROOT>     Override Home Assistant Auto-Discovery topic path

SUBCOMMANDS:
    list                         Queries and lists all paired sensor MACs from the dongle
    pair                         Enters pair scan mode to dynamically scanned and bind new sensors
    unpair <MAC>                 Deletes/unpairs a sensor MAC address from the dongle
    chime <MAC>                  Triggers chime alarms on chime-compatible sensors
    fix                          Sweeps paired sensor list and purges invalid/ghost sensors
    raw <HEX_BYTES>              Direct diagnostic raw packet write/read bypass (comma separated)
                                 (e.g. raw AA,55,43,03,04,01,49)
    help, -h, --help             Prints this CLI help manual and exits
    -V, --version                Prints the version number and exits

GLOBAL OPTIONS:
    -l, --log-level <LEVEL>      Set log verbosity (trace, debug, info, warn, error)
    --no-ansi                    Disable colorized ANSI escape codes in stdout/stderr logging

DEBUGGING:
    Run with --log-level trace to capture raw USB wire data.
    Use tools/extract_packets.py to extract packets from trace logs for test replay.
"#);
}
