use wyzesense2mqtt_rs::engine::Engine;
use wyzesense2mqtt_rs::transport::hidraw::HidrawTransport;
use wyzesense2mqtt_rs::web::start_web_server;
use wyzesense2mqtt_rs::protocol::telemetry::DongleEvent;
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;
use std::sync::Arc;

use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Setup tracing diagnostic subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // 2. Parse arguments (simple loop over std::env::args)
    let mut device_path = "/dev/hidraw0".to_string();
    let mut port: u16 = std::env::var("ANTIGRAVITY_SIDECAR_WEB_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8080);

    let args: Vec<String> = std::env::args().collect();
    let mut idx = 1;
    while idx < args.len() {
        match args[idx].as_str() {
            "--device" | "-d" => {
                if idx + 1 < args.len() {
                    device_path = args[idx + 1].clone();
                    idx += 2;
                } else {
                    return Err("Missing argument for --device".into());
                }
            }
            "--port" | "-p" => {
                if idx + 1 < args.len() {
                    port = args[idx + 1].parse()?;
                    idx += 2;
                } else {
                    return Err("Missing argument for --port".into());
                }
            }
            _ => {
                println!("Usage: bridge_web [--device PATH] [--port PORT]");
                return Ok(());
            }
        }
    }

    info!("Initializing Wyze Sense to MQTT Bridge (Rust) Web Daemon on device: {}", device_path);

    // 3. Open Hidraw Transport asynchronously
    let transport = match HidrawTransport::open(&device_path).await {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to open USB hidraw device [{}]: {}", device_path, e);
            return Err(e.into());
        }
    };

    // 4. Setup dynamic event channel (we discard them or just print them in the log since this is the control web console)
    let (event_tx, mut event_rx) = mpsc::channel::<DongleEvent>(128);
    let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel::<()>(16);

    // 5. Instantiate the engine and SensorManager
    let mut engine = Engine::new(transport, event_tx, None);
    let sensor_manager = Arc::new(std::sync::Mutex::new(SensorManager::new(
        "config/sensors.yaml".to_string(),
        "config/state.yaml".to_string(),
    )));

    let sensor_manager_clone = Arc::clone(&sensor_manager);
    let broadcast_tx_clone = broadcast_tx.clone();
    tokio::spawn(async move {
        while let Some(evt) = event_rx.recv().await {
            info!("STATE EVENT [{}]: {:?}", evt.mac, evt.data);
            let changed = {
                let mut manager = sensor_manager_clone.lock().unwrap();
                manager.dispatch_event(&evt)
            };
            if changed {
                let _ = broadcast_tx_clone.send(());
            }
        }
    });

    // Start background worker loop
    let _exit_tx = engine.start();

    // 6. Perform dongle unlock handshake
    match tokio::time::timeout(Duration::from_secs(8), engine.initialize_handshake()).await {
        Ok(Ok(_)) => info!("Dongle successfully unlocked and authenticated!"),
        Ok(Err(e)) => {
            error!("Failed during dongle handshake exchange: {}", e);
            return Err(e.into());
        }
        Err(_) => {
            error!("Dongle handshake timed out! Make sure the physical USB dongle is plugged in.");
            return Err("Handshake timeout".into());
        }
    }

    // 7. Start HTTP REST Web UI blocking thread
    start_web_server(engine, sensor_manager, broadcast_tx, port).await?;

    Ok(())
}
