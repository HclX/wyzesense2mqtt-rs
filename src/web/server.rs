use crate::engine::{Engine, EnginesMap};
use crate::protocol::packet::Packet;

use crate::protocol::telemetry::SensorType;
use crate::transport::GatewayTransport;
use serde_json::json;

use axum::{
    extract::{Path, State},
    extract::ws::WebSocketUpgrade,
    http::StatusCode,
    response::{Html, IntoResponse, sse::{Event, Sse}},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use std::convert::Infallible;
use std::sync::Arc;
use std::net::SocketAddr;

use tower_http::cors::{Any, CorsLayer};
use tracing::{info, debug, error, warn};

use crate::protocol::sensor::SensorManager;

pub struct WebState {
    pub engines: EnginesMap,
    pub sensor_manager: Arc<std::sync::Mutex<SensorManager>>,
    pub broadcast_tx: tokio::sync::broadcast::Sender<()>,
    pub event_tx: tokio::sync::mpsc::Sender<crate::protocol::telemetry::DongleEvent>,
    /// Keeps engine worker loops alive for bridge-connected dongles.
    /// When an exit_tx is dropped, its engine's background loop exits.
    pub engine_exit_handles: tokio::sync::Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
    /// Optional auth token required for WebSocket bridge connections.
    pub bridge_auth_token: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct DongleStateResponse {
    pub connected: bool,
    pub mac: Option<String>,
    pub version: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct SensorsListResponse {
    pub sensors: Vec<crate::config::state::PersistedSensorState>,
}

#[derive(Serialize, Deserialize)]
pub struct SuccessResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Serialize, Deserialize)]
pub struct ScanRequest {
    pub enable: bool,
    pub dongle_mac: String,
}

#[derive(Serialize, Deserialize)]
pub struct ScanResponse {
    pub scan_active: bool,
}

#[derive(Serialize, Deserialize)]
pub struct VerifyRequest {
    pub mac: String,
    pub sensor_type: String,
    pub dongle_mac: String,
}

#[derive(Serialize, Deserialize)]
pub struct RawPacketRequest {
    pub bytes: Vec<u8>,
    pub dongle_mac: String,
}

#[derive(Serialize, Deserialize)]
pub struct RawPacketResponse {
    pub response_bytes: Vec<u8>,
}

/// Starts the Axum web server binding to the given port and sharing Engine/SensorManager handles.
pub async fn start_web_server(
    engines: EnginesMap,
    sensor_manager: Arc<std::sync::Mutex<SensorManager>>,
    broadcast_tx: tokio::sync::broadcast::Sender<()>,
    event_tx: tokio::sync::mpsc::Sender<crate::protocol::telemetry::DongleEvent>,
    port: u16,
    bridge_auth_token: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shared_state = Arc::new(WebState {
        engines,
        sensor_manager,
        broadcast_tx,
        event_tx,
        engine_exit_handles: tokio::sync::Mutex::new(Vec::new()),
        bridge_auth_token,
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/api/dongle", get(get_dongle_state))
        .route("/api/dongles", get(list_dongles))
        .route("/api/sensors", get(list_sensors))
        .route("/api/sensors/cached", get(list_cached_sensors))
        .route("/api/sensors/:mac", delete(unpair_sensor))
        .route("/api/scan", get(get_scan_status).post(toggle_scan))
        .route("/api/verify", post(verify_scanned_sensor))
        .route("/api/chime/:mac", post(trigger_chime))
        .route("/api/fix", post(fix_sensors))
        .route("/api/raw", post(send_raw_packet))
        .route("/api/events", get(sse_handler))
        .route("/ws/bridge", get(ws_bridge_handler))
        .layer(cors)
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Web interface successfully started. Listening on http://{}", addr);
    
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}

// --- GET /ws/bridge ---
async fn ws_bridge_handler(
    ws: WebSocketUpgrade,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<SocketAddr>,
    State(state): State<Arc<WebState>>,
) -> axum::response::Response {
    // Validate bridge auth token if configured
    if let Some(ref expected_token) = state.bridge_auth_token {
        match params.get("token") {
            Some(token) if token == expected_token => { /* ok */ }
            _ => {
                warn!("Bridge auth rejected from {}", addr);
                return (StatusCode::UNAUTHORIZED, "Invalid or missing auth token").into_response();
            }
        }
    }

    let remote_addr = addr.to_string();
    let device_path = params.get("device").cloned();
    ws.on_upgrade(move |socket| handle_bridge_connection(socket, state, remote_addr, device_path))
        .into_response()
}

async fn handle_bridge_connection(
    socket: axum::extract::ws::WebSocket,
    state: Arc<WebState>,
    remote_addr: String,
    device_path: Option<String>,
) {
    use futures_util::StreamExt;
    info!("New WebSocket bridge connection from {} (device: {:?})", remote_addr, device_path);

    let (writer, reader) = socket.split();
    let transport = GatewayTransport::WebSocket {
        reader: Arc::new(tokio::sync::Mutex::new(reader)),
        writer: Arc::new(tokio::sync::Mutex::new(writer)),
    };

    let mut engine = Engine::new(
        transport,
        state.event_tx.clone(),
        None, // No local state path for remote dongles
    );
    engine.transport_label = "bridge".to_string();
    engine.device_path = device_path;
    engine.remote_addr = Some(remote_addr.clone());
    let exit_tx = engine.start();

    // Run handshake with timeout
    let mac = match tokio::time::timeout(
        std::time::Duration::from_secs(8),
        engine.initialize_handshake(),
    )
    .await
    {
        Ok(Ok(_)) => {
            let mac = engine.dongle_mac().unwrap_or("unknown").to_string();
            info!("WebSocket bridge dongle registered: MAC={}", mac);
            engine.set_auto_verify(true);

            // Warm up sensor cache from dongle NVRAM
            info!("Warming up sensors cache for bridge dongle {}...", mac);
            match engine.get_sensor_list().await {
                Ok(sensors_list) => {
                    let mut manager = state.sensor_manager.lock().unwrap();
                    // Assign NVRAM sensors to this dongle in the single source of truth
                    manager.assign_dongle(&mac, &sensors_list);
                    info!("Assigned {} NVRAM sensors to bridge dongle {}", sensors_list.len(), mac);

                    // Inject dummy events to trigger MQTT discovery for assigned sensors
                    inject_discovery_events(&manager, &state.event_tx, &sensors_list);
                }
                Err(e) => {
                    error!("Failed to get sensor list for bridge dongle {}: {}", mac, e);
                }
            }

            // Register in engines map and keep exit handle alive
            let mut map = state.engines.lock().await;
            map.insert(mac.clone(), engine);
            drop(map);
            state.engine_exit_handles.lock().await.push(exit_tx);
            mac
        }
        Ok(Err(e)) => {
            error!("WebSocket bridge handshake failed: {}", e);
            // exit_tx is dropped here, signaling the engine worker to stop
            return;
        }
        Err(_) => {
            error!("WebSocket bridge handshake timed out");
            // exit_tx is dropped here, signaling the engine worker to stop
            return;
        }
    };

    // Wait for the engine's transport to die (WebSocket close / broken pipe).
    // The reader loop fires disconnect_notify when the transport errors out.
    {
        let engines = state.engines.lock().await;
        let notify = if let Some(eng) = engines.get(&mac) {
            Arc::clone(&eng.disconnect_notify)
        } else {
            // Engine already gone — nothing to wait for
            return;
        };
        drop(engines);
        notify.notified().await;
        info!("Bridge dongle {} disconnected, cleaning up", mac);
    }

    // Remove engine from the shared map
    {
        let mut map = state.engines.lock().await;
        map.remove(&mac);
    }

    // Cleanup: unassign sensors from disconnected dongle
    {
        let mut manager = state.sensor_manager.lock().unwrap();
        manager.unassign_dongle(&mac);
        info!("Unassigned sensors from disconnected bridge dongle {}", mac);
    }
    let _ = state.broadcast_tx.send(());
}

/// Injects dummy events for each sensor MAC to trigger initial MQTT discovery & state sync.
/// Used during both local dongle startup and bridge dongle connection.
fn inject_discovery_events(
    manager: &crate::protocol::sensor::SensorManager,
    event_tx: &tokio::sync::mpsc::Sender<crate::protocol::telemetry::DongleEvent>,
    sensor_macs: &[String],
) {
    for sensor_mac in sensor_macs {
        if let Some(sensor) = manager.get_sensors().get(sensor_mac) {
            let dummy = crate::protocol::telemetry::DongleEvent {
                mac: sensor.mac.clone(),
                timestamp: std::time::SystemTime::now(),
                sensor_type: sensor.sensor_type,
                event_type: 0xFF,
                data: crate::protocol::telemetry::TelemetryData::UnknownEvent(Vec::new()),
                dongle_mac: None,
            };
            let _ = event_tx.try_send(dummy);
        }
    }
}

// --- GET / serving HTML packed UI ---
async fn serve_dashboard() -> impl IntoResponse {
    Html(HTML_CONTENT)
}

// --- GET /api/events ---
async fn sse_handler(
    State(state): State<Arc<WebState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.broadcast_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|res| match res {
        Ok(_) => Some(Ok(Event::default().data("update"))),
        Err(_) => None,
    });
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

// --- GET /api/dongle ---
async fn get_dongle_state(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let engines = state.engines.lock().await;
    if let Some((_mac, engine)) = engines.iter().next() {
        Json(DongleStateResponse {
            connected: true,
            mac: engine.dongle_mac().map(|s| s.to_string()),
            version: engine.dongle_version().map(|s| s.to_string()),
        })
    } else {
        Json(DongleStateResponse {
            connected: false,
            mac: None,
            version: None,
        })
    }
}

// --- GET /api/dongles ---
async fn list_dongles(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let engines = state.engines.lock().await;
    let manager = state.sensor_manager.lock().unwrap();

    // Group sensors by dongle_mac from the single source of truth (SensorManager)
    let dongles: Vec<serde_json::Value> = engines.iter().map(|(mac, engine)| {
        let sensors: Vec<serde_json::Value> = manager.get_sensors().values()
            .filter(|s| s.dongle_mac.as_deref() == Some(mac.as_str()))
            .map(|s| sensor_info_to_json(s))
            .collect();
        let sensor_count = sensors.len();

        serde_json::json!({
            "mac": mac,
            "version": engine.dongle_version(),
            "scanning": engine.is_scanning(),
            "transport": engine.transport_label,
            "device_path": engine.device_path,
            "remote_addr": engine.remote_addr,
            "sensors": sensors,
            "sensor_count": sensor_count,
        })
    }).collect();

    // Unassociated sensors: dongle_mac is None
    let unassociated: Vec<serde_json::Value> = manager.get_sensors().values()
        .filter(|s| s.dongle_mac.is_none())
        .map(|s| sensor_info_to_json(s))
        .collect();

    Json(json!({
        "dongles": dongles,
        "unassociated_sensors": unassociated,
    }))
}

fn sensor_info_to_json(s: &crate::protocol::sensor::WyzeSensor) -> serde_json::Value {
    serde_json::json!({
        "mac": s.mac,
        "sensor_type": s.sensor_type.as_str(),
        "last_seen": s.last_seen,
        "battery": s.battery_pct,
        "signal": s.rssi_dbm,
        "state": s.state,
    })
}

// --- GET /api/sensors ---
async fn list_sensors(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let engines = state.engines.lock().await;
    let mut all_sensors = Vec::new();
    for (_, engine) in engines.iter() {
        all_sensors.extend(engine.get_rich_sensors());
    }
    drop(engines);

    // Merge with sensor_manager data
    let manager = state.sensor_manager.lock().unwrap();
    let mut sensors = Vec::new();
    // Deduplicate by MAC, preferring sensor_manager data
    let mut seen_macs = std::collections::HashSet::new();
    for rich in &all_sensors {
        if seen_macs.contains(&rich.mac) {
            continue;
        }
        seen_macs.insert(rich.mac.clone());
        if let Some(sensor) = manager.get_sensors().get(&rich.mac) {
            sensors.push(crate::config::state::PersistedSensorState {
                mac: sensor.mac.clone(),
                sensor_type: sensor.sensor_type.as_str().to_string(),
                last_seen: sensor.last_seen,
                battery: sensor.battery_pct,
                signal: sensor.rssi_dbm,
                state: sensor.state.clone(),
                dongle_mac: sensor.dongle_mac.clone(),
            });
        } else {
            sensors.push(rich.clone());
        }
    }
    // Also include sensor_manager entries not found in engine caches
    for (mac, sensor) in manager.get_sensors().iter() {
        if !seen_macs.contains(mac) {
            sensors.push(crate::config::state::PersistedSensorState {
                mac: sensor.mac.clone(),
                sensor_type: sensor.sensor_type.as_str().to_string(),
                last_seen: sensor.last_seen,
                battery: sensor.battery_pct,
                signal: sensor.rssi_dbm,
                state: sensor.state.clone(),
                dongle_mac: sensor.dongle_mac.clone(),
            });
        }
    }
    sensors.sort_by_key(|s| s.mac.clone());
    (StatusCode::OK, Json(SensorsListResponse { sensors })).into_response()
}

// --- GET /api/sensors/cached ---
async fn list_cached_sensors(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let manager = state.sensor_manager.lock().unwrap();
    let mut sensors: Vec<crate::config::state::PersistedSensorState> = manager.get_sensors().values().map(|sensor| {
        crate::config::state::PersistedSensorState {
            mac: sensor.mac.clone(),
            sensor_type: sensor.sensor_type.as_str().to_string(),
            last_seen: sensor.last_seen,
            battery: sensor.battery_pct,
            signal: sensor.rssi_dbm,
            state: sensor.state.clone(),
            dongle_mac: sensor.dongle_mac.clone(),
        }
    }).collect();
    sensors.sort_by_key(|s| s.mac.clone());
    (StatusCode::OK, Json(SensorsListResponse { sensors })).into_response()
}

// --- DELETE /api/sensors/:mac ---
async fn unpair_sensor(
    Path(mac): Path<String>,
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    // Clone engines to avoid holding lock across await
    let engine_clones: Vec<Engine> = {
        let engines = state.engines.lock().await;
        engines.values().cloned().collect()
    };
    for mut engine in engine_clones {
        let _ = engine.delete_sensor(&mac).await;
    }
    let mut manager = state.sensor_manager.lock().unwrap();
    let _ = manager.delete_and_persist_sensor(&mac);
    let _ = state.broadcast_tx.send(());
    (
        StatusCode::OK,
        Json(SuccessResponse {
            success: true,
            message: format!("Sensor {} successfully unlinked", mac),
        }),
    )
        .into_response()
}

// --- GET /api/scan ---
async fn get_scan_status(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let engines = state.engines.lock().await;
    let scanning = engines.values().any(|e| e.is_scanning());
    (StatusCode::OK, Json(ScanResponse { scan_active: scanning })).into_response()
}

// --- POST /api/scan ---
async fn toggle_scan(
    State(state): State<Arc<WebState>>,
    Json(payload): Json<ScanRequest>,
) -> impl IntoResponse {
    let dongle_mac = payload.dongle_mac;

    // Enforce exclusive scan mode: reject if another dongle is already scanning
    if payload.enable {
        let engines = state.engines.lock().await;
        for (mac, engine) in engines.iter() {
            if mac != &dongle_mac && engine.is_scanning() {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({ "error": format!("Dongle {} is already scanning. Only one dongle may scan at a time.", mac) })),
                ).into_response();
            }
        }
        drop(engines);
    }

    // Clone engine to avoid holding lock across await
    let mut engine = {
        let engines = state.engines.lock().await;
        match engines.get(&dongle_mac) {
            Some(e) => e.clone(),
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({ "error": format!("Dongle {} not found", dongle_mac) })),
                ).into_response();
            }
        }
    };
    match engine.set_scan(payload.enable).await {
        Ok(_) => {
            let _ = state.broadcast_tx.send(());
            (
                StatusCode::OK,
                Json(ScanResponse {
                    scan_active: payload.enable,
                }),
            )
                .into_response()
        },
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// --- POST /api/verify ---
async fn verify_scanned_sensor(
    State(state): State<Arc<WebState>>,
    Json(payload): Json<VerifyRequest>,
) -> impl IntoResponse {
    // Clone engine to avoid holding lock across await
    let mut engine = {
        let engines = state.engines.lock().await;
        match engines.get(&payload.dongle_mac) {
            Some(e) => e.clone(),
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({ "error": format!("Dongle {} not found", payload.dongle_mac) })),
                ).into_response();
            }
        }
    };
    let sensor_type = payload.sensor_type.parse::<SensorType>().unwrap_or(SensorType::Unknown(0x00));

    match engine.verify_sensor(&payload.mac, sensor_type).await {
        Ok(_) => (
            StatusCode::OK,
            Json(SuccessResponse {
                success: true,
                message: format!("Sensor {} verified successfully", payload.mac),
            }),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// --- POST /api/chime/:mac ---
async fn trigger_chime(
    Path(mac): Path<String>,
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    // Clone engines to avoid holding lock across await
    let engine_clones: Vec<Engine> = {
        let engines = state.engines.lock().await;
        engines.values().cloned().collect()
    };
    for mut engine in engine_clones {
        let _ = engine.play_chime(&mac).await;
    }
    (
        StatusCode::OK,
        Json(SuccessResponse {
            success: true,
            message: format!("Chime triggered on {}", mac),
        }),
    )
        .into_response()
}

// --- POST /api/fix ---
async fn fix_sensors(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    // Clone engines to avoid holding lock across await
    let mut engine_clones: Vec<Engine> = {
        let engines = state.engines.lock().await;
        engines.values().cloned().collect()
    };
    let mut purged = Vec::new();
    let invalid_ghosts = ["00000000", "\0\0\0\0\0\0\0\0"];
    // Fix algorithm: iterate all engines, list sensors, identify invalid MAC patterns, and delete them
    for engine in engine_clones.iter_mut() {
        match engine.get_sensor_list().await {
            Ok(sensors) => {
                for mac in sensors {
                    let is_invalid = mac.chars().any(|c| !c.is_alphanumeric()) || invalid_ghosts.contains(&mac.as_str());
                    if is_invalid {
                        if let Ok(_) = engine.delete_sensor(&mac).await {
                            purged.push(mac.clone());
                            let mut manager = state.sensor_manager.lock().unwrap();
                            let _ = manager.delete_and_persist_sensor(&mac);
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Failed to get sensor list from engine during fix: {}", e);
            }
        }
    }
    if !purged.is_empty() {
        let _ = state.broadcast_tx.send(());
    }
    (
        StatusCode::OK,
        Json(json!({
            "success": true,
            "purged_count": purged.len(),
            "purged_macs": purged
        })),
    )
        .into_response()
}

// --- POST /api/raw ---
async fn send_raw_packet(
    State(state): State<Arc<WebState>>,
    Json(payload): Json<RawPacketRequest>,
) -> impl IntoResponse {
    // Clone engine to avoid holding lock across await
    let mut engine = {
        let engines = state.engines.lock().await;
        match engines.get(&payload.dongle_mac) {
            Some(e) => e.clone(),
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    format!("Dongle {} not found", payload.dongle_mac),
                ).into_response();
            }
        }
    };
    debug!("Web API sending raw packet bytes: {:?}", payload.bytes);

    // Attempt to parse the raw packet to identify what response packet ID we should wait for
    match Packet::parse(&payload.bytes) {
        Ok((pkt, _)) => {
            let expected_response = pkt.cmd() + 1;
            match engine.do_command(pkt, expected_response).await {
                Ok(resp) => (
                    StatusCode::OK,
                    Json(RawPacketResponse {
                        response_bytes: resp.to_bytes(),
                    }),
                )
                    .into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Dongle failed raw execution: {}", e)).into_response(),
            }
        }
        Err(e) => (StatusCode::BAD_REQUEST, format!("Failed to parse input bytes as Packet structure: {}", e)).into_response(),
    }
}

// Serving beautiful packed HTML Single-Page UI
const HTML_CONTENT: &str = concat!(r##"
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wyze Sense Bridge — Control Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; }
        .sensor-rows { transition: max-height 0.35s ease, opacity 0.2s ease; overflow: hidden; }
        .sensor-rows.collapsed { max-height: 0; opacity: 0; }
        .chevron { transition: transform 0.2s ease; display: inline-block; }
        .chevron.open { transform: rotate(90deg); }
        .dongle-card { transition: border-color 0.2s ease; }
        .dongle-card:hover { border-color: rgba(45, 212, 191, 0.3); }
        .tag { display: inline-flex; align-items: center; gap: 4px; font-size: 0.65rem; padding: 2px 8px; border-radius: 9999px; font-weight: 600; }
        .modal-backdrop { position: fixed; inset: 0; background: rgba(0,0,0,0.6); backdrop-filter: blur(4px); z-index: 100; display: flex; align-items: center; justify-content: center; }
        .modal-panel { background: #0f172a; border: 1px solid #1e293b; border-radius: 1rem; width: 90%; max-width: 560px; max-height: 85vh; overflow-y: auto; box-shadow: 0 25px 50px rgba(0,0,0,0.5); }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col">
    <header class="border-b border-slate-800 bg-slate-900/50 backdrop-blur sticky top-0 z-50">
        <div class="max-w-5xl w-full mx-auto px-6 py-3 flex items-center justify-between">
            <div class="flex items-center space-x-3">
                <span class="text-2xl">📡</span>
                <h1 class="text-xl font-bold tracking-tight text-teal-400">Wyze Sense Bridge</h1>
                <span class="text-xs text-slate-600 font-mono">v"##, env!("CARGO_PKG_VERSION"), r##"</span>
            </div>
            <div class="flex items-center gap-3">
                <div class="flex items-center space-x-2 bg-slate-800 px-3 py-1.5 rounded-full text-xs font-semibold text-slate-400" id="header-badge">
                    <span class="w-2 h-2 rounded-full bg-slate-600" id="status-dot"></span>
                    <span id="status-text">Connecting...</span>
                </div>
            </div>
        </div>
    </header>

    <main class="flex-1 max-w-5xl w-full mx-auto px-6 py-6 flex flex-col gap-5">
        <div class="flex items-center justify-between">
            <h2 class="text-lg font-bold text-teal-400 flex items-center"><span class="mr-2">🔋</span> Devices</h2>
            <button onclick="loadDongles()" class="text-xs text-teal-400 hover:underline">Refresh</button>
        </div>
        <div id="dongles-container" class="space-y-4">
            <div class="text-center text-sm text-slate-500 py-12">Loading devices...</div>
        </div>
    </main>

    <footer class="border-t border-slate-800 bg-slate-900/20 py-3">
        <div class="max-w-5xl w-full mx-auto px-6 text-center text-xs text-slate-500">
            Wyze Sense to MQTT Bridge (Rust) — Multi-Dongle WebSocket Architecture
        </div>
    </footer>

    <!-- Modal container -->
    <div id="modal-root"></div>

    <script>
        const API = "";
        let donglesData = { dongles: [], unassociated_sensors: [] };
        let scanState = {};
        let modalDongle = null;

        // ===== Sensor Row =====
        function sensorRow(s) {
            let batt;
            if (s.battery == null) batt = `<span class="tag border border-slate-800 text-slate-500">N/A</span>`;
            else {
                let c = "text-emerald-400 bg-emerald-950/30 border-emerald-900/30";
                if (s.battery < 20) c = "text-rose-400 bg-rose-950/30 border-rose-900/30";
                else if (s.battery < 50) c = "text-amber-400 bg-amber-950/30 border-amber-900/30";
                batt = `<span class="tag border ${c}">${s.battery}%</span>`;
            }
            let sig = "text-slate-400";
            if (s.signal > -50) sig = "text-teal-400 font-semibold";
            else if (s.signal < -80) sig = "text-rose-400 font-semibold";

            let seen = "Never";
            if (s.last_seen > 0) {
                const d = Math.floor(Date.now()/1000) - s.last_seen;
                if (d < 60) seen = "Just now"; else if (d < 3600) seen = `${Math.floor(d/60)}m ago`;
                else if (d < 86400) seen = `${Math.floor(d/3600)}h ago`; else seen = `${Math.floor(d/86400)}d ago`;
            }

            const t = (s.sensor_type||"").toLowerCase();
            let tt;
            if (t.includes("contact")||t.includes("switch")) tt = `<span class="tag border border-cyan-900 bg-cyan-950/20 text-cyan-400">🚪 Contact</span>`;
            else if (t.includes("motion")) tt = `<span class="tag border border-purple-900 bg-purple-950/20 text-purple-400">🏃 Motion</span>`;
            else if (t.includes("climate")) tt = `<span class="tag border border-sky-900 bg-sky-950/20 text-sky-400">🌡️ Climate</span>`;
            else if (t.includes("leak")) tt = `<span class="tag border border-blue-900 bg-blue-950/20 text-blue-400">💧 Leak</span>`;
            else tt = `<span class="tag border border-slate-800 bg-slate-900 text-slate-300">${s.sensor_type}</span>`;

            let st = `<span class="text-slate-500 italic text-xs">—</span>`;
            if (s.state) switch (s.state.kind) {
                case "Contact": st = s.state.is_open ? `<span class="text-rose-400 font-bold text-xs">Open</span>` : `<span class="text-emerald-400 font-bold text-xs">Closed</span>`; break;
                case "Motion": st = s.state.is_active ? `<span class="text-rose-400 font-bold text-xs">Active</span>` : `<span class="text-emerald-400 font-bold text-xs">Clear</span>`; break;
                case "Leak": st = s.state.is_wet ? `<span class="text-blue-400 font-bold text-xs">Wet</span>` : `<span class="text-emerald-400 font-bold text-xs">Dry</span>`; break;
                case "Climate": st = `<span class="text-cyan-400 font-mono text-xs">${parseFloat(s.state.temperature).toFixed(1)}°C / ${s.state.humidity}%</span>`; break;
            }

            return `<tr class="hover:bg-slate-800/30 transition text-xs">
                <td class="py-2 px-3 font-mono font-semibold text-teal-400">${s.mac}</td>
                <td class="py-2 px-3">${tt}</td><td class="py-2 px-3">${st}</td>
                <td class="py-2 px-3">${batt}</td>
                <td class="py-2 px-3"><span class="font-mono ${sig}">${s.signal} dBm</span></td>
                <td class="py-2 px-3 text-slate-400">${seen}</td>
            </tr>`;
        }

        function sensorTable(sensors) {
            if (!sensors || !sensors.length) return `<div class="text-center text-xs text-slate-500 py-3 italic">No sensors</div>`;
            return `<table class="w-full text-left text-slate-300">
                <thead class="text-slate-500 text-xs uppercase"><tr>
                    <th class="py-1.5 px-3">MAC</th><th class="py-1.5 px-3">Type</th><th class="py-1.5 px-3">State</th>
                    <th class="py-1.5 px-3">Battery</th><th class="py-1.5 px-3">Signal</th><th class="py-1.5 px-3">Seen</th>
                </tr></thead>
                <tbody class="divide-y divide-slate-800/50">${sensors.map(sensorRow).join("")}</tbody></table>`;
        }

        // ===== Dongle Card =====
        function dongleCard(d, i) {
            const n = d.sensors?.length || 0;
            const dot = `<span class="w-2.5 h-2.5 rounded-full ${d.scanning ? 'bg-amber-400 animate-pulse' : 'bg-teal-400'}"></span>`;
            let tr;
            if (d.transport === "bridge") {
                const ip = d.remote_addr ? d.remote_addr.split(':')[0] : '?';
                tr = `<span class="tag border border-violet-900 bg-violet-950/20 text-violet-400">🌐 Bridge</span>
                      <span class="text-xs text-slate-500 font-mono ml-1">${ip} · ${d.device_path||'?'}</span>`;
            } else {
                tr = `<span class="tag border border-teal-900 bg-teal-950/20 text-teal-400">🔌 Local</span>
                      <span class="text-xs text-slate-500 font-mono ml-1">${d.device_path||'?'}</span>`;
            }
            return `<div class="dongle-card bg-slate-900 rounded-2xl border border-slate-800 shadow-xl overflow-hidden">
                <div class="px-5 py-3.5 flex items-center justify-between">
                    <div class="flex items-center gap-3 cursor-pointer select-none flex-1" onclick="toggleSec(${i})">
                        <span class="chevron open text-slate-500 text-xs" id="chev-${i}">▶</span>
                        ${dot}
                        <div class="min-w-0">
                            <div class="flex items-center gap-2 flex-wrap">
                                <span class="font-bold text-sm">🕹️ ${d.mac}</span>
                                ${d.scanning?'<span class="tag border border-amber-900 bg-amber-950/20 text-amber-400">SCANNING</span>':''}
                                <span class="tag border border-slate-800 text-slate-400">${n} sensor${n!==1?'s':''}</span>
                            </div>
                            <div class="flex items-center gap-2 mt-0.5 flex-wrap">
                                <span class="text-xs text-slate-500 font-mono">${d.version||'Unknown firmware'}</span>
                                <span class="text-slate-700">·</span> ${tr}
                            </div>
                        </div>
                    </div>
                    <button onclick="openModal('${d.mac}')" class="ml-3 shrink-0 py-1.5 px-4 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-semibold rounded-lg border border-slate-700 transition flex items-center gap-1.5">
                        <span>⚙️</span> Actions
                    </button>
                </div>
                <div class="sensor-rows border-t border-slate-800/50" id="rows-${i}">
                    <div class="px-2 py-1">${sensorTable(d.sensors)}</div>
                </div>
            </div>`;
        }

        function orphanCard(sensors) {
            if (!sensors?.length) return '';
            return `<div class="dongle-card bg-slate-900/50 rounded-2xl border border-dashed border-slate-700 shadow-xl overflow-hidden">
                <div class="px-5 py-3.5 flex items-center gap-3 cursor-pointer select-none" onclick="toggleSec('orph')">
                    <span class="chevron open text-slate-500 text-xs" id="chev-orph">▶</span>
                    <span class="w-2.5 h-2.5 rounded-full bg-slate-600"></span>
                    <div><span class="font-bold text-sm text-slate-400">📦 Unassociated Sensors</span>
                        <div class="text-xs text-slate-600">Restored from state — no active dongle</div></div>
                    <span class="tag border border-slate-700 text-slate-500 ml-auto">${sensors.length}</span>
                </div>
                <div class="sensor-rows border-t border-slate-800/50" id="rows-orph">
                    <div class="px-2 py-1">${sensorTable(sensors)}</div>
                </div>
            </div>`;
        }

        function toggleSec(id) {
            document.getElementById(`rows-${id}`)?.classList.toggle("collapsed");
            document.getElementById(`chev-${id}`)?.classList.toggle("open");
        }

        // ===== Modal =====
        function openModal(mac) {
            modalDongle = mac;
            renderModal();
        }
        function closeModal() {
            // Stop any active scan when closing
            if (modalDongle && scanState[modalDongle]?.active) forceStopScan(modalDongle);
            modalDongle = null;
            document.getElementById("modal-root").innerHTML = '';
        }

        function renderModal() {
            if (!modalDongle) return;
            const d = donglesData.dongles.find(x => x.mac === modalDongle);
            if (!d) { closeModal(); return; }
            const ss = scanState[modalDongle] || {};
            const scanning = ss.active || false;

            const scanBtn = scanning
                ? `<button onclick="toggleScan('${d.mac}')" class="w-full py-2.5 bg-rose-700 hover:bg-rose-600 text-white text-sm font-semibold rounded-xl transition">⏹ Stop Scan (${ss.secondsLeft||0}s)</button>`
                : `<button onclick="toggleScan('${d.mac}')" class="w-full py-2.5 bg-teal-600 hover:bg-teal-500 text-white text-sm font-semibold rounded-xl transition">📡 Pair New Sensor</button>`;

            document.getElementById("modal-root").innerHTML = `
            <div class="modal-backdrop" onclick="if(event.target===this)closeModal()">
                <div class="modal-panel">
                    <div class="px-6 py-4 border-b border-slate-800 flex items-center justify-between">
                        <div>
                            <div class="font-bold text-base text-teal-400">⚙️ Dongle ${d.mac}</div>
                            <div class="text-xs text-slate-500 mt-0.5">${d.transport === 'bridge' ? '🌐 Bridge' : '🔌 Local'} · ${d.device_path||'?'}${d.remote_addr ? ' · '+d.remote_addr.split(':')[0] : ''}</div>
                        </div>
                        <button onclick="closeModal()" class="text-slate-500 hover:text-slate-300 text-xl leading-none px-1">✕</button>
                    </div>
                    <div class="p-6 space-y-5">
                        <!-- Pairing Center -->
                        <div>
                            <h3 class="text-sm font-semibold text-slate-300 mb-2">📡 Pairing Center</h3>
                            <p class="text-xs text-slate-500 mb-3">Put the sensor in pairing mode, then start scanning. Scan auto-stops after 60 seconds.</p>
                            ${scanBtn}
                        </div>

                        <hr class="border-slate-800">

                        <!-- Maintenance -->
                        <div>
                            <h3 class="text-sm font-semibold text-slate-300 mb-2">🧹 Maintenance</h3>
                            <button onclick="runFix('${d.mac}')" class="w-full py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm font-semibold rounded-xl border border-slate-700 transition">Purge Ghost Sensors</button>
                        </div>

                        <hr class="border-slate-800">

                        <!-- Hex Console -->
                        <div>
                            <h3 class="text-sm font-semibold text-slate-300 mb-2">💻 Hex Console</h3>
                            <div class="flex gap-2 mb-2">
                                <input id="hex-input" type="text" placeholder="AA,55,43..." class="flex-1 bg-slate-950 border border-slate-800 text-sm rounded-xl px-3 py-2 font-mono focus:outline-none focus:border-teal-500 transition">
                                <button onclick="sendRaw()" class="py-2 px-4 bg-slate-800 hover:bg-slate-700 font-semibold text-xs rounded-xl border border-slate-700 transition">Send</button>
                            </div>
                            <div class="rounded-xl border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-emerald-400 min-h-[60px] max-h-[120px] overflow-y-auto space-y-1 flex flex-col justify-end" id="console-log">
                                <div class="text-slate-500 italic">[Console ready]</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>`;
        }

        // ===== Data =====
        async function loadDongles() {
            try {
                const r = await fetch(`${API}/api/dongles`);
                donglesData = await r.json();
                render(); updateHeader();
                if (modalDongle) renderModal();
            } catch(e) {
                document.getElementById("dongles-container").innerHTML = `<div class="text-center text-sm text-rose-500 py-12">Failed to load</div>`;
            }
        }

        function render() {
            const c = document.getElementById("dongles-container");
            if (!donglesData.dongles.length && !(donglesData.unassociated_sensors||[]).length) {
                c.innerHTML = `<div class="text-center py-16"><div class="text-4xl mb-3">📡</div>
                    <div class="text-slate-400 text-sm">No dongles connected</div>
                    <div class="text-slate-600 text-xs mt-1">Connect a USB dongle or start a WebSocket bridge</div></div>`;
                return;
            }
            c.innerHTML = donglesData.dongles.map((d,i) => dongleCard(d,i)).join("") + orphanCard(donglesData.unassociated_sensors);
        }

        function updateHeader() {
            const dot = document.getElementById("status-dot"), txt = document.getElementById("status-text");
            const badge = document.getElementById("header-badge");
            const n = donglesData.dongles.length;
            if (n > 0) {
                dot.className = "w-2 h-2 rounded-full bg-teal-400";
                txt.innerText = `${n} dongle${n>1?'s':''} online`;
                badge.className = "flex items-center space-x-2 bg-teal-900/20 border border-teal-800/30 px-3 py-1.5 rounded-full text-xs font-semibold text-teal-400";
            } else {
                dot.className = "w-2 h-2 rounded-full bg-rose-500 animate-pulse";
                txt.innerText = "No dongles";
                badge.className = "flex items-center space-x-2 bg-slate-800 px-3 py-1.5 rounded-full text-xs font-semibold text-slate-400";
            }
        }

        // ===== Scan =====
        async function toggleScan(mac) {
            const ss = scanState[mac] || { active: false };
            try {
                const r = await fetch(`${API}/api/scan`, {
                    method:"POST", headers:{"Content-Type":"application/json"},
                    body: JSON.stringify({ enable: !ss.active, dongle_mac: mac })
                });
                const data = await r.json();
                if (data.scan_active) {
                    scanState[mac] = { active: true, secondsLeft: 60 };
                    clearInterval(scanState[mac].timer); clearInterval(scanState[mac].poll);
                    scanState[mac].timer = setInterval(() => {
                        scanState[mac].secondsLeft--;
                        if (scanState[mac].secondsLeft <= 0) forceStopScan(mac);
                        else renderModal();
                    }, 1000);
                    scanState[mac].poll = setInterval(async () => {
                        try {
                            const r2 = await fetch(`${API}/api/scan`); const d2 = await r2.json();
                            if (!d2.scan_active) { clearScanTimers(mac); scanState[mac].active=false; log(`🎉 Sensor paired!`); await loadDongles(); }
                        } catch(e){}
                    }, 1500);
                    log(`Scanning on dongle ${mac}...`);
                } else {
                    clearScanTimers(mac); scanState[mac] = { active:false };
                    log("Scan stopped.");
                }
                renderModal();
            } catch(e) { log("Scan toggle failed!"); }
        }

        function clearScanTimers(mac) {
            const s = scanState[mac]; if (!s) return;
            clearInterval(s.timer); clearInterval(s.poll);
        }

        async function forceStopScan(mac) {
            try { await fetch(`${API}/api/scan`, {method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({enable:false,dongle_mac:mac})}); } catch(e){}
            clearScanTimers(mac); scanState[mac] = { active:false };
            log("Scan timeout."); renderModal(); loadDongles();
        }

        // ===== Actions =====
        async function runFix(mac) {
            log(`Purging ghosts on ${mac}...`);
            try { const r = await fetch(`${API}/api/fix`,{method:"POST"}); const d = await r.json(); log(`Purged ${d.purged_count}: [${d.purged_macs.join(", ")}]`); loadDongles(); } catch(e){log("Fix failed!");}
        }

        async function sendRaw() {
            const input = document.getElementById("hex-input")?.value;
            if (!input) return;
            const bytes = input.split(",").map(s=>s.trim()).filter(s=>s.length>0).map(s=>parseInt(s,16));
            if (bytes.some(isNaN)) { log("Invalid hex!"); return; }
            log(`===> [${input.toUpperCase()}]`);
            try {
                const r = await fetch(`${API}/api/raw`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({bytes, dongle_mac: modalDongle})});
                if (r.status!==200){log(`Error: ${await r.text()}`);return;}
                const d = await r.json(); log(`<=== [${d.response_bytes.map(b=>b.toString(16).padStart(2,"0").toUpperCase()).join(",")}]`);
            } catch(e){log("Send failed");}
        }

        function log(msg) {
            const el = document.getElementById("console-log");
            if (!el) return;
            const d = document.createElement("div");
            d.innerText = `[${new Date().toLocaleTimeString()}] ${msg}`;
            el.appendChild(d); el.scrollTop = el.scrollHeight;
        }

        // ===== Init =====
        loadDongles();
        const es = new EventSource(`${API}/api/events`);
        es.onmessage = (e) => { if (e.data === "update") loadDongles(); };
    </script>
</body>
</html>
"##);

