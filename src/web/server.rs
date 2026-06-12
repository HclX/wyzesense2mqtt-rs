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
use tracing::{info, debug, error};

use crate::protocol::sensor::SensorManager;

pub struct WebState {
    pub engines: EnginesMap,
    pub sensor_manager: Arc<std::sync::Mutex<SensorManager>>,
    pub broadcast_tx: tokio::sync::broadcast::Sender<()>,
    pub event_tx: tokio::sync::mpsc::Sender<crate::protocol::telemetry::DongleEvent>,
    /// Keeps engine worker loops alive for bridge-connected dongles.
    /// When an exit_tx is dropped, its engine's background loop exits.
    pub engine_exit_handles: tokio::sync::Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
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
    pub dongle_mac: Option<String>,
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shared_state = Arc::new(WebState {
        engines,
        sensor_manager,
        broadcast_tx,
        event_tx,
        engine_exit_handles: tokio::sync::Mutex::new(Vec::new()),
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
    
    axum::serve(listener, app).await?;
    Ok(())
}

// --- GET /ws/bridge ---
async fn ws_bridge_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_bridge_connection(socket, state))
}

async fn handle_bridge_connection(socket: axum::extract::ws::WebSocket, state: Arc<WebState>) {
    use futures_util::StreamExt;
    info!("New WebSocket bridge connection established");

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
    let exit_tx = engine.start();

    // Run handshake with timeout
    match tokio::time::timeout(
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
                    if let Err(e) = manager.load_sensors(&sensors_list) {
                        error!("Failed to load sensors for bridge dongle {}: {}", mac, e);
                    } else {
                        info!("Sensors cache warmed for bridge dongle {} ({} sensors)", mac, sensors_list.len());
                        // Inject dummy events to trigger MQTT discovery for existing sensors
                        for sensor in manager.get_sensors().values() {
                            let dummy = crate::protocol::telemetry::DongleEvent {
                                mac: sensor.mac.clone(),
                                timestamp: std::time::SystemTime::now(),
                                sensor_type: sensor.sensor_type,
                                event_type: 0xFF,
                                data: crate::protocol::telemetry::TelemetryData::UnknownEvent(Vec::new()),
                            };
                            let _ = state.event_tx.try_send(dummy);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to warm up sensors for bridge dongle {}: {}", mac, e);
                }
            }

            // Register in engines map and keep exit handle alive
            let mut map = state.engines.lock().await;
            map.insert(mac, engine);
            drop(map);
            state.engine_exit_handles.lock().await.push(exit_tx);
        }
        Ok(Err(e)) => {
            error!("WebSocket bridge handshake failed: {}", e);
            return;
        }
        Err(_) => {
            error!("WebSocket bridge handshake timed out");
            return;
        }
    }

    // Keep the handler alive while the connection is active.
    // The engine's reader loop will detect when the WebSocket closes
    // and will exit via the transport read error.
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
    let mut all_engine_sensor_macs = std::collections::HashSet::new();

    let dongles: Vec<serde_json::Value> = engines.iter().map(|(mac, engine)| {
        let sensors: Vec<serde_json::Value> = engine.get_rich_sensors().iter().map(|s| {
            all_engine_sensor_macs.insert(s.mac.clone());
            sensor_to_json(s)
        }).collect();

        // Also check sensor_manager for sensors associated with this dongle
        // that may not be in engine's internal cache
        let manager_sensors: Vec<serde_json::Value> = manager.get_sensors().values()
            .filter(|s| !all_engine_sensor_macs.contains(&s.mac))
            .map(|s| sensor_info_to_json(s))
            .collect();
        // Track MACs from manager sensors
        for s in manager.get_sensors().values() {
            if !all_engine_sensor_macs.contains(&s.mac) {
                all_engine_sensor_macs.insert(s.mac.clone());
            }
        }

        let mut all_sensors = sensors;
        all_sensors.extend(manager_sensors);

        serde_json::json!({
            "mac": mac,
            "version": engine.dongle_version(),
            "scanning": engine.is_scanning(),
            "sensors": all_sensors,
            "sensor_count": all_sensors.len(),
        })
    }).collect();

    // Unassociated sensors: in sensor_manager but not in any engine
    let unassociated: Vec<serde_json::Value> = manager.get_sensors().values()
        .filter(|s| !all_engine_sensor_macs.contains(&s.mac))
        .map(|s| sensor_info_to_json(s))
        .collect();

    Json(json!({
        "dongles": dongles,
        "unassociated_sensors": unassociated,
    }))
}

fn sensor_to_json(s: &crate::config::state::PersistedSensorState) -> serde_json::Value {
    serde_json::json!({
        "mac": s.mac,
        "sensor_type": s.sensor_type,
        "last_seen": s.last_seen,
        "battery": s.battery,
        "signal": s.signal,
        "state": s.state,
    })
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
    let mut engines = state.engines.lock().await;
    for (_, engine) in engines.iter_mut() {
        let _ = engine.delete_sensor(&mac).await;
    }
    drop(engines);
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
    let dongle_mac = match payload.dongle_mac {
        Some(mac) => mac,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "dongle_mac is required for scan operations" })),
            ).into_response();
        }
    };
    let mut engines = state.engines.lock().await;
    let engine = match engines.get_mut(&dongle_mac) {
        Some(e) => e,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("Dongle {} not found", dongle_mac) })),
            ).into_response();
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
    let mut engines = state.engines.lock().await;
    let engine = match engines.get_mut(&payload.dongle_mac) {
        Some(e) => e,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("Dongle {} not found", payload.dongle_mac) })),
            ).into_response();
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
    let mut engines = state.engines.lock().await;
    for (_, engine) in engines.iter_mut() {
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
    let mut engines = state.engines.lock().await;
    let mut purged = Vec::new();
    let invalid_ghosts = ["00000000", "\0\0\0\0\0\0\0\0"];
    // Fix algorithm: iterate all engines, list sensors, identify invalid MAC patterns, and delete them
    for (_, engine) in engines.iter_mut() {
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
    drop(engines);
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
    let mut engines = state.engines.lock().await;
    let engine = match engines.get_mut(&payload.dongle_mac) {
        Some(e) => e,
        None => {
            return (
                StatusCode::NOT_FOUND,
                format!("Dongle {} not found", payload.dongle_mac),
            ).into_response();
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
const HTML_CONTENT: &str = r##"
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wyze Sense to MQTT Bridge (Rust) Control Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; }
        .dongle-section { transition: all 0.3s ease; }
        .sensor-rows { transition: max-height 0.3s ease, opacity 0.2s ease; overflow: hidden; }
        .sensor-rows.collapsed { max-height: 0; opacity: 0; }
        .chevron { transition: transform 0.2s ease; }
        .chevron.open { transform: rotate(90deg); }
        @keyframes pulse-glow { 0%, 100% { box-shadow: 0 0 0 0 rgba(45, 212, 191, 0.2); } 50% { box-shadow: 0 0 12px 4px rgba(45, 212, 191, 0.1); } }
        .dongle-card:hover { border-color: rgba(45, 212, 191, 0.3); }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col">
    <header class="border-b border-slate-800 bg-slate-900/50 backdrop-blur sticky top-0 z-50">
        <div class="max-w-[1400px] w-full mx-auto px-6 py-4 flex items-center justify-between">
            <div class="flex items-center space-x-3">
                <span class="text-2xl">📡</span>
                <h1 class="text-xl font-bold tracking-tight text-teal-400">Wyze Sense Bridge</h1>
            </div>
            <div id="header-badges" class="flex items-center gap-3">
                <div class="flex items-center space-x-2 bg-slate-800 px-3 py-1.5 rounded-full text-xs font-semibold text-slate-400">
                    <span class="w-2 h-2 rounded-full bg-slate-600" id="status-dot"></span>
                    <span id="status-text">Connecting...</span>
                </div>
            </div>
        </div>
    </header>

    <main class="flex-1 max-w-[1400px] w-full mx-auto p-6 grid grid-cols-1 lg:grid-cols-4 gap-6">
        <!-- Left Column: Controls -->
        <div class="lg:col-span-1 flex flex-col gap-6">
            <!-- Pairing Control Card -->
            <div class="bg-slate-900 rounded-2xl border border-slate-800 p-6 shadow-xl">
                <h2 class="text-lg font-bold text-teal-400 mb-4 flex items-center"><span class="mr-2">🤝</span> Pairing Center</h2>
                <p class="text-xs text-slate-400 mb-3">Select a dongle to scan for new sensors. Only one dongle can scan at a time.</p>
                <select id="scan-dongle-select" class="w-full mb-3 bg-slate-950 border border-slate-800 text-sm rounded-xl px-4 py-2.5 focus:outline-none focus:border-teal-500 transition text-slate-300">
                    <option value="">No dongles connected</option>
                </select>
                <button id="btn-scan" onclick="toggleScan()" class="w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center">
                    Start Pairing Scan
                </button>
            </div>

            <!-- Maintenance -->
            <div class="bg-slate-900 rounded-2xl border border-slate-800 p-6 shadow-xl">
                <h2 class="text-lg font-bold text-teal-400 mb-4 flex items-center"><span class="mr-2">🛠️</span> Maintenance</h2>
                <button onclick="runFix()" class="w-full py-2.5 px-4 border border-slate-700 bg-slate-800 hover:bg-slate-700 font-semibold text-sm rounded-xl transition">
                    Purge Ghost Sensors
                </button>
            </div>

            <!-- Hex Diagnostics -->
            <div class="bg-slate-900 rounded-2xl border border-slate-800 p-6 shadow-xl">
                <h2 class="text-lg font-bold text-teal-400 mb-4 flex items-center"><span class="mr-2">💻</span> Hex Console</h2>
                <div class="flex gap-2 mb-3">
                    <input id="hex-input" type="text" placeholder="AA,55,43..." class="flex-1 bg-slate-950 border border-slate-800 text-sm rounded-xl px-3 py-2 font-mono focus:outline-none focus:border-teal-500 transition">
                    <button onclick="sendRawBytes()" class="py-2 px-4 bg-slate-800 hover:bg-slate-700 font-semibold text-xs rounded-xl border border-slate-700 transition">Send</button>
                </div>
                <div class="rounded-xl border border-slate-800 bg-slate-950 p-3 font-mono text-xs text-emerald-400 min-h-[80px] max-h-[150px] overflow-y-auto space-y-1 flex flex-col justify-end" id="console-log">
                    <div class="text-slate-500 italic">[Console ready]</div>
                </div>
            </div>
        </div>

        <!-- Right Column: Dongle-Grouped Device List -->
        <div class="lg:col-span-3 flex flex-col gap-4" id="dongle-list">
            <div class="flex items-center justify-between">
                <h2 class="text-lg font-bold text-teal-400 flex items-center"><span class="mr-2">🔋</span> Devices</h2>
                <button onclick="loadDongles()" class="text-xs text-teal-400 hover:underline">Refresh</button>
            </div>
            <div id="dongles-container" class="space-y-4">
                <div class="text-center text-sm text-slate-500 py-12">Loading devices...</div>
            </div>
        </div>
    </main>

    <footer class="border-t border-slate-800 bg-slate-900/20 py-4 text-center text-xs text-slate-500">
        Wyze Sense to MQTT Bridge (Rust) v0.1.3 — Multi-Dongle WebSocket Architecture
    </footer>

    <script>
        const API_BASE = "";
        let scanActive = false;
        let donglesData = { dongles: [], unassociated_sensors: [] };

        // ===== Sensor Row HTML Builder =====
        function buildSensorRow(sensor) {
            let batteryBadge;
            if (sensor.battery === null || sensor.battery === undefined) {
                batteryBadge = `<span class="px-2 py-0.5 rounded-full text-xs font-semibold border text-slate-400 bg-slate-950/30 border-slate-900/30">N/A</span>`;
            } else {
                let bc = "text-emerald-400 bg-emerald-950/30 border-emerald-900/30";
                if (sensor.battery < 40) bc = "text-rose-400 bg-rose-950/30 border-rose-900/30";
                else if (sensor.battery < 80) bc = "text-amber-400 bg-amber-950/30 border-amber-900/30";
                batteryBadge = `<span class="px-2 py-0.5 rounded-full text-xs font-semibold border ${bc}">${sensor.battery}%</span>`;
            }

            let signalColor = "text-slate-400";
            if (sensor.signal > -50) signalColor = "text-teal-400 font-semibold";
            else if (sensor.signal < -80) signalColor = "text-rose-400 font-semibold";

            let lastSeenText = "Never";
            if (sensor.last_seen > 0) {
                const diff = Math.floor(Date.now() / 1000) - sensor.last_seen;
                if (diff < 60) lastSeenText = "Just now";
                else if (diff < 3600) lastSeenText = `${Math.floor(diff / 60)}m ago`;
                else if (diff < 86400) lastSeenText = `${Math.floor(diff / 3600)}h ago`;
                else lastSeenText = `${Math.floor(diff / 86400)}d ago`;
            }

            let typeBadge = `<span class="px-2 py-0.5 rounded-full text-xs font-semibold border border-slate-800 bg-slate-900 text-slate-300 capitalize">${sensor.sensor_type}</span>`;
            const st = sensor.sensor_type?.toLowerCase() || "";
            if (st.includes("contact")) typeBadge = `<span class="px-2 py-0.5 rounded-full text-xs font-semibold border border-cyan-950 bg-cyan-950/20 text-cyan-400">🚪 Contact</span>`;
            else if (st.includes("motion")) typeBadge = `<span class="px-2 py-0.5 rounded-full text-xs font-semibold border border-purple-950 bg-purple-950/20 text-purple-400">🏃 Motion</span>`;
            else if (st.includes("climate")) typeBadge = `<span class="px-2 py-0.5 rounded-full text-xs font-semibold border border-sky-950 bg-sky-950/20 text-sky-400">🌡️ Climate</span>`;
            else if (st.includes("leak")) typeBadge = `<span class="px-2 py-0.5 rounded-full text-xs font-semibold border border-blue-950 bg-blue-950/20 text-blue-400">💧 Leak</span>`;

            let stateBadge = `<span class="text-slate-500 italic">Unknown</span>`;
            if (sensor.state) {
                switch (sensor.state.kind) {
                    case "Contact": stateBadge = sensor.state.is_open ? `<span class="text-rose-400 font-bold">Open</span>` : `<span class="text-emerald-400 font-bold">Closed</span>`; break;
                    case "Motion": stateBadge = sensor.state.is_active ? `<span class="text-rose-400 font-bold">Active</span>` : `<span class="text-emerald-400 font-bold">Clear</span>`; break;
                    case "Leak": stateBadge = sensor.state.is_wet ? `<span class="text-blue-400 font-bold">Wet</span>` : `<span class="text-emerald-400 font-bold">Dry</span>`; break;
                    case "Climate": stateBadge = `<span class="text-cyan-400 font-mono text-xs">${parseFloat(sensor.state.temperature).toFixed(1)}°C / ${sensor.state.humidity}%</span>`; break;
                }
            }

            let actions = `<button onclick="unpairSensor('${sensor.mac}')" class="text-xs py-1 px-2.5 rounded-lg bg-rose-950 hover:bg-rose-900 text-rose-400 transition">Unpair</button>`;
            if (st.includes("chime")) {
                actions = `<button onclick="testChime('${sensor.mac}')" class="text-xs py-1 px-2.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-teal-400 transition mr-1">Chime</button>` + actions;
            }

            return `<tr class="hover:bg-slate-800/30 transition">
                <td class="py-2.5 px-4 font-mono font-semibold text-teal-400 text-xs">${sensor.mac}</td>
                <td class="py-2.5 px-4">${typeBadge}</td>
                <td class="py-2.5 px-4">${stateBadge}</td>
                <td class="py-2.5 px-4">${batteryBadge}</td>
                <td class="py-2.5 px-4"><span class="font-mono text-xs ${signalColor}">${sensor.signal} dBm</span></td>
                <td class="py-2.5 px-4 text-xs text-slate-400">${lastSeenText}</td>
                <td class="py-2.5 px-4 text-right">${actions}</td>
            </tr>`;
        }

        function buildSensorTable(sensors) {
            if (!sensors || sensors.length === 0) {
                return `<div class="text-center text-xs text-slate-500 py-4 italic">No sensors paired to this dongle</div>`;
            }
            return `<table class="w-full text-left text-sm text-slate-300">
                <thead class="text-slate-500 text-xs font-medium uppercase">
                    <tr>
                        <th class="py-2 px-4">MAC</th>
                        <th class="py-2 px-4">Type</th>
                        <th class="py-2 px-4">State</th>
                        <th class="py-2 px-4">Battery</th>
                        <th class="py-2 px-4">Signal</th>
                        <th class="py-2 px-4">Last Seen</th>
                        <th class="py-2 px-4 text-right">Actions</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-slate-800/50">
                    ${sensors.map(s => buildSensorRow(s)).join("")}
                </tbody>
            </table>`;
        }

        // ===== Build Dongle Card =====
        function buildDongleCard(dongle, index) {
            const isScanning = dongle.scanning;
            const statusDot = `<span class="w-2.5 h-2.5 rounded-full ${isScanning ? 'bg-amber-400 animate-pulse' : 'bg-teal-400'}"></span>`;
            const scanLabel = isScanning ? '<span class="text-amber-400 text-xs font-semibold ml-2">SCANNING</span>' : '';
            const sensorCount = dongle.sensors?.length || 0;

            return `<div class="dongle-card bg-slate-900 rounded-2xl border border-slate-800 shadow-xl overflow-hidden transition-all" id="dongle-${index}">
                <div class="px-5 py-4 flex items-center justify-between cursor-pointer select-none hover:bg-slate-800/30 transition" onclick="toggleDongleSection(${index})">
                    <div class="flex items-center gap-3">
                        <span class="chevron open text-slate-500 text-sm" id="chevron-${index}">▶</span>
                        ${statusDot}
                        <div>
                            <div class="flex items-center gap-2">
                                <span class="font-bold text-sm text-slate-100">🕹️ Dongle ${dongle.mac}</span>
                                ${scanLabel}
                            </div>
                            <div class="text-xs text-slate-500 font-mono mt-0.5">${dongle.version || 'Unknown firmware'}</div>
                        </div>
                    </div>
                    <div class="flex items-center gap-3">
                        <span class="text-xs text-slate-400 bg-slate-800 px-2.5 py-1 rounded-full">${sensorCount} sensor${sensorCount !== 1 ? 's' : ''}</span>
                    </div>
                </div>
                <div class="sensor-rows border-t border-slate-800/50" id="sensors-${index}">
                    <div class="px-2 py-1">
                        ${buildSensorTable(dongle.sensors)}
                    </div>
                </div>
            </div>`;
        }

        function buildUnassociatedCard(sensors) {
            if (!sensors || sensors.length === 0) return '';
            return `<div class="dongle-card bg-slate-900/60 rounded-2xl border border-dashed border-slate-700 shadow-xl overflow-hidden">
                <div class="px-5 py-4 flex items-center justify-between cursor-pointer select-none hover:bg-slate-800/30 transition" onclick="toggleDongleSection('orphan')">
                    <div class="flex items-center gap-3">
                        <span class="chevron open text-slate-500 text-sm" id="chevron-orphan">▶</span>
                        <span class="w-2.5 h-2.5 rounded-full bg-slate-600"></span>
                        <div>
                            <span class="font-bold text-sm text-slate-400">📦 Unassociated Sensors</span>
                            <div class="text-xs text-slate-600 mt-0.5">Restored from saved state — no active dongle connection</div>
                        </div>
                    </div>
                    <span class="text-xs text-slate-500 bg-slate-800 px-2.5 py-1 rounded-full">${sensors.length} sensor${sensors.length !== 1 ? 's' : ''}</span>
                </div>
                <div class="sensor-rows border-t border-slate-800/50" id="sensors-orphan">
                    <div class="px-2 py-1">
                        ${buildSensorTable(sensors)}
                    </div>
                </div>
            </div>`;
        }

        function toggleDongleSection(id) {
            const rows = document.getElementById(`sensors-${id}`);
            const chevron = document.getElementById(`chevron-${id}`);
            rows.classList.toggle("collapsed");
            chevron.classList.toggle("open");
        }

        // ===== Data Loading =====
        async function loadDongles() {
            try {
                const res = await fetch(`${API_BASE}/api/dongles`);
                donglesData = await res.json();
                renderDongles();
                updateHeaderBadges();
                updateScanSelect();
            } catch (e) {
                document.getElementById("dongles-container").innerHTML =
                    `<div class="text-center text-sm text-rose-500 py-12">Failed to load devices</div>`;
            }
        }

        function renderDongles() {
            const container = document.getElementById("dongles-container");
            if (donglesData.dongles.length === 0 && (!donglesData.unassociated_sensors || donglesData.unassociated_sensors.length === 0)) {
                container.innerHTML = `<div class="text-center py-16">
                    <div class="text-4xl mb-3">📡</div>
                    <div class="text-slate-400 text-sm">No dongles connected</div>
                    <div class="text-slate-600 text-xs mt-1">Connect a USB dongle or start a WebSocket bridge</div>
                </div>`;
                return;
            }
            let html = donglesData.dongles.map((d, i) => buildDongleCard(d, i)).join("");
            html += buildUnassociatedCard(donglesData.unassociated_sensors);
            container.innerHTML = html;
        }

        function updateHeaderBadges() {
            const dot = document.getElementById("status-dot");
            const text = document.getElementById("status-text");
            const count = donglesData.dongles.length;
            if (count > 0) {
                dot.className = "w-2 h-2 rounded-full bg-teal-400";
                text.innerText = `${count} dongle${count > 1 ? 's' : ''} online`;
                text.parentElement.classList.remove("text-slate-400");
                text.parentElement.classList.add("text-teal-400", "bg-teal-900/20", "border", "border-teal-800/30");
            } else {
                dot.className = "w-2 h-2 rounded-full bg-rose-500 animate-pulse";
                text.innerText = "No dongles";
            }
        }

        function updateScanSelect() {
            const select = document.getElementById("scan-dongle-select");
            select.innerHTML = "";
            if (donglesData.dongles.length === 0) {
                select.innerHTML = `<option value="">No dongles connected</option>`;
                return;
            }
            donglesData.dongles.forEach(d => {
                const opt = document.createElement("option");
                opt.value = d.mac;
                opt.textContent = `🕹️ ${d.mac} (${d.sensors?.length || 0} sensors)`;
                select.appendChild(opt);
            });
        }

        // ===== Scan =====
        let scanTimer = null, pairingPollTimer = null, scanSecondsLeft = 60;

        async function toggleScan() {
            const btn = document.getElementById("btn-scan");
            const select = document.getElementById("scan-dongle-select");
            const dongleMac = select.value;
            if (!dongleMac) { logToConsole("No dongle selected for scanning!"); return; }

            const nextState = !scanActive;
            try {
                const res = await fetch(`${API_BASE}/api/scan`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ enable: nextState, dongle_mac: dongleMac })
                });
                const data = await res.json();
                scanActive = data.scan_active;
                if (scanActive) {
                    clearInterval(scanTimer); clearInterval(pairingPollTimer);
                    scanSecondsLeft = 60;
                    btn.innerText = `Stop Scan (${scanSecondsLeft}s)`;
                    btn.className = "w-full py-2.5 px-4 bg-rose-700 hover:bg-rose-600 font-semibold text-sm rounded-xl transition shadow-lg shadow-rose-700/25 flex items-center justify-center";
                    logToConsole(`Scanning on dongle ${dongleMac}...`);
                    scanTimer = setInterval(async () => {
                        scanSecondsLeft--;
                        if (scanSecondsLeft <= 0) { clearInterval(scanTimer); clearInterval(pairingPollTimer); await forceDisableScan(dongleMac); }
                        else btn.innerText = `Stop Scan (${scanSecondsLeft}s)`;
                    }, 1000);
                    pairingPollTimer = setInterval(async () => {
                        const still = await checkScanStatus();
                        if (!still) {
                            clearInterval(scanTimer); clearInterval(pairingPollTimer);
                            scanActive = false;
                            logToConsole(`🎉 Sensor paired on dongle ${dongleMac}!`);
                            btn.innerText = "Start Pairing Scan";
                            btn.className = "w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center";
                            await loadDongles();
                        }
                    }, 1500);
                } else {
                    clearInterval(scanTimer); clearInterval(pairingPollTimer);
                    btn.innerText = "Start Pairing Scan";
                    btn.className = "w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center";
                    logToConsole("Scan stopped.");
                    await loadDongles();
                }
            } catch (e) { logToConsole("Failed to toggle scan!"); }
        }

        async function forceDisableScan(dongleMac) {
            try {
                await fetch(`${API_BASE}/api/scan`, {
                    method: "POST", headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ enable: false, dongle_mac: dongleMac })
                });
            } catch(e) {}
            scanActive = false;
            const btn = document.getElementById("btn-scan");
            btn.innerText = "Start Pairing Scan";
            btn.className = "w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center";
            logToConsole("Scan timeout.");
            await loadDongles();
        }

        async function checkScanStatus() {
            try { const r = await fetch(`${API_BASE}/api/scan`); const d = await r.json(); return d.scan_active; }
            catch(e) { return false; }
        }

        // ===== Actions =====
        async function unpairSensor(mac) {
            if (!confirm(`Unpair sensor ${mac}?`)) return;
            try {
                const res = await fetch(`${API_BASE}/api/sensors/${mac}`, { method: "DELETE" });
                const data = await res.json();
                if (data.success) { logToConsole(`Unpaired: ${mac}`); loadDongles(); }
            } catch(e) { logToConsole(`Failed to unpair ${mac}`); }
        }

        async function testChime(mac) {
            try {
                const res = await fetch(`${API_BASE}/api/chime/${mac}`, { method: "POST" });
                const data = await res.json();
                if (data.success) logToConsole(`Chime triggered: ${mac}`);
            } catch(e) { logToConsole(`Chime failed: ${mac}`); }
        }

        async function runFix() {
            try {
                logToConsole("Purging ghost sensors...");
                const res = await fetch(`${API_BASE}/api/fix`, { method: "POST" });
                const data = await res.json();
                logToConsole(`Purged ${data.purged_count} ghosts: [${data.purged_macs.join(", ")}]`);
                loadDongles();
            } catch(e) { logToConsole("Fix failed!"); }
        }

        async function sendRawBytes() {
            const input = document.getElementById("hex-input").value;
            const bytes = input.split(",").map(s => s.trim()).filter(s => s.length > 0).map(s => parseInt(s, 16));
            if (bytes.some(isNaN)) { logToConsole("Invalid hex!"); return; }
            logToConsole(`===> [${input.toUpperCase()}]`);
            try {
                const res = await fetch(`${API_BASE}/api/raw`, {
                    method: "POST", headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ bytes })
                });
                if (res.status !== 200) { logToConsole(`Error: ${await res.text()}`); return; }
                const data = await res.json();
                logToConsole(`<=== [${data.response_bytes.map(b => b.toString(16).padStart(2, "0").toUpperCase()).join(",")}]`);
            } catch(e) { logToConsole("Send failed"); }
        }

        function logToConsole(msg) {
            const log = document.getElementById("console-log");
            const item = document.createElement("div");
            item.innerText = `[${new Date().toLocaleTimeString()}] ${msg}`;
            log.appendChild(item);
            log.scrollTop = log.scrollHeight;
        }

        // ===== Startup =====
        loadDongles();

        // SSE for real-time updates
        const evtSource = new EventSource(`${API_BASE}/api/events`);
        evtSource.onmessage = (event) => {
            if (event.data === "update") loadDongles();
        };
    </script>
</body>
</html>
"##;
