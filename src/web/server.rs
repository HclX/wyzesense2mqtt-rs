use crate::engine::Engine;
use crate::protocol::packet::Packet;
use crate::transport::AsyncTransport;
use crate::protocol::telemetry::SensorType;
use serde_json::json;

use axum::{
    extract::{Path, State},
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
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, debug};

use crate::protocol::sensor::SensorManager;

pub struct WebState<T: AsyncTransport> {
    pub engine: Arc<Mutex<Engine<T>>>,
    pub sensor_manager: Arc<std::sync::Mutex<SensorManager>>,
    pub broadcast_tx: tokio::sync::broadcast::Sender<()>,
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
}

#[derive(Serialize, Deserialize)]
pub struct ScanResponse {
    pub scan_active: bool,
}

#[derive(Serialize, Deserialize)]
pub struct VerifyRequest {
    pub mac: String,
    pub sensor_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct RawPacketRequest {
    pub bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct RawPacketResponse {
    pub response_bytes: Vec<u8>,
}

/// Starts the Axum web server binding to the given port and sharing Engine/SensorManager handles.
pub async fn start_web_server<T: AsyncTransport + Clone + 'static>(
    engine: Engine<T>,
    sensor_manager: Arc<std::sync::Mutex<SensorManager>>,
    broadcast_tx: tokio::sync::broadcast::Sender<()>,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shared_state = Arc::new(WebState {
        engine: Arc::new(Mutex::new(engine)),
        sensor_manager,
        broadcast_tx,
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/api/dongle", get(get_dongle_state::<T>))
        .route("/api/sensors", get(list_sensors::<T>))
        .route("/api/sensors/cached", get(list_cached_sensors::<T>))
        .route("/api/sensors/:mac", delete(unpair_sensor::<T>))
        .route("/api/scan", get(get_scan_status::<T>).post(toggle_scan::<T>))
        .route("/api/verify", post(verify_scanned_sensor::<T>))
        .route("/api/chime/:mac", post(trigger_chime::<T>))
        .route("/api/fix", post(fix_sensors::<T>))
        .route("/api/raw", post(send_raw_packet::<T>))
        .route("/api/events", get(sse_handler::<T>))
        .layer(cors)
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Web interface successfully started. Listening on http://{}", addr);
    
    axum::serve(listener, app).await?;
    Ok(())
}

// --- GET / serving HTML packed UI ---
async fn serve_dashboard() -> impl IntoResponse {
    Html(HTML_CONTENT)
}

// --- GET /api/events ---
async fn sse_handler<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.broadcast_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|res| match res {
        Ok(_) => Some(Ok(Event::default().data("update"))),
        Err(_) => None,
    });
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

// --- GET /api/dongle ---
async fn get_dongle_state<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
) -> impl IntoResponse {
    let engine = state.engine.lock().await;
    Json(DongleStateResponse {
        connected: engine.dongle_mac().is_some(),
        mac: engine.dongle_mac().map(|s| s.to_string()),
        version: engine.dongle_version().map(|s| s.to_string()),
    })
}

// --- GET /api/sensors ---
async fn list_sensors<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
    match engine.get_sensor_list().await {
        Ok(mut mac_list) => {
            mac_list.sort();
            let mut sensors = Vec::new();
            let manager = state.sensor_manager.lock().unwrap();
            for mac in mac_list {
                if let Some(sensor) = manager.get_sensors().get(&mac) {
                    sensors.push(crate::config::state::PersistedSensorState {
                        mac: sensor.mac.clone(),
                        sensor_type: sensor.sensor_type.as_str().to_string(),
                        last_seen: sensor.last_seen,
                        battery: sensor.battery_pct,
                        battery_raw: sensor.battery_raw,
                        signal: sensor.rssi_dbm,
                        die_temperature_c: sensor.die_temperature_c,
                        event_sequence: sensor.event_sequence,
                        state: sensor.state.clone(),
                    });
                } else {
                    sensors.push(crate::config::state::PersistedSensorState {
                        mac: mac.clone(),
                        sensor_type: "unknown".to_string(),
                        last_seen: 0,
                        battery: Some(100),
                        battery_raw: None,
                        signal: -60,
                        die_temperature_c: None,
                        event_sequence: None,
                        state: crate::protocol::sensor::SensorState::Unknown,
                    });
                }
            }
            (StatusCode::OK, Json(SensorsListResponse { sensors })).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// --- GET /api/sensors/cached ---
async fn list_cached_sensors<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
) -> impl IntoResponse {
    let manager = state.sensor_manager.lock().unwrap();
    let mut sensors: Vec<crate::config::state::PersistedSensorState> = manager.get_sensors().values().map(|sensor| {
        crate::config::state::PersistedSensorState {
            mac: sensor.mac.clone(),
            sensor_type: sensor.sensor_type.as_str().to_string(),
            last_seen: sensor.last_seen,
            battery: sensor.battery_pct,
            battery_raw: sensor.battery_raw,
            signal: sensor.rssi_dbm,
            die_temperature_c: sensor.die_temperature_c,
            event_sequence: sensor.event_sequence,
            state: sensor.state.clone(),
        }
    }).collect();
    sensors.sort_by_key(|s| s.mac.clone());
    (StatusCode::OK, Json(SensorsListResponse { sensors })).into_response()
}

// --- DELETE /api/sensors/:mac ---
async fn unpair_sensor<T: AsyncTransport + Clone + 'static>(
    Path(mac): Path<String>,
    State(state): State<Arc<WebState<T>>>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
    match engine.delete_sensor(&mac).await {
        Ok(_) => {
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
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// --- GET /api/scan ---
async fn get_scan_status<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
) -> impl IntoResponse {
    let engine = state.engine.lock().await;
    (StatusCode::OK, Json(ScanResponse { scan_active: engine.is_scanning() })).into_response()
}

// --- POST /api/scan ---
async fn toggle_scan<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
    Json(payload): Json<ScanRequest>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
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
async fn verify_scanned_sensor<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
    Json(payload): Json<VerifyRequest>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
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
async fn trigger_chime<T: AsyncTransport + Clone + 'static>(
    Path(mac): Path<String>,
    State(state): State<Arc<WebState<T>>>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
    match engine.play_chime(&mac).await {
        Ok(_) => (
            StatusCode::OK,
            Json(SuccessResponse {
                success: true,
                message: format!("Chime triggered on {}", mac),
            }),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// --- POST /api/fix ---
async fn fix_sensors<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
    // Fix algorithm: lists sensors, identifies invalid MAC patterns, and deletes them
    match engine.get_sensor_list().await {
        Ok(sensors) => {
            let mut purged = Vec::new();
            let invalid_ghosts = ["00000000", "\0\0\0\0\0\0\0\0"];
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
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// --- POST /api/raw ---
async fn send_raw_packet<T: AsyncTransport + Clone + 'static>(
    State(state): State<Arc<WebState<T>>>,
    Json(payload): Json<RawPacketRequest>,
) -> impl IntoResponse {
    let mut engine = state.engine.lock().await;
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
    <style>
        body { font-family: 'Inter', sans-serif; }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col">
    <header class="border-b border-slate-800 bg-slate-900/50 backdrop-blur sticky top-0 z-50">
        <div class="max-w-[1400px] w-full mx-auto px-6 py-4 flex items-center justify-between">
            <div class="flex items-center space-x-3">
                <span class="text-2xl">📡</span>
                <h1 class="text-xl font-bold tracking-tight text-teal-400">Wyze Sense to MQTT Bridge (Rust) Control Dashboard</h1>
            </div>
            <div id="dongle-badge" class="flex items-center space-x-2 bg-slate-800 px-3 py-1.5 rounded-full text-xs font-semibold text-slate-400">
                <span class="w-2 h-2 rounded-full bg-rose-500 animate-pulse" id="status-dot"></span>
                <span id="status-text">USB Dongle Offline</span>
            </div>
        </div>
    </header>

    <main class="flex-1 max-w-[1400px] w-full mx-auto p-6 grid grid-cols-1 lg:grid-cols-4 gap-6">
        <!-- Left Column: System State & Control -->
        <div class="lg:col-span-1 flex flex-col gap-6">
            <!-- Dongle Metadata Card -->
            <div class="bg-slate-900 rounded-2xl border border-slate-800 p-6 shadow-xl">
                <h2 class="text-lg font-bold text-teal-400 mb-4 flex items-center"><span class="mr-2">🕹️</span> Dongle Details</h2>
                <div class="space-y-3 text-sm">
                    <div class="flex justify-between"><span class="text-slate-400">MAC Address:</span> <span class="font-mono" id="dongle-mac">N/A</span></div>
                    <div class="flex justify-between"><span class="text-slate-400">Firmware Version:</span> <span class="font-mono" id="dongle-ver">N/A</span></div>
                </div>
            </div>

            <!-- Pairing Control Card -->
            <div class="bg-slate-900 rounded-2xl border border-slate-800 p-6 shadow-xl">
                <h2 class="text-lg font-bold text-teal-400 mb-4 flex items-center"><span class="mr-2">🤝</span> Pairing Center</h2>
                <p class="text-xs text-slate-400 mb-4">Turn on pairing mode to scan for nearby Wyze Sense sensors. Tap the sensor's reset button until the red light blinks.</p>
                <button id="btn-scan" onclick="toggleScan()" class="w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center">
                    Start Pairing Scan
                </button>
            </div>

            <!-- Maintenance -->
            <div class="bg-slate-900 rounded-2xl border border-slate-800 p-6 shadow-xl">
                <h2 class="text-lg font-bold text-teal-400 mb-4 flex items-center"><span class="mr-2">🛠️</span> Maintenance</h2>
                <button onclick="runFix()" class="w-full py-2.5 px-4 border border-slate-700 bg-slate-800 hover:bg-slate-700 font-semibold text-sm rounded-xl transition">
                    Purge Ghost Sensors (Fix)
                </button>
            </div>
        </div>

        <!-- Right Column: Sensor Management & Hex Terminal -->
        <div class="lg:col-span-3 flex flex-col gap-6">
            <!-- Active Sensors List -->
            <div class="bg-slate-900 rounded-2xl border border-slate-800 p-6 shadow-xl">
                <div class="flex items-center justify-between mb-4">
                    <h2 class="text-lg font-bold text-teal-400 flex items-center"><span class="mr-2">🔋</span> Paired Sensors</h2>
                    <button onclick="loadSensors()" class="text-xs text-teal-400 hover:underline">Refresh List</button>
                </div>
                <div class="overflow-x-auto rounded-xl border border-slate-800 bg-slate-950/50">
                    <table class="w-full text-left text-sm text-slate-300">
                        <thead class="bg-slate-900 text-slate-400 text-xs font-bold uppercase border-b border-slate-800">
                            <tr>
                                <th class="py-3.5 px-4">MAC Key</th>
                                <th class="py-3.5 px-4">Type</th>
                                <th class="py-3.5 px-4">State</th>
                                <th class="py-3.5 px-4">Version</th>
                                <th class="py-3.5 px-4">Battery</th>
                                <th class="py-3.5 px-4">Signal</th>
                                <th class="py-3.5 px-4">Last Seen</th>
                                <th class="py-3.5 px-4 text-right">Actions</th>
                            </tr>
                        </thead>
                        <tbody id="sensors-tbody" class="divide-y divide-slate-800">
                            <tr>
                                <td colspan="7" class="py-8 text-center text-xs text-slate-500">Loading sensors...</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Hex Diagnostic Terminal -->
            <div class="bg-slate-900 rounded-2xl border border-slate-800 p-6 shadow-xl">
                <h2 class="text-lg font-bold text-teal-400 mb-4 flex items-center"><span class="mr-2">💻</span> Hex Diagnostics Console</h2>
                <p class="text-xs text-slate-400 mb-3">Inject raw hex command bytes directly to the USB device line and view returned replies.</p>
                
                <div class="flex gap-3 mb-4">
                    <input id="hex-input" type="text" placeholder="e.g. AA,55,43,03,04,01,49" class="flex-1 bg-slate-950 border border-slate-800 text-sm rounded-xl px-4 py-2.5 font-mono focus:outline-none focus:border-teal-500 transition">
                    <button onclick="sendRawBytes()" class="py-2.5 px-5 bg-slate-800 hover:bg-slate-700 font-semibold text-sm rounded-xl border border-slate-700 transition">
                        Send Hex
                    </button>
                </div>

                <div class="rounded-xl border border-slate-800 bg-slate-950 p-4 font-mono text-xs text-emerald-400 min-h-[120px] max-h-[200px] overflow-y-auto space-y-1 flex flex-col justify-end" id="console-log">
                    <div class="text-slate-500 italic">[Console ready. Input hex array to dispatch...]</div>
                </div>
            </div>
        </div>
    </main>

    <footer class="border-t border-slate-800 bg-slate-900/20 py-4 text-center text-xs text-slate-500">
        Wyze Sense to MQTT Bridge (Rust) Web Controller Interface v0.1.0. Compiled in Rust.
    </footer>

    <script>
        const API_BASE = "";
        let scanActive = false;

        async function loadDongleDetails() {
            try {
                const res = await fetch(`${API_BASE}/api/dongle`);
                const data = await res.json();
                if (data.connected) {
                    document.getElementById("dongle-mac").innerText = data.mac;
                    document.getElementById("dongle-ver").innerText = data.version;
                    
                    const badge = document.getElementById("dongle-badge");
                    badge.classList.remove("text-slate-400");
                    badge.classList.add("text-teal-400", "bg-teal-900/20", "border", "border-teal-800/30");
                    document.getElementById("status-dot").className = "w-2 h-2 rounded-full bg-teal-400";
                    document.getElementById("status-text").innerText = "USB Dongle Online";
                }
            } catch(e) {
                console.error("Failed to load dongle details:", e);
            }
        }

        async function loadSensorsFromEndpoint(url) {
            const tbody = document.getElementById("sensors-tbody");
            try {
                const res = await fetch(url);
                const data = await res.json();
                tbody.innerHTML = "";
                if (data.sensors.length === 0) {
                    tbody.innerHTML = `<tr><td colspan="7" class="py-8 text-center text-xs text-slate-500">No sensors paired to this bridge yet.</td></tr>`;
                    return;
                }
                data.sensors.forEach(sensor => {
                    // Formulate Battery Badge
                    let batteryBadge;
                    if (sensor.battery === null || sensor.battery === undefined) {
                        batteryBadge = `<span class="px-2.5 py-1 rounded-full text-xs font-semibold border text-slate-400 bg-slate-950/30 border-slate-900/30">N/A</span>`;
                    } else {
                        let batteryColor = "text-emerald-400 bg-emerald-950/30 border-emerald-900/30";
                        if (sensor.battery < 40) {
                            batteryColor = "text-rose-400 bg-rose-950/30 border-rose-900/30";
                        } else if (sensor.battery < 80) {
                            batteryColor = "text-amber-400 bg-amber-950/30 border-amber-900/30";
                        }
                        batteryBadge = `<span class="px-2.5 py-1 rounded-full text-xs font-semibold border ${batteryColor}">${sensor.battery}%</span>`;
                    }

                    // Formulate Signal Badge (RSSI)
                    let signalColor = "text-slate-400";
                    if (sensor.signal > -50) signalColor = "text-teal-400 font-semibold";
                    else if (sensor.signal < -80) signalColor = "text-rose-400 font-semibold";
                    const signalBadge = `<span class="font-mono text-xs ${signalColor}">${sensor.signal} dBm</span>`;

                    // Formulate Last Seen text (relative or timestamp)
                    let lastSeenText = "Never";
                    if (sensor.last_seen > 0) {
                        const diffSecs = Math.floor(Date.now() / 1000) - sensor.last_seen;
                        if (diffSecs < 60) lastSeenText = "Just now";
                        else if (diffSecs < 3600) lastSeenText = `${Math.floor(diffSecs / 60)}m ago`;
                        else lastSeenText = `${Math.floor(diffSecs / 3600)}h ago`;
                    }

                    // Formulate Sensor Type Badge
                    let typeBadge = `<span class="px-2.5 py-1 rounded-full text-xs font-semibold border border-slate-800 bg-slate-900 text-slate-300 capitalize">${sensor.sensor_type}</span>`;
                    if (sensor.sensor_type === "contact" || sensor.sensor_type === "ContactV1" || sensor.sensor_type === "ContactV2") {
                        typeBadge = `<span class="px-2.5 py-1 rounded-full text-xs font-semibold border border-cyan-950 bg-cyan-950/20 text-cyan-400 capitalize">🚪 Contact</span>`;
                    } else if (sensor.sensor_type === "motion" || sensor.sensor_type === "MotionV1" || sensor.sensor_type === "MotionV2") {
                        typeBadge = `<span class="px-2.5 py-1 rounded-full text-xs font-semibold border border-purple-950 bg-purple-950/20 text-purple-400 capitalize">🏃 Motion</span>`;
                    }

                    // Formulate State Badge
                    let stateBadge = `<span class="text-slate-500 italic">Unknown</span>`;
                    if (sensor.state) {
                        switch (sensor.state.kind) {
                            case "Contact":
                                if (sensor.state.is_open) {
                                    stateBadge = `<span class="text-rose-400 font-bold">Open</span>`;
                                } else {
                                    stateBadge = `<span class="text-emerald-400 font-bold">Closed</span>`;
                                }
                                break;
                            case "Motion":
                                if (sensor.state.is_active) {
                                    stateBadge = `<span class="text-rose-400 font-bold">Active</span>`;
                                } else {
                                    stateBadge = `<span class="text-emerald-400 font-bold">Clear</span>`;
                                }
                                break;
                            case "Leak":
                                if (sensor.state.is_wet) {
                                    stateBadge = `<span class="text-blue-400 font-bold">Wet</span>`;
                                } else {
                                    stateBadge = `<span class="text-emerald-400 font-bold">Dry</span>`;
                                }
                                break;
                            case "Climate":
                                stateBadge = `<span class="text-cyan-400 font-mono">${sensor.state.temperature}°C / ${sensor.state.humidity}%</span>`;
                                break;
                            case "Chime":
                                stateBadge = `<span class="text-slate-400">Ready</span>`;
                                break;
                            case "Unknown":
                            default:
                                stateBadge = `<span class="text-slate-500 italic">Unknown</span>`;
                                break;
                        }
                    }

                    // Formulate Actions column buttons dynamically!
                    let actionsHtml = `<button onclick="unpairSensor('${sensor.mac}')" class="text-xs py-1.5 px-3 rounded-lg bg-rose-950 hover:bg-rose-900 text-rose-400 transition">Unpair</button>`;
                    if (sensor.sensor_type === "chime" || sensor.sensor_type === "Chime") {
                        actionsHtml = `
                            <button onclick="testChime('${sensor.mac}')" class="text-xs py-1.5 px-3 rounded-lg bg-slate-800 hover:bg-slate-700 text-teal-400 transition mr-2">Chime</button>
                            <button onclick="unpairSensor('${sensor.mac}')" class="text-xs py-1.5 px-3 rounded-lg bg-rose-950 hover:bg-rose-900 text-rose-400 transition">Unpair</button>
                        `;
                    }

                    const tr = document.createElement("tr");
                    tr.className = "hover:bg-slate-900/50 transition";
                    tr.innerHTML = `
                        <td class="py-3.5 px-4 font-mono font-semibold text-teal-400">${sensor.mac}</td>
                        <td class="py-3.5 px-4">${typeBadge}</td>
                        <td class="py-3.5 px-4">${stateBadge}</td>
                        <td class="py-3.5 px-4 font-mono text-xs text-slate-400">${sensor.version}</td>
                        <td class="py-3.5 px-4">${batteryBadge}</td>
                        <td class="py-3.5 px-4">${signalBadge}</td>
                        <td class="py-3.5 px-4 text-xs text-slate-400">${lastSeenText}</td>
                        <td class="py-3.5 px-4 text-right">${actionsHtml}</td>
                    `;
                    tbody.appendChild(tr);
                });
            } catch (e) {
                tbody.innerHTML = `<tr><td colspan="7" class="py-8 text-center text-xs text-rose-500 font-semibold">Error fetching sensors list</td></tr>`;
            }
        }

        async function loadSensors() {
            await loadSensorsFromEndpoint(`${API_BASE}/api/sensors`);
        }

        async function loadCachedSensors() {
            await loadSensorsFromEndpoint(`${API_BASE}/api/sensors/cached`);
        }

        async function checkScanStatus() {
            try {
                const res = await fetch(`${API_BASE}/api/scan`);
                const data = await res.json();
                return data.scan_active;
            } catch (e) {
                console.error("Failed to check scan status:", e);
                return false;
            }
        }

        let scanTimer = null;
        let pairingPollTimer = null;
        let scanSecondsLeft = 60;

        async function forceDisableScan() {
            const btn = document.getElementById("btn-scan");
            try {
                const res = await fetch(`${API_BASE}/api/scan`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ enable: false })
                });
                const data = await res.json();
                scanActive = data.scan_active;
                clearInterval(scanTimer);
                clearInterval(pairingPollTimer);
                btn.innerText = "Start Pairing Scan";
                btn.className = "w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center";
                logToConsole("Pairing scan stopped.");
                await loadSensors(); // Force slow physical sweep on exit to sync final list
            } catch (e) {
                logToConsole("Failed to automatically turn off scan mode!");
            }
        }

        async function toggleScan() {
            const btn = document.getElementById("btn-scan");
            const nextState = !scanActive;
            try {
                const res = await fetch(`${API_BASE}/api/scan`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ enable: nextState })
                });
                const data = await res.json();
                scanActive = data.scan_active;
                if (scanActive) {
                    clearInterval(scanTimer);
                    clearInterval(pairingPollTimer);

                    scanSecondsLeft = 60;
                    btn.innerText = `Stop Pairing Scan (${scanSecondsLeft}s)`;
                    btn.className = "w-full py-2.5 px-4 bg-rose-700 hover:bg-rose-600 font-semibold text-sm rounded-xl transition shadow-lg shadow-rose-700/25 flex items-center justify-center";
                    logToConsole("Pairing scan active. Press side pin button on Wyze Sense sensor.");

                    scanTimer = setInterval(async () => {
                        scanSecondsLeft--;
                        if (scanSecondsLeft <= 0) {
                            clearInterval(scanTimer);
                            clearInterval(pairingPollTimer);
                            logToConsole("Pairing scan timeout reached. Automatically disabling scan mode.");
                            await forceDisableScan();
                        } else {
                            btn.innerText = `Stop Pairing Scan (${scanSecondsLeft}s)`;
                        }
                    }, 1000);

                    // Active Polling: Check scan active status directly from RAM status (0ms overhead!)
                    pairingPollTimer = setInterval(async () => {
                        const isStillScanning = await checkScanStatus();
                        if (!isStillScanning) {
                            clearInterval(scanTimer);
                            clearInterval(pairingPollTimer);
                            scanActive = false;
                            
                            logToConsole(`🎉 SUCCESS: Sensor successfully paired/re-paired! Pairing scan stopped.`);
                            
                            btn.innerText = "Start Pairing Scan";
                            btn.className = "w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center";
                            await loadSensors(); // Trigger single slow sweep on success to sync list
                        }
                    }, 1500);

                } else {
                    clearInterval(scanTimer);
                    clearInterval(pairingPollTimer);
                    btn.innerText = "Start Pairing Scan";
                    btn.className = "w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center";
                    logToConsole("Pairing scan stopped.");
                    await loadSensors(); // Trigger single slow sweep on exit
                }
            } catch (e) {
                logToConsole("Failed to toggle scan mode!");
            }
        }

        async function unpairSensor(mac) {
            if (!confirm(`Are you sure you want to unpair sensor ${mac}?`)) return;
            try {
                const res = await fetch(`${API_BASE}/api/sensors/${mac}`, { method: "DELETE" });
                const data = await res.json();
                if (data.success) {
                    logToConsole(`Unpaired sensor: ${mac}`);
                    loadSensors();
                }
            } catch (e) {
                logToConsole(`Failed to delete sensor ${mac}!`);
            }
        }

        async function testChime(mac) {
            try {
                const res = await fetch(`${API_BASE}/api/chime/${mac}`, { method: "POST" });
                const data = await res.json();
                if (data.success) {
                    logToConsole(`Triggered chime alarm on ${mac}`);
                }
            } catch(e) {
                logToConsole(`Failed to trigger chime on ${mac}`);
            }
        }

        async function runFix() {
            try {
                logToConsole("Running ghost sensor fix purge...");
                const res = await fetch(`${API_BASE}/api/fix`, { method: "POST" });
                const data = await res.json();
                logToConsole(`Fix complete. Purged ${data.purged_count} invalid MAC sensors: [${data.purged_macs.join(", ")}]`);
                loadSensors();
            } catch (e) {
                logToConsole("Failed to run fix diagnostics!");
            }
        }

        async function sendRawBytes() {
            const input = document.getElementById("hex-input").value;
            const bytes = input.split(",")
                .map(s => s.trim())
                .filter(s => s.length > 0)
                .map(s => parseInt(s, 16));
            
            if (bytes.some(isNaN)) {
                logToConsole("Error: Input contains invalid hexadecimal values!");
                return;
            }

            logToConsole(`===> Write raw: [${input.toUpperCase()}]`);
            try {
                const res = await fetch(`${API_BASE}/api/raw`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ bytes })
                });
                if (res.status !== 200) {
                    const errText = await res.text();
                    logToConsole(`!!! Error: ${errText}`);
                    return;
                }
                const data = await res.json();
                const hexResp = data.response_bytes.map(b => b.toString(16).padStart(2, "0").toUpperCase()).join(",");
                logToConsole(`<=== Reply raw: [${hexResp}]`);
            } catch (e) {
                logToConsole("!!! Error: Connection timed out or endpoint failed");
            }
        }

        function logToConsole(message) {
            const log = document.getElementById("console-log");
            const item = document.createElement("div");
            item.innerText = `[${new Date().toLocaleTimeString()}] ${message}`;
            log.appendChild(item);
            log.scrollTop = log.scrollHeight;
        }

        // Background synchronization for scan state
        async function syncScanState() {
            const isScanning = await checkScanStatus();
            if (isScanning !== scanActive) {
                scanActive = isScanning;
                const btn = document.getElementById("btn-scan");
                if (scanActive) {
                    // Scan started externally
                    btn.innerText = "Stop Pairing Scan (Active)";
                    btn.className = "w-full py-2.5 px-4 bg-rose-700 hover:bg-rose-600 font-semibold text-sm rounded-xl transition shadow-lg shadow-rose-700/25 flex items-center justify-center";
                    logToConsole("Pairing scan detected as active.");
                } else {
                    // Scan stopped externally
                    clearInterval(scanTimer);
                    clearInterval(pairingPollTimer);
                    btn.innerText = "Start Pairing Scan";
                    btn.className = "w-full py-2.5 px-4 bg-teal-600 hover:bg-teal-500 font-semibold text-sm rounded-xl transition shadow-lg shadow-teal-600/25 flex items-center justify-center";
                    logToConsole("Pairing scan stopped.");
                    await loadSensors();
                }
            }
        }

        // Startup Loaders
        loadDongleDetails();
        loadSensors();
        
        // SSE Listener for real-time updates
        const evtSource = new EventSource(`${API_BASE}/api/events`);
        evtSource.onmessage = (event) => {
            if (event.data === "update") {
                loadCachedSensors();
                syncScanState();
            }
        };
    </script>
</body>
</html>
"##;
