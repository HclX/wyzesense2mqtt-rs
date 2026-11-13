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

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::SinkExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, debug, error, warn};

use crate::protocol::sensor::SensorManager;
use crate::config::dongles::DonglesConfig;
use crate::gateway::mqtt::DongleStatus;

pub struct WebState {
    pub engines: EnginesMap,
    pub sensor_manager: Arc<std::sync::Mutex<SensorManager>>,
    /// User-assigned dongle names, persisted in config/dongles.yaml.
    pub dongle_names: Arc<std::sync::Mutex<DonglesConfig>>,
    pub dongle_config_path: String,
    pub broadcast_tx: tokio::sync::broadcast::Sender<()>,
    pub event_tx: tokio::sync::mpsc::Sender<crate::protocol::telemetry::DongleEvent>,
    /// Bridge dongle connects/disconnects, for the MQTT dongle status topic.
    pub dongle_status_tx: tokio::sync::mpsc::Sender<DongleStatus>,
    /// Optional auth token required for WebSocket bridge connections.
    pub bridge_auth_token: Option<String>,
    pub bridge: BridgeSettings,
}

/// Timing for `/ws/bridge` connections.
pub struct BridgeSettings {
    /// Time allowed for the whole dongle handshake.
    pub handshake_timeout: Duration,
    /// How long each handshake Inquiry waits before it is retried.
    pub inquiry_timeout: Duration,
    /// How often each bridge client is pinged.
    pub ping_interval: Duration,
    /// Consecutive unanswered pings after which the client is dropped.
    pub max_missed_pongs: u32,
}

impl Default for BridgeSettings {
    fn default() -> Self {
        Self {
            handshake_timeout: crate::engine::HANDSHAKE_TIMEOUT,
            inquiry_timeout: Duration::from_secs(2),
            ping_interval: Duration::from_secs(30),
            max_missed_pongs: 3,
        }
    }
}

type BridgeReader = Arc<tokio::sync::Mutex<crate::transport::WsStream>>;
type BridgeWriter = Arc<tokio::sync::Mutex<crate::transport::WsSink>>;

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

#[derive(Deserialize)]
pub struct RenameRequest {
    pub name: String,
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

#[derive(Serialize, Deserialize, Default)]
pub struct FixRequest {
    #[serde(default)]
    pub dongle_mac: Option<String>,
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
    dongle_status_tx: tokio::sync::mpsc::Sender<DongleStatus>,
    dongle_config_path: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dongle_names = DonglesConfig::load_from_yaml(&dongle_config_path).unwrap_or_default();
    let shared_state = Arc::new(WebState {
        engines,
        sensor_manager,
        dongle_names: Arc::new(std::sync::Mutex::new(dongle_names)),
        dongle_config_path,
        broadcast_tx,
        event_tx,
        dongle_status_tx,
        bridge_auth_token,
        bridge: BridgeSettings::default(),
    });

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Web interface successfully started. Listening on http://{}", addr);
    
    axum::serve(listener, router(shared_state).into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}

fn router(shared_state: Arc<WebState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/", get(serve_dashboard))
        .route("/api/dongle", get(get_dongle_state))
        .route("/api/dongles", get(list_dongles))
        .route("/api/sensors", get(list_sensors))
        .route("/api/sensors/cached", get(list_cached_sensors))
        .route("/api/sensors/:mac", delete(unpair_sensor))
        .route("/api/sensors/:mac/rename", post(rename_sensor))
        .route("/api/dongles/:mac/rename", post(rename_dongle))
        .route("/api/scan", get(get_scan_status).post(toggle_scan))
        .route("/api/verify", post(verify_scanned_sensor))
        .route("/api/chime/:mac", post(trigger_chime))
        .route("/api/fix", post(fix_sensors))
        .route("/api/raw", post(send_raw_packet))
        .route("/api/events", get(sse_handler))
        .route("/ws/bridge", get(ws_bridge_handler))
        .layer(cors)
        .with_state(shared_state)
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
    socket: WebSocket,
    state: Arc<WebState>,
    remote_addr: String,
    device_path: Option<String>,
) {
    use futures_util::StreamExt;
    info!("New WebSocket bridge connection from {} (device: {:?})", remote_addr, device_path);

    let (writer, reader) = socket.split();
    let reader: BridgeReader = Arc::new(tokio::sync::Mutex::new(Box::new(reader)));
    let writer: BridgeWriter = Arc::new(tokio::sync::Mutex::new(Box::new(writer)));
    let pongs = Arc::new(AtomicU64::new(0));
    let transport = GatewayTransport::WebSocket {
        reader: Arc::clone(&reader),
        writer: Arc::clone(&writer),
        pending: Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
        pongs: Arc::clone(&pongs),
    };

    let mut engine = Engine::new(transport, state.event_tx.clone());
    engine.transport_label = "bridge".to_string();
    engine.device_path = device_path;
    engine.remote_addr = Some(remote_addr.clone());
    engine.inquiry_timeout = state.bridge.inquiry_timeout;
    // Held for the life of the connection; dropping it stops the engine worker.
    let exit_tx = engine.start();

    let handshake = match tokio::time::timeout(state.bridge.handshake_timeout, engine.initialize_handshake()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("timed out".to_string()),
    };
    if let Err(reason) = handshake {
        error!("WebSocket bridge handshake with {} failed: {}", remote_addr, reason);
        drop(exit_tx);
        // Without a close frame the client keeps thinking it is connected and
        // never reconnects for a new handshake.
        close_bridge(&reader, &writer, 1011, &format!("handshake failed: {}", reason)).await;
        return;
    }

    let mac = engine.dongle_mac().unwrap_or("unknown").to_string();
    let version = engine.dongle_version().map(|s| s.to_string());
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

    // Register in engines map
    let disconnect_notify = Arc::clone(&engine.disconnect_notify);
    state.engines.lock().await.insert(mac.clone(), engine);
    let _ = state.dongle_status_tx.try_send(DongleStatus { mac: mac.clone(), online: true, version });
    let _ = state.broadcast_tx.send(());

    // Wait for the transport to die (WebSocket close / broken pipe; the reader
    // loop fires disconnect_notify) or for the client to stop answering pings,
    // which is how a half-open TCP connection shows up.
    tokio::select! {
        _ = disconnect_notify.notified() => {
            info!("Bridge dongle {} disconnected, cleaning up", mac);
        }
        _ = heartbeat(&writer, &pongs, state.bridge.ping_interval, state.bridge.max_missed_pongs) => {
            warn!("Bridge dongle {} missed {} pings, dropping connection", mac, state.bridge.max_missed_pongs);
            close_bridge(&reader, &writer, 1011, "heartbeat timeout").await;
        }
    }
    drop(exit_tx);

    // Remove engine from the shared map, unless the client has already
    // reconnected and a new connection registered the same dongle.
    let still_ours = {
        let mut map = state.engines.lock().await;
        let ours = map.get(&mac).is_some_and(|e| Arc::ptr_eq(&e.disconnect_notify, &disconnect_notify));
        if ours {
            map.remove(&mac);
        }
        ours
    };
    if !still_ours {
        info!("Bridge dongle {} already re-registered by a newer connection", mac);
        return;
    }

    // Cleanup: unassign sensors from disconnected dongle
    {
        let mut manager = state.sensor_manager.lock().unwrap();
        manager.unassign_dongle(&mac);
        info!("Unassigned sensors from disconnected bridge dongle {}", mac);
    }
    let _ = state.dongle_status_tx.try_send(DongleStatus { mac, online: false, version: None });
    let _ = state.broadcast_tx.send(());
}

/// Pings the bridge client every `interval` and returns once `max_missed`
/// pings in a row went unanswered (or a ping could not be sent).
async fn heartbeat(writer: &BridgeWriter, pongs: &AtomicU64, interval: Duration, max_missed: u32) {
    let mut missed = 0;
    loop {
        let before = pongs.load(Ordering::SeqCst);
        // A send that blocks for a whole interval (full TCP buffer) counts as dead too.
        let sent = tokio::time::timeout(interval, async {
            writer.lock().await.send(Message::Ping(Vec::new())).await
        })
        .await;
        if !matches!(sent, Ok(Ok(()))) {
            return;
        }
        tokio::time::sleep(interval).await;
        if pongs.load(Ordering::SeqCst) == before {
            missed += 1;
            debug!("Bridge client missed ping ({}/{})", missed, max_missed);
            if missed >= max_missed {
                return;
            }
        } else {
            missed = 0;
        }
    }
}

/// Sends a close frame so the bridge client knows to reconnect.
async fn close_bridge(reader: &BridgeReader, writer: &BridgeWriter, code: u16, reason: &str) {
    // Close reasons are limited to 123 bytes.
    let mut end = reason.len().min(123);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    let frame = CloseFrame { code, reason: reason[..end].to_string().into() };
    let sent = tokio::time::timeout(Duration::from_secs(5), async {
        writer.lock().await.send(Message::Close(Some(frame))).await
    })
    .await;
    if !matches!(sent, Ok(Ok(()))) {
        debug!("Could not send close frame to bridge client");
        return;
    }
    // Keep the socket open briefly until the peer completes the close
    // handshake; dropping it at once while the peer still has Pong/Close frames
    // in flight makes the kernel answer with a TCP RST that discards our Close.
    let (reader, writer) = (Arc::clone(reader), Arc::clone(writer));
    tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            let mut r = reader.lock().await;
            while let Some(Ok(msg)) = r.next().await {
                if matches!(msg, Message::Close(_)) {
                    break;
                }
            }
        })
        .await;
        drop(writer);
    });
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
    let names = state.dongle_names.lock().unwrap();

    // Group sensors by dongle_mac from the single source of truth (SensorManager)
    let mut dongles: Vec<serde_json::Value> = engines.iter().map(|(mac, engine)| {
        // Unsorted: the dashboard sorts the sensors of every gateway together.
        let sensors: Vec<serde_json::Value> = manager.get_sensors().values()
            .filter(|s| s.dongle_mac.as_deref() == Some(mac.as_str()))
            .map(|s| sensor_info_to_json(s))
            .collect();
        let sensor_count = sensors.len();

        serde_json::json!({
            "mac": mac,
            "name": names.name_of(mac),
            "version": engine.dongle_version(),
            "scanning": engine.is_scanning(),
            "transport": engine.transport_label,
            "device_path": engine.device_path,
            "remote_addr": engine.remote_addr,
            "sensors": sensors,
            "sensor_count": sensor_count,
        })
    }).collect();
    dongles.sort_by_key(|d| {
        let mac = d["mac"].as_str().unwrap_or("");
        let label = d["name"].as_str().filter(|s| !s.is_empty()).unwrap_or(mac);
        (label.to_ascii_lowercase(), mac.to_string())
    });

    // Unassociated or offline-gateway sensors: dongle_mac is None or points to a gateway not in `engines`
    let unassociated: Vec<serde_json::Value> = manager.get_sensors().values()
        .filter(|s| s.dongle_mac.as_deref().is_none_or(|m| !engines.contains_key(m)))
        .map(|s| sensor_info_to_json(s))
        .collect();

    let dongle_names: std::collections::HashMap<String, String> = names
        .dongles
        .iter()
        .map(|(k, v)| (k.clone(), v.name.clone()))
        .collect();

    Json(json!({
        "dongles": dongles,
        "unassociated_sensors": unassociated,
        "dongle_names": dongle_names,
    }))
}

fn sensor_info_to_json(s: &crate::protocol::sensor::WyzeSensor) -> serde_json::Value {
    serde_json::json!({
        "mac": s.mac,
        "name": s.friendly_name,
        "sensor_type": s.sensor_type.as_str(),
        "sw_version": s.sw_version,
        "last_seen": s.last_seen,
        "battery": s.battery_pct,
        "battery_raw": s.battery_raw,
        "signal": s.rssi_dbm,
        "die_temperature_c": s.die_temperature_c,
        "event_sequence": s.event_sequence,
        "state": s.state,
        "dongle_mac": s.dongle_mac,
        // Offline = not heard from within its timeout (see WyzeSensor::is_online).
        // Matches `online` in the MQTT state payload; derived live, not persisted.
        "online": s.is_online,
    })
}

// --- GET /api/sensors ---
async fn list_sensors(
    State(state): State<Arc<WebState>>,
) -> impl IntoResponse {
    let manager = state.sensor_manager.lock().unwrap();
    let mut sensors: Vec<crate::config::state::PersistedSensorState> = manager
        .get_sensors()
        .values()
        .map(|sensor| crate::config::state::PersistedSensorState {
            mac: sensor.mac.clone(),
            sensor_type: sensor.sensor_type.as_str().to_string(),
            last_seen: sensor.last_seen,
            battery: sensor.battery_pct,
            battery_raw: sensor.battery_raw,
            signal: sensor.rssi_dbm,
            die_temperature_c: sensor.die_temperature_c,
            event_sequence: sensor.event_sequence,
            state: sensor.state.clone(),
            dongle_mac: sensor.dongle_mac.clone(),
        })
        .collect();
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
            battery_raw: sensor.battery_raw,
            signal: sensor.rssi_dbm,
            die_temperature_c: sensor.die_temperature_c,
            event_sequence: sensor.event_sequence,
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

// --- POST /api/sensors/:mac/rename ---
async fn rename_sensor(
    Path(mac): Path<String>,
    State(state): State<Arc<WebState>>,
    Json(req): Json<RenameRequest>,
) -> impl IntoResponse {
    let name = req.name.trim().to_string();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "name must not be empty" }))).into_response();
    }

    let result = {
        let mut manager = state.sensor_manager.lock().unwrap();
        manager.rename_sensor(&mac, &name)
    };

    match result {
        Ok(()) => {
            let _ = state.broadcast_tx.send(());
            (StatusCode::OK, Json(json!({ "success": true, "name": name }))).into_response()
        }
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({ "error": e }))).into_response(),
    }
}

// --- POST /api/dongles/:mac/rename ---
async fn rename_dongle(
    Path(mac): Path<String>,
    State(state): State<Arc<WebState>>,
    Json(req): Json<RenameRequest>,
) -> impl IntoResponse {
    let name = req.name.trim().to_string();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "name must not be empty" }))).into_response();
    }

    let result = {
        let mut names = state.dongle_names.lock().unwrap();
        names.set_name(&mac, &name);
        names.save_to_yaml_atomic(&state.dongle_config_path)
    };

    match result {
        Ok(()) => {
            let _ = state.broadcast_tx.send(());
            (StatusCode::OK, Json(json!({ "success": true, "name": name }))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("could not save dongle name: {}", e) })),
        )
            .into_response(),
    }
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

    // Match the pairing sequence in Engine's auto-verify worker:
    // 1. Exchange R1 token with the sensor
    if let Err(e) = engine.get_sensor_r1(&payload.mac).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    // 2. Disable scan mode before committing verification
    let _ = engine.set_scan(false).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 3. Commit sensor verification to NVRAM and emit Paired event
    match engine.verify_sensor(&payload.mac, sensor_type).await {
        Ok(_) => {
            let paired_evt = crate::protocol::telemetry::DongleEvent {
                mac: payload.mac.clone(),
                timestamp: std::time::SystemTime::now(),
                sensor_type,
                event_type: 0xFE,
                data: crate::protocol::telemetry::TelemetryData::Paired { version: 0 },
                dongle_mac: Some(payload.dongle_mac.clone()),
            };
            let _ = state.event_tx.send(paired_evt).await;
            let _ = state.broadcast_tx.send(());
            (
                StatusCode::OK,
                Json(SuccessResponse {
                    success: true,
                    message: format!("Sensor {} verified successfully", payload.mac),
                }),
            )
                .into_response()
        }
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
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: FixRequest = if body.iter().all(|b| b.is_ascii_whitespace()) {
        FixRequest::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": e.to_string() })),
                )
                    .into_response();
            }
        }
    };
    // Clone targeted engine (or all engines when dongle_mac is omitted) to avoid holding lock across await
    let mut engine_clones: Vec<Engine> = {
        let engines = state.engines.lock().await;
        match req.dongle_mac.as_deref() {
            Some(mac) if !mac.is_empty() => match engines.get(mac) {
                Some(e) => vec![e.clone()],
                None => {
                    return (
                        StatusCode::NOT_FOUND,
                        Json(json!({ "error": format!("Dongle {} not found", mac) })),
                    )
                        .into_response();
                }
            },
            _ => engines.values().cloned().collect(),
        }
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
            let expected_response = match pkt.cmd() {
                crate::protocol::packet::commands::CMD_GET_SENSOR_LIST
                | crate::protocol::packet::commands::CMD_SEND_KEYPAD => pkt.cmd(),
                cmd => cmd + 1,
            };
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
        .chevron { transition: transform .2s ease; display: inline-block; }
        .chevron.open { transform: rotate(90deg); }
        .detail-wrap { display: grid; grid-template-rows: 0fr; transition: grid-template-rows .25s ease; }
        .detail-wrap.open { grid-template-rows: 1fr; }
        .detail-inner { overflow: hidden; min-height: 0; }
        .dongle-detail { display: grid; grid-template-rows: 0fr; transition: grid-template-rows .25s ease; }
        .dongle-detail.open { grid-template-rows: 1fr; }
        .dongle-detail-inner { overflow: hidden; min-height: 0; }
        .sensor-row { transition: background-color .15s ease; }
        .sensor-row:hover, .sensor-row.open { background: rgba(30, 41, 59, .35); }
        .sensor-row.offline { background: rgba(127, 29, 29, .22); }
        .sensor-row.offline:hover, .sensor-row.offline.open { background: rgba(127, 29, 29, .32); }
        .sensor-card { transition: border-color .2s ease; }
        .sensor-card.offline { background: rgba(127, 29, 29, .12); }
        .sensor-card.open { border-color: rgba(45, 212, 191, .35); }
        .tag { display: inline-flex; align-items: center; gap: 4px; font-size: .65rem; padding: 2px 8px; border-radius: 9999px; font-weight: 600; white-space: nowrap; }
        .modal-backdrop { position: fixed; inset: 0; background: rgba(0, 0, 0, .6); backdrop-filter: blur(4px); z-index: 100; display: none; align-items: center; justify-content: center; }
        .modal-backdrop.open { display: flex; }
        .modal-panel { background: #0f172a; border: 1px solid #1e293b; border-radius: 1rem; width: 90%; max-width: 460px; max-height: 85vh; overflow-y: auto; box-shadow: 0 25px 50px rgba(0, 0, 0, .5); }
        /* Touch: keep controls at least 40px tall on phones. */
        @media (max-width: 639px) {
            button { min-height: 40px; }
            .modal-panel { width: calc(100% - 1.5rem); }
        }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col">
    <header class="border-b border-slate-800 bg-slate-900/50 backdrop-blur sticky top-0 z-50">
        <div class="max-w-5xl w-full mx-auto px-4 sm:px-6 py-3 flex items-center justify-between gap-2">
            <div class="flex items-center space-x-3 min-w-0">
                <span class="text-2xl">📡</span>
                <h1 class="text-lg sm:text-xl font-bold tracking-tight text-teal-400 truncate">Wyze Sense Bridge</h1>
                <span class="hidden sm:inline text-xs text-slate-600 font-mono">v"##, env!("CARGO_PKG_VERSION"), r##"</span>
            </div>
            <div class="flex items-center gap-3 shrink-0">
                <div class="flex items-center space-x-2 bg-slate-800 px-3 py-1.5 rounded-full text-xs font-semibold text-slate-400" id="header-badge">
                    <span class="w-2 h-2 rounded-full bg-slate-600" id="status-dot"></span>
                    <span id="status-text">Connecting...</span>
                </div>
            </div>
        </div>
    </header>

    <main class="flex-1 max-w-5xl w-full mx-auto px-4 sm:px-6 py-6 flex flex-col gap-5">
        <div class="flex items-center justify-between">
            <h2 class="text-lg font-bold text-teal-400 flex items-center"><span class="mr-2">🔋</span> Devices</h2>
            <button onclick="loadDongles()" class="text-xs text-teal-400 hover:underline">Refresh</button>
        </div>
        <div id="dongles-container" class="space-y-4">
            <div class="text-center text-sm text-slate-500 py-12">Loading devices...</div>
        </div>
    </main>

    <footer class="border-t border-slate-800 bg-slate-900/20 py-3">
        <div class="max-w-5xl w-full mx-auto px-4 sm:px-6 text-center text-xs text-slate-500">
            Wyze Sense to MQTT Bridge (Rust) — Multi-Dongle WebSocket Architecture
        </div>
    </footer>

    <div id="rename-modal" class="modal-backdrop" onclick="if(event.target===this)closeRename()">
        <div class="modal-panel p-5">
            <div class="flex items-center justify-between mb-1">
                <h3 id="rename-title" class="font-bold text-base text-teal-400">✏️ Rename</h3>
                <button onclick="closeRename()" class="text-slate-500 hover:text-slate-300 text-xl leading-none px-2">✕</button>
            </div>
            <div class="text-xs text-slate-500 font-mono mb-4" id="rename-subtitle"></div>
            <label class="block text-[10px] uppercase tracking-wide text-slate-600 mb-1">Name</label>
            <input id="rename-input" maxlength="64" autocomplete="off" class="w-full bg-slate-950 border border-slate-800 text-sm rounded-xl px-3 py-2.5 focus:outline-none focus:border-teal-500" onkeydown="if(event.key==='Enter'){event.preventDefault();saveRename()}else if(event.key==='Escape')closeRename()">
            <div class="mt-1 text-[11px] text-rose-400 hidden" id="rename-error">A name is required.</div>
            <div class="flex justify-end gap-2 mt-5">
                <button onclick="closeRename()" class="py-2 px-4 bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm font-semibold rounded-xl border border-slate-700">Cancel</button>
                <button onclick="saveRename()" class="py-2 px-4 bg-teal-600 hover:bg-teal-500 text-white text-sm font-semibold rounded-xl">Save</button>
            </div>
        </div>
    </div>

    <div id="pair-confirm-modal" class="modal-backdrop" onclick="if(event.target===this)closePairConfirm()">
        <div class="modal-panel p-5">
            <div class="flex items-center justify-between mb-1">
                <h3 class="font-bold text-base text-teal-400">✅ Sensor paired</h3>
                <button onclick="closePairConfirm()" class="text-slate-500 hover:text-slate-300 text-xl leading-none px-2">✕</button>
            </div>
            <p class="text-xs text-slate-500 mb-4">The following sensor was just paired. Check the details before you put it to use.</p>
            <div id="pair-confirm-list" class="flex flex-col gap-2"></div>
            <div class="flex justify-end gap-2 mt-5">
                <button onclick="closePairConfirm()" class="py-2 px-4 bg-teal-600 hover:bg-teal-500 text-white text-sm font-semibold rounded-xl">Done</button>
            </div>
        </div>
    </div>

    <script>
        const API = "";
        let donglesData = { dongles: [], unassociated_sensors: [] };
        let scanState = {};
        let openSensors = new Set();
        let openDongles = new Set();
        let gatewaysFolderOpen = false;
        let sensorsFolderOpen = true;
        let hexLogs = {};
        let renameTarget = null;
        let pendingRender = false;
        let sensorSort = { key: "name", dir: "asc" };
        const SORT_COLS = [
            { key: "name", label: "Sensor" },
            { key: "gateway", label: "Gateway" },
            { key: "type", label: "Type" },
            { key: "state", label: "State" },
            { key: "battery", label: "Battery" },
            { key: "signal", label: "Signal" },
            { key: "seen", label: "Seen" },
        ];

        // ===== Helpers =====
        const esc = (t) => String(t ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

        function seenAge(ts) {
            if (!ts) return null;
            const d = Math.floor(Date.now() / 1000) - ts;
            return d < 0 ? 0 : d;
        }
        function relSeen(ts) {
            const d = seenAge(ts);
            if (d == null) return "Never";
            if (d < 60) return d < 5 ? "Just now" : `${d}s ago`;
            if (d < 3600) return `${Math.floor(d / 60)}m ago`;
            if (d < 86400) return `${Math.floor(d / 3600)}h ago`;
            return `${Math.floor(d / 86400)}d ago`;
        }
        function absSeen(ts) {
            if (!ts) return "Never";
            return new Date(ts * 1000).toLocaleString();
        }

        function dongleByMac(mac) {
            if (!mac) return null;
            const m = String(mac).toUpperCase();
            return (donglesData.dongles || []).find(d => String(d.mac || "").toUpperCase() === m) || null;
        }
        function allSensors() {
            return [...(donglesData.dongles || []).flatMap(d => d.sensors || []), ...(donglesData.unassociated_sensors || [])];
        }
        function sensorByMac(mac) { return allSensors().find(s => s.mac === mac) || null; }
        function dongleName(mac) {
            if (!mac) return "";
            const d = dongleByMac(mac);
            if (d?.name) return d.name;
            const cfgName = donglesData.dongle_names?.[String(mac).toUpperCase()];
            return cfgName || mac;
        }
        function sensorName(mac) { const s = sensorByMac(mac); return s?.name || mac; }

        function typeLabel(t) {
            return {
                switch: "Contact v1", switchv2: "Contact v2",
                motion: "Motion v1", motionv2: "Motion v2",
                leak: "Leak v2", climate: "Climate v2", chime: "Chime v1",
                keypad: "Keypad",
            }[t] || (t ? String(t) : "Unknown");
        }
        function typeTag(s) {
            const t = (s.sensor_type || "").toLowerCase();
            if (t.includes("contact") || t.includes("switch")) return `<span class="tag border border-cyan-900 bg-cyan-950/20 text-cyan-400">🚪 Contact</span>`;
            if (t.includes("motion")) return `<span class="tag border border-purple-900 bg-purple-950/20 text-purple-400">🏃 Motion</span>`;
            if (t.includes("climate")) return `<span class="tag border border-sky-900 bg-sky-950/20 text-sky-400">🌡️ Climate</span>`;
            if (t.includes("leak")) return `<span class="tag border border-blue-900 bg-blue-950/20 text-blue-400">💧 Leak</span>`;
            if (t.includes("keypad")) return `<span class="tag border border-fuchsia-900 bg-fuchsia-950/20 text-fuchsia-400">⌨️ Keypad</span>`;
            return `<span class="tag border border-slate-800 bg-slate-900 text-slate-300">${esc(s.sensor_type || "unknown")}</span>`;
        }
        // Offline = the gateway has not heard from this sensor within its timeout
        // (online === false in /api/dongles). Missing data does not mark it offline.
        function isOffline(s) { return s.online === false; }
        function offlineReason(s) {
            const d = seenAge(s.last_seen);
            const span = d == null ? null
                : d < 60 ? `${d}s`
                : d < 3600 ? `${Math.floor(d / 60)}m`
                : d < 86400 ? `${Math.floor(d / 3600)}h`
                : `${Math.floor(d / 86400)}d`;
            return span
                ? `No report in ${span}; check its battery or whether it's in range`
                : "Never heard from; check its battery or whether it's in range";
        }
        function offlineTag(s) {
            return isOffline(s)
                ? `<span class="tag border border-rose-900 bg-rose-950/40 text-rose-400" title="${esc(offlineReason(s))}">Offline</span>`
                : "";
        }
        function stateTag(s) {
            const st = s.state;
            if (!st) return `<span class="text-slate-500 italic text-xs">—</span>`;
            switch (st.kind) {
                case "Contact": return st.is_open ? `<span class="text-rose-400 font-bold text-xs">Open</span>` : `<span class="text-emerald-400 font-bold text-xs">Closed</span>`;
                case "Motion": return st.is_active ? `<span class="text-rose-400 font-bold text-xs">Active</span>` : `<span class="text-emerald-400 font-bold text-xs">Clear</span>`;
                case "Leak": return st.is_wet ? `<span class="text-blue-400 font-bold text-xs">Wet</span>` : `<span class="text-emerald-400 font-bold text-xs">Dry</span>`;
                case "Keypad": return st.motion_active ? `<span class="text-rose-400 font-bold text-xs">Motion</span>` : `<span class="text-emerald-400 font-bold text-xs">Clear</span>`;
                case "Climate": {
                    const t = Number(st.temperature);
                    return `<span class="text-cyan-400 font-mono text-xs">${isFinite(t) ? t.toFixed(1) : "—"}°C / ${esc(st.humidity ?? "—")}%</span>`;
                }
            }
            return `<span class="text-slate-500 italic text-xs">—</span>`;
        }
        function sigText(rssi) {
            let cls = "text-slate-400", label = "Fair";
            if (rssi > -50) { cls = "text-teal-400 font-semibold"; label = "Excellent"; }
            else if (rssi > -65) { cls = "text-emerald-400"; label = "Good"; }
            else if (rssi < -80) { cls = "text-rose-400 font-semibold"; label = "Weak"; }
            return {cls, label};
        }
        function signalBars(rssi) {
            const lvl = rssi > -50 ? 4 : rssi > -65 ? 3 : rssi > -80 ? 2 : 1;
            return `<svg viewBox="0 0 11 8" class="w-4 h-3 shrink-0" aria-hidden="true">${[2,4,6,8].map((h,i) => `<rect x="${i*3}" y="${8-h}" width="2" height="${h}" rx="1" fill="currentColor" opacity="${i < lvl ? 1 : .22}"/>`).join('')}</svg>`;
        }
        function signalInline(rssi) {
            const s = sigText(rssi);
            return `<span class="inline-flex items-center gap-1 font-mono ${s.cls}">${signalBars(rssi)}<span>${rssi} dBm</span></span>`;
        }
        function batteryIcon(pct) {
            const w = pct == null ? 0 : Math.max(pct > 0 ? 1 : 0, Math.round(pct / 100 * 11));
            return `<svg viewBox="0 0 18 10" class="w-[17px] h-[10px] shrink-0" aria-hidden="true">
                <rect x=".5" y=".5" width="14" height="9" rx="2" fill="none" stroke="currentColor"/>
                <rect x="16" y="3" width="1.6" height="4" rx=".6" fill="currentColor"/>
                <rect x="2" y="2" width="${w}" height="6" rx="1" fill="currentColor"/>
            </svg>`;
        }
        function batteryTag(pct) {
            if (pct == null) return `<span class="tag border border-slate-800 text-slate-500">${batteryIcon(null)}N/A</span>`;
            let c = "text-emerald-400 bg-emerald-950/30 border-emerald-900/30";
            if (pct < 20) c = "text-rose-400 bg-rose-950/30 border-rose-900/30";
            else if (pct < 50) c = "text-amber-400 bg-amber-950/30 border-amber-900/30";
            return `<span class="tag border ${c}">${batteryIcon(pct)}${pct}%</span>`;
        }
        function detailCell(label, value, sub) {
            return `<div class="min-w-0">
                <div class="text-[10px] uppercase tracking-wide text-slate-600">${label}</div>
                <div class="mt-0.5 text-xs text-slate-300 break-words">${value}${sub ? `<span class="text-slate-600"> · ${sub}</span>` : ''}</div>
            </div>`;
        }

        // ===== Sensor detail =====
        function detailCard(s, d) {
            const gatewayMac = d?.mac || s.dongle_mac || null;
            const gatewayLabel = gatewayMac ? (d?.name || dongleName(gatewayMac)) : null;
            const gatewayNamed = gatewayMac && gatewayLabel && gatewayLabel !== gatewayMac;
            const gatewayValue = gatewayLabel
                ? `<span class="${gatewayNamed ? 'font-semibold text-slate-200' : 'font-mono'}">${esc(gatewayLabel)}</span>`
                : "—";
            const gatewaySub = gatewayNamed ? `<span class="font-mono">${esc(gatewayMac)}</span>` : null;
            const volts = s.battery_raw != null ? (s.battery_raw / 32).toFixed(2) + " V" : null;
            const emac = esc(s.mac);
            return `<div class="rounded-xl border border-slate-800 bg-slate-950/70 p-3 sm:p-4">
                <div class="flex items-center justify-between gap-2 mb-3">
                    <div class="min-w-0">
                        <div class="font-semibold text-sm text-slate-200 truncate">${esc(s.name || s.mac)}</div>
                        <div class="font-mono text-[11px] text-teal-500/80">${emac}</div>
                    </div>
                    <button onclick="openRename('${emac}')" class="shrink-0 inline-flex items-center gap-1.5 py-1.5 px-3 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">✏️ Rename</button>
                </div>
                <div class="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-3">
                    ${detailCell("Sensor type", esc(typeLabel(s.sensor_type)))}
                    ${detailCell("Firmware", esc(s.sw_version || "Unknown"))}
                    ${detailCell("MAC", `<span class="font-mono">${emac}</span>`)}
                    ${detailCell("Gateway", gatewayValue, gatewaySub)}
                    ${detailCell("Status", isOffline(s) ? `<span class="text-rose-400 font-bold text-xs" title="${esc(offlineReason(s))}">● Offline</span>` : `<span class="text-emerald-400 font-bold text-xs">● Online</span>`)}
                    ${detailCell("Last seen", esc(absSeen(s.last_seen)), esc(relSeen(s.last_seen)))}
                    ${detailCell("Signal", signalInline(s.signal), sigText(s.signal).label)}
                    ${detailCell("Battery", s.battery == null ? `${batteryIcon(null)} External power` : batteryTag(s.battery), volts)}
                    ${detailCell("Die temperature", s.die_temperature_c == null ? "—" : `${esc(s.die_temperature_c)} °C`)}
                    ${detailCell("Event sequence", s.event_sequence == null ? "—" : `#${esc(s.event_sequence)}`)}
                    ${detailCell("State", stateTag(s))}
                </div>
            </div>`;
        }

        function gatewayLabel(s, d) {
            const mac = d?.mac || s.dongle_mac || null;
            if (!d && !mac) return "Unassociated";
            return d?.name || dongleName(mac);
        }
        function gatewayTag(s, d) {
            const mac = d?.mac || s.dongle_mac || null;
            if (!d && !mac) {
                return `<span class="tag border border-slate-700 bg-slate-900/60 text-slate-400">📦 Unassociated</span>`;
            }
            const label = gatewayLabel(s, d);
            if (!d) {
                return `<span class="tag border border-slate-700 bg-slate-900/60 text-slate-400" title="${esc(mac)}">📦 ${esc(label)}</span>`;
            }
            const title = label !== mac ? `${label} (${mac})` : mac;
            return `<span class="tag border border-slate-800 bg-slate-950/60 text-slate-300" title="${esc(title)}">🕹️ ${esc(label)}</span>`;
        }

        // ===== Sensor sorting =====
        function sensorSortName(s) {
            return String(s.name || s.mac || "").toLowerCase();
        }
        function sensorStateRank(s) {
            const st = s.state;
            if (!st) return 3;
            switch (st.kind) {
                case "Contact": return st.is_open ? 0 : 1;
                case "Motion": return st.is_active ? 0 : 1;
                case "Leak": return st.is_wet ? 0 : 1;
                case "Keypad": return st.motion_active ? 0 : 1;
                case "Climate": return 2;
            }
            return 3;
        }
        function decorateSensor(s, d) {
            const temp = Number(s.state?.temperature);
            return {
                s,
                d,
                nameKey: sensorSortName(s),
                gwKey: gatewayLabel(s, d).toLowerCase(),
                typeKey: typeLabel(s.sensor_type).toLowerCase(),
                stateRank: sensorStateRank(s),
                temp: Number.isFinite(temp) ? temp : null,
                battery: s.battery,
                signal: s.signal,
                seen: seenAge(s.last_seen),
            };
        }
        function compareNullableNumber(va, vb, dir, byName) {
            if (va == null && vb == null) return dir * byName();
            if (va == null) return 1;
            if (vb == null) return -1;
            return dir * ((va - vb) || byName());
        }
        function compareSensors(a, b) {
            const dir = sensorSort.dir === "desc" ? -1 : 1;
            const byName = () => a.nameKey.localeCompare(b.nameKey);
            switch (sensorSort.key) {
                case "name":
                    return dir * byName();
                case "gateway":
                    return dir * (a.gwKey.localeCompare(b.gwKey) || byName());
                case "type":
                    return dir * (a.typeKey.localeCompare(b.typeKey) || byName());
                case "state": {
                    if (a.stateRank !== b.stateRank) return dir * (a.stateRank - b.stateRank);
                    if (a.stateRank === 2) return compareNullableNumber(a.temp, b.temp, dir, byName);
                    return dir * byName();
                }
                case "battery":
                    return compareNullableNumber(a.battery, b.battery, dir, byName);
                case "signal":
                    return compareNullableNumber(a.signal, b.signal, dir, byName);
                case "seen":
                    return compareNullableNumber(a.seen, b.seen, dir, byName);
            }
            return 0;
        }
        function clickSensorSort(key) {
            if (sensorSort.key === key) {
                sensorSort.dir = sensorSort.dir === "asc" ? "desc" : "asc";
            } else {
                sensorSort.key = key;
                sensorSort.dir = "asc";
            }
            render();
        }
        function changeSensorSortKey(key) {
            sensorSort.key = key;
            render();
        }
        function toggleSensorSortDir() {
            sensorSort.dir = sensorSort.dir === "asc" ? "desc" : "asc";
            render();
        }

        // ===== Sensor rows / cards =====
        function desktopRow(s, d) {
            const open = openSensors.has(s.mac);
            const emac = esc(s.mac);
            return `<tr data-sensor-row="${emac}" class="sensor-row cursor-pointer text-xs border-t border-slate-800/40${open ? ' open' : ''}${isOffline(s) ? ' offline' : ''}" onclick="toggleSensor('${emac}')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();toggleSensor('${emac}')}" tabindex="0">
                    <td class="py-2.5 pl-3 pr-1 w-6"><span data-sensor-chev="${emac}" class="chevron text-slate-500 text-[10px]${open ? ' open' : ''}">▶</span></td>
                    <td class="py-2.5 px-2">
                        <div class="font-semibold text-slate-200 truncate max-w-[11rem]">${esc(s.name || s.mac)}</div>
                        <div class="font-mono text-[11px] text-teal-500/80">${emac}</div>
                        ${isOffline(s) ? `<div class="mt-1">${offlineTag(s)}</div>` : ""}
                    </td>
                    <td class="py-2.5 px-2">${gatewayTag(s, d)}</td>
                    <td class="py-2.5 px-2">${typeTag(s)}</td>
                    <td class="py-2.5 px-2">${stateTag(s)}</td>
                    <td class="py-2.5 px-2">${batteryTag(s.battery)}</td>
                    <td class="py-2.5 px-2">${signalInline(s.signal)}</td>
                    <td class="py-2.5 px-2 text-slate-400">${relSeen(s.last_seen)}</td>
                </tr>
                <tr data-sensor-detail-tr="${emac}" class="${open ? '' : 'hidden'}">
                    <td colspan="8" class="p-0">
                        <div data-sensor-wrap="${emac}" class="detail-wrap${open ? ' open' : ''}"><div class="detail-inner"><div class="px-3 pb-3 pt-0">${detailCard(s, d)}</div></div></div>
                    </td>
                </tr>`;
        }

        function mobileCard(s, d) {
            const open = openSensors.has(s.mac);
            const emac = esc(s.mac);
            return `<div data-sensor-row="${emac}" class="sensor-card border-b border-slate-800/50 last:border-b-0${open ? ' open' : ''}${isOffline(s) ? ' offline' : ''}">
                <button class="w-full text-left px-4 py-3 flex items-start gap-3" onclick="toggleSensor('${emac}')">
                    <span data-sensor-chev="${emac}" class="chevron ${open ? 'open ' : ''}mt-1 text-slate-500 text-[10px]">▶</span>
                    <div class="min-w-0 flex-1">
                        <div class="flex items-start justify-between gap-2">
                            <div class="min-w-0">
                                <div class="font-semibold text-sm text-slate-100 truncate">${esc(s.name || s.mac)}</div>
                                <div class="font-mono text-[11px] text-teal-500/80 truncate">${emac}</div>
                            </div>
                            <div class="shrink-0">${stateTag(s)}</div>
                        </div>
                        <div class="mt-2 flex flex-wrap items-center gap-1.5">${typeTag(s)}${gatewayTag(s, d)}${batteryTag(s.battery)}${offlineTag(s)}</div>
                        <div class="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-slate-500">
                            ${signalInline(s.signal)}
                            <span>Seen ${relSeen(s.last_seen)}</span>
                        </div>
                    </div>
                </button>
                <div data-sensor-wrap="${emac}" class="detail-wrap${open ? ' open' : ''}"><div class="detail-inner"><div class="px-4 pb-3">${detailCard(s, d)}</div></div></div>
            </div>`;
        }

        function sensorsCard(dongles, orphans) {
            const items = [
                ...(dongles || []).flatMap(d => (d.sensors || []).map(s => decorateSensor(s, d))),
                ...(orphans || []).map(s => decorateSensor(s, null)),
            ].sort(compareSensors);
            const ths = SORT_COLS.map(c => {
                const active = sensorSort.key === c.key;
                const arrow = active ? (sensorSort.dir === "asc" ? " ▲" : " ▼") : "";
                return `<th onclick="clickSensorSort('${c.key}')" class="py-1.5 px-2 cursor-pointer select-none hover:text-slate-300 transition ${active ? 'text-teal-400 font-bold' : ''}">${c.label}${arrow}</th>`;
            }).join("");
            const sortControls = `<div class="flex items-center gap-1.5 text-xs" onclick="event.stopPropagation()" onkeydown="event.stopPropagation()">
                <span class="text-slate-500">Sort:</span>
                <select onchange="changeSensorSortKey(this.value)" class="bg-slate-950 border border-slate-800 text-slate-200 text-xs font-semibold rounded-lg px-2.5 py-1 focus:outline-none focus:border-teal-500">
                    ${SORT_COLS.map(c => `<option value="${c.key}" ${sensorSort.key === c.key ? 'selected' : ''}>${c.label}</option>`).join("")}
                </select>
                <button onclick="toggleSensorSortDir()" class="py-1 px-2.5 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-semibold rounded-lg border border-slate-700 transition" title="Toggle sort direction">${sensorSort.dir === "asc" ? "▲ Asc" : "▼ Desc"}</button>
            </div>`;
            const offlineCount = items.filter(({s}) => isOffline(s)).length;
            const body = !items.length
                ? `<div class="text-center text-xs text-slate-500 py-6 italic">No sensors</div>`
                : `<table class="hidden sm:table w-full text-left text-slate-300">
                    <thead class="text-slate-500 text-[11px] uppercase">
                        <tr>
                            <th class="py-1.5 pl-3 pr-1"></th>
                            ${ths}
                        </tr>
                    </thead>
                    <tbody>${items.map(({s, d}) => desktopRow(s, d)).join("")}</tbody>
                </table>
                <div class="sm:hidden">${items.map(({s, d}) => mobileCard(s, d)).join("")}</div>`;
            return `<div class="bg-slate-900 rounded-2xl border border-slate-800 shadow-xl overflow-hidden">
                <div class="px-4 sm:px-5 py-3 flex flex-wrap items-center justify-between gap-2 cursor-pointer hover:bg-slate-800/40 transition" onclick="toggleSensorsFolder()" onkeydown="if(event.target===event.currentTarget&&(event.key==='Enter'||event.key===' ')){event.preventDefault();toggleSensorsFolder()}" tabindex="0">
                    <div class="flex items-center gap-2.5">
                        <span id="sensors-folder-chev" class="chevron text-slate-500 text-[10px] shrink-0${sensorsFolderOpen ? ' open' : ''}">▶</span>
                        <span class="font-bold text-sm text-slate-200">📡 Sensors</span>
                        <span class="tag border border-slate-800 text-slate-400">${items.length}</span>
                        ${offlineCount ? `<span class="tag border border-rose-900 bg-rose-950/40 text-rose-400">${offlineCount} offline</span>` : ""}
                    </div>
                    ${sortControls}
                </div>
                <div id="sensors-folder-body" class="dongle-detail${sensorsFolderOpen ? ' open' : ''}">
                    <div class="dongle-detail-inner">
                        <div class="border-t border-slate-800/60 px-2 py-1">${body}</div>
                    </div>
                </div>
            </div>`;
        }

        function toggleSensorsFolder() {
            sensorsFolderOpen = !sensorsFolderOpen;
            document.getElementById("sensors-folder-body")?.classList.toggle("open", sensorsFolderOpen);
            document.getElementById("sensors-folder-chev")?.classList.toggle("open", sensorsFolderOpen);
        }

        function toggleSensor(mac) {
            const open = !openSensors.has(mac);
            if (open) openSensors.add(mac); else openSensors.delete(mac);
            const sel = CSS.escape(mac);
            document.querySelectorAll(`[data-sensor-wrap="${sel}"]`).forEach(el => el.classList.toggle('open', open));
            document.querySelectorAll(`[data-sensor-chev="${sel}"]`).forEach(el => el.classList.toggle('open', open));
            document.querySelectorAll(`[data-sensor-row="${sel}"]`).forEach(el => el.classList.toggle('open', open));
            document.querySelectorAll(`[data-sensor-detail-tr="${sel}"]`).forEach(el => el.classList.toggle('hidden', !open));
        }

        // ===== Dongle =====
        function dongleDetails(d) {
            const conn = d.transport === "bridge" ? (d.remote_addr || "—") : (d.device_path || "—");
            const emac = esc(d.mac);
            return `<div class="border-t border-slate-800/50 bg-slate-950/40 px-4 sm:px-5 py-4">
                <div class="flex items-start justify-between gap-2 mb-3">
                    <div class="min-w-0">
                        <div class="text-[10px] uppercase tracking-wide text-slate-600 mb-1">Dongle</div>
                        <div class="font-semibold text-sm text-slate-200 break-all">${esc(d.name || ("Dongle " + d.mac))}</div>
                        <div class="font-mono text-[11px] text-teal-500/80">${emac}</div>
                    </div>
                    <button onclick="openDongleRename('${emac}')" class="shrink-0 inline-flex items-center gap-1.5 py-1.5 px-3 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">✏️ Rename</button>
                </div>
                <div class="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-3">
                    ${detailCell("Transport", d.transport === "bridge" ? "🌐 Bridge" : "🔌 Local")}
                    ${detailCell("Connection", `<span class="font-mono break-all">${esc(conn)}</span>`)}
                    ${detailCell("Firmware", esc(d.version || "Unknown"))}
                </div>
                ${dongleActions(d)}
            </div>`;
        }



        function dongleActions(d) {
            const ss = scanState[d.mac] || {active: false};
            const scanning = !!ss.active;
            const emac = esc(d.mac);
            const pairBtn = `<button data-pair-btn="${emac}" onclick="toggleScan('${emac}')" class="w-full py-2.5 ${scanning ? 'bg-rose-700 hover:bg-rose-600' : 'bg-teal-600 hover:bg-teal-500'} text-white text-xs font-semibold rounded-lg transition">${scanning ? `⏹ Stop scan${ss.secondsLeft != null ? ` (${ss.secondsLeft}s)` : ""}` : "📡 Pair new sensor"}</button>`;
            return `<div class="mt-4 border-t border-slate-800/60 pt-3">
                <div class="text-[10px] uppercase tracking-wide text-slate-600 mb-2">⚙️ Actions</div>
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-3">
                    <div class="rounded-xl border border-slate-800 bg-slate-900/40 p-3">
                        <div class="text-xs font-semibold text-slate-300 mb-1">📡 Pairing center</div>
                        <p class="text-[11px] text-slate-500 mb-2">Put the sensor in pairing mode, then start scanning. Scan auto-stops after 60 seconds.</p>
                        ${pairBtn}
                    </div>
                    <div class="rounded-xl border border-slate-800 bg-slate-900/40 p-3">
                        <div class="text-xs font-semibold text-slate-300 mb-1">🧹 Maintenance</div>
                        <p class="text-[11px] text-slate-500 mb-2">Remove invalid or ghost sensor entries from the dongle's NVRAM.</p>
                        <button onclick="runFix('${emac}', this)" class="w-full py-2.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">Purge ghost sensors</button>
                    </div>
                    <div class="rounded-xl border border-slate-800 bg-slate-900/40 p-3 lg:col-span-2">
                        <div class="text-xs font-semibold text-slate-300 mb-1">💻 Hex console</div>
                        <p class="text-[11px] text-slate-500 mb-2">Send a raw packet and see the dongle's reply.</p>
                        <div class="flex gap-2">
                            <input data-hex-input="${emac}" placeholder="AA,55,43..." class="flex-1 min-w-0 bg-slate-950 border border-slate-800 text-xs rounded-lg px-3 py-2 font-mono focus:outline-none focus:border-teal-500">
                            <button onclick="sendRaw('${emac}')" class="shrink-0 py-2 px-4 bg-slate-800 hover:bg-slate-700 font-semibold text-xs rounded-lg border border-slate-700 transition">Send</button>
                        </div>
                        <div data-hex-log="${emac}" class="rounded-lg border border-slate-800 bg-slate-950 p-2 mt-2 font-mono text-[11px] text-emerald-400 min-h-[44px] max-h-[120px] overflow-y-auto overflow-x-hidden break-all space-y-0.5">${hexLogHtml(d.mac)}</div>
                    </div>
                </div>
            </div>`;
        }

        function hexLogHtml(mac) {
            const lines = hexLogs[mac] || [];
            if (!lines.length) return `<div class="text-slate-500 italic">[Console ready]</div>`;
            return lines.map(l => `<div class="${l.cls || ''}">${esc(l.text)}</div>`).join('');
        }
        function pushHex(mac, text, cls) {
            if (!hexLogs[mac]) hexLogs[mac] = [];
            hexLogs[mac].push({text, cls: cls || ''});
            if (hexLogs[mac].length > 200) hexLogs[mac].shift();
            const el = document.querySelector(`[data-hex-log="${CSS.escape(mac)}"]`);
            if (el) { el.innerHTML = hexLogHtml(mac); el.scrollTop = el.scrollHeight; }
        }

        function dongleCard(d) {
            const n = (d.sensors || []).length;
            const open = openDongles.has(d.mac);
            const emac = esc(d.mac);
            const dot = `<span class="w-2.5 h-2.5 rounded-full shrink-0 ${d.scanning ? 'bg-amber-400 animate-pulse' : 'bg-teal-400'}"></span>`;
            const tr = d.transport === "bridge"
                ? `<span class="tag border border-violet-900 bg-violet-950/20 text-violet-400">🌐 Bridge</span><span class="text-xs text-slate-500 font-mono ml-1">${esc(d.remote_addr || "?")}</span>`
                : `<span class="tag border border-teal-900 bg-teal-950/20 text-teal-400">🔌 Local</span><span class="text-xs text-slate-500 font-mono ml-1">${esc(d.device_path || "?")}</span>`;
            return `<div class="border-t border-slate-800/60 first:border-t-0">
                <div class="px-4 sm:px-5 py-3.5 flex items-center gap-3 cursor-pointer hover:bg-slate-800/40 transition" onclick="toggleDongleDetails('${emac}')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();toggleDongleDetails('${emac}')}" tabindex="0">
                    <span data-dongle-chev="${emac}" class="chevron text-slate-500 text-[10px] shrink-0${open ? ' open' : ''}">▶</span>
                    ${dot}
                    <div class="min-w-0">
                        <div class="flex items-center gap-2 flex-wrap">
                            <span class="font-bold text-sm break-all">🕹️ ${esc(d.name || ("Dongle " + d.mac))}</span>
                            ${d.scanning ? '<span class="tag border border-amber-900 bg-amber-950/20 text-amber-400">SCANNING</span>' : ''}
                            <span class="tag border border-slate-800 text-slate-400">${n} sensor${n !== 1 ? 's' : ''}</span>
                        </div>
                        <div class="flex items-center gap-2 mt-0.5 flex-wrap">
                            <span class="text-xs text-slate-500 font-mono">${emac}</span>
                            <span class="text-slate-700">·</span>
                            <span class="text-xs text-slate-500 font-mono">${esc(d.version || 'Unknown firmware')}</span>
                            <span class="text-slate-700">·</span> ${tr}
                        </div>
                    </div>
                </div>
                <div data-dongle-detail="${emac}" class="dongle-detail${open ? ' open' : ''}"><div class="dongle-detail-inner">${dongleDetails(d)}</div></div>
            </div>`;
        }

        function gatewaysCard(dongles) {
            const list = dongles || [];
            if (!list.length) return '';
            const anyScanning = list.some(d => d.scanning);
            const names = list.map(d => esc(d.name || d.mac)).join(", ");
            return `<div class="bg-slate-900 rounded-2xl border border-slate-800 shadow-xl overflow-hidden">
                <div class="px-4 sm:px-5 py-3 flex items-center justify-between gap-2 cursor-pointer hover:bg-slate-800/40 transition" onclick="toggleGatewaysFolder()" onkeydown="if(event.target===event.currentTarget&&(event.key==='Enter'||event.key===' ')){event.preventDefault();toggleGatewaysFolder()}" tabindex="0">
                    <div class="flex items-center gap-2.5 shrink-0">
                        <span id="gateways-folder-chev" class="chevron text-slate-500 text-[10px] shrink-0${gatewaysFolderOpen ? ' open' : ''}">▶</span>
                        <span class="font-bold text-sm text-slate-200">🕹️ Gateways</span>
                        <span class="tag border border-slate-800 text-slate-400">${list.length}</span>
                        ${anyScanning ? '<span class="tag border border-amber-900 bg-amber-950/20 text-amber-400">SCANNING</span>' : ''}
                    </div>
                    <div class="text-xs text-slate-500 font-mono truncate min-w-0">${names}</div>
                </div>
                <div id="gateways-folder-body" class="dongle-detail${gatewaysFolderOpen ? ' open' : ''}">
                    <div class="dongle-detail-inner">
                        <div class="border-t border-slate-800/60">${list.map(dongleCard).join("")}</div>
                    </div>
                </div>
            </div>`;
        }

        function toggleGatewaysFolder() {
            gatewaysFolderOpen = !gatewaysFolderOpen;
            document.getElementById("gateways-folder-body")?.classList.toggle("open", gatewaysFolderOpen);
            document.getElementById("gateways-folder-chev")?.classList.toggle("open", gatewaysFolderOpen);
        }

        function toggleDongleDetails(mac) {
            const open = !openDongles.has(mac);
            if (open) openDongles.add(mac); else openDongles.delete(mac);
            const sel = CSS.escape(mac);
            document.querySelectorAll(`[data-dongle-detail="${sel}"]`).forEach(el => el.classList.toggle('open', open));
            document.querySelectorAll(`[data-dongle-chev="${sel}"]`).forEach(el => el.classList.toggle('open', open));
        }

        // ===== Rename =====
        async function postJson(path, body) {
            const r = await fetch(`${API}${path}`, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body || {})});
            const data = await r.json().catch(() => ({}));
            return {ok: r.ok, status: r.status, data};
        }

        function openRename(mac) {
            const s = sensorByMac(mac);
            if (!s) return;
            renameTarget = {kind: "sensor", key: mac};
            document.getElementById("rename-title").textContent = "✏️ Rename sensor";
            document.getElementById("rename-subtitle").textContent = mac;
            showRename(s.name || "");
        }
        function openDongleRename(mac) {
            const d = dongleByMac(mac);
            if (!d) return;
            renameTarget = {kind: "dongle", key: mac};
            document.getElementById("rename-title").textContent = "✏️ Rename dongle";
            document.getElementById("rename-subtitle").textContent = `${mac} · ${d.transport === "bridge" ? "Bridge" : "Local"}`;
            showRename(d.name || "");
        }
        function showRename(value) {
            document.getElementById("rename-error").classList.add("hidden");
            document.getElementById("rename-input").value = value;
            document.getElementById("rename-modal").classList.add("open");
            setTimeout(() => { const i = document.getElementById("rename-input"); i.focus(); i.select(); }, 0);
        }
        function closeRename() {
            document.getElementById("rename-modal").classList.remove("open");
            renameTarget = null;
        }
        async function saveRename() {
            if (!renameTarget) return;
            const input = document.getElementById("rename-input");
            const name = input.value.trim();
            if (!name) {
                document.getElementById("rename-error").classList.remove("hidden");
                input.focus();
                return;
            }
            const path = renameTarget.kind === "sensor"
                ? `/api/sensors/${encodeURIComponent(renameTarget.key)}/rename`
                : `/api/dongles/${encodeURIComponent(renameTarget.key)}/rename`;
            const {ok, data} = await postJson(path, {name});
            if (!ok) {
                toast(data.error || `Rename failed (${data.status || "error"})`, "error");
                return;
            }
            closeRename();
            await loadDongles();
            toast(`Renamed to "${name}"`, "ok");
        }

        // ===== Scan =====
        function clearScanTimers(mac) {
            const s = scanState[mac];
            if (!s) return;
            clearInterval(s.timer);
            clearInterval(s.poll);
        }
        function updatePairButton(mac) {
            const s = scanState[mac] || {active: false};
            document.querySelectorAll(`[data-pair-btn="${CSS.escape(mac)}"]`).forEach(btn => {
                btn.textContent = s.active ? `⏹ Stop scan${s.secondsLeft != null ? ` (${s.secondsLeft}s)` : ""}` : "📡 Pair new sensor";
                btn.classList.toggle("bg-rose-700", !!s.active);
                btn.classList.toggle("hover:bg-rose-600", !!s.active);
                btn.classList.toggle("bg-teal-600", !s.active);
                btn.classList.toggle("hover:bg-teal-500", !s.active);
            });
        }
        async function toggleScan(mac) {
            const ss = scanState[mac] || {active: false};
            try {
                const {ok, data} = await postJson("/api/scan", {enable: !ss.active, dongle_mac: mac});
                if (!ok) { toast(data.error || "Scan toggle failed", "error"); return; }
                if (data.scan_active) {
                    clearScanTimers(mac);
                    const beforeMacs = new Set(allSensors().map(s => s.mac));
                    scanState[mac] = {active: true, secondsLeft: 60, beforeMacs};
                    scanState[mac].timer = setInterval(() => {
                        const s = scanState[mac];
                        if (!s) return;
                        s.secondsLeft--;
                        if (s.secondsLeft <= 0) forceStopScan(mac); else updatePairButton(mac);
                    }, 1000);
                    scanState[mac].poll = setInterval(async () => {
                        try {
                            const r = await fetch(`${API}/api/scan`);
                            const d = await r.json();
                            if (!d.scan_active) {
                                await new Promise(res => setTimeout(res, 300));
                                await loadDongles();
                            }
                        } catch (e) {}
                    }, 1500);
                    toast(`Scanning on ${dongleName(mac)} (60s)`, "ok");
                } else {
                    clearScanTimers(mac);
                    scanState[mac] = {active: false};
                }
                updatePairButton(mac);
                await loadDongles();
            } catch (e) { toast("Scan toggle failed", "error"); }
        }
        async function forceStopScan(mac) {
            try { await postJson("/api/scan", {enable: false, dongle_mac: mac}); } catch (e) {}
            clearScanTimers(mac);
            scanState[mac] = {active: false};
            updatePairButton(mac);
            await loadDongles();
        }

        // ===== Maintenance / raw =====
        async function runFix(mac, btn) {
            toast(`Purging ghost sensors on ${dongleName(mac)}…`, "ok");
            try {
                const {ok, data: d} = await postJson("/api/fix", {dongle_mac: mac});
                if (!ok) { toast(d.error || "Purge failed", "error"); return; }
                toast(`Purged ${d.purged_count || 0} ghost sensor${d.purged_count === 1 ? "" : "s"}`, "ok");
                await loadDongles();
            } catch (e) { toast("Purge failed", "error"); }
        }
        async function sendRaw(mac) {
            const input = document.querySelector(`[data-hex-input="${CSS.escape(mac)}"]`);
            if (!input) return;
            const raw = input.value.trim();
            if (!raw) return;
            const bytes = raw.split(",").map(s => s.trim()).filter(Boolean).map(s => parseInt(s, 16));
            if (!bytes.length || bytes.some(isNaN)) { toast("Invalid hex bytes", "error"); return; }
            const ts = new Date().toLocaleTimeString();
            pushHex(mac, `[${ts}] ===> [${raw.toUpperCase()}]`);
            try {
                const r = await fetch(`${API}/api/raw`, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({bytes, dongle_mac: mac})});
                if (!r.ok) { pushHex(mac, `[${ts}] <=== error: ${await r.text()}`, "text-rose-400"); return; }
                const d = await r.json();
                const hex = (d.response_bytes || []).map(b => b.toString(16).padStart(2, "0").toUpperCase()).join(", ");
                pushHex(mac, `[${ts}] <=== [${hex}]`, "text-slate-400");
                input.value = "";
            } catch (e) { pushHex(mac, `[${ts}] <=== send failed`, "text-rose-400"); }
        }

        // ===== Load / render =====
        function updateHeader() {
            const dot = document.getElementById("status-dot"), txt = document.getElementById("status-text");
            const badge = document.getElementById("header-badge");
            const n = (donglesData.dongles || []).length;
            if (n > 0) {
                dot.className = "w-2 h-2 rounded-full bg-teal-400";
                txt.innerText = `${n} dongle${n > 1 ? "s" : ""} online`;
                badge.className = "flex items-center space-x-2 bg-teal-900/20 border border-teal-800/30 px-3 py-1.5 rounded-full text-xs font-semibold text-teal-400";
            } else {
                dot.className = "w-2 h-2 rounded-full bg-rose-500 animate-pulse";
                txt.innerText = "No dongles";
                badge.className = "flex items-center space-x-2 bg-slate-800 px-3 py-1.5 rounded-full text-xs font-semibold text-slate-400";
            }
        }

        function render() {
            const c = document.getElementById("dongles-container");
            const dongles = donglesData.dongles || [];
            const orphans = donglesData.unassociated_sensors || [];
            if (!dongles.length && !orphans.length) {
                c.innerHTML = `<div class="text-center py-16"><div class="text-4xl mb-3">📡</div>
                    <div class="text-slate-400 text-sm">No dongles connected</div>
                    <div class="text-slate-600 text-xs mt-1">Connect a USB dongle or start a WebSocket bridge</div></div>`;
                return;
            }
            c.innerHTML = gatewaysCard(dongles)
                + sensorsCard(dongles, orphans);
        }

        function editingInline() {
            const el = document.activeElement;
            const c = document.getElementById("dongles-container");
            return !!(el && c && c.contains(el) && (el.tagName === "INPUT" || el.tagName === "TEXTAREA" || el.tagName === "SELECT"));
        }

        async function loadDongles() {
            try {
                const r = await fetch(`${API}/api/dongles`);
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                donglesData = await r.json();
                // Reflect a scan started or stopped; clean up timers and confirm only when a genuinely new sensor paired.
                (donglesData.dongles || []).forEach(d => {
                    const ss = scanState[d.mac];
                    if (d.scanning && !ss?.active) {
                        const beforeMacs = new Set(allSensors().map(s => s.mac));
                        scanState[d.mac] = {active: true, secondsLeft: null, beforeMacs};
                        gatewaysFolderOpen = true;
                        openDongles.add(d.mac);
                    }
                    if (!d.scanning && ss?.active) {
                        const pairedSensors = ss.beforeMacs ? (d.sensors || []).filter(s => !ss.beforeMacs.has(s.mac)) : [];
                        clearScanTimers(d.mac);
                        scanState[d.mac] = {active: false};
                        if (pairedSensors.length) showPairConfirm(pairedSensors, d);
                    }
                });
                Object.keys(scanState).forEach(mac => {
                    if (scanState[mac]?.active && !dongleByMac(mac)) {
                        clearScanTimers(mac);
                        scanState[mac] = {active: false};
                    }
                });
                updateHeader();
                if (editingInline()) { pendingRender = true; return; }
                render();
                pendingRender = false;
            } catch (e) {
                document.getElementById("dongles-container").innerHTML = `<div class="text-center text-sm text-rose-500 py-12">Failed to load</div>`;
            }
        }

        // ===== Pairing confirmation =====
        // Shown when a scan finishes and new sensor(s) appeared: names the
        // device(s) that were just paired instead of a bare "Sensor paired".
        function showPairConfirm(pairedSensors, dongle) {
            const list = document.getElementById("pair-confirm-list");
            if (!list) return;
            list.innerHTML = pairedSensors.map(s => {
                const emac = esc(s.mac);
                return `<div class="rounded-xl border border-slate-800 bg-slate-950/70 p-3">
                    <div class="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2">
                        <div class="min-w-0">
                            <div class="font-semibold text-sm text-slate-200 break-words">${esc(s.name || s.mac)}</div>
                            <div class="font-mono text-[11px] text-teal-500/80">${emac}</div>
                        </div>
                        <div class="shrink-0 flex items-center gap-1.5">
                            <button onclick="renamePairedSensor('${emac}')" class="py-1.5 px-2.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">✏️ Rename</button>
                            <button onclick="viewPairedSensor('${emac}')" class="py-1.5 px-2.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">View</button>
                        </div>
                    </div>
                    <div class="mt-2 flex flex-wrap items-center gap-1.5">${typeTag(s)}${gatewayTag(s, dongle)}</div>
                    <div class="mt-1.5 text-[11px] text-slate-500">Seen ${relSeen(s.last_seen)}</div>
                </div>`;
            }).join("");
            document.getElementById("pair-confirm-modal").classList.add("open");
        }
        function closePairConfirm() {
            document.getElementById("pair-confirm-modal")?.classList.remove("open");
        }
        function renamePairedSensor(mac) {
            closePairConfirm();
            openRename(mac);
        }
        function viewPairedSensor(mac) {
            closePairConfirm();
            if (!sensorsFolderOpen) toggleSensorsFolder();
            render();
            if (!openSensors.has(mac)) toggleSensor(mac);
            document.querySelector(`[data-sensor-row="${CSS.escape(mac)}"]`)?.scrollIntoView({block: "nearest"});
        }

        // ===== Toast =====
        function toast(message, tone) {
            const el = document.createElement("div");
            const error = tone === "error";
            el.className = `fixed top-4 left-1/2 -translate-x-1/2 z-[110] max-w-lg w-[calc(100%-2rem)] text-sm ${error ? "text-rose-200 bg-rose-950 border-rose-800" : "text-teal-200 bg-teal-950 border-teal-800"} border rounded-xl px-4 py-3 shadow-2xl`;
            el.textContent = message;
            el.onclick = () => el.remove();
            document.body.appendChild(el);
            setTimeout(() => el.remove(), error ? 8000 : 4000);
        }

        document.addEventListener("focusout", () => {
            if (!pendingRender) return;
            setTimeout(() => {
                if (!editingInline()) { pendingRender = false; render(); }
            }, 0);
        });

        // ===== Init =====
        loadDongles();
        const es = new EventSource(`${API}/api/events`);
        es.onmessage = (e) => { if (e.data === "update") loadDongles(); };
    </script>
</body>
</html>

"##);


#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::packet::{commands, PacketPayload};
    use crate::transport::AsyncTransport;
    use crate::transport::replay::ReplayTransport;
    use crate::transport::virtual_dongle::VirtualDongle;
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    type Client = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

    async fn start_server(bridge: BridgeSettings) -> (Client, Arc<WebState>, mpsc::Receiver<DongleStatus>) {
        let (event_tx, _) = mpsc::channel(128);
        let (dongle_status_tx, dongle_status_rx) = mpsc::channel(8);
        let state = Arc::new(WebState {
            engines: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            sensor_manager: Arc::new(std::sync::Mutex::new(SensorManager::new(
                "/nonexistent/sensors.yaml".to_string(),
                "/nonexistent/state.yaml".to_string(),
            ))),
            dongle_names: Arc::new(std::sync::Mutex::new(DonglesConfig::default())),
            dongle_config_path: "/nonexistent/dongles.yaml".to_string(),
            broadcast_tx: tokio::sync::broadcast::channel(16).0,
            event_tx,
            dongle_status_tx,
            bridge_auth_token: None,
            bridge,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Arc::clone(&state)).into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move { axum::serve(listener, app).await });
        let (client, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws/bridge", addr)).await.unwrap();
        (client, state, dongle_status_rx)
    }

    /// Relays frames between the bridge connection and a virtual dongle until `until` completes.
    async fn pump_until<F: std::future::Future>(client: &mut Client, dongle: &mut ReplayTransport, until: F) -> F::Output {
        tokio::pin!(until);
        let mut buf = [0u8; 1024];
        loop {
            tokio::select! {
                out = &mut until => return out,
                msg = client.next() => match msg {
                    Some(Ok(WsMessage::Binary(data))) => dongle.write(&data).await.unwrap(),
                    Some(Ok(_)) => {}
                    other => panic!("bridge connection ended: {:?}", other),
                },
                n = dongle.read(&mut buf) => {
                    client.send(WsMessage::Binary(buf[..n.unwrap()].to_vec())).await.unwrap();
                }
            }
        }
    }

    /// Reads until the server's close frame, returning it and how many Inquiries came before it.
    async fn read_until_close(client: &mut Client) -> (u16, String, usize) {
        let mut inquiries = 0;
        loop {
            match client.next().await {
                Some(Ok(WsMessage::Binary(data))) => {
                    if Packet::parse(&data).is_ok_and(|(p, _)| p.cmd() == commands::CMD_INQUIRY) {
                        inquiries += 1;
                    }
                }
                Some(Ok(WsMessage::Close(Some(frame)))) => {
                    return (frame.code.into(), frame.reason.to_string(), inquiries);
                }
                Some(Ok(_)) => {}
                other => panic!("connection ended without a close frame: {:?}", other),
            }
        }
    }

    fn fast_settings() -> BridgeSettings {
        BridgeSettings {
            handshake_timeout: Duration::from_secs(3),
            inquiry_timeout: Duration::from_millis(200),
            ping_interval: Duration::from_millis(100),
            max_missed_pongs: 3,
        }
    }

    #[tokio::test]
    async fn handshake_failure_closes_bridge_connection() {
        let (mut client, state, _status_rx) = start_server(fast_settings()).await;

        // Never answer the handshake.
        let (code, reason, inquiries) = tokio::time::timeout(Duration::from_secs(5), read_until_close(&mut client))
            .await
            .expect("server did not close the connection");

        assert_eq!(code, 1011);
        assert!(reason.starts_with("handshake failed"), "reason: {}", reason);
        assert_eq!(inquiries, 3, "Inquiry should be retried before giving up");
        assert!(state.engines.lock().await.is_empty());
    }

    #[tokio::test]
    async fn handshake_survives_one_lost_inquiry() {
        let (mut client, state, mut status_rx) = start_server(fast_settings()).await;
        let dongle = VirtualDongle::new("DONGLE01", "V1.0").with_sensors(vec![]);
        let mut transport = dongle.replay_transport().clone();

        // Drop the first Inquiry, then behave.
        loop {
            if let Some(Ok(WsMessage::Binary(data))) = client.next().await {
                if Packet::parse(&data).is_ok_and(|(p, _)| p.cmd() == commands::CMD_INQUIRY) {
                    break;
                }
            }
        }
        let status = tokio::time::timeout(Duration::from_secs(5), pump_until(&mut client, &mut transport, status_rx.recv()))
            .await
            .expect("dongle never registered")
            .unwrap();
        assert_eq!(status, DongleStatus { mac: "DONGLE01".into(), online: true, version: Some("V1.0".into()) });
        assert!(state.engines.lock().await.contains_key("DONGLE01"));
    }

    #[tokio::test]
    async fn unresponsive_bridge_client_is_dropped() {
        let (mut client, state, mut status_rx) = start_server(fast_settings()).await;
        let dongle = VirtualDongle::new("DONGLE01", "V1.0").with_sensors(vec![]);
        let mut transport = dongle.replay_transport().clone();

        let status = tokio::time::timeout(Duration::from_secs(5), pump_until(&mut client, &mut transport, status_rx.recv()))
            .await
            .expect("dongle never registered")
            .unwrap();
        assert!(status.online);

        // While the client keeps reading (and so answers pings), it stays connected.
        pump_until(&mut client, &mut transport, tokio::time::sleep(Duration::from_millis(600))).await;
        assert!(status_rx.try_recv().is_err());
        assert!(state.engines.lock().await.contains_key("DONGLE01"));

        // Stop reading: pings go unanswered, but the TCP connection stays up.
        let status = tokio::time::timeout(Duration::from_secs(3), status_rx.recv())
            .await
            .expect("dongle was not dropped")
            .unwrap();
        assert_eq!(status, DongleStatus { mac: "DONGLE01".into(), online: false, version: None });
        assert!(state.engines.lock().await.is_empty());

        let (code, reason, _) = tokio::time::timeout(Duration::from_secs(2), read_until_close(&mut client))
            .await
            .expect("no close frame");
        assert_eq!((code, reason.as_str()), (1011, "heartbeat timeout"));
    }

    #[tokio::test]
    async fn api_raw_handles_standard_and_ack_only_commands() {
        let dongle = VirtualDongle::new("DONGLE01", "V1.0");

        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(64);
        let (dongle_status_tx, _dongle_status_rx) = tokio::sync::mpsc::channel(16);
        let mut engine = Engine::new(dongle.transport(), event_tx.clone());
        let _exit_tx = engine.start();
        engine.initialize_handshake().await.unwrap();

        let engines: EnginesMap = Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        engines.lock().await.insert("DONGLE01".to_string(), engine);

        let state = Arc::new(WebState {
            engines,
            sensor_manager: Arc::new(std::sync::Mutex::new(SensorManager::new(
                "/nonexistent/sensors.yaml".to_string(),
                "/nonexistent/state.yaml".to_string(),
            ))),
            dongle_names: Arc::new(std::sync::Mutex::new(DonglesConfig::default())),
            dongle_config_path: "/nonexistent/dongles.yaml".to_string(),
            broadcast_tx: tokio::sync::broadcast::channel(16).0,
            event_tx,
            dongle_status_tx,
            bridge_auth_token: None,
            bridge: fast_settings(),
        });

        // 1. Standard command: CMD_GET_MAC (0x4304 -> 0x4305)
        dongle.replay_transport().register_response(
            commands::CMD_GET_MAC,
            Packet::new_sync((commands::CMD_MAC_RESPONSE & 0xFF) as u8, b"DONGLE01".to_vec()).to_bytes(),
        );
        let get_mac = Packet::new_sync(0x04, vec![]).to_bytes();
        let resp = send_raw_packet(
            State(Arc::clone(&state)),
            Json(RawPacketRequest { bytes: get_mac, dongle_mac: "DONGLE01".to_string() }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        // 2. ACK-only command: CMD_SEND_KEYPAD (0x5353 -> ACK(0x5353))
        dongle.replay_transport().register_response(
            commands::CMD_SEND_KEYPAD,
            Packet::new_ack(commands::CMD_SEND_KEYPAD).to_bytes(),
        );
        let keypad_pkt = Packet::new_async((commands::CMD_SEND_KEYPAD & 0xFF) as u8, b"77KEYPAD\x01".to_vec()).to_bytes();
        let resp = tokio::time::timeout(
            Duration::from_millis(500),
            send_raw_packet(
                State(Arc::clone(&state)),
                Json(RawPacketRequest { bytes: keypad_pkt, dongle_mac: "DONGLE01".to_string() }),
            ),
        )
        .await
        .expect("ACK-only command via /api/raw must not time out")
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    fn parse_written_commands(bytes: &[u8]) -> Vec<u16> {
        let mut cmds = Vec::new();
        let mut slice = bytes;
        while !slice.is_empty() {
            match Packet::parse(slice) {
                Ok((pkt, consumed)) => {
                    if !matches!(pkt.payload, PacketPayload::Ack(_)) {
                        cmds.push(pkt.cmd());
                    }
                    slice = &slice[consumed..];
                }
                Err(_) => break,
            }
        }
        cmds
    }

    #[tokio::test]
    async fn api_fix_targets_requested_dongle_only() {
        let (_client, state, _status_rx) = start_server(fast_settings()).await;

        let d1 = VirtualDongle::new("DONGLE01", "V1.0").with_sensors(vec!["00000000".into()]);
        let d2 = VirtualDongle::new("DONGLE02", "V1.0").with_sensors(vec!["00000000".into()]);

        let (tx1, _) = mpsc::channel(16);
        let mut e1 = Engine::new(d1.transport(), tx1);
        let _exit1 = e1.start();
        e1.initialize_handshake().await.unwrap();

        let (tx2, _) = mpsc::channel(16);
        let mut e2 = Engine::new(d2.transport(), tx2);
        let _exit2 = e2.start();
        e2.initialize_handshake().await.unwrap();

        d1.replay_transport().clear_written();
        d2.replay_transport().clear_written();

        {
            let mut engines = state.engines.lock().await;
            engines.insert("DONGLE01".into(), e1);
            engines.insert("DONGLE02".into(), e2);
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Arc::clone(&state)).into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let http = reqwest::Client::new();
        let resp = http
            .post(format!("http://{}/api/fix", addr))
            .json(&serde_json::json!({ "dongle_mac": "DONGLE01" }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let cmds1 = parse_written_commands(&d1.replay_transport().get_written());
        let cmds2 = parse_written_commands(&d2.replay_transport().get_written());
        assert!(
            cmds1.contains(&commands::CMD_DELETE_SENSOR),
            "expected DONGLE01 to purge 00000000, got commands: {:04X?}",
            cmds1
        );
        assert!(
            cmds2.is_empty(),
            "expected DONGLE02 not to be touched when dongle_mac=DONGLE01, got commands: {:04X?}",
            cmds2
        );
    }

    #[tokio::test]
    async fn api_verify_exchanges_r1_stops_scan_and_emits_paired_event() {
        let (event_tx, mut event_rx) = mpsc::channel(128);
        let (dongle_status_tx, _) = mpsc::channel(8);
        let state = Arc::new(WebState {
            engines: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            sensor_manager: Arc::new(std::sync::Mutex::new(SensorManager::new(
                "/nonexistent/sensors.yaml".to_string(),
                "/nonexistent/state.yaml".to_string(),
            ))),
            dongle_names: Arc::new(std::sync::Mutex::new(DonglesConfig::default())),
            dongle_config_path: "/nonexistent/dongles.yaml".to_string(),
            broadcast_tx: tokio::sync::broadcast::channel(16).0,
            event_tx: event_tx.clone(),
            dongle_status_tx,
            bridge_auth_token: None,
            bridge: fast_settings(),
        });

        let d1 = VirtualDongle::new("DONGLE01", "V1.0");
        d1.replay_transport().register_response(
            commands::CMD_GET_R1,
            Packet::new_async(0x22, vec![0x42; 16]).to_bytes(),
        );

        let mut e1 = Engine::new(d1.transport(), event_tx);
        let _exit1 = e1.start();
        e1.initialize_handshake().await.unwrap();
        e1.set_scan(true).await.unwrap();
        assert!(e1.is_scanning());
        d1.replay_transport().clear_written();

        state.engines.lock().await.insert("DONGLE01".into(), e1.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Arc::clone(&state)).into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let http = reqwest::Client::new();
        let resp = http
            .post(format!("http://{}/api/verify", addr))
            .json(&serde_json::json!({
                "mac": "SENSOR01",
                "dongle_mac": "DONGLE01",
                "sensor_type": "contact_v2"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let cmds = parse_written_commands(&d1.replay_transport().get_written());
        assert_eq!(
            cmds,
            vec![
                commands::CMD_GET_R1,
                commands::CMD_SET_SCAN,
                commands::CMD_VERIFY_SENSOR,
            ],
            "verify endpoint should exchange R1, stop scan, and verify sensor in order"
        );
        assert!(!e1.is_scanning(), "scan mode should be disabled after manual verify");

        let evt = event_rx.try_recv().expect("expected Paired event to be emitted");
        assert_eq!(evt.mac, "SENSOR01");
        assert_eq!(evt.dongle_mac.as_deref(), Some("DONGLE01"));
        assert!(matches!(evt.data, crate::protocol::telemetry::TelemetryData::Paired { .. }));
    }

    #[tokio::test]
    async fn list_dongles_includes_offline_gateway_sensors_and_dongle_names() {
        let (event_tx, _) = mpsc::channel(128);
        let (dongle_status_tx, _) = mpsc::channel(8);
        let mut dongle_names = DonglesConfig::default();
        dongle_names.set_name("OFFLINE01", "Garage Relay");

        let mut manager = SensorManager::new(
            "/nonexistent/sensors.yaml".to_string(),
            "/nonexistent/state.yaml".to_string(),
        );
        manager.assign_dongle("OFFLINE01", &["77000001".to_string()]);

        let state = Arc::new(WebState {
            engines: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            sensor_manager: Arc::new(std::sync::Mutex::new(manager)),
            dongle_names: Arc::new(std::sync::Mutex::new(dongle_names)),
            dongle_config_path: "/nonexistent/dongles.yaml".to_string(),
            broadcast_tx: tokio::sync::broadcast::channel(16).0,
            event_tx,
            dongle_status_tx,
            bridge_auth_token: None,
            bridge: fast_settings(),
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Arc::clone(&state)).into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let http = reqwest::Client::new();
        let body: serde_json::Value = http
            .get(format!("http://{}/api/dongles", addr))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let orphans = body["unassociated_sensors"].as_array().unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0]["mac"], "77000001");
        assert_eq!(orphans[0]["dongle_mac"], "OFFLINE01");
        assert_eq!(body["dongle_names"]["OFFLINE01"], "Garage Relay");
    }

    #[tokio::test]
    async fn dongles_api_exposes_online_status_and_dashboard_highlights_offline() {
        use crate::protocol::sensor::WyzeSensor;

        let (event_tx, _) = mpsc::channel(128);
        let (dongle_status_tx, _) = mpsc::channel(8);
        let d1 = VirtualDongle::new("DONGLE01", "V1.0");
        let mut e1 = Engine::new(d1.transport(), event_tx.clone());
        let _exit1 = e1.start();
        e1.initialize_handshake().await.unwrap();

        let mut manager = SensorManager::new(
            "/nonexistent/sensors.yaml".to_string(),
            "/nonexistent/state.yaml".to_string(),
        );
        let mut live = WyzeSensor::new(
            "77000001".to_string(),
            SensorType::ContactV2,
            "Live Sensor".to_string(),
        );
        live.is_online = true;
        live.dongle_mac = Some("DONGLE01".to_string());
        manager
            .get_sensors_mut()
            .insert("77000001".to_string(), live);
        let mut offline = WyzeSensor::new(
            "77000002".to_string(),
            SensorType::MotionV2,
            "Offline Sensor".to_string(),
        );
        offline.is_online = false;
        offline.last_seen = 1_000;
        manager
            .get_sensors_mut()
            .insert("77000002".to_string(), offline);

        let mut engines_map = std::collections::HashMap::new();
        engines_map.insert("DONGLE01".to_string(), e1);
        let state = Arc::new(WebState {
            engines: Arc::new(tokio::sync::Mutex::new(engines_map)),
            sensor_manager: Arc::new(std::sync::Mutex::new(manager)),
            dongle_names: Arc::new(std::sync::Mutex::new(DonglesConfig::default())),
            dongle_config_path: "/nonexistent/dongles.yaml".to_string(),
            broadcast_tx: tokio::sync::broadcast::channel(16).0,
            event_tx,
            dongle_status_tx,
            bridge_auth_token: None,
            bridge: fast_settings(),
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Arc::clone(&state)).into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let http = reqwest::Client::new();
        let body: serde_json::Value = http
            .get(format!("http://{}/api/dongles", addr))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let dongle_sensors = body["dongles"][0]["sensors"].as_array().unwrap();
        assert_eq!(dongle_sensors.len(), 1);
        assert_eq!(dongle_sensors[0]["mac"], "77000001");
        assert_eq!(dongle_sensors[0]["online"], true);
        assert!(
            dongle_sensors[0].get("is_online").is_none(),
            "/api/dongles should use `online` like the MQTT state payload"
        );

        let orphans = body["unassociated_sensors"].as_array().unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0]["mac"], "77000002");
        assert_eq!(orphans[0]["online"], false);

        let html = http
            .get(format!("http://{}/", addr))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            html.contains("sensor-row.offline") && html.contains("sensor-card.offline"),
            "dashboard should style offline sensor rows and cards"
        );

        // Execute the served dashboard script against the real /api/dongles
        // payload when Node is available (always present on ubuntu-latest CI).
        let node_available = std::process::Command::new("node")
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success());
        if !node_available {
            assert!(
                std::env::var_os("CI").is_none(),
                "node must be available in CI to run dashboard behavioural assertions"
            );
            return;
        }

        let script_start = html
            .find("<script>\n")
            .expect("inline dashboard <script> should exist")
            + "<script>\n".len();
        let script_end = html[script_start..]
            .find("</script>")
            .expect("closing </script> should exist")
            + script_start;
        let dashboard_js = &html[script_start..script_end];

        let harness = r#"
const vm = require("vm");
const assert = require("assert");
const payload = JSON.parse(process.env.DONGLES_JSON);
const scriptCode = process.env.DASHBOARD_JS;
const els = new Map();
const mk = () => ({
  innerHTML: "", innerText: "", textContent: "", className: "", value: "",
  classList: { s: new Set(), add(...c){c.forEach(x=>this.s.add(x));}, remove(...c){c.forEach(x=>this.s.delete(x));}, toggle(c,f){if(f===undefined){this.s.has(c)?this.s.delete(c):this.s.add(c);}else if(f)this.s.add(c);else this.s.delete(c);}, contains(c){return this.s.has(c);} },
  contains: () => false,
});
const ctx = {
  console, Date, Math, Number, String, Object, Array, Set, Promise,
  encodeURIComponent, parseInt, isNaN, isFinite,
  setTimeout: () => 1, clearTimeout: () => {}, setInterval: () => 1, clearInterval: () => {},
  CSS: { escape: s => s },
  EventSource: function() {},
  fetch: async () => ({ ok: true, status: 200, json: async () => payload }),
  document: {
    activeElement: null,
    body: { appendChild() {} },
    getElementById: id => { if (!els.has(id)) els.set(id, mk()); return els.get(id); },
    querySelector: () => null,
    querySelectorAll: () => [],
    addEventListener: () => {},
    createElement: () => mk(),
  },
};
vm.createContext(ctx);
vm.runInContext(scriptCode, ctx);
(async () => {
  await ctx.loadDongles();
  const out = els.get("dongles-container").innerHTML;
  assert(out.includes("1 offline"), "Sensors header should show '1 offline'");
  assert(out.includes(">Offline</span>"), "Offline sensor should render an Offline badge");
  assert(out.includes("check its battery or whether it&#39;s in range"), "Tooltip should suggest next steps");
  assert(!out.includes("Dead") && !out.includes("💀"), "Dashboard should use Offline wording rather than Dead");
  const offlineRow = out.match(/<tr data-sensor-row="77000002"[^>]*>/);
  const liveRow = out.match(/<tr data-sensor-row="77000001"[^>]*>/);
  assert(offlineRow && offlineRow[0].includes("offline"), "Offline sensor row should carry .offline class");
  assert(liveRow && !liveRow[0].includes("offline"), "Online sensor row must not carry .offline class");
})().catch(e => { console.error(e); process.exit(1); });
"#;
        let out = std::process::Command::new("node")
            .arg("-e")
            .arg(harness)
            .env("DONGLES_JSON", serde_json::to_string(&body).unwrap())
            .env("DASHBOARD_JS", dashboard_js)
            .output()
            .expect("failed to execute node");
        assert!(
            out.status.success(),
            "dashboard JS assertion failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[tokio::test]
    async fn dashboard_pairing_confirmation_names_the_paired_device() {
        use crate::protocol::sensor::WyzeSensor;

        let dir = std::env::temp_dir().join(format!("w2m_pair_confirm_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let sensors_path = dir.join("sensors.yaml").to_string_lossy().to_string();
        let state_path = dir.join("state.yaml").to_string_lossy().to_string();
        let dongles_path = dir.join("dongles.yaml").to_string_lossy().to_string();

        let (event_tx, _) = mpsc::channel(128);
        let (dongle_status_tx, _) = mpsc::channel(8);

        let d_a = VirtualDongle::new("DONGLE0A", "V1.0");
        let mut e_a = Engine::new(d_a.transport(), event_tx.clone());
        let _exit_a = e_a.start();
        e_a.initialize_handshake().await.unwrap();

        let d_b = VirtualDongle::new("DONGLE0B", "V1.0");
        d_b.register_extra_scan_responses(2, 2);
        let mut e_b = Engine::new(d_b.transport(), event_tx.clone());
        let _exit_b = e_b.start();
        e_b.initialize_handshake().await.unwrap();

        let mut dongle_names = DonglesConfig::default();
        dongle_names.set_name("DONGLE0B", "Living Room");

        let mut manager = SensorManager::new(sensors_path, state_path);
        let mut existing = WyzeSensor::new(
            "77000001".to_string(),
            SensorType::ContactV2,
            "Front Door".to_string(),
        );
        existing.dongle_mac = Some("DONGLE0A".to_string());
        manager
            .get_sensors_mut()
            .insert("77000001".to_string(), existing);

        let mut engines = std::collections::HashMap::new();
        engines.insert("DONGLE0A".to_string(), e_a);
        engines.insert("DONGLE0B".to_string(), e_b.clone());

        let state = Arc::new(WebState {
            engines: Arc::new(tokio::sync::Mutex::new(engines)),
            sensor_manager: Arc::new(std::sync::Mutex::new(manager)),
            dongle_names: Arc::new(std::sync::Mutex::new(dongle_names)),
            dongle_config_path: dongles_path,
            broadcast_tx: tokio::sync::broadcast::channel(16).0,
            event_tx,
            dongle_status_tx,
            bridge_auth_token: None,
            bridge: fast_settings(),
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Arc::clone(&state)).into_make_service_with_connect_info::<SocketAddr>();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let base_url = format!("http://{}", addr);
        let html = reqwest::Client::new()
            .get(format!("{}/", base_url))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            html.contains("id=\"pair-confirm-modal\""),
            "dashboard should render the pairing confirmation modal element"
        );

        let node_available = std::process::Command::new("node")
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success());
        if !node_available {
            let _ = std::fs::remove_dir_all(&dir);
            assert!(
                std::env::var_os("CI").is_none(),
                "node must be available in CI to run dashboard behavioural assertions"
            );
            return;
        }

        let script_start = html
            .find("<script>\n")
            .expect("inline dashboard <script> should exist")
            + "<script>\n".len();
        let script_end = html[script_start..]
            .find("</script>")
            .expect("closing </script> should exist")
            + script_start;
        let dashboard_js = html[script_start..script_end].to_string();

        // Spawn a Node runner that drives the real dashboard script against our
        // live Axum server in two phases:
        //   Phase 1: scan already running on DONGLE0B, existing sensor 77000001
        //            moves from DONGLE0A to DONGLE0B -> must NOT open dialog.
        //   Phase 2: scan started via toggleScan("DONGLE0B"), brand-new sensor
        //            77AB1234 pairs -> opens dialog with Rename + View actions.
        e_b.set_scan(true).await.unwrap();

        let state_for_node = Arc::clone(&state);
        let e_b_for_node = e_b.clone();
        let ctrl_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ctrl_addr = ctrl_listener.local_addr().unwrap();
        let ctrl_app = Router::new()
            .route(
                "/relink-existing",
                post({
                    let st = Arc::clone(&state_for_node);
                    let eb = e_b_for_node.clone();
                    move || {
                        let st = Arc::clone(&st);
                        let mut eb = eb.clone();
                        async move {
                            st.sensor_manager
                                .lock()
                                .unwrap()
                                .get_sensors_mut()
                                .get_mut("77000001")
                                .unwrap()
                                .dongle_mac = Some("DONGLE0B".to_string());
                            eb.set_scan(false).await.unwrap();
                            StatusCode::OK
                        }
                    }
                }),
            )
            .route(
                "/pair-new",
                post(move || {
                    let st = Arc::clone(&state_for_node);
                    let mut eb = e_b_for_node.clone();
                    async move {
                        let mut s = WyzeSensor::new(
                            "77AB1234".to_string(),
                            SensorType::ContactV2,
                            "Wyze SWITCHV2 1234".to_string(),
                        );
                        s.dongle_mac = Some("DONGLE0B".to_string());
                        s.last_seen = 1_700_000_000;
                        st.sensor_manager
                            .lock()
                            .unwrap()
                            .get_sensors_mut()
                            .insert("77AB1234".to_string(), s);
                        eb.set_scan(false).await.unwrap();
                        StatusCode::OK
                    }
                }),
            );
        tokio::spawn(async move { axum::serve(ctrl_listener, ctrl_app).await });

        let harness = r#"
const vm = require("vm");
const assert = require("assert");
const base = process.env.BASE_URL;
const ctrl = process.env.CTRL_URL;
const scriptCode = process.env.DASHBOARD_JS.replace('const API = "";', `const API = ${JSON.stringify(base)};`);

const els = new Map();
const toasts = [];
const mk = () => ({
  innerHTML: "", innerText: "", textContent: "", className: "", value: "",
  focus() {}, select() {}, remove() {}, scrollIntoView() {},
  classList: { s: new Set(), add(...c){c.forEach(x=>this.s.add(x));}, remove(...c){c.forEach(x=>this.s.delete(x));}, toggle(c,f){if(f===undefined){this.s.has(c)?this.s.delete(c):this.s.add(c);}else if(f)this.s.add(c);else this.s.delete(c);}, contains(c){return this.s.has(c);} },
  contains: () => false,
});
const ctx = {
  console, Date, Math, Number, String, Object, Array, Set, Promise,
  encodeURIComponent, parseInt, isNaN, isFinite, fetch,
  setTimeout: (fn, ms) => { if (ms === 0) fn(); return 1; },
  clearTimeout: () => {},
  setInterval: () => 1,
  clearInterval: () => {},
  CSS: { escape: s => s },
  EventSource: function() {},
  document: {
    activeElement: null,
    body: { appendChild(el) { toasts.push(el.textContent); } },
    getElementById: id => { if (!els.has(id)) els.set(id, mk()); return els.get(id); },
    querySelector: () => null,
    querySelectorAll: () => [],
    addEventListener: () => {},
    createElement: () => mk(),
  },
};
vm.createContext(ctx);
vm.runInContext(scriptCode, ctx);

(async () => {
  // Initial load while DONGLE0B is already scanning (simulates another open tab).
  await ctx.loadDongles();
  assert(!els.get("pair-confirm-modal")?.classList.contains("open"));

  // Existing sensor 77000001 moves from DONGLE0A to DONGLE0B while scanning, then scan stops.
  await fetch(`${ctrl}/relink-existing`, { method: "POST" });
  await ctx.loadDongles();
  assert(
    !els.get("pair-confirm-modal")?.classList.contains("open"),
    "re-linking an already-known sensor during a scan must not trigger a false pairing dialog"
  );
  assert(!toasts.some(t => t.includes("Sensor paired")), "generic 'Sensor paired' toast must not appear");

  // Start a new scan on DONGLE0B from the dashboard, then pair a brand-new sensor.
  await ctx.toggleScan("DONGLE0B");
  await fetch(`${ctrl}/pair-new`, { method: "POST" });
  await ctx.loadDongles();

  assert(
    els.get("pair-confirm-modal").classList.contains("open"),
    "newly paired sensor should open the confirmation modal"
  );
  const cardHtml = els.get("pair-confirm-list").innerHTML;
  assert(cardHtml.includes("Wyze SWITCHV2 1234"), "dialog should show sensor default name");
  assert(cardHtml.includes("77AB1234"), "dialog should show sensor MAC");
  assert(cardHtml.includes("Contact"), "dialog should show sensor type tag");
  assert(cardHtml.includes("Living Room"), "dialog should show gateway name tag");
  assert(!cardHtml.includes("Gateway:"), "gateway must not be repeated on a second 'Gateway:' line");

  // View button expands the sensor in the list and closes the modal.
  ctx.viewPairedSensor("77AB1234");
  assert(!els.get("pair-confirm-modal").classList.contains("open"), "View should close confirmation modal");
  assert(els.get("dongles-container").innerHTML.includes("77AB1234"), "sensor should render in list");

  // Re-open and exercise the Rename shortcut end-to-end against POST /api/sensors/:mac/rename.
  els.get("pair-confirm-modal").classList.add("open");
  ctx.renamePairedSensor("77AB1234");
  assert(!els.get("pair-confirm-modal").classList.contains("open"), "Rename should close confirmation modal");
  assert(els.get("rename-modal").classList.contains("open"), "Rename should open rename modal");
  assert.strictEqual(els.get("rename-input").value, "Wyze SWITCHV2 1234");
  els.get("rename-input").value = "Back Patio";
  await ctx.saveRename();
  assert(!els.get("rename-modal").classList.contains("open"), "saveRename should close rename modal");
})().catch(e => { console.error(e); process.exit(1); });
"#;

        let out = tokio::task::spawn_blocking(move || {
            std::process::Command::new("node")
                .arg("-e")
                .arg(harness)
                .env("BASE_URL", &base_url)
                .env("CTRL_URL", format!("http://{}", ctrl_addr))
                .env("DASHBOARD_JS", &dashboard_js)
                .output()
                .expect("failed to execute node")
        })
        .await
        .unwrap();

        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            out.status.success(),
            "dashboard pairing confirmation JS test failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            state.sensor_manager.lock().unwrap().get_sensors()["77AB1234"].friendly_name,
            "Back Patio",
            "Rename shortcut in pairing dialog should persist the new sensor name"
        );
    }
}
