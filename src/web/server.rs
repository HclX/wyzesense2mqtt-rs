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
    /// Roaming coordinator, when roaming is enabled.
    pub roaming: Option<Arc<crate::roaming::Roaming>>,
    /// OpenWyzeSense firmware install/update/restore for local USB dongles.
    pub firmware: Option<Arc<crate::firmware::Firmware>>,
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

type BridgeWriter = Arc<tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocket, Message>>>;

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
    roaming: Option<Arc<crate::roaming::Roaming>>,
    firmware: Option<Arc<crate::firmware::Firmware>>,
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
        roaming,
        firmware,
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
        .route("/api/roaming", get(roaming_status))
        .route("/api/firmware", get(firmware_status))
        .route("/api/firmware/install", post(firmware_install))
        .route("/api/firmware/confirm", post(firmware_confirm))
        .route("/api/firmware/cancel", post(firmware_cancel))
        .route("/api/firmware/restore", post(firmware_restore))
        .route("/api/firmware/relay", post(firmware_relay))
        .route("/api/firmware/sensors", post(firmware_sensors))
        .route("/api/firmware/sensors/cancel", post(firmware_sensors_cancel))
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
    let writer: BridgeWriter = Arc::new(tokio::sync::Mutex::new(writer));
    let pongs = Arc::new(AtomicU64::new(0));
    let transport = GatewayTransport::WebSocket {
        reader: Arc::new(tokio::sync::Mutex::new(reader)),
        writer: Arc::clone(&writer),
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
        close_bridge(&writer, 1011, &format!("handshake failed: {}", reason)).await;
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
    if let Some(r) = &state.roaming {
        // In the background: the heartbeat below must start right away.
        let (r, mac) = (Arc::clone(r), mac.clone());
        tokio::spawn(async move { r.gateway_connected(&mac).await });
    }

    // Wait for the transport to die (WebSocket close / broken pipe; the reader
    // loop fires disconnect_notify) or for the client to stop answering pings,
    // which is how a half-open TCP connection shows up.
    tokio::select! {
        _ = disconnect_notify.notified() => {
            info!("Bridge dongle {} disconnected, cleaning up", mac);
        }
        _ = heartbeat(&writer, &pongs, state.bridge.ping_interval, state.bridge.max_missed_pongs) => {
            warn!("Bridge dongle {} missed {} pings, dropping connection", mac, state.bridge.max_missed_pongs);
            close_bridge(&writer, 1011, "heartbeat timeout").await;
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

    if let Some(r) = &state.roaming {
        r.gateway_disconnected(&mac);
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
async fn close_bridge(writer: &BridgeWriter, code: u16, reason: &str) {
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
    }
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
                dongle_mac: None, overheard_short: None,
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
    // Sensors sharing a short address with another (they can't roam; pair them again)
    let conflicts = state.roaming.as_ref().map(|r| r.status_json()["address_conflicts"].clone()).unwrap_or_default();
    let with_conflict = |mut j: serde_json::Value| {
        if let Some(c) = j["mac"].as_str().and_then(|m| conflicts.get(m)).cloned() {
            j["address_conflict"] = c;
        }
        j
    };
    let engines = state.engines.lock().await;
    // Roaming's view of each gateway, asked before the sensor manager is locked:
    // roaming takes its own lock and then the sensors', so never the reverse.
    let ows: std::collections::HashMap<String, Option<serde_json::Value>> = engines
        .keys()
        .map(|mac| (mac.clone(), state.roaming.as_ref().and_then(|r| r.gateway_json(mac))))
        .collect();
    let manager = state.sensor_manager.lock().unwrap();
    let names = state.dongle_names.lock().unwrap();

    // Group sensors by dongle_mac from the single source of truth (SensorManager)
    let mut dongles: Vec<serde_json::Value> = engines.iter().map(|(mac, engine)| {
        let mut sensors: Vec<serde_json::Value> = manager.get_sensors().values()
            .filter(|s| s.dongle_mac.as_deref() == Some(mac.as_str()))
            .map(|s| with_conflict(sensor_info_to_json(s)))
            .collect();
        sensors.sort_by_key(|s| {
            (s["name"].as_str().unwrap_or("").to_string(), s["mac"].as_str().unwrap_or("").to_string())
        });
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
            // OpenWyzeSense firmware details, null for a stock dongle
            "openwyzesense": ows.get(mac).cloned().flatten(),
        })
    }).collect();
    dongles.sort_by_key(|d| {
        (d["name"].as_str().unwrap_or("").to_string(), d["mac"].as_str().unwrap_or("").to_string())
    });

    // Unassociated sensors: dongle_mac is None
    let mut unassociated: Vec<serde_json::Value> = manager.get_sensors().values()
        .filter(|s| s.dongle_mac.is_none())
        .map(|s| sensor_info_to_json(s))
        .collect();
    unassociated.sort_by_key(|s| {
        (s["name"].as_str().unwrap_or("").to_string(), s["mac"].as_str().unwrap_or("").to_string())
    });

    Json(json!({
        "dongles": dongles,
        "unassociated_sensors": unassociated,
    }))
}

fn sensor_info_to_json(s: &crate::protocol::sensor::WyzeSensor) -> serde_json::Value {
    let mut heard: Vec<serde_json::Value> = s
        .heard
        .iter()
        .map(|(mac, (rssi, at))| serde_json::json!({ "mac": mac, "rssi": rssi, "at": at }))
        .collect();
    // Strongest first, so the dashboard can show the useful ones without sorting.
    heard.sort_by_key(|h| std::cmp::Reverse(h["rssi"].as_i64().unwrap_or(i64::MIN)));

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
        "heard": heard,
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
            heard: Default::default(),
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
            heard: Default::default(),
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
    if let (true, Some(r)) = (payload.enable, &state.roaming) {
        if let Err(e) = r.prepare_pairing(&dongle_mac).await {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": format!("Cannot pair on dongle {}: {}", dongle_mac, e) })),
            ).into_response();
        }
    }
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

// --- GET /api/roaming ---
async fn roaming_status(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    match &state.roaming {
        Some(r) => (StatusCode::OK, Json(json!({ "enabled": true, "status": r.status_json() }))),
        None => (StatusCode::OK, Json(json!({ "enabled": false }))),
    }
}

// --- OpenWyzeSense firmware for local USB dongles ---

#[derive(Deserialize)]
struct FirmwareRequest {
    dongle: String,
    /// A relay's admin password, when it has one (never stored).
    #[serde(default)]
    password: Option<String>,
}

fn firmware_result(r: Result<crate::firmware::Job, String>) -> (StatusCode, Json<serde_json::Value>) {
    match r {
        Ok(job) => (StatusCode::OK, Json(json!({ "job": job }))),
        Err(e) => (StatusCode::CONFLICT, Json(json!({ "error": e }))),
    }
}

fn firmware_unavailable() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "firmware updates are not available here" })))
}

// GET /api/firmware
async fn firmware_status(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    match &state.firmware {
        Some(f) => (StatusCode::OK, Json(f.status().await)),
        None => (StatusCode::OK, Json(json!({ "release": crate::firmware::RELEASE_VERSION, "job": null, "dongles": {} }))),
    }
}

// POST /api/firmware/install {dongle}: download, back up, then wait for confirm
async fn firmware_install(State(state): State<Arc<WebState>>, Json(req): Json<FirmwareRequest>) -> impl IntoResponse {
    match &state.firmware {
        Some(f) => firmware_result(f.start_install(&req.dongle.to_uppercase()).await),
        None => firmware_unavailable(),
    }
}

// POST /api/firmware/confirm: flash the job in review
async fn firmware_confirm(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    match &state.firmware {
        Some(f) => firmware_result(f.confirm().await),
        None => firmware_unavailable(),
    }
}

// POST /api/firmware/cancel: drop the job in review
async fn firmware_cancel(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    match &state.firmware {
        Some(f) => match f.cancel() {
            Ok(()) => (StatusCode::OK, Json(json!({ "job": f.job() }))),
            Err(e) => (StatusCode::CONFLICT, Json(json!({ "error": e }))),
        },
        None => firmware_unavailable(),
    }
}

// POST /api/firmware/restore {dongle}: stock firmware back from its backup
async fn firmware_restore(State(state): State<Arc<WebState>>, Json(req): Json<FirmwareRequest>) -> impl IntoResponse {
    match &state.firmware {
        Some(f) => firmware_result(f.start_restore(&req.dongle.to_uppercase()).await),
        None => firmware_unavailable(),
    }
}

// POST /api/firmware/relay {dongle, password?}: a bridged relay's own firmware
async fn firmware_relay(State(state): State<Arc<WebState>>, Json(req): Json<FirmwareRequest>) -> impl IntoResponse {
    match &state.firmware {
        Some(f) => firmware_result(f.start_relay_update(&req.dongle.to_uppercase(), req.password).await),
        None => firmware_unavailable(),
    }
}

#[derive(Deserialize)]
struct SensorUpdateRequest {
    gateway: String,
    sensors: Vec<String>,
    board: String,
}

// POST /api/firmware/sensors {gateway, sensors, board}: sensors over the air through an OpenWyzeSense gateway
async fn firmware_sensors(State(state): State<Arc<WebState>>, Json(req): Json<SensorUpdateRequest>) -> impl IntoResponse {
    match &state.firmware {
        Some(f) => match f.start_sensor_update(&req.gateway.to_uppercase(), req.sensors, &req.board).await {
            Ok(job) => (StatusCode::OK, Json(json!({ "sensor_job": job }))),
            Err(e) => (StatusCode::CONFLICT, Json(json!({ "error": e }))),
        },
        None => firmware_unavailable(),
    }
}

// POST /api/firmware/sensors/cancel
async fn firmware_sensors_cancel(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    match &state.firmware {
        Some(f) => match f.cancel_sensor_update() {
            Ok(()) => (StatusCode::OK, Json(json!({ "ok": true }))),
            Err(e) => (StatusCode::CONFLICT, Json(json!({ "error": e }))),
        },
        None => firmware_unavailable(),
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
            let expected_response = match (pkt.cmd(), pkt.payload_bytes().and_then(|b| b.first())) {
                (crate::protocol::ows::CMD_OWS, Some(&sub)) => crate::protocol::ows::response_key(sub),
                (crate::protocol::packet::commands::CMD_GET_SENSOR_LIST, _)
                | (crate::protocol::packet::commands::CMD_SEND_KEYPAD, _) => pkt.cmd(),
                (cmd, _) => cmd + 1,
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
        .sensor-card { transition: border-color .2s ease; }
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

    <script>
        const API = "";
        let donglesData = { dongles: [], unassociated_sensors: [] };
        let fwData = { release: null, job: null, sensor_job: null, dongles: {} };
        let roamingData = { enabled: false };
        let fwDismissed = null;
        let sensorDismissed = null;
        let scanState = {};
        let openSensors = new Set();
        let openDongles = new Set();
        let heardExpanded = new Set();
        let hexLogs = {};
        let renameTarget = null;
        let pendingRender = false;

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

        function dongleByMac(mac) { return (donglesData.dongles || []).find(d => d.mac === mac) || null; }
        function allSensors() {
            return [...(donglesData.dongles || []).flatMap(d => d.sensors || []), ...(donglesData.unassociated_sensors || [])];
        }
        function sensorByMac(mac) { return allSensors().find(s => s.mac === mac) || null; }
        function dongleName(mac) { const d = dongleByMac(mac); return d?.name || mac; }
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
        function conflictText(c) {
            return c.held_by
                ? `It uses short address ${c.short}, which ${c.held_by} holds on ${c.gateway}`
                : `Gateway ${c.gateway} answers it without holding it in its table`;
        }

        // ===== OTA / firmware state =====
        function fwBusy() {
            return !!(fwData.job && ["download","backup","review","flash","replug","restart"].includes(fwData.job.step));
        }
        function otaEntry(d, s) {
            if (!d) return null;
            const f = fwData.dongles?.[d.mac];
            const ota = f?.ota;
            if (!ota || !ota.supported) return null;
            const entry = (ota.sensors || []).find(x => x.mac === s.mac);
            if (!entry || !entry.board) return null;
            return {board: entry.board, target: fwData.release || "", entry};
        }
        function sensorJobRow(mac) {
            const j = fwData.sensor_job;
            if (!j || !["download","running"].includes(j.step)) return null;
            return (j.sensors || []).find(r => r.mac === mac) || null;
        }

        // ===== Heard-by gateways =====
        function heardBlock(s) {
            if (!roamingData.enabled) return '';
            const rows = [...(s.heard || [])].sort((a, b) => b.rssi - a.rssi);
            if (!rows.length) return '';
            const n = rows.length;
            const best = rows[0].rssi;
            const open = heardExpanded.has(s.mac);
            const visible = open ? rows : rows.slice(0, 3);
            const items = rows.map((h, i) => {
                const hidden = !open && i >= 3;
                const owner = s.dongle_mac === h.mac;
                const cls = sigText(h.rssi).cls;
                return `<div data-heard-extra="${s.mac}" class="${hidden ? 'hidden ' : ''}flex items-center justify-between gap-2 rounded-lg border ${owner ? 'border-teal-800/60 bg-teal-950/20' : 'border-slate-800 bg-slate-900/40'} px-3 py-1.5 text-xs">
                    <div class="flex items-center gap-2 min-w-0">
                        <span class="w-1.5 h-1.5 rounded-full ${owner ? 'bg-teal-400' : 'bg-slate-600'} shrink-0"></span>
                        <span class="font-mono text-slate-300 truncate">${esc(h.mac)}</span>
                        ${owner ? '<span class="tag border border-teal-900 bg-teal-950/30 text-teal-400">owner</span>' : (h.rssi === best ? '<span class="tag border border-emerald-900 bg-emerald-950/20 text-emerald-400">best</span>' : '')}
                    </div>
                    <div class="flex items-center gap-2 shrink-0">
                        ${signalInline(h.rssi)}
                        <span class="text-slate-600">${relSeen(h.at)}</span>
                    </div>
                </div>`;
            }).join('');
            return `<div class="mt-4">
                <div class="flex items-center gap-2 mb-1.5 flex-wrap">
                    <div class="text-[10px] uppercase tracking-wide text-slate-600">Heard by ${n} gateway${n !== 1 ? 's' : ''}</div>
                    <span class="tag border border-teal-900 bg-teal-950/20 text-teal-400">roaming${n > 1 ? ` · best ${best} dBm` : ''}</span>
                </div>
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-1.5">${items}</div>
                ${n > 3 ? `<button type="button" data-heard-btn="${s.mac}" data-total="${n}" onclick="toggleHeardList('${s.mac}', this)" class="mt-2 text-[11px] font-semibold text-teal-400 hover:text-teal-300 px-1">${open ? 'Show less' : `Show all ${n}`}</button>` : ''}
            </div>`;
        }
        function toggleHeardList(mac, btn) {
            const extras = document.querySelectorAll(`[data-heard-extra="${mac}"]`);
            const hidden = [...extras].some(el => el.classList.contains('hidden'));
            extras.forEach(el => el.classList.toggle('hidden', !hidden));
            const total = btn ? btn.dataset.total : "";
            document.querySelectorAll(`[data-heard-btn="${mac}"]`).forEach(b => {
                b.textContent = hidden ? "Show less" : `Show all ${b.dataset.total || total}`;
            });
            if (hidden) heardExpanded.add(mac); else heardExpanded.delete(mac);
        }

        // ===== Sensor detail =====
        function detailCard(s, d) {
            const gatewayMac = d?.mac || s.dongle_mac || null;
            const volts = s.battery_raw != null ? (s.battery_raw / 32).toFixed(2) + " V" : null;
            const ota = otaEntry(d, s);
            const job = sensorJobRow(s.mac);
            const updating = !!job;
            const conflict = s.address_conflict;
            return `<div class="rounded-xl border border-slate-800 bg-slate-950/70 p-3 sm:p-4">
                <div class="flex items-center justify-between gap-2 mb-3">
                    <div class="min-w-0">
                        <div class="font-semibold text-sm text-slate-200 truncate">${esc(s.name || s.mac)}</div>
                        <div class="font-mono text-[11px] text-teal-500/80">${esc(s.mac)}</div>
                    </div>
                    <button onclick="openRename('${s.mac}')" class="shrink-0 inline-flex items-center gap-1.5 py-1.5 px-3 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">✏️ Rename</button>
                </div>
                <div class="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-3">
                    ${detailCell("Sensor type", esc(typeLabel(s.sensor_type)))}
                    ${detailCell("Firmware", esc(s.sw_version || "Unknown"))}
                    ${detailCell("MAC", `<span class="font-mono">${esc(s.mac)}</span>`)}
                    ${detailCell("Gateway", `<span class="font-mono">${esc(gatewayMac || "—")}</span>`)}
                    ${detailCell("Last seen", esc(absSeen(s.last_seen)), esc(relSeen(s.last_seen)))}
                    ${detailCell("Signal", signalInline(s.signal), sigText(s.signal).label)}
                    ${detailCell("Battery", s.battery == null ? `${batteryIcon(null)} External power` : batteryTag(s.battery), volts)}
                    ${detailCell("Die temperature", s.die_temperature_c == null ? "—" : `${esc(s.die_temperature_c)} °C`)}
                    ${detailCell("Event sequence", s.event_sequence == null ? "—" : `#${esc(s.event_sequence)}`)}
                    ${detailCell("State", stateTag(s))}
                </div>
                ${ota ? `<div class="mt-3 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 rounded-xl border border-emerald-900/60 bg-emerald-950/20 px-3 py-2.5">
                    <div class="text-xs min-w-0">
                        <div class="font-semibold text-emerald-400">OpenWyzeSense ${esc(ota.target)} is available</div>
                        <div class="text-slate-500 mt-0.5">Current ${esc(s.sw_version || "unknown")} · ${esc(ota.board)} · press the sensor's button briefly when asked</div>
                    </div>
                    <button ${updating ? 'disabled' : ''} onclick="fwUpdateSensor('${d.mac}', '${s.mac}', '${ota.board}')" class="ota-btn-${s.mac} shrink-0 py-2 px-4 ${updating ? 'bg-slate-700 cursor-not-allowed opacity-60' : 'bg-emerald-700 hover:bg-emerald-600'} text-white text-xs font-semibold rounded-lg transition">${updating ? 'Updating…' : '⬆️ Update sensor'}</button>
                </div>` : ''}
                ${heardBlock(s)}
                ${conflict ? `<div class="mt-3 text-xs text-amber-400 bg-amber-950/20 border border-amber-900/40 rounded-lg px-3 py-2">
                    This sensor ${esc(conflictText(conflict))}. It shares a short address with another sensor and can't roam — pair it again.
                </div>` : ''}
            </div>`;
        }

        function updateBadge(s, d) {
            return otaEntry(d, s)
                ? `<span class="tag border border-emerald-900 bg-emerald-950/20 text-emerald-400" title="OpenWyzeSense update available">⬆️ update</span>`
                : '';
        }
        function conflictBadge(s) {
            return s.address_conflict
                ? `<span class="tag border border-amber-900 bg-amber-950/30 text-amber-400" title="${esc(conflictText(s.address_conflict))}">⚠ address</span>`
                : '';
        }

        // ===== Sensor rows / cards =====
        function desktopRow(s, d) {
            const open = openSensors.has(s.mac);
            const mac = s.mac;
            return `<tr data-sensor-row="${mac}" class="sensor-row cursor-pointer text-xs border-t border-slate-800/40${open ? ' open' : ''}" onclick="toggleSensor('${mac}')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();toggleSensor('${mac}')}" tabindex="0">
                    <td class="py-2.5 pl-3 pr-1 w-6"><span data-sensor-chev="${mac}" class="chevron text-slate-500 text-[10px]${open ? ' open' : ''}">▶</span></td>
                    <td class="py-2.5 px-2">
                        <div class="font-semibold text-slate-200 truncate max-w-[11rem]">${esc(s.name || s.mac)}</div>
                        <div class="font-mono text-[11px] text-teal-500/80">${esc(mac)}</div>
                    </td>
                    <td class="py-2.5 px-2">${typeTag(s)}${updateBadge(s, d)}${conflictBadge(s)}</td>
                    <td class="py-2.5 px-2">${stateTag(s)}</td>
                    <td class="py-2.5 px-2">${batteryTag(s.battery)}</td>
                    <td class="py-2.5 px-2">${signalInline(s.signal)}</td>
                    <td class="py-2.5 px-2 text-slate-400">${relSeen(s.last_seen)}</td>
                </tr>
                <tr data-sensor-detail-tr="${mac}" class="${open ? '' : 'hidden'}">
                    <td colspan="7" class="p-0">
                        <div data-sensor-wrap="${mac}" class="detail-wrap${open ? ' open' : ''}"><div class="detail-inner"><div class="px-3 pb-3 pt-0">${detailCard(s, d)}</div></div></div>
                    </td>
                </tr>`;
        }

        function mobileCard(s, d) {
            const open = openSensors.has(s.mac);
            const mac = s.mac;
            return `<div data-sensor-row="${mac}" class="sensor-card border-b border-slate-800/50 last:border-b-0${open ? ' open' : ''}">
                <button class="w-full text-left px-4 py-3 flex items-start gap-3" onclick="toggleSensor('${mac}')">
                    <span data-sensor-chev="${mac}" class="chevron ${open ? 'open ' : ''}mt-1 text-slate-500 text-[10px]">▶</span>
                    <div class="min-w-0 flex-1">
                        <div class="flex items-start justify-between gap-2">
                            <div class="min-w-0">
                                <div class="font-semibold text-sm text-slate-100 truncate">${esc(s.name || s.mac)}</div>
                                <div class="font-mono text-[11px] text-teal-500/80 truncate">${esc(mac)}</div>
                            </div>
                            <div class="shrink-0">${stateTag(s)}</div>
                        </div>
                        <div class="mt-2 flex flex-wrap items-center gap-1.5">${typeTag(s)}${updateBadge(s, d)}${conflictBadge(s)}${batteryTag(s.battery)}</div>
                        <div class="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-slate-500">
                            ${signalInline(s.signal)}
                            <span>Seen ${relSeen(s.last_seen)}</span>
                        </div>
                    </div>
                </button>
                <div data-sensor-wrap="${mac}" class="detail-wrap${open ? ' open' : ''}"><div class="detail-inner"><div class="px-4 pb-3">${detailCard(s, d)}</div></div></div>
            </div>`;
        }

        function sensorList(sensors, d) {
            if (!sensors || !sensors.length) return `<div class="text-center text-xs text-slate-500 py-3 italic">No sensors</div>`;
            return `<table class="hidden sm:table w-full text-left text-slate-300">
                    <thead class="text-slate-500 text-[11px] uppercase">
                        <tr>
                            <th class="py-1.5 pl-3 pr-1"></th>
                            <th class="py-1.5 px-2">Sensor</th><th class="py-1.5 px-2">Type</th><th class="py-1.5 px-2">State</th>
                            <th class="py-1.5 px-2">Battery</th><th class="py-1.5 px-2">Signal</th><th class="py-1.5 px-2">Seen</th>
                        </tr>
                    </thead>
                    <tbody>${sensors.map(s => desktopRow(s, d)).join("")}</tbody>
                </table>
                <div class="sm:hidden">${sensors.map(s => mobileCard(s, d)).join("")}</div>`;
        }

        function toggleSensor(mac) {
            const open = !openSensors.has(mac);
            if (open) openSensors.add(mac); else openSensors.delete(mac);
            document.querySelectorAll(`[data-sensor-wrap="${mac}"]`).forEach(el => el.classList.toggle('open', open));
            document.querySelectorAll(`[data-sensor-chev="${mac}"]`).forEach(el => el.classList.toggle('open', open));
            document.querySelectorAll(`[data-sensor-row="${mac}"]`).forEach(el => el.classList.toggle('open', open));
            document.querySelectorAll(`[data-sensor-detail-tr="${mac}"]`).forEach(el => el.classList.toggle('hidden', !open));
        }

        // ===== Dongle =====
        function dongleDetails(d) {
            const o = d.openwyzesense;
            const conn = d.transport === "bridge" ? (d.remote_addr || "—") : (d.device_path || "—");
            return `<div class="border-t border-slate-800/50 bg-slate-950/40 px-4 sm:px-5 py-4">
                <div class="flex items-start justify-between gap-2 mb-3">
                    <div class="min-w-0">
                        <div class="text-[10px] uppercase tracking-wide text-slate-600 mb-1">Dongle</div>
                        <div class="font-semibold text-sm text-slate-200 break-all">${esc(d.name || ("Dongle " + d.mac))}</div>
                        <div class="font-mono text-[11px] text-teal-500/80">${esc(d.mac)}</div>
                    </div>
                    <button onclick="openDongleRename('${d.mac}')" class="shrink-0 inline-flex items-center gap-1.5 py-1.5 px-3 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">✏️ Rename</button>
                </div>
                <div class="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-3">
                    ${detailCell("Transport", d.transport === "bridge" ? "🌐 Bridge" : "🔌 Local")}
                    ${detailCell("Connection", `<span class="font-mono break-all">${esc(conn)}</span>`)}
                    ${detailCell("Firmware", esc(d.version || "Unknown"))}
                    ${detailCell("OpenWyzeSense", o ? `${esc(o.version || "—")} <span class="text-slate-600">· ${esc(o.build?.product || "")}</span>` : "Stock")}
                    ${detailCell("Network", `<span class="font-mono text-[11px] break-all">${esc(o?.network || "—")}</span>`)}
                    ${detailCell("Roaming", o ? (o.roaming ? "On" : "Off") : "Not supported")}
                    ${detailCell("Flags", o ? `<span class="font-mono text-[11px]">${esc(o.flags_supported || "—")} / ${esc(o.flags_enabled || "—")}</span>` : "—", o ? "supported / enabled" : null)}
                    ${detailCell("Build ID", `<span class="font-mono text-[11px] break-all">${esc(o?.build?.id || "—")}</span>`)}
                </div>
                ${dongleFirmware(d)}
                ${dongleActions(d)}
            </div>`;
        }

        function relayFirmware(d, r, busy) {
            if (!r) return '';
            const current = r.build && fwData.release && r.build.startsWith(fwData.release);
            const pw = r.auth ? `<input data-relay-pass="${d.mac}" type="password" placeholder="Relay admin password (not stored)" class="w-full mb-2 bg-slate-950 border border-slate-800 text-sm rounded-xl px-3 py-2 focus:outline-none focus:border-teal-500">` : '';
            return `<div class="mt-4 border-t border-slate-800/60 pt-3">
                <div class="text-[10px] uppercase tracking-wide text-slate-600 mb-1.5">🔧 Relay firmware</div>
                <p class="text-xs text-slate-500 mb-2 break-words">${esc(r.name || "")}: <span class="text-slate-300">${esc(r.chip || "")} · ${esc(r.build || "")}</span>.
                    The relay restarts into the new firmware and updates its contact board's radio, then reconnects (about a minute).
                    A build that can't rejoin Wi-Fi rolls back by itself.</p>
                ${r.product ? `${pw}<button ${busy ? 'disabled' : ''} onclick="fwRelay('${d.mac}')" class="w-full py-2.5 bg-emerald-700 hover:bg-emerald-600 disabled:opacity-40 text-white text-xs font-semibold rounded-lg transition">${current ? 'Reinstall' : 'Update relay to'} OpenWyzeSense ${esc(fwData.release || "")}</button>`
                    : `<p class="text-xs text-amber-400">No relay firmware for a ${esc(r.chip || "unknown")}.</p>`}
            </div>`;
        }

        function dongleFirmware(d) {
            const f = fwData.dongles?.[d.mac];
            if (!f) return '';
            const busy = fwBusy();
            if (d.transport === "bridge") return relayFirmware(d, f.relay, busy);
            const ows = !!d.openwyzesense;
            const release = esc(fwData.release || "");
            const install = ows ? `Reinstall / update to OpenWyzeSense ${release}` : `Install OpenWyzeSense ${release}`;
            const restore = ows && f.can_restore_stock
                ? `<button ${busy ? 'disabled' : ''} onclick="fwRestore('${d.mac}')" class="w-full mt-2 py-2.5 bg-slate-800 hover:bg-slate-700 disabled:opacity-40 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">Restore stock firmware</button>`
                : '';
            const backups = (f.backups || []).length;
            return `<div class="mt-4 border-t border-slate-800/60 pt-3">
                <div class="flex items-center gap-2 mb-1.5 flex-wrap">
                    <div class="text-[10px] uppercase tracking-wide text-slate-600">🔧 Firmware</div>
                    ${busy ? '<span class="tag border border-amber-900 bg-amber-950/20 text-amber-400">job running</span>' : ''}
                </div>
                <p class="text-xs text-slate-500 mb-2 break-words">Running: <span class="text-slate-300">${esc(f.firmware || "unknown")}</span>.
                    The dongle is backed up first (the only way back to stock), then you see which sensors carry over before anything is written.
                    It is offline for about a minute.${backups ? ` ${backups} backup${backups > 1 ? 's' : ''} saved.` : ''}</p>
                <button ${busy ? 'disabled' : ''} onclick="fwInstall('${d.mac}')" class="w-full py-2.5 bg-emerald-700 hover:bg-emerald-600 disabled:opacity-40 text-white text-xs font-semibold rounded-lg transition">${install}</button>
                ${restore}
            </div>`;
        }

        function dongleActions(d) {
            const ss = scanState[d.mac] || {active: false};
            const scanning = !!ss.active;
            const pairBtn = `<button data-pair-btn="${d.mac}" onclick="toggleScan('${d.mac}')" class="w-full py-2.5 ${scanning ? 'bg-rose-700 hover:bg-rose-600' : 'bg-teal-600 hover:bg-teal-500'} text-white text-xs font-semibold rounded-lg transition">${scanning ? `⏹ Stop scan${ss.secondsLeft != null ? ` (${ss.secondsLeft}s)` : ""}` : "📡 Pair new sensor"}</button>`;
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
                        <button onclick="runFix('${d.mac}', this)" class="w-full py-2.5 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700 transition">Purge ghost sensors</button>
                    </div>
                    <div class="rounded-xl border border-slate-800 bg-slate-900/40 p-3 lg:col-span-2">
                        <div class="text-xs font-semibold text-slate-300 mb-1">💻 Hex console</div>
                        <p class="text-[11px] text-slate-500 mb-2">Send a raw packet and see the dongle's reply.</p>
                        <div class="flex gap-2">
                            <input data-hex-input="${d.mac}" placeholder="AA,55,43..." class="flex-1 min-w-0 bg-slate-950 border border-slate-800 text-xs rounded-lg px-3 py-2 font-mono focus:outline-none focus:border-teal-500">
                            <button onclick="sendRaw('${d.mac}')" class="shrink-0 py-2 px-4 bg-slate-800 hover:bg-slate-700 font-semibold text-xs rounded-lg border border-slate-700 transition">Send</button>
                        </div>
                        <div data-hex-log="${d.mac}" class="rounded-lg border border-slate-800 bg-slate-950 p-2 mt-2 font-mono text-[11px] text-emerald-400 min-h-[44px] max-h-[120px] overflow-y-auto overflow-x-hidden break-all space-y-0.5">${hexLogHtml(d.mac)}</div>
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
            const el = document.querySelector(`[data-hex-log="${mac}"]`);
            if (el) { el.innerHTML = hexLogHtml(mac); el.scrollTop = el.scrollHeight; }
        }

        function dongleCard(d) {
            const n = (d.sensors || []).length;
            const open = openDongles.has(d.mac);
            const dot = `<span class="w-2.5 h-2.5 rounded-full shrink-0 ${d.scanning ? 'bg-amber-400 animate-pulse' : 'bg-teal-400'}"></span>`;
            const tr = d.transport === "bridge"
                ? `<span class="tag border border-violet-900 bg-violet-950/20 text-violet-400">🌐 Bridge</span><span class="text-xs text-slate-500 font-mono ml-1">${esc(d.remote_addr || "?")}</span>`
                : `<span class="tag border border-teal-900 bg-teal-950/20 text-teal-400">🔌 Local</span><span class="text-xs text-slate-500 font-mono ml-1">${esc(d.device_path || "?")}</span>`;
            return `<div class="bg-slate-900 rounded-2xl border border-slate-800 shadow-xl overflow-hidden">
                <div class="px-4 sm:px-5 py-3.5 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 sm:gap-0">
                    <div class="flex items-center gap-3 min-w-0">
                        ${dot}
                        <div class="min-w-0">
                            <div class="flex items-center gap-2 flex-wrap">
                                <span class="font-bold text-sm break-all">🕹️ ${esc(d.name || ("Dongle " + d.mac))}</span>
                                ${d.scanning ? '<span class="tag border border-amber-900 bg-amber-950/20 text-amber-400">SCANNING</span>' : ''}
                                <span class="tag border border-slate-800 text-slate-400">${n} sensor${n !== 1 ? 's' : ''}</span>
                                ${d.openwyzesense ? `<span class="tag border border-emerald-900 bg-emerald-950/20 text-emerald-400">OpenWyzeSense ${esc(d.openwyzesense.version || "")}${d.openwyzesense.roaming ? ' · roaming' : ''}</span>` : (d.transport !== 'bridge' ? '<span class="tag border border-slate-800 text-slate-500">Stock firmware</span>' : '')}
                            </div>
                            <div class="flex items-center gap-2 mt-0.5 flex-wrap">
                                <span class="text-xs text-slate-500 font-mono">${esc(d.mac)}</span>
                                <span class="text-slate-700">·</span>
                                <span class="text-xs text-slate-500 font-mono">${esc(d.version || 'Unknown firmware')}</span>
                                <span class="text-slate-700">·</span> ${tr}
                            </div>
                        </div>
                    </div>
                    <button data-dongle-btn="${d.mac}" onclick="toggleDongleDetails('${d.mac}')" class="w-full sm:w-auto sm:ml-3 shrink-0 py-1.5 px-4 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-semibold rounded-lg border border-slate-700 transition flex items-center justify-center gap-1.5 whitespace-nowrap">${open ? 'ⓘ Hide details' : 'ⓘ Details'}</button>
                </div>
                <div data-dongle-detail="${d.mac}" class="dongle-detail${open ? ' open' : ''}"><div class="dongle-detail-inner">${dongleDetails(d)}</div></div>
                <div class="border-t border-slate-800/50">
                    <div class="px-2 py-1">${sensorList(d.sensors, d)}</div>
                </div>
            </div>`;
        }

        function orphanCard(sensors) {
            if (!sensors?.length) return '';
            return `<div class="bg-slate-900/50 rounded-2xl border border-dashed border-slate-700 shadow-xl overflow-hidden">
                <div class="px-4 sm:px-5 py-3.5 flex items-center gap-3">
                    <span class="w-2.5 h-2.5 rounded-full bg-slate-600 shrink-0"></span>
                    <div class="min-w-0"><span class="font-bold text-sm text-slate-400">📦 Unassociated Sensors</span>
                        <div class="text-xs text-slate-600">Restored from state — no active dongle</div></div>
                    <span class="tag border border-slate-700 text-slate-500 ml-auto shrink-0">${sensors.length}</span>
                </div>
                <div class="border-t border-slate-800/50"><div class="px-2 py-1">${sensorList(sensors, null)}</div></div>
            </div>`;
        }

        function toggleDongleDetails(mac) {
            const open = !openDongles.has(mac);
            if (open) openDongles.add(mac); else openDongles.delete(mac);
            document.querySelectorAll(`[data-dongle-detail="${mac}"]`).forEach(el => el.classList.toggle('open', open));
            document.querySelectorAll(`[data-dongle-btn="${mac}"]`).forEach(btn => { btn.innerHTML = open ? 'ⓘ Hide details' : 'ⓘ Details'; });
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
            document.querySelectorAll(`[data-pair-btn="${mac}"]`).forEach(btn => {
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
                    scanState[mac] = {active: true, secondsLeft: 60};
                    clearScanTimers(mac);
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
                                clearScanTimers(mac);
                                scanState[mac].active = false;
                                updatePairButton(mac);
                                toast("Sensor paired", "ok");
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
                const r = await fetch(`${API}/api/fix`, {method: "POST"});
                const d = await r.json().catch(() => ({}));
                if (!r.ok) { toast(d.error || "Purge failed", "error"); return; }
                toast(`Purged ${d.purged_count || 0} ghost sensor${d.purged_count === 1 ? "" : "s"}`, "ok");
                await loadDongles();
            } catch (e) { toast("Purge failed", "error"); }
        }
        async function sendRaw(mac) {
            const input = document.querySelector(`[data-hex-input="${mac}"]`);
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

        // ===== Firmware =====
        async function fwPost(path, body) {
            const {ok, data} = await postJson(`/api/firmware/${path}`, body);
            await loadDongles();
            if (!ok) toast(data.error || "Firmware request failed", "error");
            return ok;
        }
        async function fwInstall(mac) { fwDismissed = null; if (await fwPost("install", {dongle: mac})) toast(`Firmware install started on ${dongleName(mac)}`, "ok"); }
        async function fwConfirm() { await fwPost("confirm"); }
        async function fwCancel() { await fwPost("cancel"); }
        async function fwRestore(mac) { fwDismissed = null; if (await fwPost("restore", {dongle: mac})) toast(`Restoring stock firmware on ${dongleName(mac)}`, "ok"); }
        async function fwRelay(mac) {
            fwDismissed = null;
            const password = document.querySelector(`[data-relay-pass="${mac}"]`)?.value || null;
            if (await fwPost("relay", {dongle: mac, password})) toast(`Relay update started on ${dongleName(mac)}`, "ok");
        }
        async function fwUpdateSensor(gateway, mac, board) {
            sensorDismissed = null;
            if (await fwPost("sensors", {gateway, sensors: [mac], board})) toast(`Sensor update queued for ${sensorName(mac)}`, "ok");
        }

        // ===== Firmware job cards =====
        function fwJobCard() {
            const j = fwData.job;
            if (!j || j.id === fwDismissed || j.step === "cancelled") return "";
            const pct = j.total ? Math.floor(j.done * 100 / j.total) : null;
            const bar = pct === null ? "" : `<div class="w-full bg-slate-800 rounded-full h-2 mt-2"><div class="bg-emerald-500 h-2 rounded-full transition-all" style="width:${pct}%"></div></div>`;
            const what = j.kind === "restore" ? "Restoring stock firmware"
                : j.kind === "relay" ? `Updating relay firmware to ${esc(j.target || fwData.release)}`
                : `Installing OpenWyzeSense ${esc(j.target || fwData.release)}`;
            let body = "", buttons = "";
            const dismiss = `<button onclick="fwDismissed=${j.id};render()" class="py-1.5 px-4 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700">Dismiss</button>`;
            switch (j.step) {
                case "download": body = "Downloading and checking the firmware…"; break;
                case "backup": body = `Backing up the whole chip. The dongle is offline meanwhile.${pct !== null ? ` ${pct}%` : ""}`; break;
                case "review": {
                    const c = j.carried || {sensors: []};
                    const repair = j.dropped.filter(x => x.mac);
                    body = `Backed up (it was running <b>${esc(j.current)}</b>; saved as ${esc(j.backup)}).<br>
                        <span class="text-emerald-400">${c.sensors.length} sensor${c.sensors.length !== 1 ? "s" : ""} carry over</span>${c.sensors.length ? ": " + c.sensors.map(esc).join(", ") : ""}${c.network ? "" : " · <span class=\"text-amber-400\">no network identity found</span>"}.
                        ${j.dropped.length ? `<div class="mt-2 text-amber-400">Not carried over${repair.length ? ` (pair ${repair.map(x => esc(x.mac)).join(", ")} again afterwards)` : ""}:<ul class="list-disc ml-5">${j.dropped.map(x => `<li>${esc(x.what)}: ${esc(x.why)}</li>`).join("")}</ul></div>` : ""}`;
                    buttons = `<button onclick="fwConfirm()" class="py-1.5 px-4 bg-emerald-700 hover:bg-emerald-600 text-white text-xs font-semibold rounded-lg">Install now</button>
                        <button onclick="fwCancel()" class="py-1.5 px-4 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700">Cancel</button>`;
                    break;
                }
                case "flash": body = j.kind === "relay"
                    ? `Uploading to the relay (it was running ${esc(j.current)}).${pct !== null ? ` ${pct}%` : ""}`
                    : `Writing (${esc(j.phase || "starting")}). Don't unplug the dongle.${pct !== null ? ` ${pct}%` : ""}`; break;
                case "restart": body = "The relay is restarting into the new firmware and updating its radio; it reconnects in about a minute."; break;
                case "replug": body = '<span class="text-amber-400 font-semibold">Written and verified. Unplug the dongle and plug it back in.</span>'; break;
                case "done": body = `<span class="text-emerald-400">Done.</span> ${j.kind === "relay" ? `The relay runs ${esc(j.current)}.` : j.kind === "restore" ? "The dongle runs stock firmware again; sensors paired since the backup need pairing again." : "The dongle runs OpenWyzeSense; its features stay off until turned on in the openwyzesense config section."}
                        ${j.dropped.filter(x => x.mac).length ? `<span class="text-amber-400">Pair ${j.dropped.filter(x => x.mac).map(x => esc(x.mac)).join(", ")} again.</span>` : ""}`;
                    buttons = dismiss; break;
                case "failed": body = `<span class="text-rose-400">Failed: ${esc(j.message)}</span>`; buttons = dismiss; break;
            }
            return `<div class="bg-slate-900 rounded-2xl border border-emerald-900 shadow-xl px-5 py-4 mb-4">
                <div class="font-bold text-sm text-emerald-400 break-words">🔧 ${what} · dongle ${esc(j.dongle)}</div>
                <div class="text-xs text-slate-400 mt-1 break-words">${body}</div>${bar}
                ${buttons ? `<div class="flex flex-wrap gap-2 mt-3">${buttons}</div>` : ""}
            </div>`;
        }

        function sensorJobCard() {
            const j = fwData.sensor_job;
            if (!j || j.id === sensorDismissed) return "";
            const active = ["download", "running"].includes(j.step);
            const label = {queued: "Waiting for its turn", press_button: "Press its button briefly now (under 1 s)", updating: "Updating", done: "Done", refused: "Turned down", failed: "Failed", timed_out: "Timed out (button not pressed, or not running OpenWyzeSense)", stopped: "Stopped"};
            const color = {done: "text-emerald-400", refused: "text-amber-400", failed: "text-rose-400", timed_out: "text-amber-400", press_button: "text-teal-300 font-semibold"};
            const rows = j.sensors.map(r => {
                const pct = r.total ? Math.floor(r.done * 100 / r.total) : null;
                return `<div class="py-1"><div class="flex flex-wrap justify-between gap-x-2 text-xs"><span class="font-mono text-slate-300 break-all">${esc(r.mac)}</span>
                    <span class="min-w-0 break-words ${color[r.state] || "text-slate-400"}">${label[r.state] || esc(r.state)}${r.state === "updating" && pct !== null ? ` ${pct}%` : ""}${r.message ? `: ${esc(r.message)}` : ""}</span></div>
                    ${r.state === "updating" && pct !== null ? `<div class="w-full bg-slate-800 rounded-full h-1.5 mt-1"><div class="bg-emerald-500 h-1.5 rounded-full" style="width:${pct}%"></div></div>` : ""}</div>`;
            }).join("");
            const head = j.step === "download" ? "Downloading the sensor firmware…"
                : j.step === "running" ? "Offers are up for 6 minutes after the last activity."
                : j.step === "failed" ? `<span class="text-rose-400">Failed: ${esc(j.message)}</span>`
                : j.step === "cancelled" ? "Cancelled." : "Finished.";
            const btn = active
                ? `<button onclick="fwPost('sensors/cancel')" class="py-1.5 px-4 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700">Stop</button>`
                : `<button onclick="sensorDismissed=${j.id};render()" class="py-1.5 px-4 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold rounded-lg border border-slate-700">Dismiss</button>`;
            return `<div class="bg-slate-900 rounded-2xl border border-emerald-900 shadow-xl px-5 py-4 mb-4">
                <div class="font-bold text-sm text-emerald-400 break-words">📶 Updating ${esc(j.board)} sensors to ${esc(j.target)} · gateway ${esc(j.gateway)}</div>
                <div class="text-xs text-slate-400 mt-1 mb-2 break-words">${head}</div>${rows}
                <div class="flex flex-wrap gap-2 mt-3">${btn}</div></div>`;
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
                c.innerHTML = fwJobCard() + sensorJobCard() + `<div class="text-center py-16"><div class="text-4xl mb-3">📡</div>
                    <div class="text-slate-400 text-sm">No dongles connected</div>
                    <div class="text-slate-600 text-xs mt-1">Connect a USB dongle or start a WebSocket bridge</div></div>`;
                return;
            }
            c.innerHTML = fwJobCard() + sensorJobCard()
                + dongles.map(dongleCard).join("") + orphanCard(orphans);
        }

        function editingInline() {
            const el = document.activeElement;
            const c = document.getElementById("dongles-container");
            return !!(el && c && c.contains(el) && (el.tagName === "INPUT" || el.tagName === "TEXTAREA" || el.tagName === "SELECT"));
        }

        async function loadDongles() {
            try {
                const [r, fr, rr] = await Promise.all([
                    fetch(`${API}/api/dongles`),
                    fetch(`${API}/api/firmware`),
                    fetch(`${API}/api/roaming`),
                ]);
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                donglesData = await r.json();
                fwData = await fr.json().catch(() => fwData);
                roamingData = await rr.json().catch(() => roamingData);
                // Reflect a scan started elsewhere; local timers own their countdown.
                (donglesData.dongles || []).forEach(d => {
                    const ss = scanState[d.mac];
                    if (d.scanning && !ss?.active) scanState[d.mac] = {active: true, secondsLeft: null};
                    if (!d.scanning && ss?.active && !ss.timer) scanState[d.mac] = {active: false};
                });
                updateHeader();
                if (editingInline()) { pendingRender = true; return; }
                render();
                pendingRender = false;
            } catch (e) {
                document.getElementById("dongles-container").innerHTML = `<div class="text-center text-sm text-rose-500 py-12">Failed to load</div>`;
            }
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
    use crate::protocol::packet::commands;
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
            roaming: None,
            firmware: None,
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
    async fn api_raw_handles_standard_ows_and_ack_only_commands() {
        use crate::protocol::ows::{self, NetworkIdentity};
        let net = NetworkIdentity { pan: 0x65FE, short: 0x0000, ext: 0x00124B00E2E0000A };
        let dongle = VirtualDongle::new("DONGLE01", "OWS").with_ows(net, vec![]);

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
            roaming: None,
            firmware: None,
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

        // 2. OpenWyzeSense command: CMD_OWS (0x53F0 [0x01] -> 0x53F1 [0x01, ...])
        let ows_probe = Packet::new_async((ows::CMD_OWS & 0xFF) as u8, vec![ows::SUB_FLAGS]).to_bytes();
        let resp = tokio::time::timeout(
            Duration::from_millis(500),
            send_raw_packet(
                State(Arc::clone(&state)),
                Json(RawPacketRequest { bytes: ows_probe, dongle_mac: "DONGLE01".to_string() }),
            ),
        )
        .await
        .expect("0x53F0 via /api/raw must not time out waiting for 0x53F1")
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        // 3. ACK-only command: CMD_SEND_KEYPAD (0x5353 -> ACK(0x5353))
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
}
