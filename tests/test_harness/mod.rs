//! Test harness that boots the full system stack (minus MQTT) for E2E testing.
//!
//! The `TestHarness` creates engines from `VirtualDongle`s, wires up the
//! `SensorManager`, `EnginesMap`, and the Axum web server on a random port.
//! Tests interact with the system exclusively via HTTP and the event channel.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;

use wyzesense2mqtt_rs::engine::{Engine, EnginesMap};
use wyzesense2mqtt_rs::protocol::sensor::SensorManager;
use wyzesense2mqtt_rs::protocol::telemetry::DongleEvent;
use wyzesense2mqtt_rs::transport::virtual_dongle::VirtualDongle;
use wyzesense2mqtt_rs::web::start_web_server;

/// Full-stack test environment.
pub struct TestHarness {
    pub client: Client,
    pub base_url: String,
    pub engines: EnginesMap,
    pub sensor_manager: Arc<Mutex<SensorManager>>,
    pub event_rx: mpsc::Receiver<DongleEvent>,
    _exit_handles: Vec<tokio::sync::oneshot::Sender<()>>,
}

impl TestHarness {
    /// Boot the full system with the given virtual dongles.
    ///
    /// For each dongle:
    /// 1. Creates an `Engine` with the dongle's transport
    /// 2. Performs the handshake
    /// 3. Runs `get_sensor_list()` for dongles with pre-paired sensors
    /// 4. Registers engine in the `EnginesMap`
    /// 5. Assigns sensors in the `SensorManager`
    ///
    /// Then starts the web server on a random port.
    pub async fn boot(dongles: Vec<VirtualDongle>) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let (event_tx, event_rx) = mpsc::channel::<DongleEvent>(128);
        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<()>(16);

        let engines: EnginesMap =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

        // Use temp files for state persistence in tests
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_path = format!("/tmp/test_harness_{}_config.yaml", test_id);
        let state_path = format!("/tmp/test_harness_{}_state.yaml", test_id);

        let sensor_manager = Arc::new(Mutex::new(SensorManager::new(
            config_path,
            state_path,
        )));

        let mut exit_handles = Vec::new();

        for dongle in &dongles {
            let mut engine = Engine::new(
                dongle.transport(),
                event_tx.clone(),
                None,
            );
            let exit_tx = engine.start();

            // Handshake
            engine.initialize_handshake().await
                .unwrap_or_else(|e| panic!("Handshake failed for dongle {}: {}", dongle.mac(), e));

            let mac = engine.dongle_mac().unwrap_or("unknown").to_string();

            // Sensor list warm-up
            match engine.get_sensor_list().await {
                Ok(sensors_list) => {
                    let mut manager = sensor_manager.lock().unwrap();
                    manager.assign_dongle(&mac, &sensors_list);
                }
                Err(e) => {
                    tracing::warn!("Failed to get sensor list for {}: {}", mac, e);
                }
            }

            // Register in engines map
            let mut map = engines.lock().await;
            map.insert(mac, engine);
            drop(map);

            exit_handles.push(exit_tx);
        }

        // Find a free port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        // Start web server
        let engines_server = Arc::clone(&engines);
        let sm_server = Arc::clone(&sensor_manager);
        let broadcast_server = broadcast_tx.clone();
        let event_tx_server = event_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = start_web_server(
                engines_server, sm_server, broadcast_server, event_tx_server, port, None,
            ).await {
                panic!("Web server failed: {}", e);
            }
        });

        // Give the server a moment to bind
        tokio::time::sleep(Duration::from_millis(100)).await;

        TestHarness {
            client: Client::new(),
            base_url: format!("http://127.0.0.1:{}", port),
            engines,
            sensor_manager,
            event_rx,
            _exit_handles: exit_handles,
        }
    }

    // -----------------------------------------------------------------------
    // HTTP convenience methods
    // -----------------------------------------------------------------------

    /// GET /api/dongles — returns the full JSON response body.
    pub async fn get_dongles(&self) -> Value {
        let resp = self.client
            .get(&format!("{}/api/dongles", self.base_url))
            .send().await.unwrap();
        assert_eq!(resp.status(), 200, "GET /api/dongles failed");
        resp.json().await.unwrap()
    }

    /// POST /api/scan — enable or disable scan on a dongle.
    pub async fn set_scan(&self, dongle_mac: &str, enable: bool) -> reqwest::Response {
        self.client
            .post(&format!("{}/api/scan", self.base_url))
            .json(&serde_json::json!({ "enable": enable, "dongle_mac": dongle_mac }))
            .send().await.unwrap()
    }

    /// POST /api/verify — verify a scanned sensor.
    pub async fn verify_sensor(&self, dongle_mac: &str, sensor_mac: &str, sensor_type: &str) -> reqwest::Response {
        self.client
            .post(&format!("{}/api/verify", self.base_url))
            .json(&serde_json::json!({
                "dongle_mac": dongle_mac,
                "mac": sensor_mac,
                "sensor_type": sensor_type,
            }))
            .send().await.unwrap()
    }

    /// DELETE /api/sensors/:mac — unpair a sensor.
    pub async fn delete_sensor(&self, sensor_mac: &str) -> reqwest::Response {
        self.client
            .delete(&format!("{}/api/sensors/{}", self.base_url, sensor_mac))
            .send().await.unwrap()
    }

    /// POST /api/raw — send a raw packet.
    pub async fn send_raw(&self, dongle_mac: &str, bytes: Vec<u8>) -> reqwest::Response {
        self.client
            .post(&format!("{}/api/raw", self.base_url))
            .json(&serde_json::json!({ "bytes": bytes, "dongle_mac": dongle_mac }))
            .send().await.unwrap()
    }

    /// POST /api/fix — trigger ghost sensor cleanup.
    pub async fn fix_sensors(&self) -> reqwest::Response {
        self.client
            .post(&format!("{}/api/fix", self.base_url))
            .send().await.unwrap()
    }

    /// POST /api/chime/:mac — trigger chime.
    pub async fn trigger_chime(&self, sensor_mac: &str) -> reqwest::Response {
        self.client
            .post(&format!("{}/api/chime/{}", self.base_url, sensor_mac))
            .send().await.unwrap()
    }

    // -----------------------------------------------------------------------
    // Event helpers
    // -----------------------------------------------------------------------

    /// Wait for a DongleEvent on the event channel with a timeout.
    /// Returns `None` if the timeout expires.
    pub async fn expect_event(&mut self, timeout_ms: u64) -> Option<DongleEvent> {
        tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            self.event_rx.recv(),
        ).await.ok().flatten()
    }

    /// Drain all pending events and return them.
    pub async fn drain_events(&mut self) -> Vec<DongleEvent> {
        let mut events = Vec::new();
        loop {
            match tokio::time::timeout(
                Duration::from_millis(100),
                self.event_rx.recv(),
            ).await {
                Ok(Some(event)) => events.push(event),
                _ => break,
            }
        }
        events
    }

    /// Wait for a specific dongle to disconnect and clean up its engine.
    pub async fn await_dongle_disconnect(&self, mac: &str, timeout_ms: u64) -> bool {
        let notify = {
            let engines = self.engines.lock().await;
            match engines.get(mac) {
                Some(eng) => std::sync::Arc::clone(&eng.disconnect_notify),
                None => return true,
            }
        };
        let result = tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            notify.notified(),
        ).await;
        if result.is_ok() {
            let mut map = self.engines.lock().await;
            map.remove(mac);
            let mut manager = self.sensor_manager.lock().unwrap();
            manager.unassign_dongle(mac);
            true
        } else {
            false
        }
    }
}
