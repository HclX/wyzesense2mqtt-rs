use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

use crate::config::sensors::SensorsConfig;
use crate::config::state::SystemState;

pub struct AvailabilityMonitor {
    sensors_config: SensorsConfig,
    state: Arc<RwLock<SystemState>>,
    default_timeout: Duration,
    check_interval: Duration,
    offline_tx: Sender<String>, // Sends MAC of sensor that went offline
    online_sensors: Arc<RwLock<HashMap<String, bool>>>, // Tracks in-memory online status
}

impl AvailabilityMonitor {
    pub fn new(
        sensors_config: SensorsConfig,
        state: Arc<RwLock<SystemState>>,
        default_timeout: Duration,
        check_interval: Duration,
        offline_tx: Sender<String>,
    ) -> Self {
        let online_sensors = Arc::new(RwLock::new(HashMap::new()));
        Self {
            sensors_config,
            state,
            default_timeout,
            check_interval,
            offline_tx,
            online_sensors,
        }
    }

    pub async fn start(self, mut shutdown_rx: tokio::sync::oneshot::Receiver<()>) {
        let mut interval = tokio::time::interval(self.check_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.check_availabilities().await;
                }
                _ = &mut shutdown_rx => {
                    info!("Shutting down availability monitor");
                    break;
                }
            }
        }
    }

    async fn check_availabilities(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let state = self.state.read().await;
        let mut online_sensors = self.online_sensors.write().await;

        for (mac, metadata) in &self.sensors_config.sensors {
            let timeout_sec = metadata.timeout_sec.unwrap_or(self.default_timeout.as_secs());
            
            if let Some(sensor_state) = state.sensors.get(mac) {
                let is_online = now - sensor_state.last_seen < timeout_sec;
                let was_online = *online_sensors.get(mac).unwrap_or(&true); // Default to true

                if !is_online && was_online {
                    warn!("Sensor {} ({}) timed out (last seen {}s ago)", mac, metadata.name, now - sensor_state.last_seen);
                    online_sensors.insert(mac.clone(), false);
                    if let Err(e) = self.offline_tx.send(mac.clone()).await {
                        warn!("Failed to send offline notification for {}: {}", mac, e);
                    }
                } else if is_online && !was_online {
                    info!("Sensor {} ({}) came back online", mac, metadata.name);
                    online_sensors.insert(mac.clone(), true);
                } else if online_sensors.get(mac).is_none() {
                    // Initialize
                    online_sensors.insert(mac.clone(), is_online);
                }
            }
        }
    }
}
