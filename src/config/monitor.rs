use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

use crate::protocol::sensor::SensorManager;

pub struct AvailabilityMonitor {
    sensor_manager: Arc<std::sync::Mutex<SensorManager>>,
    check_interval: Duration,
    offline_tx: Sender<String>, // Sends MAC of sensor that went offline
}

impl AvailabilityMonitor {
    pub fn new(
        sensor_manager: Arc<std::sync::Mutex<SensorManager>>,
        check_interval: Duration,
        offline_tx: Sender<String>,
    ) -> Self {
        Self {
            sensor_manager,
            check_interval,
            offline_tx,
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
        let newly_offline = {
            let mut manager = self.sensor_manager.lock().unwrap();
            manager.check_timeouts()
        };

        for mac in newly_offline {
            if let Err(e) = self.offline_tx.send(mac.clone()).await {
                warn!("Failed to send offline notification for {}: {}", mac, e);
            }
        }
    }
}
