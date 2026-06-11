use rumqttc::v5::{AsyncClient, MqttOptions, Event};
use rumqttc::v5::mqttbytes::{QoS, v5::Packet as MqttPacket};
use tokio::sync::mpsc;
use crate::protocol::telemetry::{DongleEvent, TelemetryData};
use crate::protocol::sensor::SensorManager;
use tracing::{info, error, debug};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq)]
pub enum GatewayCommand {
    Scan(bool),
    Delete(String), // MAC
    Reload,
}

pub struct MqttGateway {
    client: AsyncClient,
    event_loop: rumqttc::v5::EventLoop,
    event_rx: mpsc::Receiver<DongleEvent>,
    cmd_tx: mpsc::Sender<GatewayCommand>,
    topic_root: String,
    published_discovery: Arc<tokio::sync::Mutex<HashSet<String>>>,
    sensor_manager: Arc<Mutex<SensorManager>>,
    broadcast_tx: tokio::sync::broadcast::Sender<()>,
}

impl MqttGateway {
    pub fn new(
        mut mqtt_options: MqttOptions,
        event_rx: mpsc::Receiver<DongleEvent>,
        cmd_tx: mpsc::Sender<GatewayCommand>,
        topic_root: String,
        sensor_manager: Arc<Mutex<SensorManager>>,
        broadcast_tx: tokio::sync::broadcast::Sender<()>,
    ) -> Self {
        mqtt_options.set_clean_start(false);
        let mut connect_props = rumqttc::v5::mqttbytes::v5::ConnectProperties::default();
        connect_props.session_expiry_interval = Some(900); // Keep session for 15m (longer than 10m Will Delay)
        mqtt_options.set_connect_properties(connect_props);

        let (client, event_loop) = AsyncClient::new(mqtt_options, 10);

        Self {
            client,
            event_loop,
            event_rx,
            cmd_tx,
            topic_root,
            published_discovery: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            sensor_manager,
            broadcast_tx,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.clone();
        let topic_root = self.topic_root.clone();
        let cmd_tx = self.cmd_tx.clone();
        let mut event_loop = self.event_loop;

        let control_topic_scan = format!("{}/scan", topic_root);
        let control_topic_remove = format!("{}/remove", topic_root);
        let control_topic_reload = format!("{}/reload", topic_root);

        tokio::spawn(async move {
            let status_topic = format!("{}/status", topic_root);
            if let Err(e) = client.publish(&status_topic, QoS::AtLeastOnce, true, "online").await {
                error!("Failed to publish main bridge status online: {}", e);
            } else {
                info!("Published main bridge status: online");
            }

            if let Err(e) = client.subscribe(&control_topic_scan, QoS::AtLeastOnce).await {
                error!("Failed to subscribe to scan topic: {}", e);
            }
            if let Err(e) = client.subscribe(&control_topic_remove, QoS::AtLeastOnce).await {
                error!("Failed to subscribe to remove topic: {}", e);
            }
            if let Err(e) = client.subscribe(&control_topic_reload, QoS::AtLeastOnce).await {
                error!("Failed to subscribe to reload topic: {}", e);
            }
            info!("Subscribed to MQTT control topics.");

            loop {
                match event_loop.poll().await {
                    Ok(notification) => {
                        if let Event::Incoming(MqttPacket::Publish(publish)) = notification {
                            let topic = String::from_utf8_lossy(&publish.topic).to_string();
                            let payload = String::from_utf8_lossy(&publish.payload).trim().to_string();
                            debug!("Received MQTT message on {}: {}", topic, payload);

                            if topic == control_topic_scan {
                                let enable = payload == "1" || payload.eq_ignore_ascii_case("ON") || payload.eq_ignore_ascii_case("true");
                                info!("Received scan command: {}", enable);
                                let _ = cmd_tx.send(GatewayCommand::Scan(enable)).await;
                            } else if topic == control_topic_remove {
                                info!("Received remove command for MAC: {}", payload);
                                let _ = cmd_tx.send(GatewayCommand::Delete(payload)).await;
                            } else if topic == control_topic_reload {
                                info!("Received reload command");
                                let _ = cmd_tx.send(GatewayCommand::Reload).await;
                            }
                        }
                    }
                    Err(e) => {
                        error!("MQTT event loop error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });

        let mut event_rx = self.event_rx;
        let client = self.client.clone();
        let topic_root = self.topic_root.clone();
        let published_discovery = self.published_discovery.clone();
        let sensor_manager_worker = self.sensor_manager.clone();
        let broadcast_tx = self.broadcast_tx.clone();

        loop {
            if let Some(event) = event_rx.recv().await {
                debug!("Gateway received event for MAC: {}", event.mac);

                let mut is_online = !matches!(event.data, TelemetryData::Offline);

                // 1. Dispatch event to SensorManager
                {
                    let mut manager = sensor_manager_worker.lock().unwrap();
                    if manager.dispatch_event(&event) {
                        let _ = broadcast_tx.send(());
                        if let Some(sensor) = manager.get_sensors().get(&event.mac) {
                            is_online = sensor.is_online;
                        }
                    }
                }

                // 2. Generate and Publish Home Assistant Discovery topic if not already published
                {
                    let mut published = published_discovery.lock().await;
                    if !published.contains(&event.mac) {
                        let discovery_payloads = {
                            let manager = sensor_manager_worker.lock().unwrap();
                            manager.get_sensors().get(&event.mac)
                                .map(|sensor| sensor.get_discovery_payloads(&topic_root))
                        };

                        if let Some(payloads) = discovery_payloads {
                            info!("Publishing Home Assistant Discovery for MAC: {}", event.mac);
                            for (topic, payload) in payloads {
                                let payload_str = serde_json::to_string(&payload).unwrap();
                                debug!("Publishing discovery config to {}: {}", topic, payload_str);
                                if let Err(e) = client.publish(&topic, QoS::AtLeastOnce, true, payload_str).await {
                                    error!("Failed to publish discovery to {}: {}", topic, e);
                                }
                            }
                            published.insert(event.mac.clone());
                            
                            // Sleep briefly to give Home Assistant time to process the discovery 
                            // payload and instantiate the entity before we blast the initial state.
                            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        }
                    }
                }

                // 3. Publish availability topic
                let availability_topic = format!("{}/{}/status", topic_root, event.mac);
                let availability_payload = if is_online { "online" } else { "offline" };
                if let Err(e) = client.publish(&availability_topic, QoS::AtLeastOnce, true, availability_payload).await {
                    error!("Failed to publish availability: {}", e);
                }

                // 4. Publish State Topic
                {
                    let state_payload = {
                        let manager = sensor_manager_worker.lock().unwrap();
                        manager.get_sensors().get(&event.mac)
                            .map(|sensor| sensor.get_state_payload())
                    };

                    if let Some(payload) = state_payload {
                        let state_topic = format!("{}/{}", topic_root, event.mac);
                        let state_str = serde_json::to_string(&payload).unwrap();
                        debug!("Publishing state to {}: {}", state_topic, state_str);
                        if let Err(e) = client.publish(&state_topic, QoS::AtLeastOnce, false, state_str).await {
                            error!("Failed to publish state: {}", e);
                        }
                    }
                }
            } else {
                info!("Gateway event channel closed. Stopping gateway.");
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::telemetry::{SensorType, TelemetryData};
    use crate::protocol::sensor::WyzeSensor;
    use std::time::SystemTime;

    #[test]
    fn test_contact_discovery() {
        let sensor = WyzeSensor::new(
            "ABC12345".to_string(),
            SensorType::ContactV1,
            "Wyze Sense ABC12345".to_string(),
        );
        let payloads = sensor.get_discovery_payloads("wyzesense");
        assert_eq!(payloads.len(), 3);
        
        let contact_topic = "homeassistant/binary_sensor/wyzesense_ABC12345/state/config";
        let contact_payload = payloads.iter().find(|(t, _)| t == contact_topic).unwrap().1.clone();
        
        assert_eq!(contact_payload["device_class"], "opening");
        assert_eq!(contact_payload["unique_id"], "wyzesense_ABC12345_state");
        assert_eq!(contact_payload["state_topic"], "wyzesense/ABC12345");
        assert_eq!(contact_payload["json_attributes_topic"], "wyzesense/ABC12345");
    }

    #[test]
    fn test_climate_discovery() {
        let sensor = WyzeSensor::new(
            "ABC12345".to_string(),
            SensorType::ClimateV2,
            "Wyze Sense ABC12345".to_string()
        );
        let payloads = sensor.get_discovery_payloads("wyzesense");
        assert_eq!(payloads.len(), 4);
        
        let temp_topic = "homeassistant/sensor/wyzesense_ABC12345/temperature/config";
        let temp_payload = payloads.iter().find(|(t, _)| t == temp_topic).unwrap().1.clone();
        assert_eq!(temp_payload["device_class"], "temperature");
        assert_eq!(temp_payload["state_class"], "measurement");
        assert_eq!(temp_payload["unit_of_measurement"], "°C");
        assert_eq!(temp_payload["json_attributes_topic"], "wyzesense/ABC12345");

        let hum_topic = "homeassistant/sensor/wyzesense_ABC12345/humidity/config";
        let hum_payload = payloads.iter().find(|(t, _)| t == hum_topic).unwrap().1.clone();
        assert_eq!(hum_payload["device_class"], "humidity");
        assert_eq!(hum_payload["state_class"], "measurement");
        assert_eq!(hum_payload["unit_of_measurement"], "%");
        assert_eq!(hum_payload["json_attributes_topic"], "wyzesense/ABC12345");
    }

    #[test]
    fn test_leak_discovery() {
        let sensor = WyzeSensor::new(
            "ABC12345".to_string(),
            SensorType::LeakV2,
            "Wyze Sense ABC12345".to_string(),
        );
        let payloads = sensor.get_discovery_payloads("wyzesense");
        assert_eq!(payloads.len(), 5); // battery + signal + main moisture + probe available + probe moisture

        let leak_topic = "homeassistant/binary_sensor/wyzesense_ABC12345/state/config";
        let leak_payload = payloads.iter().find(|(t, _)| t == leak_topic).unwrap().1.clone();
        assert_eq!(leak_payload["device_class"], "moisture");

        let probe_avail_topic = "homeassistant/binary_sensor/wyzesense_ABC12345/probe_available/config";
        let probe_avail_payload = payloads.iter().find(|(t, _)| t == probe_avail_topic).unwrap().1.clone();
        assert_eq!(probe_avail_payload["device_class"], "connectivity");

        let probe_state_topic = "homeassistant/binary_sensor/wyzesense_ABC12345/probe_state/config";
        let probe_state_payload = payloads.iter().find(|(t, _)| t == probe_state_topic).unwrap().1.clone();
        assert_eq!(probe_state_payload["device_class"], "moisture");
        // Probe moisture entity uses availability_template to go unavailable when probe disconnected
        assert_eq!(probe_state_payload["availability_template"],
            "{{ 'online' if value_json.probe_available else 'offline' }}");
        assert_eq!(probe_state_payload["payload_available"], "online");
        assert_eq!(probe_state_payload["payload_not_available"], "offline");
    }

    #[test]
    fn test_contact_state_payload() {
        let mut sensor = WyzeSensor::new(
            "ABC12345".to_string(),
            SensorType::ContactV1,
            "Wyze Sense ABC12345".to_string(),
        );
        let event = DongleEvent {
            mac: "ABC12345".to_string(),
            timestamp: SystemTime::now(),
            sensor_type: SensorType::ContactV1,
            event_type: 0xA1,
            data: TelemetryData::Alarm {
                battery: 90,
                rssi: -60,
                state: 1,
                die_temperature_c: 22,
                event_sequence: 0,
            },
        };
        sensor.update_from_event(&event).unwrap();
        let payload = sensor.get_state_payload();
        assert_eq!(payload["state"], "open");
        // raw battery=90 on a 3V coin cell curve → ~20% capacity (not the misleading 90%)
        assert_eq!(payload["battery"], 20);
        assert_eq!(payload["signal_strength"], -60);
    }

    #[test]
    fn test_leak_state_payload() {
        let mut sensor = WyzeSensor::new(
            "ABC12345".to_string(),
            SensorType::LeakV2,
            "Wyze Sense ABC12345".to_string(),
        );
        let event = DongleEvent {
            mac: "ABC12345".to_string(),
            timestamp: SystemTime::now(),
            sensor_type: SensorType::LeakV2,
            event_type: 0xEA,
            data: TelemetryData::Leak {
                battery: 85,
                rssi: -55,
                state: 1,
                probe_state: 1,
                probe_available: true,
            },
        };
        sensor.update_from_event(&event).unwrap();
        let payload = sensor.get_state_payload();
        assert_eq!(payload["state"], "wet");
        assert_eq!(payload["probe_state"], "wet");
        assert_eq!(payload["probe_available"], true);
    }

    #[test]
    fn test_leak_state_payload_no_probe() {
        let mut sensor = WyzeSensor::new(
            "ABC12345".to_string(),
            SensorType::LeakV2,
            "Wyze Sense ABC12345".to_string(),
        );
        let event = DongleEvent {
            mac: "ABC12345".to_string(),
            timestamp: SystemTime::now(),
            sensor_type: SensorType::LeakV2,
            event_type: 0xEA,
            data: TelemetryData::Leak {
                battery: 85,
                rssi: -55,
                state: 0,
                probe_state: 0,
                probe_available: false,
            },
        };
        sensor.update_from_event(&event).unwrap();
        let payload = sensor.get_state_payload();
        assert_eq!(payload["state"], "dry");
        assert_eq!(payload["probe_available"], false);
        // probe_state should always be present (defaults to "dry" when probe disconnected)
        assert_eq!(payload["probe_state"], "dry");
    }
}
