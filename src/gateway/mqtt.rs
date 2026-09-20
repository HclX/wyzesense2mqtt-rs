use rumqttc::v5::{AsyncClient, MqttOptions, Event};
use rumqttc::v5::mqttbytes::{QoS, v5::Packet as MqttPacket};
use tokio::sync::mpsc;
use crate::protocol::telemetry::{DongleEvent, TelemetryData};
use crate::protocol::sensor::SensorManager;
use crate::engine::EnginesMap;
use tracing::{info, error, debug, warn};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq)]
pub enum GatewayCommand {
    Scan { enable: bool, dongle_mac: String },
    Delete { sensor_mac: String, dongle_mac: Option<String> },
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
    engines: EnginesMap,
}

impl MqttGateway {
    pub fn new(
        mut mqtt_options: MqttOptions,
        event_rx: mpsc::Receiver<DongleEvent>,
        cmd_tx: mpsc::Sender<GatewayCommand>,
        topic_root: String,
        sensor_manager: Arc<Mutex<SensorManager>>,
        broadcast_tx: tokio::sync::broadcast::Sender<()>,
        engines: EnginesMap,
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
            engines,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.client.clone();
        let topic_root = self.topic_root.clone();
        let cmd_tx = self.cmd_tx.clone();
        let mut event_loop = self.event_loop;
        let engines = self.engines.clone();
        let sensor_manager_conn = self.sensor_manager.clone();
        let published_discovery_conn = self.published_discovery.clone();

        let control_topic_scan = format!("{}/scan", topic_root);
        let control_topic_remove = format!("{}/remove", topic_root);
        let control_topic_reload = format!("{}/reload", topic_root);
        let dongle_scan_wildcard = format!("{}/dongle/+/scan/set", topic_root);

        let control_topic_scan_loop = control_topic_scan.clone();
        let control_topic_remove_loop = control_topic_remove.clone();
        let control_topic_reload_loop = control_topic_reload.clone();

        let dongle_topic_prefix = format!("{}/dongle/", topic_root);
        let dongle_topic_suffix = "/scan/set";

        // DEDICATED EVENT LOOP TASK
        // This task's ONLY job is to keep the MQTT connection alive and process incoming packets.
        // It must never be blocked by queue backpressure, so the on-connect announce burst
        // (below) is spawned off into its own task rather than awaited inline here.
        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(notification) => {
                        match notification {
                            Event::Incoming(MqttPacket::ConnAck(_)) => {
                                // Fires on the initial connect AND on every reconnect. A
                                // reconnect can mean the broker we're now talking to has lost
                                // everything it was retaining (e.g. it was restarted
                                // independently of this process, or as part of a staggered
                                // "restart all containers" routine) so we always re-announce,
                                // never assuming a previous announce is still visible to it.
                                info!("MQTT connected. Announcing bridge/dongle/sensor state.");
                                let client = client.clone();
                                let topic_root = topic_root.clone();
                                let control_topic_scan = control_topic_scan.clone();
                                let control_topic_remove = control_topic_remove.clone();
                                let control_topic_reload = control_topic_reload.clone();
                                let dongle_scan_wildcard = dongle_scan_wildcard.clone();
                                let engines = engines.clone();
                                let sensor_manager_conn = sensor_manager_conn.clone();
                                let published_discovery_conn = published_discovery_conn.clone();
                                tokio::spawn(async move {
                                    announce_on_connect(
                                        &client,
                                        &topic_root,
                                        &control_topic_scan,
                                        &control_topic_remove,
                                        &control_topic_reload,
                                        &dongle_scan_wildcard,
                                        &engines,
                                        &sensor_manager_conn,
                                        &published_discovery_conn,
                                    ).await;
                                });
                            }
                            Event::Incoming(MqttPacket::Publish(publish)) => {
                                let topic = String::from_utf8_lossy(&publish.topic).to_string();
                                let payload = String::from_utf8_lossy(&publish.payload).trim().to_string();
                                debug!("Received MQTT message on {}: {}", topic, payload);

                                if topic == control_topic_scan_loop {
                                    warn!("Legacy scan topic used without dongle_mac target. Ignoring. Use dongle/{{mac}}/scan instead.");
                                    // Legacy broadcast scan is not supported per exclusive scan design.
                                    let _ = payload;
                                } else if topic.starts_with(&dongle_topic_prefix) && topic.ends_with(dongle_topic_suffix) {
                                    // Per-dongle scan command: {topic_root}/dongle/{mac}/scan/set
                                    let inner = &topic[dongle_topic_prefix.len()..topic.len() - dongle_topic_suffix.len()];
                                    let dongle_mac = inner.to_string();
                                    let enable = payload == "1" || payload.eq_ignore_ascii_case("ON") || payload.eq_ignore_ascii_case("true");
                                    info!("Per-dongle scan command: dongle={}, enable={}", dongle_mac, enable);
                                    let _ = cmd_tx.send(GatewayCommand::Scan { enable, dongle_mac }).await;
                                } else if topic == control_topic_remove_loop {
                                    info!("Received remove command for MAC: {}", payload);
                                    let _ = cmd_tx.send(GatewayCommand::Delete { sensor_mac: payload, dongle_mac: None }).await;
                                } else if topic == control_topic_reload_loop {
                                    info!("Received reload command");
                                    let _ = cmd_tx.send(GatewayCommand::Reload).await;
                                }
                            }
                            _ => {}
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

                // Dispatch event to SensorManager
                {
                    let mut manager = sensor_manager_worker.lock().unwrap();
                    if manager.dispatch_event(&event) {
                        let _ = broadcast_tx.send(());
                        if let Some(sensor) = manager.get_sensors().get(&event.mac) {
                            is_online = sensor.is_online;
                        }
                    }
                }

                // Publish discovery (if not already published this connection), availability, and state.
                announce_sensor(&client, &topic_root, &event.mac, is_online, &sensor_manager_worker, &published_discovery, false).await;
            } else {
                info!("Gateway event channel closed. Stopping gateway.");
                break;
            }
        }

        Ok(())
    }
}

/// Publishes (or re-publishes) a single sensor's HA discovery config, availability
/// status, and current state. `force_discovery` re-sends the discovery config even
/// if this process believes it already sent it earlier — used after an MQTT
/// (re)connect, since the broker may no longer be holding what we last gave it.
async fn announce_sensor(
    client: &AsyncClient,
    topic_root: &str,
    mac: &str,
    is_online: bool,
    sensor_manager: &Arc<Mutex<SensorManager>>,
    published_discovery: &Arc<tokio::sync::Mutex<HashSet<String>>>,
    force_discovery: bool,
) {
    // Publish discovery config if not already published (or if forced)
    {
        let mut published = published_discovery.lock().await;
        if force_discovery {
            published.remove(mac);
        }
        if !published.contains(mac) {
            let discovery_payloads = {
                let manager = sensor_manager.lock().unwrap();
                manager.get_sensors().get(mac)
                    .map(|sensor| sensor.get_discovery_payloads(topic_root))
            };

            // A sensor whose type isn't known yet (e.g. discovered only via NVRAM,
            // not yet from a real telemetry packet) yields no payloads here. Don't
            // mark it published in that case, or it will never get a discovery
            // config once its type does become known.
            if let Some(payloads) = discovery_payloads {
                if !payloads.is_empty() {
                    info!("Publishing Home Assistant Discovery for MAC: {}", mac);
                    for (topic, payload) in payloads {
                        let payload_str = serde_json::to_string(&payload).unwrap();
                        debug!("Publishing discovery config to {}: {}", topic, payload_str);
                        if let Err(e) = client.publish(&topic, QoS::AtLeastOnce, true, payload_str).await {
                            error!("Failed to publish discovery to {}: {}", topic, e);
                        }
                    }
                    published.insert(mac.to_string());

                    // Sleep briefly to give Home Assistant time to process the discovery
                    // payload and instantiate the entity before we blast the initial state.
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
    }

    // Publish availability topic
    let availability_topic = format!("{}/{}/status", topic_root, mac);
    let availability_payload = if is_online { "online" } else { "offline" };
    if let Err(e) = client.publish(&availability_topic, QoS::AtLeastOnce, true, availability_payload).await {
        error!("Failed to publish availability for {}: {}", mac, e);
    }

    // Publish state topic
    let state_payload = {
        let manager = sensor_manager.lock().unwrap();
        manager.get_sensors().get(mac).map(|sensor| sensor.get_state_payload())
    };
    if let Some(payload) = state_payload {
        let state_topic = format!("{}/{}", topic_root, mac);
        let state_str = serde_json::to_string(&payload).unwrap();
        debug!("Publishing state to {}: {}", state_topic, state_str);
        if let Err(e) = client.publish(&state_topic, QoS::AtLeastOnce, false, state_str).await {
            error!("Failed to publish state: {}", e);
        }
    }
}

/// Runs on every MQTT connect (initial and reconnect): publishes the bridge's own
/// "online" status, (re)subscribes to control topics, (re)publishes discovery for
/// every known dongle, and force-re-announces every known sensor. This is what
/// closes the gap where a container-restart order outside our control leaves this
/// process talking to a broker that never received (or has since lost) our state.
#[allow(clippy::too_many_arguments)]
async fn announce_on_connect(
    client: &AsyncClient,
    topic_root: &str,
    control_topic_scan: &str,
    control_topic_remove: &str,
    control_topic_reload: &str,
    dongle_scan_wildcard: &str,
    engines: &EnginesMap,
    sensor_manager: &Arc<Mutex<SensorManager>>,
    published_discovery: &Arc<tokio::sync::Mutex<HashSet<String>>>,
) {
    let status_topic = format!("{}/status", topic_root);
    if let Err(e) = client.publish(&status_topic, QoS::AtLeastOnce, true, "online").await {
        error!("Failed to publish main bridge status online: {}", e);
    } else {
        info!("Published main bridge status: online");
    }

    if let Err(e) = client.subscribe(control_topic_scan, QoS::AtLeastOnce).await {
        error!("Failed to subscribe to scan topic: {}", e);
    }
    if let Err(e) = client.subscribe(control_topic_remove, QoS::AtLeastOnce).await {
        error!("Failed to subscribe to remove topic: {}", e);
    }
    if let Err(e) = client.subscribe(control_topic_reload, QoS::AtLeastOnce).await {
        error!("Failed to subscribe to reload topic: {}", e);
    }
    // Subscribe to per-dongle scan command topics (Phase 4)
    if let Err(e) = client.subscribe(dongle_scan_wildcard, QoS::AtLeastOnce).await {
        error!("Failed to subscribe to dongle scan wildcard topic: {}", e);
    }
    info!("Subscribed to MQTT control topics.");

    // Publish dongle discovery for all registered engines
    {
        let map = engines.lock().await;
        for (mac, engine) in map.iter() {
            let version = engine.dongle_version().unwrap_or("unknown").to_string();
            if let Err(e) = publish_dongle_discovery(client, topic_root, mac, &version).await {
                error!("Failed to publish dongle discovery for {}: {}", mac, e);
            } else {
                info!("Published HA dongle discovery for {}", mac);
            }
        }
    }

    // Force-re-announce every known sensor: discovery config + availability + state.
    let sensor_macs: Vec<String> = {
        let manager = sensor_manager.lock().unwrap();
        manager.get_sensors().keys().cloned().collect()
    };
    let count = sensor_macs.len();
    for mac in sensor_macs {
        let is_online = {
            let manager = sensor_manager.lock().unwrap();
            manager.get_sensors().get(&mac).map(|s| s.is_online).unwrap_or(false)
        };
        announce_sensor(client, topic_root, &mac, is_online, sensor_manager, published_discovery, true).await;
    }
    info!("Re-announced {} known sensor(s) after MQTT connect.", count);
}

/// Publishes MQTT auto-discovery config payloads for a dongle device in Home Assistant.
async fn publish_dongle_discovery(
    client: &AsyncClient,
    topic_root: &str,
    dongle_mac: &str,
    dongle_version: &str,
) -> Result<(), rumqttc::v5::ClientError> {
    let device_id = format!("wyzesense_dongle_{}", dongle_mac.to_lowercase());
    let device = serde_json::json!({
        "identifiers": [&device_id],
        "name": format!("Wyze Dongle {}", dongle_mac),
        "manufacturer": "Wyze",
        "model": "WLPP1 USB Dongle",
        "sw_version": dongle_version,
        "via_device": "wyzesense2mqtt_bridge"
    });

    // 1. Binary sensor: connectivity (online/offline)
    let status_config = serde_json::json!({
        "name": "Status",
        "unique_id": format!("{}_status", device_id),
        "device_class": "connectivity",
        "state_topic": format!("{}/dongle/{}/status", topic_root, dongle_mac),
        "payload_on": "online",
        "payload_off": "offline",
        "device": device,
        "entity_category": "diagnostic"
    });
    let config_topic = format!("homeassistant/binary_sensor/{}/status/config", device_id);
    client.publish(config_topic, QoS::AtLeastOnce, true,
        serde_json::to_vec(&status_config).unwrap()).await?;

    // 2. Sensor: firmware version (diagnostic)
    let version_config = serde_json::json!({
        "name": "Firmware Version",
        "unique_id": format!("{}_firmware", device_id),
        "state_topic": format!("{}/dongle/{}/state", topic_root, dongle_mac),
        "value_template": "{{ value_json.version }}",
        "icon": "mdi:chip",
        "device": device,
        "entity_category": "diagnostic"
    });
    let config_topic = format!("homeassistant/sensor/{}/firmware/config", device_id);
    client.publish(config_topic, QoS::AtLeastOnce, true,
        serde_json::to_vec(&version_config).unwrap()).await?;

    // 3. Sensor: connected sensors count (diagnostic)
    let count_config = serde_json::json!({
        "name": "Connected Sensors",
        "unique_id": format!("{}_sensor_count", device_id),
        "state_topic": format!("{}/dongle/{}/state", topic_root, dongle_mac),
        "value_template": "{{ value_json.sensor_count }}",
        "icon": "mdi:counter",
        "device": device,
        "entity_category": "diagnostic"
    });
    let config_topic = format!("homeassistant/sensor/{}/sensor_count/config", device_id);
    client.publish(config_topic, QoS::AtLeastOnce, true,
        serde_json::to_vec(&count_config).unwrap()).await?;

    // 4. Switch: scan mode
    let scan_config = serde_json::json!({
        "name": "Scan Mode",
        "unique_id": format!("{}_scan", device_id),
        "state_topic": format!("{}/dongle/{}/state", topic_root, dongle_mac),
        "value_template": "{{ value_json.scanning }}",
        "command_topic": format!("{}/dongle/{}/scan/set", topic_root, dongle_mac),
        "payload_on": "ON",
        "payload_off": "OFF",
        "state_on": "true",
        "state_off": "false",
        "icon": "mdi:magnify-scan",
        "device": device,
    });
    let config_topic = format!("homeassistant/switch/{}/scan/config", device_id);
    client.publish(config_topic, QoS::AtLeastOnce, true,
        serde_json::to_vec(&scan_config).unwrap()).await?;

    // 5. Publish initial status and state
    let status_topic = format!("{}/dongle/{}/status", topic_root, dongle_mac);
    client.publish(status_topic, QoS::AtLeastOnce, true, "online".as_bytes().to_vec()).await?;

    let state_topic = format!("{}/dongle/{}/state", topic_root, dongle_mac);
    let state = serde_json::json!({
        "version": dongle_version,
        "sensor_count": 0,
        "scanning": false
    });
    client.publish(state_topic, QoS::AtLeastOnce, true,
        serde_json::to_vec(&state).unwrap()).await?;

    Ok(())
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
        assert_eq!(payloads.len(), 5); // battery + battery_voltage + die_temperature + signal + state
        
        let contact_topic = "homeassistant/binary_sensor/wyzesense_ABC12345/state/config";
        let contact_payload = payloads.iter().find(|(t, _)| t == contact_topic).unwrap().1.clone();
        
        assert_eq!(contact_payload["device_class"], "opening");
        assert_eq!(contact_payload["unique_id"], "wyzesense_ABC12345_state");
        assert_eq!(contact_payload["state_topic"], "wyzesense/ABC12345");
        assert_eq!(contact_payload["json_attributes_topic"], "wyzesense/ABC12345");

        // Verify battery voltage diagnostic entity
        let voltage_topic = "homeassistant/sensor/wyzesense_ABC12345/battery_voltage/config";
        let voltage_payload = payloads.iter().find(|(t, _)| t == voltage_topic).unwrap().1.clone();
        assert_eq!(voltage_payload["device_class"], "voltage");
        assert_eq!(voltage_payload["unit_of_measurement"], "V");
        assert_eq!(voltage_payload["entity_category"], "diagnostic");
    }

    #[test]
    fn test_climate_discovery() {
        let sensor = WyzeSensor::new(
            "ABC12345".to_string(),
            SensorType::ClimateV2,
            "Wyze Sense ABC12345".to_string()
        );
        let payloads = sensor.get_discovery_payloads("wyzesense");
        assert_eq!(payloads.len(), 6); // battery + battery_voltage + die_temperature + signal + temp + humidity
        
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
        assert_eq!(payloads.len(), 7); // battery + battery_voltage + die_temperature + signal + main moisture + probe available + probe moisture

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
        dongle_mac: None, };
        sensor.update_from_event(&event).unwrap();
        let payload = sensor.get_state_payload();
        assert_eq!(payload["state"], "open");
        // raw battery=90 on a 3V coin cell curve → 50% capacity (2.81V, plateau ending)
        assert_eq!(payload["battery"], 50);
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
        dongle_mac: None, };
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
        dongle_mac: None, };
        sensor.update_from_event(&event).unwrap();
        let payload = sensor.get_state_payload();
        assert_eq!(payload["state"], "dry");
        assert_eq!(payload["probe_available"], false);
        // probe_state should always be present (defaults to "dry" when probe disconnected)
        assert_eq!(payload["probe_state"], "dry");
    }
}
