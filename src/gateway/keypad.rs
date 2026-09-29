//! Answers keypad requests on behalf of the Wyze Hub, with Home Assistant as
//! the source of truth for the alarm state and PIN validation.
//!
//! - HA publishes its `alarm_control_panel` state (e.g. Alarmo's `alarmo/state`)
//!   to the configured alarm state topic; the keypad is told that state.
//! - Button presses go to a non-retained event topic (HA `event` entity).
//! - Arm/disarm requests, with the PIN when one was typed, go to a non-retained
//!   action topic for an HA automation to carry out. The keypad gets its answer
//!   once the alarm state changes accordingly, or a rejection on timeout or when
//!   HA publishes `invalid` to the PIN result topic.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rumqttc::v5::AsyncClient;
use rumqttc::v5::mqttbytes::QoS;
use serde_json::json;
use tokio::sync::{watch, Notify};
use tracing::{debug, error, info, warn};

use crate::config::app_config::KeypadConfig;
use crate::engine::EnginesMap;
use crate::protocol::keypad::{decide, KeypadAction, KeypadDecision, KeypadEvent, KeypadPin, KeypadReply, KeypadStatus};
use crate::protocol::sensor::{keypad_action_topic, keypad_event_topic};
use crate::protocol::telemetry::{DongleEvent, TelemetryData};

/// A button press is only remembered for a following PIN this long.
const PENDING_ACTION_TTL: Duration = Duration::from_secs(60);

pub struct KeypadController {
    client: AsyncClient,
    topic_root: String,
    engines: EnginesMap,
    alarm_state_topic: String,
    alarm_state: watch::Sender<Option<String>>,
    pin_rejected: Notify,
    pending: Mutex<HashMap<String, (KeypadAction, Instant)>>,
    timeout: Duration,
}

impl KeypadController {
    pub fn new(config: &KeypadConfig, client: AsyncClient, topic_root: &str, engines: EnginesMap) -> Arc<Self> {
        let alarm_state_topic = config
            .alarm_state_topic
            .clone()
            .unwrap_or_else(|| format!("{}/keypad/alarm_state", topic_root));
        Arc::new(Self {
            client,
            topic_root: topic_root.to_string(),
            engines,
            alarm_state_topic,
            alarm_state: watch::channel(None).0,
            pin_rejected: Notify::new(),
            pending: Mutex::new(HashMap::new()),
            timeout: Duration::from_millis(config.response_timeout_ms),
        })
    }

    pub fn alarm_state_topic(&self) -> &str {
        &self.alarm_state_topic
    }

    /// HA publishes `invalid` here to reject a PIN without waiting for the timeout.
    pub fn pin_result_topic(&self) -> String {
        format!("{}/keypad/pin_result", self.topic_root)
    }

    pub fn on_alarm_state(&self, payload: &str) {
        let state = payload.trim().to_string();
        if KeypadStatus::from_ha_state(&state).is_none() {
            warn!("Keypad: ignoring unrecognized alarm state {:?} on {}", state, self.alarm_state_topic);
            return;
        }
        info!("Keypad: alarm state is now {}", state);
        self.alarm_state.send_replace(Some(state));
    }

    pub fn on_pin_result(&self, payload: &str) {
        if payload.trim().eq_ignore_ascii_case("invalid") {
            self.pin_rejected.notify_waiters();
        }
    }

    fn current_status(&self) -> KeypadStatus {
        match self.alarm_state.borrow().as_deref().and_then(KeypadStatus::from_ha_state) {
            Some(status) => status,
            None => {
                warn!("Keypad: no alarm state received on {} yet; reporting disarmed", self.alarm_state_topic);
                KeypadStatus::DISARMED
            }
        }
    }

    fn set_pending(&self, mac: &str, action: KeypadAction) {
        self.pending.lock().unwrap().insert(mac.to_string(), (action, Instant::now()));
    }

    fn take_pending(&self, mac: &str) -> Option<KeypadAction> {
        self.pending
            .lock()
            .unwrap()
            .remove(mac)
            .filter(|(_, at)| at.elapsed() < PENDING_ACTION_TTL)
            .map(|(action, _)| action)
    }

    /// Handles one keypad event. Spawn it: a request can wait on Home Assistant.
    pub async fn handle(self: Arc<Self>, event: DongleEvent) {
        let TelemetryData::Keypad { event: kp_event, .. } = &event.data else {
            return;
        };
        let mac = event.mac.as_str();
        let Some(dongle_mac) = event.dongle_mac.as_deref() else {
            warn!("Keypad {}: event without a dongle, cannot reply", mac);
            return;
        };
        debug!("Keypad {}: {:?}", mac, kp_event);

        if let KeypadEvent::Button(button) = kp_event {
            let payload = json!({ "event_type": button.event_type() }).to_string();
            if let Err(e) = self.client.publish(keypad_event_topic(&self.topic_root, mac), QoS::AtLeastOnce, false, payload).await {
                error!("Keypad {}: failed to publish button event: {}", mac, e);
            }
        }

        let pending = match kp_event {
            KeypadEvent::Pin(_) => self.take_pending(mac),
            _ => None,
        };
        let reply = match decide(kp_event, self.current_status(), pending) {
            KeypadDecision::Reply(reply) => {
                if let KeypadEvent::Button(button) = kp_event {
                    if let Some(action) = KeypadAction::from_button(*button) {
                        self.set_pending(mac, action);
                    }
                }
                reply
            }
            KeypadDecision::Request { action, pin } => self.request(mac, action, pin).await,
            KeypadDecision::Ignore => return,
        };
        self.send_reply(dongle_mac, mac, reply).await;
    }

    /// Asks HA to carry out `action` and returns the reply for the keypad.
    async fn request(&self, mac: &str, action: KeypadAction, pin: Option<KeypadPin>) -> KeypadReply {
        let mut state_rx = self.alarm_state.subscribe();
        let rejected = self.pin_rejected.notified();
        tokio::pin!(rejected);
        rejected.as_mut().enable();

        let already = state_rx.borrow_and_update().as_deref().and_then(|s| action.completed_by(s));
        let outcome = match already {
            Some(status) => Some(status),
            None => {
                let mut payload = json!({ "action": action.as_str(), "keypad": mac });
                if let Some(pin) = &pin {
                    payload["code"] = json!(pin.code());
                }
                // Never retained: the payload can carry a PIN.
                if let Err(e) = self.client.publish(keypad_action_topic(&self.topic_root, mac), QoS::AtLeastOnce, false, payload.to_string()).await {
                    error!("Keypad {}: failed to publish {} request: {}", mac, action.as_str(), e);
                }
                info!("Keypad {}: requested {}{}", mac, action.as_str(), if pin.is_some() { " with PIN" } else { "" });

                let wait = async {
                    loop {
                        tokio::select! {
                            _ = &mut rejected => return None,
                            changed = state_rx.changed() => {
                                if changed.is_err() {
                                    return None;
                                }
                                if let Some(status) = state_rx.borrow_and_update().as_deref().and_then(|s| action.completed_by(s)) {
                                    return Some(status);
                                }
                            }
                        }
                    }
                };
                tokio::time::timeout(self.timeout, wait).await.ok().flatten()
            }
        };

        match (outcome, pin) {
            (Some(status), _) => KeypadReply::Status { status, awaiting_pin: false },
            (None, Some(_)) => {
                info!("Keypad {}: {} with PIN was not carried out; rejecting PIN", mac, action.as_str());
                KeypadReply::PinRejected
            }
            (None, None) => {
                // No state change without a PIN: have the keypad collect one.
                self.set_pending(mac, action);
                KeypadReply::Status { status: self.current_status(), awaiting_pin: true }
            }
        }
    }

    async fn send_reply(&self, dongle_mac: &str, mac: &str, reply: KeypadReply) {
        let engine = self.engines.lock().await.get(dongle_mac).cloned();
        let Some(mut engine) = engine else {
            warn!("Keypad {}: dongle {} is not connected, cannot reply", mac, dongle_mac);
            return;
        };
        match engine.send_keypad_reply(mac, reply).await {
            Ok(()) => info!("Keypad {}: replied {:?}", mac, reply),
            Err(e) => error!("Keypad {}: failed to send reply {:?}: {}", mac, reply, e),
        }
    }
}
