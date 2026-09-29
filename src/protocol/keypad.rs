//! Wyze Sense keypad protocol: inbound events and the Hub-side replies.
//!
//! Decoded from UART captures of a Wyze Sense Hub talking to its CC1310
//! radio (same framing as the Sense Bridge dongle, see `packet.rs`).
//!
//! Inbound (`0x5355`, event `0xEA`, sensor type `0x05`), payload offsets:
//! ```text
//!   0x00      event type (0xEA)
//!   0x01-0x08 MAC, ASCII
//!   0x09      sensor type (0x05)
//!   0x0A      len: number of bytes from 0x0B up to (not including) the signal byte
//!   0x0B-0x0D unknown header (0x0D always 0x00)
//!   0x0E      subtype
//!   0x0F..    subtype data (PIN digit count = len - 6)
//!   0x09+len  constant per keypad
//!   0x0A+len  per-keypad sequence counter (+1 per event)
//!   0x0B+len  signal (negate for dBm)
//! ```
//!
//! Outbound (`0x5353`), sent by the Hub only in reply to a keypad request.
//! The Hub never pushes mode changes; a keypad learns the current mode from
//! these replies. Payload:
//! ```text
//!   MAC(8 ASCII) | 0xE9 | n | data[n]      data[0] = inbound subtype + 1
//! ```

use std::fmt;

use crate::protocol::packet::{commands, Packet};

/// Marker byte between the MAC and the data length in outbound keypad frames.
const OUTBOUND_MARKER: u8 = 0xE9;

pub const SUBTYPE_BUTTON: u8 = 0x02;
pub const SUBTYPE_PROFILE_REQUEST: u8 = 0x06;
pub const SUBTYPE_PIN: u8 = 0x08;
pub const SUBTYPE_MOTION: u8 = 0x0A;

const REPLY_STATUS: u8 = 0x03;
const REPLY_PROFILE: u8 = 0x07;
const REPLY_PIN_REJECTED: u8 = 0x09;
const REPLY_MOTION: u8 = 0x0B;

/// Byte range of the PIN digits in an ALARM2 payload, if it is a keypad PIN
/// event. Used to redact PINs from packet logs.
pub fn pin_digit_range(payload: &[u8]) -> Option<std::ops::Range<usize>> {
    const SENSOR_TYPE: usize = 0x09;
    const LEN: usize = 0x0A;
    const SUBTYPE: usize = 0x0E;
    if payload.len() <= SUBTYPE || payload[SENSOR_TYPE] != 0x05 || payload[SUBTYPE] != SUBTYPE_PIN {
        return None;
    }
    // Digits run from 0x0F up to the per-keypad constant at 0x09 + len.
    let end = (SENSOR_TYPE + payload[LEN] as usize).clamp(SUBTYPE + 1, payload.len());
    Some(SUBTYPE + 1..end)
}

/// Physical buttons on the keypad (subtype `0x02`).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KeypadButton {
    Disarm,
    Home,
    Away,
    /// Side/emergency button. The Hub only ACKs it; it never replies.
    Panic,
}

impl KeypadButton {
    fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            0x01 => Some(Self::Disarm),
            0x02 => Some(Self::Home),
            0x03 => Some(Self::Away),
            0x04 => Some(Self::Panic),
            _ => None,
        }
    }

    /// Event type published to the Home Assistant `event` entity.
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Disarm => "disarm",
            Self::Home => "arm_home",
            Self::Away => "arm_away",
            Self::Panic => "panic",
        }
    }
}

/// PIN digits as typed on the keypad. `Debug` is redacted so a PIN never
/// ends up in logs through `{:?}` of a telemetry event.
#[derive(Clone, PartialEq, Eq)]
pub struct KeypadPin(Vec<u8>);

impl KeypadPin {
    pub fn new(digits: Vec<u8>) -> Self {
        Self(digits)
    }

    /// The PIN as a string of decimal digits, e.g. "1234".
    pub fn code(&self) -> String {
        self.0.iter().map(|d| char::from(b'0' + (d % 10))).collect()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for KeypadPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KeypadPin(<{} digits redacted>)", self.0.len())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum KeypadEvent {
    Button(KeypadButton),
    /// Sent when PIN entry starts; the Hub answers with the current mode.
    ProfileRequest,
    Pin(KeypadPin),
    /// Built-in motion sensor: `true` = detected, `false` = cleared.
    Motion(bool),
    Unknown { subtype: u8, data: Vec<u8> },
}

impl KeypadEvent {
    /// Parses the subtype and its data (payload bytes from offset `0x0F` up to
    /// the per-keypad constant byte).
    pub fn parse(subtype: u8, data: &[u8]) -> Self {
        match subtype {
            SUBTYPE_BUTTON => match data.first().and_then(|&raw| KeypadButton::from_raw(raw)) {
                Some(button) => Self::Button(button),
                None => Self::Unknown { subtype, data: data.to_vec() },
            },
            SUBTYPE_PROFILE_REQUEST => Self::ProfileRequest,
            SUBTYPE_PIN => Self::Pin(KeypadPin::new(data.to_vec())),
            SUBTYPE_MOTION => Self::Motion(data.first() == Some(&0x01)),
            _ => Self::Unknown { subtype, data: data.to_vec() },
        }
    }
}

/// Mode byte the keypad understands.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum KeypadMode {
    Disarmed = 0x01,
    Home = 0x02,
    Away = 0x03,
    /// Entry delay running or siren sounding (the Hub uses one value for both).
    Triggered = 0x04,
}

/// Second byte of status/motion replies. Interpretation from captures:
/// `0x02` exit delay starting, `0x01` entry delay running, `0x04` steady state.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum DelayPhase {
    Entry = 0x01,
    Exit = 0x02,
    None = 0x04,
}

/// What a keypad is told about the alarm.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct KeypadStatus {
    pub mode: KeypadMode,
    pub phase: DelayPhase,
}

impl KeypadStatus {
    pub const DISARMED: Self = Self { mode: KeypadMode::Disarmed, phase: DelayPhase::None };

    /// Maps a Home Assistant `alarm_control_panel` state (as published by
    /// Alarmo or a manual alarm panel) to what the keypad should display.
    ///
    /// During the exit delay the Hub keeps reporting "disarmed" until the
    /// delay ends, so `arming` maps to Disarmed.
    pub fn from_ha_state(state: &str) -> Option<Self> {
        let (mode, phase) = match state.trim().to_ascii_lowercase().as_str() {
            "disarmed" | "arming" | "disarming" => (KeypadMode::Disarmed, DelayPhase::None),
            "armed_home" | "armed_night" => (KeypadMode::Home, DelayPhase::None),
            "armed_away" | "armed_vacation" | "armed_custom_bypass" => (KeypadMode::Away, DelayPhase::None),
            "pending" | "triggered" => (KeypadMode::Triggered, DelayPhase::Entry),
            _ => return None,
        };
        Some(Self { mode, phase })
    }
}

/// An alarm action requested from the keypad, to be carried out by Home Assistant.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KeypadAction {
    Disarm,
    ArmHome,
    ArmAway,
}

impl KeypadAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disarm => "disarm",
            Self::ArmHome => "arm_home",
            Self::ArmAway => "arm_away",
        }
    }

    pub fn from_button(button: KeypadButton) -> Option<Self> {
        match button {
            KeypadButton::Disarm => Some(Self::Disarm),
            KeypadButton::Home => Some(Self::ArmHome),
            KeypadButton::Away => Some(Self::ArmAway),
            KeypadButton::Panic => None,
        }
    }

    /// Whether Home Assistant reaching `ha_state` means this action succeeded,
    /// and if so, the status to confirm to the keypad.
    pub fn completed_by(&self, ha_state: &str) -> Option<KeypadStatus> {
        let state = ha_state.trim().to_ascii_lowercase();
        let target = match self {
            Self::Disarm => {
                return (state == "disarmed").then_some(KeypadStatus::DISARMED);
            }
            Self::ArmHome => KeypadMode::Home,
            Self::ArmAway => KeypadMode::Away,
        };
        if state == "arming" {
            // Exit delay started: the Hub confirms with the target mode and phase 0x02.
            return Some(KeypadStatus { mode: target, phase: DelayPhase::Exit });
        }
        KeypadStatus::from_ha_state(&state).filter(|s| s.mode == target)
    }
}

/// Reply sent to a keypad (`0x5353`).
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KeypadReply {
    /// `03 <mode> <phase> <FF|00>`: answer to a button or PIN.
    /// `awaiting_pin` (FF) asks the keypad to collect a PIN.
    Status { status: KeypadStatus, awaiting_pin: bool },
    /// `03 FF FF 00`: PIN received while no action was pending.
    NoAction,
    /// `07 <mode>`: answer to a profile request.
    Profile(KeypadMode),
    /// `09 00`: wrong PIN.
    PinRejected,
    /// `0B <mode> <phase>`: answer to motion detected.
    Motion(KeypadStatus),
}

impl KeypadReply {
    pub fn data(&self) -> Vec<u8> {
        match self {
            Self::Status { status, awaiting_pin } => vec![
                REPLY_STATUS,
                status.mode as u8,
                status.phase as u8,
                if *awaiting_pin { 0xFF } else { 0x00 },
            ],
            Self::NoAction => vec![REPLY_STATUS, 0xFF, 0xFF, 0x00],
            Self::Profile(mode) => vec![REPLY_PROFILE, *mode as u8],
            Self::PinRejected => vec![REPLY_PIN_REJECTED, 0x00],
            Self::Motion(status) => vec![REPLY_MOTION, status.mode as u8, status.phase as u8],
        }
    }

    /// Builds the outbound `0x5353` packet for a keypad.
    pub fn to_packet(&self, keypad_mac: &str) -> Result<Packet, &'static str> {
        if keypad_mac.len() != 8 || !keypad_mac.is_ascii() {
            return Err("Keypad MAC must be 8 ASCII characters");
        }
        let data = self.data();
        let mut payload = keypad_mac.as_bytes().to_vec();
        payload.push(OUTBOUND_MARKER);
        payload.push(data.len() as u8);
        payload.extend_from_slice(&data);
        Ok(Packet::new_async((commands::CMD_SEND_KEYPAD & 0xFF) as u8, payload))
    }
}

/// What the gateway does with a keypad event.
#[derive(Debug, Clone, PartialEq)]
pub enum KeypadDecision {
    /// Reply straight away, no Home Assistant round trip.
    Reply(KeypadReply),
    /// Ask Home Assistant to perform `action` (with the PIN, if any) and reply
    /// once it has, or tell the keypad the request needs a PIN / was rejected.
    Request { action: KeypadAction, pin: Option<KeypadPin> },
    /// ACK only (the engine already ACKs every inbound frame).
    Ignore,
}

/// Decides how to answer a keypad event, mirroring the Hub's behaviour.
///
/// `pending` is the action selected by the last button press on this keypad,
/// consumed by a following PIN.
pub fn decide(
    event: &KeypadEvent,
    current: KeypadStatus,
    pending: Option<KeypadAction>,
) -> KeypadDecision {
    match event {
        KeypadEvent::Button(KeypadButton::Disarm) => KeypadDecision::Reply(KeypadReply::Status {
            status: current,
            awaiting_pin: true,
        }),
        KeypadEvent::Button(KeypadButton::Home) => KeypadDecision::Request { action: KeypadAction::ArmHome, pin: None },
        KeypadEvent::Button(KeypadButton::Away) => KeypadDecision::Request { action: KeypadAction::ArmAway, pin: None },
        KeypadEvent::Button(KeypadButton::Panic) => KeypadDecision::Ignore,
        KeypadEvent::ProfileRequest => KeypadDecision::Reply(KeypadReply::Profile(current.mode)),
        KeypadEvent::Pin(pin) => {
            // A PIN typed without a button press disarms (seen during the entry delay).
            let action = pending.or((current.mode != KeypadMode::Disarmed).then_some(KeypadAction::Disarm));
            match action {
                Some(action) => KeypadDecision::Request { action, pin: Some(pin.clone()) },
                None => KeypadDecision::Reply(KeypadReply::NoAction),
            }
        }
        KeypadEvent::Motion(true) => KeypadDecision::Reply(KeypadReply::Motion(current)),
        KeypadEvent::Motion(false) => KeypadDecision::Ignore,
        KeypadEvent::Unknown { .. } => KeypadDecision::Ignore,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::telemetry::{DongleEvent, SensorType, TelemetryData};

    fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace().map(|b| u8::from_str_radix(b, 16).unwrap()).collect()
    }

    fn inbound(frame: &str) -> DongleEvent {
        let (pkt, consumed) = Packet::parse(&hex(frame)).unwrap();
        assert_eq!(consumed, hex(frame).len());
        assert_eq!(pkt.cmd(), commands::CMD_ALARM2);
        DongleEvent::parse_alarm2(pkt.payload_bytes().unwrap()).unwrap()
    }

    fn keypad_event(frame: &str) -> KeypadEvent {
        match inbound(frame).data {
            TelemetryData::Keypad { event, .. } => event,
            other => panic!("expected keypad telemetry, got {:?}", other),
        }
    }

    const HOME: KeypadStatus = KeypadStatus { mode: KeypadMode::Home, phase: DelayPhase::None };
    const AWAY: KeypadStatus = KeypadStatus { mode: KeypadMode::Away, phase: DelayPhase::None };
    const TRIGGERED: KeypadStatus = KeypadStatus { mode: KeypadMode::Triggered, phase: DelayPhase::Entry };

    // --- Inbound, frames from Hub UART captures ---

    #[test]
    fn parses_disarm_button() {
        let evt = inbound("55 AA 53 17 55 EA 37 37 43 30 36 36 43 30 05 08 17 87 00 02 01 00 8B 7A 13 06 2E");
        assert_eq!(evt.mac, "77C066C0");
        assert_eq!(evt.sensor_type, SensorType::Keypad);
        assert_eq!(
            evt.data,
            TelemetryData::Keypad { event: KeypadEvent::Button(KeypadButton::Disarm), rssi: -0x13, sequence: 0x7A }
        );
    }

    #[test]
    fn parses_side_button_as_panic() {
        let event = keypad_event("55 AA 53 17 55 EA 37 37 43 30 36 36 43 30 05 08 17 89 00 02 04 00 8B 8C 14 06 46");
        assert_eq!(event, KeypadEvent::Button(KeypadButton::Panic));
    }

    #[test]
    fn parses_profile_request() {
        let event = keypad_event("55 AA 53 16 55 EA 37 37 43 30 36 36 43 30 05 07 17 8C 00 06 FF 8B 7B 13 07 34");
        assert_eq!(event, KeypadEvent::ProfileRequest);
    }

    #[test]
    fn parses_pin() {
        let event = keypad_event("55 AA 53 19 55 EA 37 37 43 30 36 36 43 30 05 0A 17 89 00 08 01 02 03 04 8B 7C 14 06 46");
        match event {
            KeypadEvent::Pin(pin) => assert_eq!(pin.code(), "1234"),
            other => panic!("expected PIN, got {:?}", other),
        }
    }

    #[test]
    fn parses_motion() {
        let event = keypad_event("55 AA 53 17 55 EA 37 37 43 35 31 37 37 39 05 08 11 92 00 0A 01 00 A1 2C 38 06 26");
        assert_eq!(event, KeypadEvent::Motion(true));
        assert_eq!(KeypadEvent::parse(SUBTYPE_MOTION, &[0x00, 0x00]), KeypadEvent::Motion(false));
    }

    #[test]
    fn parses_variable_length_pin() {
        let event = KeypadEvent::parse(SUBTYPE_PIN, &[5, 6, 7, 8, 5, 6, 7, 8]);
        match event {
            KeypadEvent::Pin(pin) => assert_eq!(pin.code(), "56785678"),
            other => panic!("expected PIN, got {:?}", other),
        }
    }

    #[test]
    fn pin_debug_is_redacted() {
        let debug = format!("{:?}", KeypadEvent::Pin(KeypadPin::new(vec![1, 2, 3, 4])));
        assert_eq!(debug, "Pin(KeypadPin(<4 digits redacted>))");
    }

    #[test]
    fn packet_display_redacts_pin() {
        let frame = hex("55 AA 53 19 55 EA 37 37 43 30 36 36 43 30 05 0A 17 89 00 08 01 02 03 04 8B 7C 14 06 46");
        let (pkt, _) = Packet::parse(&frame).unwrap();
        let shown = pkt.to_string();
        assert!(shown.contains(",08,**,**,**,**,8b,7c,14"), "{}", shown);
        // Non-PIN keypad events are shown in full
        let frame = hex("55 AA 53 17 55 EA 37 37 43 30 36 36 43 30 05 08 17 87 00 02 01 00 8B 7A 13 06 2E");
        let (pkt, _) = Packet::parse(&frame).unwrap();
        assert!(!pkt.to_string().contains("**"));
    }

    // --- Outbound, byte-exact against the Hub's replies ---

    fn outbound(reply: KeypadReply, mac: &str) -> Vec<u8> {
        reply.to_packet(mac).unwrap().to_bytes()
    }

    #[test]
    fn encodes_status_awaiting_pin() {
        let reply = KeypadReply::Status { status: AWAY, awaiting_pin: true };
        assert_eq!(outbound(reply, "77C066C0"), hex("AA 55 53 11 53 37 37 43 30 36 36 43 30 E9 04 03 03 04 FF 05 6C"));
    }

    #[test]
    fn encodes_disarm_confirmation() {
        let reply = KeypadReply::Status { status: KeypadStatus::DISARMED, awaiting_pin: false };
        assert_eq!(outbound(reply, "77C066C0"), hex("AA 55 53 11 53 37 37 43 30 36 36 43 30 E9 04 03 01 04 00 04 6B"));
    }

    #[test]
    fn encodes_profile() {
        assert_eq!(
            outbound(KeypadReply::Profile(KeypadMode::Away), "77C066C0"),
            hex("AA 55 53 0F 53 37 37 43 30 36 36 43 30 E9 02 07 03 04 69")
        );
        assert_eq!(
            outbound(KeypadReply::Profile(KeypadMode::Triggered), "77C0CE7F"),
            hex("AA 55 53 0F 53 37 37 43 30 43 45 37 46 E9 02 07 04 04 90")
        );
    }

    #[test]
    fn encodes_pin_rejected() {
        assert_eq!(
            outbound(KeypadReply::PinRejected, "77C066C0"),
            hex("AA 55 53 0F 53 37 37 43 30 36 36 43 30 E9 02 09 00 04 68")
        );
    }

    #[test]
    fn encodes_motion() {
        assert_eq!(
            outbound(KeypadReply::Motion(HOME), "77C51779"),
            hex("AA 55 53 10 53 37 37 43 35 31 37 37 39 E9 03 0B 02 04 04 70")
        );
    }

    #[test]
    fn rejects_bad_mac() {
        assert!(KeypadReply::PinRejected.to_packet("SHORT").is_err());
    }

    // --- HA state mapping ---

    #[test]
    fn maps_ha_states() {
        assert_eq!(KeypadStatus::from_ha_state("disarmed"), Some(KeypadStatus::DISARMED));
        assert_eq!(KeypadStatus::from_ha_state("arming"), Some(KeypadStatus::DISARMED));
        assert_eq!(KeypadStatus::from_ha_state("armed_home"), Some(HOME));
        assert_eq!(KeypadStatus::from_ha_state("ARMED_AWAY"), Some(AWAY));
        assert_eq!(KeypadStatus::from_ha_state("pending"), Some(TRIGGERED));
        assert_eq!(KeypadStatus::from_ha_state("triggered"), Some(TRIGGERED));
        assert_eq!(KeypadStatus::from_ha_state("unavailable"), None);
    }

    #[test]
    fn action_completion() {
        assert_eq!(KeypadAction::Disarm.completed_by("disarmed"), Some(KeypadStatus::DISARMED));
        assert_eq!(KeypadAction::Disarm.completed_by("armed_home"), None);
        assert_eq!(
            KeypadAction::ArmHome.completed_by("arming"),
            Some(KeypadStatus { mode: KeypadMode::Home, phase: DelayPhase::Exit })
        );
        assert_eq!(KeypadAction::ArmAway.completed_by("armed_away"), Some(AWAY));
        assert_eq!(KeypadAction::ArmAway.completed_by("armed_home"), None);
        // Exit-delay confirmation matches the Hub: 03 03 02 00
        let reply = KeypadReply::Status {
            status: KeypadAction::ArmAway.completed_by("arming").unwrap(),
            awaiting_pin: false,
        };
        assert_eq!(reply.data(), vec![0x03, 0x03, 0x02, 0x00]);
    }

    // --- Decisions ---

    #[test]
    fn disarm_button_asks_for_pin() {
        assert_eq!(
            decide(&KeypadEvent::Button(KeypadButton::Disarm), AWAY, None),
            KeypadDecision::Reply(KeypadReply::Status { status: AWAY, awaiting_pin: true })
        );
    }

    #[test]
    fn arm_buttons_request_action() {
        assert_eq!(
            decide(&KeypadEvent::Button(KeypadButton::Away), KeypadStatus::DISARMED, None),
            KeypadDecision::Request { action: KeypadAction::ArmAway, pin: None }
        );
    }

    #[test]
    fn pin_uses_pending_action() {
        let pin = KeypadPin::new(vec![1, 2, 3, 4]);
        assert_eq!(
            decide(&KeypadEvent::Pin(pin.clone()), KeypadStatus::DISARMED, Some(KeypadAction::ArmHome)),
            KeypadDecision::Request { action: KeypadAction::ArmHome, pin: Some(pin) }
        );
    }

    #[test]
    fn pin_alone_disarms_when_armed() {
        let pin = KeypadPin::new(vec![1, 2, 3, 4]);
        assert_eq!(
            decide(&KeypadEvent::Pin(pin.clone()), TRIGGERED, None),
            KeypadDecision::Request { action: KeypadAction::Disarm, pin: Some(pin) }
        );
    }

    #[test]
    fn pin_with_nothing_to_do() {
        let pin = KeypadPin::new(vec![1, 2, 3, 4]);
        assert_eq!(
            decide(&KeypadEvent::Pin(pin), KeypadStatus::DISARMED, None),
            KeypadDecision::Reply(KeypadReply::NoAction)
        );
    }

    #[test]
    fn profile_and_motion_report_current_mode() {
        assert_eq!(
            decide(&KeypadEvent::ProfileRequest, TRIGGERED, None),
            KeypadDecision::Reply(KeypadReply::Profile(KeypadMode::Triggered))
        );
        assert_eq!(decide(&KeypadEvent::Motion(true), HOME, None), KeypadDecision::Reply(KeypadReply::Motion(HOME)));
        assert_eq!(decide(&KeypadEvent::Motion(false), HOME, None), KeypadDecision::Ignore);
        assert_eq!(decide(&KeypadEvent::Button(KeypadButton::Panic), HOME, None), KeypadDecision::Ignore);
    }
}
