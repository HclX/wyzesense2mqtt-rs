use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;

// --- Default Serde helper functions ---
fn default_dongle() -> String { "auto".to_string() }
fn default_true() -> bool { true }
fn default_false() -> bool { false }
fn default_web_port() -> u16 { 8080 }
fn default_mqtt_port() -> u16 { 1883 }
fn default_self_topic() -> String { "wyzesense2mqtt".to_string() }
fn default_hass_topic() -> String { "homeassistant".to_string() }
fn default_log_level() -> String { "info".to_string() }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UsbConfig {
    #[serde(default = "default_dongle")]
    pub dongle: String,
}

impl Default for UsbConfig {
    fn default() -> Self {
        Self { dongle: default_dongle() }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WebConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_web_port")]
    pub port: u16,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            port: default_web_port(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MqttConfig {
    #[serde(default = "default_false")]
    pub enabled: bool,
    pub host: Option<String>,
    #[serde(default = "default_mqtt_port")]
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default = "default_self_topic")]
    pub self_topic_root: String,
    #[serde(default = "default_hass_topic")]
    pub hass_topic_root: String,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            enabled: default_false(),
            host: None,
            port: default_mqtt_port(),
            username: None,
            password: None,
            self_topic_root: default_self_topic(),
            hass_topic_root: default_hass_topic(),
        }
    }
}

fn default_max_log_files() -> usize { 7 }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Optional path to log file. If set, logs are written to a daily-rotating
    /// file instead of stdout. Example: "logs/wyzesense2mqtt-rs.log"
    #[serde(default)]
    pub log_file: Option<String>,
    /// Maximum number of rotated log files to keep (default: 7).
    /// Older files are automatically deleted.
    #[serde(default = "default_max_log_files")]
    pub max_log_files: usize,
    #[serde(default = "default_false")]
    pub no_ansi: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            log_file: None,
            max_log_files: default_max_log_files(),
            no_ansi: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BridgeConfig {
    #[serde(default = "default_false")]
    pub enabled: bool,
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self { enabled: default_false(), auth_token: None }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub usb: UsbConfig,
    #[serde(default)]
    pub web: WebConfig,
    #[serde(default)]
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub bridge: BridgeConfig,
}

impl AppConfig {
    /// Loads configuration from a YAML file, falling back to environment variables and defaults.
    pub fn load<P: AsRef<Path>>(path: Option<P>) -> Self {
        let mut config = Self::default();

        // 1. Load from YAML if path exists
        if let Some(ref p) = path {
            if p.as_ref().exists() {
                if let Ok(loaded) = Self::load_from_yaml(p) {
                    config = loaded;
                }
            }
        } else {
            // Try fallback default file ./config.yaml
            let default_path = Path::new("config.yaml");
            if default_path.exists() {
                if let Ok(loaded) = Self::load_from_yaml(default_path) {
                    config = loaded;
                }
            }
        }

        // 2. Override with Environment Variables. String-valued vars treat an
        // empty value as "not set" (rather than an explicit blank) so that
        // e.g. a docker-compose file forwarding `MQTT_HOST: "${MQTT_HOST:-}"`
        // doesn't clobber a value the user set directly in config.yaml just
        // because they never bothered to also set it on the host.
        if let Some(val) = std::env::var("USB_DONGLE").ok().filter(|v| !v.is_empty()) { config.usb.dongle = val; }
        if let Ok(val) = std::env::var("WEB_ENABLED") { config.web.enabled = val.parse().unwrap_or(config.web.enabled); }
        if let Ok(val) = std::env::var("WEB_PORT") { config.web.port = val.parse().unwrap_or(config.web.port); }
        if let Ok(val) = std::env::var("MQTT_ENABLED") { config.mqtt.enabled = val.parse().unwrap_or(config.mqtt.enabled); }
        if let Some(val) = std::env::var("MQTT_HOST").ok().filter(|v| !v.is_empty()) { config.mqtt.host = Some(val); }
        if let Ok(val) = std::env::var("MQTT_PORT") { config.mqtt.port = val.parse().unwrap_or(config.mqtt.port); }
        if let Some(val) = std::env::var("MQTT_USERNAME").ok().filter(|v| !v.is_empty()) { config.mqtt.username = Some(val); }
        if let Some(val) = std::env::var("MQTT_PASSWORD").ok().filter(|v| !v.is_empty()) { config.mqtt.password = Some(val); }
        if let Some(val) = std::env::var("SELF_TOPIC_ROOT").ok().filter(|v| !v.is_empty()) { config.mqtt.self_topic_root = val; }
        if let Some(val) = std::env::var("HASS_TOPIC_ROOT").ok().filter(|v| !v.is_empty()) { config.mqtt.hass_topic_root = val; }
        if let Some(val) = std::env::var("LOG_LEVEL").ok().filter(|v| !v.is_empty()) { config.logging.level = val; }
        if let Ok(val) = std::env::var("LOG_NO_ANSI") { config.logging.no_ansi = val.parse().unwrap_or(config.logging.no_ansi); }
        if let Ok(val) = std::env::var("BRIDGE_ENABLED") { config.bridge.enabled = val.parse().unwrap_or(config.bridge.enabled); }
        if let Some(val) = std::env::var("BRIDGE_AUTH_TOKEN").ok().filter(|v| !v.is_empty()) { config.bridge.auth_token = Some(val); }

        // Ensure that if host is set, MQTT is enabled
        if config.mqtt.host.is_some() {
            config.mqtt.enabled = true;
        }

        config
    }

    fn load_from_yaml<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: AppConfig = serde_yaml::from_str(&contents)?;
        Ok(config)
    }

    /// Generates a clean sectioned YAML template string.
    pub fn to_template_string() -> String {
        r#"# Wyze Sense to MQTT Bridge (Rust) Sectioned Configuration Template
# ----------------------------------------------

# USB Dongle Settings
# (Set to "auto" to dynamically discover the dongle via sysfs)
usb:
  dongle: "auto"

# Web Console Panel Settings
web:
  enabled: true
  port: 8080

# MQTT Broker Gateway Integration Settings
# (Set host to automatically enable publishing)
mqtt:
  enabled: false
  host: "localhost"
  port: 1883
  username: "homeassistant"
  password: "your_secure_password"
  self_topic_root: "wyzesense2mqtt"
  hass_topic_root: "homeassistant"

# WebSocket Bridge Settings
# (Optional token-based authentication for remote dongle bridges)
bridge:
  enabled: false
  # auth_token: "your_secret_token"

# Diagnostics Structural Logging Level
# (Options: trace, debug, info, warn, error)
# Set log_file to enable daily-rotating file logging (default: stdout only)
# Relative paths are resolved against this config file's directory.
# max_log_files controls how many rotated files to keep (default: 7)
logging:
  level: "info"
  # log_file: "logs/wyzesense2mqtt-rs.log"
  # max_log_files: 7
"#.to_string()
    }
}
