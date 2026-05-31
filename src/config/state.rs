use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::fs::{self, File};
use std::io::{Write, Read};

use crate::protocol::sensor::SensorState;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct PersistedSensorState {
    pub mac: String,
    pub sensor_type: String, // e.g. "motion", "contact", "chime"
    pub last_seen: u64,      // Epoch seconds
    #[serde(default = "default_battery")]
    pub battery: Option<u8>, // 0..100, None for mains-powered devices (e.g., Chime)
    pub signal: i8,          // RSSI dBm
    #[serde(default)]
    pub state: SensorState,  // Type-specific state (persisted)
}

fn default_battery() -> Option<u8> {
    Some(100)
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct SystemState {
    pub sensors: HashMap<String, PersistedSensorState>,
}

impl SystemState {
    pub fn load_from_yaml<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        if !path.as_ref().exists() {
            return Ok(SystemState::default());
        }
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let state: SystemState = serde_yaml::from_str(&contents)?;
        Ok(state)
    }

    pub fn save_to_yaml_atomic<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let path = path.as_ref();
        
        // Automatically create parent directory if missing
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = path.with_extension("tmp");

        // 1. Write to tmp file
        {
            let mut file = File::create(&tmp_path)?;
            let yaml_data = serde_yaml::to_string(self)?;
            file.write_all(yaml_data.as_bytes())?;
            file.flush()?;
            file.sync_all()?; // Ensure it's persisted to disk
        }

        // 2. Rename tmp to target (atomic)
        fs::rename(&tmp_path, path)?;

        Ok(())
    }
}
