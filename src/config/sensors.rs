use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::fs::{self, File};
use std::io::{Write, Read};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SensorMetadata {
    pub name: String,
    pub r#type: String, // e.g., "motion", "contact"
    pub timeout_sec: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SensorsConfig {
    pub sensors: HashMap<String, SensorMetadata>,
}

impl SensorsConfig {
    pub fn load_from_yaml<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let path_ref = path.as_ref();
        if !path_ref.exists() {
            return Ok(SensorsConfig { sensors: HashMap::new() });
        }
        let mut file = File::open(path_ref)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: SensorsConfig = serde_yaml::from_str(&contents)?;
        Ok(config)
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
