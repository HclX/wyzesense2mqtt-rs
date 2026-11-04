use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

/// Display metadata for a dongle. Kept apart from sensors.yaml because a
/// dongle's identity is its MAC, and its name survives unplugging/replugging.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DongleMetadata {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DonglesConfig {
    #[serde(default)]
    pub dongles: HashMap<String, DongleMetadata>,
}

impl DonglesConfig {
    pub fn load_from_yaml<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let path_ref = path.as_ref();
        if !path_ref.exists() {
            return Ok(DonglesConfig::default());
        }
        let mut file = File::open(path_ref)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: DonglesConfig = serde_yaml::from_str(&contents)?;
        Ok(config)
    }

    pub fn save_to_yaml_atomic<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = path.with_extension("tmp");

        {
            let mut file = File::create(&tmp_path)?;
            let yaml_data = serde_yaml::to_string(self)?;
            file.write_all(yaml_data.as_bytes())?;
            file.flush()?;
            file.sync_all()?;
        }

        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// The display name for a dongle MAC, if one has been set.
    pub fn name_of(&self, mac: &str) -> Option<&str> {
        self.dongles
            .get(&mac.to_ascii_uppercase())
            .map(|m| m.name.as_str())
    }

    pub fn set_name(&mut self, mac: &str, name: &str) {
        self.dongles.insert(
            mac.to_ascii_uppercase(),
            DongleMetadata { name: name.to_string() },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_keyed_by_uppercase_mac() {
        let mut config = DonglesConfig::default();
        config.set_name("77a85a36", "Living Room");
        assert_eq!(config.name_of("77A85A36"), Some("Living Room"));
        assert_eq!(config.name_of("77a85a36"), Some("Living Room"));
    }

    #[test]
    fn round_trips_through_yaml() {
        let dir = std::env::temp_dir().join(format!("wyze-dongles-{}", std::process::id()));
        let path = dir.join("dongles.yaml");
        let mut config = DonglesConfig::default();
        config.set_name("77A85A36", "Living Room");
        config.save_to_yaml_atomic(&path).unwrap();

        let loaded = DonglesConfig::load_from_yaml(&path).unwrap();
        assert_eq!(loaded.name_of("77A85A36"), Some("Living Room"));
        let _ = std::fs::remove_dir_all(dir);
    }
}
