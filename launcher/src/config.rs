use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const CONFIG_FILE_NAME: &str = "launcher_config.json";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct LauncherConfig {
    pub game_dir: Option<PathBuf>,
    pub last_hub_addr: Option<String>,
    pub last_player_name: Option<String>,
}

impl LauncherConfig {
    /// Determines the location to read/write `launcher_config.json`.
    /// 1. Checks if `launcher_config.json` exists in current working directory.
    /// 2. Checks if `launcher_config.json` exists next to the current executable.
    /// 3. Defaults to next to the current executable (or cwd fallback).
    pub fn config_path() -> PathBuf {
        let cwd_path = PathBuf::from(CONFIG_FILE_NAME);
        if cwd_path.exists() {
            return cwd_path;
        }

        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(parent) = exe_path.parent() {
                let exe_dir_path = parent.join(CONFIG_FILE_NAME);
                if exe_dir_path.exists() {
                    return exe_dir_path;
                }
                return exe_dir_path;
            }
        }

        cwd_path
    }

    pub fn load() -> Self {
        let path = Self::config_path();
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Ok(cfg) = serde_json::from_str(&content) {
                tracing::info!(path = %path.display(), "Loaded launcher configuration");
                return cfg;
            }
        }
        Self::default()
    }

    pub fn save(&self) -> Result<(), String> {
        let path = Self::config_path();
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize configuration: {e}"))?;
        std::fs::write(&path, content)
            .map_err(|e| format!("Failed to write configuration file {}: {e}", path.display()))?;
        tracing::debug!(path = %path.display(), "Saved launcher configuration");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_launcher_config_serialization() {
        let cfg = LauncherConfig {
            game_dir: Some(PathBuf::from("C:\\Games\\SonicR")),
            last_hub_addr: Some("127.0.0.1:8080".to_string()),
            last_player_name: Some("Sonic Speed".to_string()),
        };

        let json = serde_json::to_string_pretty(&cfg).unwrap();
        let deserialized: LauncherConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, deserialized);
    }
}
