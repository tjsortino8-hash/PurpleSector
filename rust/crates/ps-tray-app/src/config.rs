//! Configuration module — loads/saves app settings from a TOML file.
//!
//! Config file location:
//!   Windows: %APPDATA%\PurpleSector\ps-tray-app\config.toml
//!   Linux:   ~/.config/purplesector/ps-tray-app/config.toml
//!   macOS:   ~/Library/Application Support/com.purplesector.ps-tray-app/config.toml

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Which sim to capture telemetry from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimType {
    #[serde(rename = "ac")]
    AssettoCorsaSS,
    #[serde(rename = "acc")]
    AssettoCorsa,
    #[serde(rename = "demo")]
    Demo,
}

impl SimType {
    pub fn label(&self) -> &'static str {
        match self {
            SimType::AssettoCorsaSS => "Assetto Corsa",
            SimType::AssettoCorsa => "ACC",
            SimType::Demo => "Demo / Replay",
        }
    }

    pub fn all() -> &'static [SimType] {
        &[SimType::AssettoCorsaSS, SimType::AssettoCorsa, SimType::Demo]
    }
}

impl std::fmt::Display for SimType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Persistent tray app configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Which sim to capture from.
    pub sim_type: SimType,

    /// gRPC gateway endpoint URL.
    pub grpc_endpoint: String,

    /// Username (placeholder for auth).
    pub username: String,

    /// Password (placeholder — will be replaced by OIDC flow).
    #[serde(default)]
    pub password: String,

    /// ACC-specific: connection password from broadcasting.json.
    #[serde(default)]
    pub acc_connection_password: String,

    /// ACC-specific: update interval in ms (default 100 = 10Hz).
    #[serde(default = "default_acc_update_interval")]
    pub acc_update_interval_ms: u32,

    /// Batch flush interval in milliseconds.
    #[serde(default = "default_batch_interval_ms")]
    pub batch_interval_ms: u64,
}

fn default_acc_update_interval() -> u32 {
    100
}
fn default_batch_interval_ms() -> u64 {
    100
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            sim_type: SimType::AssettoCorsaSS,
            grpc_endpoint: "http://localhost:50051".into(),
            username: String::new(),
            password: String::new(),
            acc_connection_password: String::new(),
            acc_update_interval_ms: default_acc_update_interval(),
            batch_interval_ms: default_batch_interval_ms(),
        }
    }
}

impl AppConfig {
    /// Returns the platform-appropriate config file path.
    pub fn config_path() -> PathBuf {
        if let Some(proj_dirs) =
            directories::ProjectDirs::from("com", "purplesector", "ps-tray-app")
        {
            let dir = proj_dirs.config_dir();
            dir.join("config.toml")
        } else {
            PathBuf::from("ps-tray-app-config.toml")
        }
    }

    /// Load config from disk, or return default if not found.
    pub fn load() -> Self {
        let path = Self::config_path();
        match std::fs::read_to_string(&path) {
            Ok(contents) => match toml::from_str(&contents) {
                Ok(config) => {
                    tracing::info!("Config loaded from {}", path.display());
                    config
                }
                Err(e) => {
                    tracing::warn!("Failed to parse config ({}), using defaults: {e}", path.display());
                    Self::default()
                }
            },
            Err(_) => {
                tracing::info!("No config found at {}, using defaults", path.display());
                Self::default()
            }
        }
    }

    /// Save config to disk.
    pub fn save(&self) -> anyhow::Result<()> {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let contents = toml::to_string_pretty(self)?;
        std::fs::write(&path, contents)?;
        tracing::info!("Config saved to {}", path.display());
        Ok(())
    }
}
