use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Full daemon configuration.
/// All fields have defaults — the user can run without a config file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub identity_path: PathBuf,
    pub listen_addrs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub listen: String,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetworkConfig {
    pub bootstrap_peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub download_dir: PathBuf,
    pub database_path: PathBuf,
}

// --- Defaults ----------------------------------------------------------------

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            identity_path: default_config_dir().join("identity.key"),
            listen_addrs: vec![
                "/ip4/0.0.0.0/tcp/4321".to_string(),
                "/ip6/::/tcp/4321".to_string(),
            ],
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7070".to_string(),
            token: None,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            download_dir: dirs::download_dir()
                .unwrap_or_else(|| PathBuf::from("~/Downloads"))
                .join("rucio"),
            database_path: default_data_dir().join("rucio.db"),
        }
    }
}

// --- Loading -----------------------------------------------------------------

impl Config {
    /// Load configuration from `path`, or from the default location if `None`,
    /// falling back to built-in defaults if no file exists.
    pub fn load(path: Option<&std::path::Path>) -> Result<Self> {
        let resolved = path
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| default_config_dir().join("config.toml"));

        if resolved.exists() {
            let contents = std::fs::read_to_string(&resolved)?;
            let config: Config = toml::from_str(&contents)?;
            Ok(config)
        } else {
            Ok(Config::default())
        }
    }

    /// Persist the current configuration to disk.
    pub fn save(&self) -> Result<()> {
        let path = default_config_dir().join("config.toml");
        std::fs::create_dir_all(path.parent().unwrap())?;
        let contents = toml::to_string_pretty(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }
}

// --- Helpers -----------------------------------------------------------------

fn default_config_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("~/.config"))
        .join("rucio")
}

fn default_data_dir() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("~/.local/share"))
        .join("rucio")
}
