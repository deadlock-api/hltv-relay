use clap::Parser;

use crate::error::AppError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageBackend {
    Memory,
    Redis,
}

impl std::fmt::Display for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory => write!(f, "memory"),
            Self::Redis => write!(f, "redis"),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "hltv-relay", about = "HLTV broadcast relay server")]
struct Cli {
    /// Port to listen on
    #[arg(long, env = "HLTV_RELAY_PORT")]
    port: Option<u16>,

    /// Host to bind to
    #[arg(long, env = "HLTV_RELAY_HOST")]
    host: Option<String>,

    /// Storage backend: memory or redis
    #[arg(long, env = "HLTV_RELAY_STORAGE")]
    storage: Option<String>,

    /// Redis connection URL
    #[arg(long, env = "HLTV_RELAY_REDIS_URL")]
    redis_url: Option<String>,

    /// Authentication mode (comma-separated): key, allow-all, network
    #[arg(long, env = "HLTV_RELAY_AUTH_MODE")]
    auth_mode: Option<String>,

    /// Authentication key for key-based auth
    #[arg(long, env = "HLTV_RELAY_AUTH_KEY")]
    auth_key: Option<String>,

    /// Allowed networks in CIDR notation (comma-separated)
    #[arg(long, env = "HLTV_RELAY_ALLOWED_NETWORKS")]
    allowed_networks: Option<String>,

    /// Fragment delay for sync endpoint (number of fragments behind latest)
    #[arg(long, env = "HLTV_RELAY_FRAGMENT_DELAY")]
    fragment_delay: Option<i32>,
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) port: u16,
    pub(crate) host: String,
    pub(crate) storage: StorageBackend,
    pub(crate) redis_url: String,
    pub(crate) auth_modes: Vec<String>,
    pub(crate) auth_key: Option<String>,
    pub(crate) allowed_networks: Vec<String>,
    pub(crate) fragment_delay: i32,
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Configuration:")?;
        writeln!(f, "  host: {}", self.host)?;
        writeln!(f, "  port: {}", self.port)?;
        writeln!(f, "  storage: {}", self.storage)?;
        writeln!(f, "  redis_url: {}", self.redis_url)?;
        writeln!(f, "  auth_modes: {:?}", self.auth_modes)?;
        writeln!(f, "  allowed_networks: {:?}", self.allowed_networks)?;
        writeln!(f, "  fragment_delay: {}", self.fragment_delay)?;
        Ok(())
    }
}

fn parse_storage(value: &str) -> Result<StorageBackend, AppError> {
    match value {
        "memory" => Ok(StorageBackend::Memory),
        "redis" => Ok(StorageBackend::Redis),
        other => Err(AppError::ConfigError(format!(
            "invalid storage backend: '{other}', expected 'memory' or 'redis'"
        ))),
    }
}

fn parse_comma_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

impl Config {
    /// Load configuration with precedence: CLI args > env vars > defaults.
    ///
    /// Clap handles CLI > env automatically.
    pub(crate) fn load() -> Result<Self, AppError> {
        let cli = Cli::parse();

        let port = cli.port.unwrap_or(3000);
        let host = cli.host.unwrap_or_else(|| "0.0.0.0".to_owned());
        let storage_str = cli.storage.unwrap_or_else(|| "memory".to_owned());
        let storage = parse_storage(&storage_str)?;
        let redis_url = cli
            .redis_url
            .unwrap_or_else(|| "redis://127.0.0.1:6379".to_owned());
        let auth_mode_str = cli.auth_mode.unwrap_or_default();
        let auth_modes = parse_comma_list(&auth_mode_str);
        let auth_key = cli.auth_key;
        let networks_str = cli.allowed_networks.unwrap_or_default();
        let allowed_networks = parse_comma_list(&networks_str);
        let fragment_delay = cli.fragment_delay.unwrap_or(8);

        Ok(Self {
            port,
            host,
            storage,
            redis_url,
            auth_modes,
            auth_key,
            allowed_networks,
            fragment_delay,
        })
    }
}
