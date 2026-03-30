use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tracing::info;

/// Application configuration, loaded entirely from environment variables.
#[derive(Clone)]
pub struct Config {
    pub rpc_url: String,
    pub database_url: String,
    pub program_id: Pubkey,
    /// Path to an Anchor IDL JSON file on disk (optional if `idl_account` is set).
    pub idl_path: Option<String>,
    /// On-chain IDL account address (optional if `idl_path` is set).
    pub idl_account: Option<String>,
    pub indexing_mode: IndexingMode,
    pub api_port: u16,
    pub batch_size: usize,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    pub poll_interval_ms: u64,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("rpc_url", &"<redacted>")
            .field("database_url", &"<redacted>")
            .field("program_id", &self.program_id)
            .field("idl_path", &self.idl_path)
            .field("idl_account", &self.idl_account)
            .field("indexing_mode", &self.indexing_mode)
            .field("api_port", &self.api_port)
            .field("batch_size", &self.batch_size)
            .field("max_retries", &self.max_retries)
            .field("retry_delay_ms", &self.retry_delay_ms)
            .field("poll_interval_ms", &self.poll_interval_ms)
            .finish()
    }
}

/// How the indexer fetches data.
#[derive(Debug, Clone)]
pub enum IndexingMode {
    /// Process a slot range `[start, end]`.
    BatchSlots { start: u64, end: u64 },
    /// Process a comma-separated list of transaction signatures.
    BatchSignatures { signatures: Vec<String> },
    /// Poll for new transactions, backfilling from last checkpoint.
    Realtime,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let rpc_url = env_or("RPC_URL", "https://api.mainnet-beta.solana.com");
        let database_url = required("DATABASE_URL")?;
        let program_id = Pubkey::from_str(&required("PROGRAM_ID")?)
            .map_err(|e| anyhow::anyhow!("Invalid PROGRAM_ID: {e}"))?;

        let idl_path = std::env::var("IDL_PATH").ok();
        let idl_account = std::env::var("IDL_ACCOUNT").ok();
        anyhow::ensure!(
            idl_path.is_some() || idl_account.is_some(),
            "At least one of IDL_PATH or IDL_ACCOUNT must be set"
        );

        let indexing_mode = match std::env::var("INDEXING_MODE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "batch_slots" => {
                let start = required("BATCH_START_SLOT")?.parse()?;
                let end = required("BATCH_END_SLOT")?.parse()?;
                anyhow::ensure!(start <= end, "BATCH_START_SLOT must be <= BATCH_END_SLOT");
                IndexingMode::BatchSlots { start, end }
            }
            "batch_signatures" => {
                let raw = required("BATCH_SIGNATURES")?;
                let sigs: Vec<String> = raw.split(',').map(|s| s.trim().to_string()).collect();
                anyhow::ensure!(!sigs.is_empty(), "BATCH_SIGNATURES must not be empty");
                IndexingMode::BatchSignatures { signatures: sigs }
            }
            _ => IndexingMode::Realtime,
        };

        let api_port = env_or("API_PORT", "3000").parse()?;
        let batch_size = env_or("BATCH_SIZE", "100").parse()?;
        let max_retries = env_or("MAX_RETRIES", "5").parse()?;
        let retry_delay_ms = env_or("RETRY_DELAY_MS", "500").parse()?;
        let poll_interval_ms = env_or("POLL_INTERVAL_MS", "2000").parse()?;

        let cfg = Self {
            rpc_url,
            database_url,
            program_id,
            idl_path,
            idl_account,
            indexing_mode,
            api_port,
            batch_size,
            max_retries,
            retry_delay_ms,
            poll_interval_ms,
        };

        info!(?cfg.indexing_mode, %cfg.program_id, %cfg.api_port, "Configuration loaded");
        Ok(cfg)
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn required(key: &str) -> anyhow::Result<String> {
    std::env::var(key).map_err(|_| anyhow::anyhow!("Missing required env var: {key}"))
}
