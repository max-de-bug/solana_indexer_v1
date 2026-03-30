use thiserror::Error;

/// Unified error type for the indexer.
#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Solana client error: {0}")]
    SolanaClient(#[from] solana_client::client_error::ClientError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("IDL error: {0}")]
    Idl(String),

    #[error("Decode error at offset {offset}: {message}")]
    Decode { offset: usize, message: String },

    #[error("Config error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
