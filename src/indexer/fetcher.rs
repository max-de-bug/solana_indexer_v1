use anyhow::{anyhow, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::str::FromStr;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

/// Async RPC wrapper with exponential backoff retries.
pub struct Fetcher {
    rpc: RpcClient,
    max_retries: u32,
    initial_delay: Duration,
    cancel: CancellationToken,
}

impl Fetcher {
    pub fn new(rpc_url: &str, max_retries: u32, initial_delay_ms: u64, cancel: CancellationToken) -> Self {
        Self {
            rpc: RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::confirmed(),
            ),
            max_retries,
            initial_delay: Duration::from_millis(initial_delay_ms),
            cancel,
        }
    }

    pub async fn get_slot(&self) -> Result<u64, IndexerError> {
        self.retry("get_slot", || self.rpc.get_slot()).await
    }

    pub async fn get_signatures(
        &self,
        program: &Pubkey,
        before: Option<&str>,
        until: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>, IndexerError> {
        let before_sig = before.map(Signature::from_str).transpose()
            .map_err(|e| IndexerError::Rpc(format!("Invalid before sig: {e}")))?;
        let until_sig = until.map(Signature::from_str).transpose()
            .map_err(|e| IndexerError::Rpc(format!("Invalid until sig: {e}")))?;

        let config = GetConfirmedSignaturesForAddress2Config {
            before: before_sig,
            until: until_sig,
            limit: Some(limit),
            commitment: Some(CommitmentConfig::confirmed()),
        };
        let program = *program;

        self.retry("get_signatures", || {
            self.rpc.get_signatures_for_address_with_config(&program, config.clone())
        }).await
    }

    pub async fn get_transaction(
        &self,
        sig: &str,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, IndexerError> {
        let signature = Signature::from_str(sig)
            .map_err(|e| IndexerError::Rpc(format!("Invalid signature: {e}")))?;
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        self.retry("get_transaction", || {
            self.rpc.get_transaction_with_config(&signature, config)
        }).await
    }

    /// Generic async retry with exponential backoff capped at 30 s.
    /// Respects the cancellation token during backoff sleeps.
    async fn retry<F, Fut, T>(&self, op: &str, f: F) -> Result<T, IndexerError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, solana_client::client_error::ClientError>>,
    {
        let mut delay = self.initial_delay;
        for attempt in 0..=self.max_retries {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt == self.max_retries {
                        error!(%op, attempt, error = %e, "Max retries exhausted");
                        return Err(IndexerError::Rpc(format!("{op}: {e}")));
                    }
                    warn!(%op, attempt, error = %e, retry_in = ?delay, "Retrying");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.cancel.cancelled() => {
                            return Err(IndexerError::Rpc(format!("{op}: cancelled during retry")));
                        }
                    }
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        unreachable!()
    }
}

