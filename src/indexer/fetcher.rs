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

macro_rules! retry_rpc {
    ($self:expr, $op:expr, $rpc:ident, $body:expr) => {{
        let mut delay = $self.initial_delay;
        let mut attempt = 0;
        loop {
            let $rpc = &$self.rpcs[attempt as usize % $self.rpcs.len()];
            match $body.await {
                Ok(v) => break Ok(v),
                Err(e) => {
                    if attempt == $self.max_retries {
                        error!(op = $op, attempt, error = %e, "Max retries exhausted");
                        break Err(anyhow::anyhow!("{}: {}", $op, e));
                    }
                    warn!(op = $op, attempt, error = %e, retry_in = ?delay, "Retrying");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = $self.cancel.cancelled() => {
                            break Err(anyhow::anyhow!("{}: cancelled during retry", $op));
                        }
                    }
                    delay = (delay * 2).min(Duration::from_secs(30));
                    attempt += 1;
                }
            }
        }
    }};
}

/// Async RPC wrapper with exponential backoff retries and round-robin failover.
pub struct Fetcher {
    rpcs: Vec<RpcClient>,
    max_retries: u32,
    initial_delay: Duration,
    cancel: CancellationToken,
}

impl Fetcher {
    pub fn new(rpc_urls: &[String], max_retries: u32, initial_delay_ms: u64, cancel: CancellationToken) -> Self {
        let rpcs = rpc_urls
            .iter()
            .map(|url| {
                RpcClient::new_with_commitment(url.to_string(), CommitmentConfig::confirmed())
            })
            .collect();

        Self {
            rpcs,
            max_retries,
            initial_delay: Duration::from_millis(initial_delay_ms),
            cancel,
        }
    }

    pub async fn get_slot(&self) -> anyhow::Result<u64> {
        retry_rpc!(self, "get_slot", rpc, rpc.get_slot())
    }

    pub async fn get_signatures(
        &self,
        program: &Pubkey,
        before: Option<&str>,
        until: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let before_sig = before.map(Signature::from_str).transpose()
            .map_err(|e| anyhow::anyhow!("Invalid before sig: {e}"))?;
        let until_sig = until.map(Signature::from_str).transpose()
            .map_err(|e| anyhow::anyhow!("Invalid until sig: {e}"))?;

        let program = *program;

        retry_rpc!(self, "get_signatures", rpc, {
            let config = GetConfirmedSignaturesForAddress2Config {
                before: before_sig,
                until: until_sig,
                limit: Some(limit),
                commitment: Some(CommitmentConfig::confirmed()),
            };
            rpc.get_signatures_for_address_with_config(&program, config)
        })
    }

    pub async fn get_transaction(
        &self,
        sig: &str,
    ) -> anyhow::Result<EncodedConfirmedTransactionWithStatusMeta> {
        let signature = Signature::from_str(sig)
            .map_err(|e| anyhow::anyhow!("Invalid signature: {e}"))?;
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };

        retry_rpc!(self, "get_transaction", rpc, {
            rpc.get_transaction_with_config(&signature, config)
        })
    }
}
