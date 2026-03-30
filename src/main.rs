mod api;
mod config;
mod db;
mod error;
mod idl;
mod indexer;

use crate::config::Config;
use crate::idl::AnchorIdl;
use crate::indexer::fetcher::Fetcher;
use crate::indexer::IndexerState;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ---- Bootstrap ----------------------------------------------------------
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "solana_indexer_v1=info,sqlx=warn".into()),
        )
        .with_target(true)
        .init();

    info!("Starting Solana Indexer V1");

    let config = Config::from_env()?;
    let pool = db::create_pool(&config.database_url).await?;
    db::init_schema(&pool).await?;

    // ---- IDL loading (file → on-chain) --------------------------------------
    let idl = load_idl(&config).await?;

    // ---- Cancellation -------------------------------------------------------
    let cancel = CancellationToken::new();

    // ---- API server ---------------------------------------------------------
    let api_state = Arc::new(api::ApiState { pool: pool.clone() });
    let app = api::router(api_state).layer(CorsLayer::permissive());
    let port = config.api_port;
    let api_cancel = cancel.clone();

    let api_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await.expect("Failed to bind API port");
        info!(%port, "API server listening");
        axum::serve(listener, app)
            .with_graceful_shutdown(api_cancel.cancelled_owned())
            .await
            .expect("API server error");
    });

    // ---- Indexer ------------------------------------------------------------
    let fetcher = Fetcher::new(
        &config.rpc_url,
        config.max_retries,
        config.retry_delay_ms,
    );

    let state = Arc::new(IndexerState {
        pool: pool.clone(),
        idl,
        config: config.clone(),
        fetcher,
        cancel: cancel.clone(),
    });

    let indexer_handle = tokio::spawn(indexer::run(state));

    // ---- Graceful shutdown --------------------------------------------------
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT — shutting down");
            cancel.cancel();
        }
        res = indexer_handle => {
            match res {
                Ok(Ok(())) => info!("Indexer finished"),
                Ok(Err(e)) => error!(error = %e, "Indexer error"),
                Err(e) => error!(error = %e, "Indexer panicked"),
            }
            cancel.cancel();
        }
    }

    let _ = api_handle.await;
    pool.close().await;
    info!("Shutdown complete");
    Ok(())
}

/// Load IDL: file first, then on-chain.
async fn load_idl(config: &Config) -> anyhow::Result<AnchorIdl> {
    if let Some(ref path) = config.idl_path {
        if std::fs::metadata(path).is_ok() {
            info!(%path, "Loading IDL from file");
            return AnchorIdl::from_file(path);
        }
    }
    if let Some(ref addr) = config.idl_account {
        let pk = Pubkey::from_str(addr)
            .map_err(|e| anyhow::anyhow!("Invalid IDL_ACCOUNT: {e}"))?;
        let rpc = RpcClient::new_with_commitment(
            config.rpc_url.clone(), CommitmentConfig::confirmed(),
        );
        info!(%addr, "Loading IDL from chain");
        return AnchorIdl::from_chain(&rpc, &pk).await;
    }
    anyhow::bail!("No IDL source: set IDL_PATH or IDL_ACCOUNT")
}
