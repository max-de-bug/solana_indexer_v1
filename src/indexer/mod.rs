pub mod decoder;
pub mod fetcher;

use crate::config::{Config, IndexingMode};
use crate::db;
use crate::idl::{AnchorIdl, IdlTypeDef};
use crate::indexer::decoder::{decode_fields, match_instruction};
use crate::indexer::fetcher::Fetcher;
use serde_json::json;
use solana_transaction_status::EncodedTransaction;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Shared state for the indexer task.
pub struct IndexerState {
    pub pool: PgPool,
    pub idl: AnchorIdl,
    pub config: Config,
    pub fetcher: Fetcher,
    pub cancel: CancellationToken,
    /// Pre-computed type lookup — built once at startup.
    pub type_map: HashMap<String, IdlTypeDef>,
}

/// Dispatch to the configured indexing strategy.
pub async fn run(state: Arc<IndexerState>) -> anyhow::Result<()> {
    match &state.config.indexing_mode {
        IndexingMode::BatchSlots { start, end } => {
            run_batch_slots(state.clone(), *start, *end).await
        }
        IndexingMode::BatchSignatures { signatures } => {
            let sigs = signatures.clone();
            run_batch_signatures(state.clone(), &sigs).await
        }
        IndexingMode::Realtime => run_realtime(state.clone()).await,
    }
}

// ---------------------------------------------------------------------------
// Batch: slot range
// ---------------------------------------------------------------------------

async fn run_batch_slots(
    state: Arc<IndexerState>,
    start_slot: u64,
    end_slot: u64,
) -> anyhow::Result<()> {
    info!(%start_slot, %end_slot, "Starting batch-slot indexing");
    let mut before: Option<String> = None;
    let mut total = 0u64;

    loop {
        if state.cancel.is_cancelled() { break; }

        let sigs = state.fetcher.get_signatures(
            &state.config.program_id, before.as_deref(), None, state.config.batch_size,
        ).await?;

        if sigs.is_empty() { break; }

        for si in &sigs {
            if si.slot < start_slot {
                info!(%total, "Reached start_slot boundary");
                return Ok(());
            }
            if si.slot > end_slot { continue; }
            if let Err(e) = process_sig(&state, &si.signature, si.slot).await {
                warn!(sig = %si.signature, error = %e, "Failed — queued for retry");
                let _ = db::record_failed_signature(
                    &state.pool, &si.signature, si.slot, &e.to_string(),
                ).await;
            }
            total += 1;
            if total % 100 == 0 {
                db::update_sync_state(
                    &state.pool, &state.config.program_id.to_string(),
                    si.slot, Some(&si.signature),
                ).await?;
                info!(%total, slot = si.slot, "Progress");
            }
        }
        before = sigs.last().map(|s| s.signature.clone());
    }
    info!(%total, "Batch-slot indexing complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Batch: specific signatures
// ---------------------------------------------------------------------------

async fn run_batch_signatures(
    state: Arc<IndexerState>,
    signatures: &[String],
) -> anyhow::Result<()> {
    info!(count = signatures.len(), "Starting batch-signature indexing");
    for (i, sig) in signatures.iter().enumerate() {
        if state.cancel.is_cancelled() { break; }
        if let Err(e) = process_sig(&state, sig, 0).await {
            warn!(%sig, error = %e, "Failed — queued for retry");
            let _ = db::record_failed_signature(
                &state.pool, sig, 0, &e.to_string(),
            ).await;
        }
        if (i + 1) % 50 == 0 {
            info!(progress = i + 1, total = signatures.len(), "Progress");
        }
    }
    info!("Batch-signature indexing complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Real-time with cold-start backfill
// ---------------------------------------------------------------------------

async fn run_realtime(state: Arc<IndexerState>) -> anyhow::Result<()> {
    let pid = state.config.program_id.to_string();

    // Cold-start: backfill from last checkpoint.
    let last = db::get_last_processed(&state.pool, &pid).await?;
    let last_sig = last.as_ref().and_then(|(_, s)| s.clone());
    if last_sig.is_some() {
        info!("Cold start: backfilling from last checkpoint");
    } else {
        info!("Fresh start: no previous state");
    }
    backfill(&state, last_sig.as_deref()).await?;

    // Polling loop.
    let poll = std::time::Duration::from_millis(state.config.poll_interval_ms);
    info!(interval_ms = state.config.poll_interval_ms, "Entering real-time loop");

    loop {
        if state.cancel.is_cancelled() { break; }

        let latest = db::get_last_processed(&state.pool, &pid).await?;
        let until = latest.and_then(|(_, s)| s);

        let sigs = state.fetcher.get_signatures(
            &state.config.program_id, None, until.as_deref(), state.config.batch_size,
        ).await?;

        if sigs.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                _ = state.cancel.cancelled() => break,
            }
            continue;
        }

        for si in sigs.iter().rev() {
            if state.cancel.is_cancelled() { break; }
            if let Err(e) = process_sig(&state, &si.signature, si.slot).await {
                warn!(sig = %si.signature, error = %e, "Failed — queued for retry");
                let _ = db::record_failed_signature(
                    &state.pool, &si.signature, si.slot, &e.to_string(),
                ).await;
            }
        }

        if let Some(newest) = sigs.first() {
            db::update_sync_state(&state.pool, &pid, newest.slot, Some(&newest.signature)).await?;
        }
        info!(new_txs = sigs.len(), "Polled new transactions");

        // Periodically retry failed signatures.
        retry_failed_signatures(&state).await;
    }
    Ok(())
}

async fn backfill(state: &IndexerState, until: Option<&str>) -> anyhow::Result<()> {
    let mut before: Option<String> = None;
    let mut total = 0u64;

    loop {
        if state.cancel.is_cancelled() { break; }
        let sigs = state.fetcher.get_signatures(
            &state.config.program_id, before.as_deref(), until, state.config.batch_size,
        ).await?;
        if sigs.is_empty() { break; }

        for si in sigs.iter().rev() {
            if let Err(e) = process_sig(state, &si.signature, si.slot).await {
                warn!(sig = %si.signature, error = %e, "Backfill failed — queued for retry");
                let _ = db::record_failed_signature(
                    &state.pool, &si.signature, si.slot, &e.to_string(),
                ).await;
            }
            total += 1;
        }
        before = sigs.last().map(|s| s.signature.clone());

        let pid = state.config.program_id.to_string();
        if let Some(newest) = sigs.first() {
            db::update_sync_state(&state.pool, &pid, newest.slot, Some(&newest.signature)).await?;
        }
        if total % 500 == 0 {
            info!(%total, "Backfill progress");
        }
    }
    info!(%total, "Backfill complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction processing
// ---------------------------------------------------------------------------

async fn process_sig(state: &IndexerState, sig: &str, _hint_slot: u64) -> anyhow::Result<()> {
    // Dedup: skip already-indexed.
    if db::transaction_exists(&state.pool, sig).await? {
        return Ok(());
    }

    let tx = state.fetcher.get_transaction(sig).await?;
    let slot = tx.slot;
    let block_time = tx.block_time;
    let meta = tx.transaction.meta.as_ref();
    let success = meta.map_or(true, |m| m.err.is_none());
    let fee = meta.map(|m| m.fee);
    let err_msg = meta.and_then(|m| m.err.as_ref()).map(|e| format!("{e:?}"));

    // Extract signer from the transaction.
    let signer = extract_signer(&tx.transaction.transaction);

    // Atomic: insert transaction + all instructions in one DB transaction.
    let mut db_tx = state.pool.begin().await?;

    db::insert_transaction(
        &mut *db_tx, sig, slot, block_time, success, fee,
        err_msg.as_deref(), signer.as_deref(),
    ).await?;

    // Decode instructions.
    let encoded_tx = &tx.transaction.transaction;
    decode_and_store_tx(state, &mut db_tx, sig, slot, encoded_tx).await?;

    db_tx.commit().await?;
    debug!(%sig, %slot, "Indexed");
    Ok(())
}

fn extract_signer(encoded: &EncodedTransaction) -> Option<String> {
    match encoded {
        EncodedTransaction::Binary(blob, _) => {
            let bytes = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD, blob,
            ).ok()?;
            let tx: solana_sdk::transaction::VersionedTransaction =
                bincode::deserialize(&bytes).ok()?;
            tx.message.static_account_keys().first().map(|k| k.to_string())
        }
        _ => None,
    }
}

async fn decode_and_store_tx(
    state: &IndexerState,
    db_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tx_sig: &str,
    _slot: u64,
    encoded: &EncodedTransaction,
) -> anyhow::Result<()> {
    let pid_str = state.config.program_id.to_string();

    let tx_bytes = match encoded {
        EncodedTransaction::Binary(blob, _) => {
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, blob)
                .map_err(|e| anyhow::anyhow!("Base64 decode failed: {e}"))?
        }
        _ => return Ok(()),
    };

    let tx: solana_sdk::transaction::VersionedTransaction =
        bincode::deserialize(&tx_bytes)
            .map_err(|e| anyhow::anyhow!("Bincode deserialize failed: {e}"))?;

    let keys = tx.message.static_account_keys();
    let type_map: HashMap<String, &crate::idl::IdlTypeDef> =
        state.type_map.iter().map(|(k, v)| (k.clone(), v)).collect();

    for (ix_idx, ix) in tx.message.instructions().iter().enumerate() {
        let prog = keys.get(ix.program_id_index as usize)
            .map(|k| k.to_string()).unwrap_or_default();
        if prog != pid_str { continue; }

        let accounts: Vec<String> = ix.accounts.iter()
            .filter_map(|&i| keys.get(i as usize).map(|k| k.to_string()))
            .collect();

        if let Some((ix_def, remaining)) = match_instruction(&ix.data, &state.idl) {
            let args = decode_fields(remaining, &ix_def.args, &type_map)
                .unwrap_or_else(|e| { warn!(ix = %ix_def.name, error = %e, "Partial decode"); serde_json::Value::Null });

            db::insert_instruction(
                &mut **db_tx, tx_sig, ix_idx as i32, &ix_def.name, &pid_str,
                &args, &json!(accounts), Some(&ix.data),
            ).await?;
        } else {
            db::insert_instruction(
                &mut **db_tx, tx_sig, ix_idx as i32, "unknown", &pid_str,
                &serde_json::Value::Null, &json!(accounts), Some(&ix.data),
            ).await?;
        }
    }
    Ok(())
}

/// Retry signatures from the dead-letter queue.
async fn retry_failed_signatures(state: &IndexerState) {
    let retryable = match db::get_retryable_signatures(&state.pool, 20).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "Failed to fetch retryable signatures");
            return;
        }
    };

    if retryable.is_empty() {
        return;
    }

    info!(count = retryable.len(), "Retrying failed signatures");
    for (sig, slot) in &retryable {
        match process_sig(state, sig, *slot).await {
            Ok(()) => {
                info!(%sig, "Retry succeeded");
                let _ = db::remove_failed_signature(&state.pool, sig).await;
            }
            Err(e) => {
                warn!(%sig, error = %e, "Retry still failing");
                let _ = db::record_failed_signature(
                    &state.pool, sig, *slot, &e.to_string(),
                ).await;
            }
        }
    }
}
