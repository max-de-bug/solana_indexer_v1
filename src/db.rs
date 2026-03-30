use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tracing::info;


// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

pub async fn create_pool(url: &str) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new().max_connections(10).connect(url).await?;
    info!("Connected to PostgreSQL");
    Ok(pool)
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

pub async fn init_schema(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS sync_state (
            id            SERIAL      PRIMARY KEY,
            program_id    TEXT        NOT NULL UNIQUE,
            last_slot     BIGINT      NOT NULL DEFAULT 0,
            last_signature TEXT,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS transactions (
            id          BIGSERIAL   PRIMARY KEY,
            signature   TEXT        NOT NULL UNIQUE,
            slot        BIGINT      NOT NULL,
            block_time  TIMESTAMPTZ,
            success     BOOLEAN     NOT NULL,
            fee         BIGINT,
            err_msg     TEXT,
            signer      TEXT,
            indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS instructions (
            id                     BIGSERIAL   PRIMARY KEY,
            transaction_signature  TEXT        NOT NULL REFERENCES transactions(signature),
            instruction_index      INTEGER     NOT NULL,
            instruction_name       TEXT        NOT NULL,
            program_id             TEXT        NOT NULL,
            args                   JSONB,
            accounts               JSONB,
            raw_data               BYTEA,
            indexed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS failed_signatures (
            id          BIGSERIAL   PRIMARY KEY,
            signature   TEXT        NOT NULL,
            slot        BIGINT      NOT NULL DEFAULT 0,
            error       TEXT        NOT NULL,
            retries     INTEGER     NOT NULL DEFAULT 0,
            next_retry  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (signature)
        )",
    ).execute(pool).await?;

    for idx in &[
        "CREATE INDEX IF NOT EXISTS idx_tx_slot      ON transactions(slot)",
        "CREATE INDEX IF NOT EXISTS idx_tx_signer    ON transactions(signer)",
        "CREATE INDEX IF NOT EXISTS idx_ix_name      ON instructions(instruction_name)",
        "CREATE INDEX IF NOT EXISTS idx_ix_tx_sig    ON instructions(transaction_signature)",
        "CREATE INDEX IF NOT EXISTS idx_fail_retry   ON failed_signatures(next_retry) WHERE retries < 5",
    ] {
        sqlx::query(idx).execute(pool).await?;
    }

    info!("Database schema initialised");
    Ok(())
}

// ---------------------------------------------------------------------------
// Sync state
// ---------------------------------------------------------------------------

pub async fn get_last_processed(
    pool: &PgPool,
    program_id: &str,
) -> anyhow::Result<Option<(u64, Option<String>)>> {
    let row: Option<(i64, Option<String>)> = sqlx::query_as(
        "SELECT last_slot, last_signature FROM sync_state WHERE program_id = $1",
    ).bind(program_id).fetch_optional(pool).await?;
    Ok(row.map(|(s, sig)| (s as u64, sig)))
}

pub async fn update_sync_state(
    pool: &PgPool,
    program_id: &str,
    slot: u64,
    sig: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO sync_state (program_id, last_slot, last_signature, updated_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (program_id) DO UPDATE SET
             last_slot = EXCLUDED.last_slot,
             last_signature = EXCLUDED.last_signature,
             updated_at = NOW()",
    ).bind(program_id).bind(slot as i64).bind(sig).execute(pool).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Dedup
// ---------------------------------------------------------------------------

pub async fn transaction_exists(pool: &PgPool, signature: &str) -> anyhow::Result<bool> {
    let (exists,): (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM transactions WHERE signature = $1)",
    ).bind(signature).fetch_one(pool).await?;
    Ok(exists)
}

// ---------------------------------------------------------------------------
// Inserts
// ---------------------------------------------------------------------------

pub async fn insert_transaction<'e, E>(
    executor: E,
    sig: &str,
    slot: u64,
    block_time: Option<i64>,
    success: bool,
    fee: Option<u64>,
    err_msg: Option<&str>,
    signer: Option<&str>,
) -> anyhow::Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "INSERT INTO transactions (signature, slot, block_time, success, fee, err_msg, signer)
         VALUES ($1, $2, to_timestamp($3), $4, $5, $6, $7)
         ON CONFLICT (signature) DO NOTHING",
    )
    .bind(sig)
    .bind(slot as i64)
    .bind(block_time.map(|t| t as f64))
    .bind(success)
    .bind(fee.map(|f| f as i64))
    .bind(err_msg)
    .bind(signer)
    .execute(executor).await?;
    Ok(())
}

pub async fn insert_instruction<'e, E>(
    executor: E,
    tx_sig: &str,
    ix_index: i32,
    ix_name: &str,
    program_id: &str,
    args: &serde_json::Value,
    accounts: &serde_json::Value,
    raw_data: Option<&[u8]>,
) -> anyhow::Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "INSERT INTO instructions
            (transaction_signature, instruction_index, instruction_name, program_id, args, accounts, raw_data)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(tx_sig)
    .bind(ix_index)
    .bind(ix_name)
    .bind(program_id)
    .bind(args)
    .bind(accounts)
    .bind(raw_data)
    .execute(executor).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Queries (used by API)
// ---------------------------------------------------------------------------

/// Get a transaction and its instructions by signature.
pub async fn get_transaction(
    pool: &PgPool,
    sig: &str,
) -> anyhow::Result<Option<serde_json::Value>> {
    let tx_row = sqlx::query(
        "SELECT signature, slot, block_time, success, fee, err_msg, signer, indexed_at
         FROM transactions WHERE signature = $1",
    ).bind(sig).fetch_optional(pool).await?;

    let tx_row = match tx_row {
        Some(r) => r,
        None => return Ok(None),
    };

    let ix_rows = sqlx::query(
        "SELECT instruction_index, instruction_name, program_id, args, accounts
         FROM instructions WHERE transaction_signature = $1
         ORDER BY instruction_index",
    ).bind(sig).fetch_all(pool).await?;

    let instructions: Vec<serde_json::Value> = ix_rows.iter().map(|r| {
        serde_json::json!({
            "index": r.get::<i32, _>("instruction_index"),
            "name": r.get::<String, _>("instruction_name"),
            "program_id": r.get::<String, _>("program_id"),
            "args": r.get::<Option<serde_json::Value>, _>("args"),
            "accounts": r.get::<Option<serde_json::Value>, _>("accounts"),
        })
    }).collect();

    Ok(Some(serde_json::json!({
        "signature": tx_row.get::<String, _>("signature"),
        "slot": tx_row.get::<i64, _>("slot"),
        "block_time": tx_row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("block_time"),
        "success": tx_row.get::<bool, _>("success"),
        "fee": tx_row.get::<Option<i64>, _>("fee"),
        "err_msg": tx_row.get::<Option<String>, _>("err_msg"),
        "signer": tx_row.get::<Option<String>, _>("signer"),
        "indexed_at": tx_row.get::<chrono::DateTime<chrono::Utc>, _>("indexed_at"),
        "instructions": instructions,
    })))
}

/// List transactions with optional filters.
pub async fn list_transactions(
    pool: &PgPool,
    instruction_name: Option<&str>,
    signer: Option<&str>,
    limit: i64,
    offset: i64,
) -> anyhow::Result<Vec<serde_json::Value>> {
    // Build query dynamically with typed parameters.
    let (sql, needs_name, needs_signer) = build_list_query(instruction_name, signer);

    let mut query = sqlx::query(&sql);
    if needs_name {
        query = query.bind(instruction_name.unwrap());
    }
    if needs_signer {
        query = query.bind(signer.unwrap());
    }
    query = query.bind(limit).bind(offset);

    let rows = query.fetch_all(pool).await?;

    let results: Vec<serde_json::Value> = rows.iter().map(|r| {
        serde_json::json!({
            "signature": r.get::<String, _>("signature"),
            "slot": r.get::<i64, _>("slot"),
            "block_time": r.get::<Option<chrono::DateTime<chrono::Utc>>, _>("block_time"),
            "success": r.get::<bool, _>("success"),
            "signer": r.get::<Option<String>, _>("signer"),
            "indexed_at": r.get::<chrono::DateTime<chrono::Utc>, _>("indexed_at"),
        })
    }).collect();

    Ok(results)
}

fn build_list_query(name: Option<&str>, signer: Option<&str>) -> (String, bool, bool) {
    let mut sql = String::from(
        "SELECT DISTINCT t.signature, t.slot, t.block_time, t.success, t.signer, t.indexed_at
         FROM transactions t"
    );
    let mut idx = 1u32;
    let needs_name = name.is_some();
    let needs_signer = signer.is_some();

    if needs_name {
        sql.push_str(" JOIN instructions i ON i.transaction_signature = t.signature");
    }
    sql.push_str(" WHERE 1=1");

    if needs_name {
        sql.push_str(&format!(" AND i.instruction_name = ${idx}"));
        idx += 1;
    }
    if needs_signer {
        sql.push_str(&format!(" AND t.signer = ${idx}"));
        idx += 1;
    }

    sql.push_str(&format!(" ORDER BY t.slot DESC LIMIT ${idx} OFFSET ${}", idx + 1));

    (sql, needs_name, needs_signer)
}

// ---------------------------------------------------------------------------
// Failed signature tracking (dead-letter queue)
// ---------------------------------------------------------------------------

/// Record a failed signature for later retry.
pub async fn record_failed_signature(
    pool: &PgPool,
    signature: &str,
    slot: u64,
    error: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO failed_signatures (signature, slot, error, retries, next_retry, updated_at)
         VALUES ($1, $2, $3, 1, NOW() + INTERVAL '30 seconds', NOW())
         ON CONFLICT (signature) DO UPDATE SET
             retries = failed_signatures.retries + 1,
             error   = EXCLUDED.error,
             next_retry = NOW() + (INTERVAL '30 seconds' * (failed_signatures.retries + 1)),
             updated_at = NOW()",
    )
    .bind(signature)
    .bind(slot as i64)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch signatures that are due for retry (max 5 retries).
pub async fn get_retryable_signatures(
    pool: &PgPool,
    limit: i64,
) -> anyhow::Result<Vec<(String, u64)>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT signature, slot FROM failed_signatures
         WHERE retries < 5 AND next_retry <= NOW()
         ORDER BY next_retry ASC
         LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(s, slot)| (s, slot as u64)).collect())
}

/// Remove a signature from the dead-letter queue after successful processing.
pub async fn remove_failed_signature(
    pool: &PgPool,
    signature: &str,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM failed_signatures WHERE signature = $1")
        .bind(signature)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_list_query() {
        // No filters
        let (sql, name, signer) = build_list_query(None, None);
        assert!(!name);
        assert!(!signer);
        assert!(!sql.contains("JOIN instructions"));
        assert!(sql.contains("LIMIT $1 OFFSET $2"));

        // With name
        let (sql, name, signer) = build_list_query(Some("initialize"), None);
        assert!(name);
        assert!(!signer);
        assert!(sql.contains("JOIN instructions i ON"));
        assert!(sql.contains("i.instruction_name = $1"));
        assert!(sql.contains("LIMIT $2 OFFSET $3"));

        // With both
        let (sql, name, signer) = build_list_query(Some("swap"), Some("pub_key_1"));
        assert!(name);
        assert!(signer);
        assert!(sql.contains("i.instruction_name = $1"));
        assert!(sql.contains("t.signer = $2"));
        assert!(sql.contains("LIMIT $3 OFFSET $4"));
    }
}
