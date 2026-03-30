use axum::{
    error_handling::HandleErrorLayer,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    BoxError, Json, Router,
};
use serde::Deserialize;
use std::time::Duration;
use sqlx::PgPool;
use std::sync::Arc;
use tower::{limit::RateLimitLayer, ServiceBuilder};
use tracing::error;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

pub struct ApiState {
    pub pool: PgPool,
}

pub fn router(state: Arc<ApiState>) -> Router {
    // 100 requests per second rate limit globally
    let rate_limit = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(|err: BoxError| async move {
            (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({ "error": format!("Rate limit exceeded: {err}") })),
            )
        }))
        .layer(RateLimitLayer::new(100, Duration::from_secs(1)));

    Router::new()
        .route("/health", get(health))
        .route("/api/v1/tx/{signature}", get(get_transaction))
        .route("/api/v1/transactions", get(list_transactions))
        .layer(rate_limit)
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Query params
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Filter by instruction name.
    pub name: Option<String>,
    /// Filter by signer public key.
    pub signer: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn health() -> &'static str {
    "OK"
}

/// GET /api/v1/tx/:signature — full transaction with decoded instructions.
async fn get_transaction(
    State(state): State<Arc<ApiState>>,
    Path(signature): Path<String>,
) -> impl IntoResponse {
    match crate::db::get_transaction(&state.pool, &signature).await {
        Ok(Some(tx)) => Json(serde_json::json!({ "data": tx })).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Transaction not found" })),
        ).into_response(),
        Err(e) => {
            error!(error = %e, "get_transaction failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            ).into_response()
        }
    }
}

/// GET /api/v1/transactions?name=&signer=&limit=&offset=
async fn list_transactions(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<ListQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).min(500);
    let offset = q.offset.unwrap_or(0);

    match crate::db::list_transactions(
        &state.pool,
        q.name.as_deref(),
        q.signer.as_deref(),
        limit,
        offset,
    ).await {
        Ok(results) => {
            let count = results.len();
            Json(serde_json::json!({ "data": results, "count": count })).into_response()
        }
        Err(e) => {
            error!(error = %e, "list_transactions failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "Internal server error" })),
            ).into_response()
        }
    }
}
