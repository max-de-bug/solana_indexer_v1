# Solana Indexer V1

A production-ready Solana program indexer with Anchor IDL-based instruction decoding.

## Features

- **IDL-driven decoding** — Accepts an Anchor IDL as a JSON file or on-chain account address
- **Batch & real-time indexing** — Slot range, signature list, or real-time polling with cold-start backfill
- **Full Borsh decoding** — Instruction arguments decoded into structured JSONB
- **REST API** — Get transaction by signature; list transactions with filters (instruction name, signer)
- **Exponential backoff** — All RPC calls retried with configurable delays
- **Graceful shutdown** — `CancellationToken` ensures clean termination
- **Docker Compose** — Start everything with `docker compose up`

## Quick Start

```bash
# 1. Copy and configure environment
cp .env.example .env
# Edit .env with your RPC URL, PROGRAM_ID, and IDL_PATH

# 2. Start with Docker Compose
docker compose up --build

# 3. Or run locally
cargo run
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Health check |
| `GET /api/v1/tx/{signature}` | Full transaction with decoded instructions |
| `GET /api/v1/transactions` | List transactions with filters |

### Query Parameters for `/api/v1/transactions`

| Param | Type | Description |
|---|---|---|
| `name` | string | Filter by instruction name |
| `signer` | string | Filter by signer public key |
| `limit` | int | Max results (default 50, max 500) |
| `offset` | int | Pagination offset |

### Example

```bash
# Get a specific transaction
curl http://localhost:3000/api/v1/tx/5UfD...signature

# List transactions by instruction name
curl "http://localhost:3000/api/v1/transactions?name=initialize&limit=10"

# List transactions by signer
curl "http://localhost:3000/api/v1/transactions?signer=ABC...pubkey"
```

## Configuration

All via environment variables — see [.env.example](.env.example).

| Variable | Required | Default | Description |
|---|---|---|---|
| `RPC_URL` | No | mainnet | Solana RPC endpoint |
| `DATABASE_URL` | **Yes** | — | PostgreSQL connection string |
| `PROGRAM_ID` | **Yes** | — | Program to index |
| `IDL_PATH` | One of | — | Path to Anchor IDL JSON file |
| `IDL_ACCOUNT` | these | — | On-chain IDL account address |
| `INDEXING_MODE` | No | `realtime` | `realtime`, `batch_slots`, `batch_signatures` |
| `API_PORT` | No | `3000` | REST API port |
| `MAX_RETRIES` | No | `5` | Max RPC retry attempts |
| `RETRY_DELAY_MS` | No | `500` | Initial retry delay |

## Architecture

```
src/
├── main.rs        — Bootstrap, IDL cascade, graceful shutdown
├── config.rs      — Environment-based configuration
├── idl.rs         — Anchor IDL model + on-chain loading
├── db.rs          — PostgreSQL schema, queries, indexing transactions
├── api.rs         — REST API (axum) with rate-limiting
└── indexer/
    ├── mod.rs     — Indexing modes (batch + realtime)
    ├── fetcher.rs — Async RPC with exponential backoff
    └── decoder.rs — Borsh instruction argument decoding
```
