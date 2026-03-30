# --- Build ---
FROM rust:1.77-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true

COPY src/ src/
RUN cargo build --release

# --- Runtime ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libpq5 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/solana-indexer-v1 /usr/local/bin/indexer

ENTRYPOINT ["indexer"]
