# syntax=docker/dockerfile:1

# ---- build -----------------------------------------------------------------
# questdb-rs 7 needs Rust >= 1.91; edition 2024 needs >= 1.85.
FROM rust:1.98-slim-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency compilation separately from source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src target/release/deps/chud_money-* target/release/chud-money*

COPY src ./src
RUN cargo build --release --locked

# ---- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim

# TLS to Kalshi/Mongo/QuestDB uses bundled webpki roots, but system certs are
# cheap insurance if a native-certs feature is ever enabled.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /app --shell /usr/sbin/nologin app

WORKDIR /app
COPY --from=builder /app/target/release/chud-money /usr/local/bin/chud-money

USER app

# All configuration comes from the environment (see .env.example). Mount the
# Kalshi private key and point KALSHI_PRIVATE_KEY_PATH at it, e.g.
#   docker run --env-file .env.local -e KALSHI_PRIVATE_KEY_PATH=/run/secrets/kalshi_key \
#     -v ~/.ssh/id_kalshi:/run/secrets/kalshi_key:ro -p 3000:3000 chud-money
ENV BIND_ADDR=0.0.0.0:3000 \
    RUST_LOG=info

EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/chud-money"]
