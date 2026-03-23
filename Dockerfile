FROM rust:1.94-slim-trixie AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates gcc libssl-dev pkg-config cmake build-essential curl
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --bin hltv-relay

FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl-dev openssl libc6 curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/hltv-relay /usr/local/bin/hltv-relay
ENTRYPOINT ["/usr/local/bin/hltv-relay"]
