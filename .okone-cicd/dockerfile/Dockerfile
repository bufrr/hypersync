# hypersync gateway image
# build:  docker build -t hypersync:latest .
# run  :  see docker-compose snippet in the deployment doc (gateway needs no
#         runtime deps; curl is included so the same image can also run `peerd`
#         if you choose to containerize it with /var/run/docker.sock mounted)

FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release && strip target/release/hypersync

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/hypersync /usr/local/bin/hypersync
ENTRYPOINT ["/usr/local/bin/hypersync"]
# default: full gateway in production mode; peers.json is expected at /pd
# (bind-mount the peerd data dir there). Override CMD for other subcommands.
CMD ["gateway", "/pd/peers.json", "--push", "--cache", "--live", "5"]
