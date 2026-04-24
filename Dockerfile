# === Builder ===
FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cargo build --release -p rune-server

# === Runtime ===
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && groupadd -r rune && useradd -r -g rune rune

COPY --from=builder /build/target/release/rune-server /usr/local/bin/rune-server

ENV RUNE_SERVER__HTTP_HOST=0.0.0.0
ENV RUNE_SERVER__HTTP_PORT=50060
ENV RUNE_SERVER__GRPC_HOST=0.0.0.0
ENV RUNE_SERVER__GRPC_PORT=50070
ENV RUNE_LOG__LEVEL=info

EXPOSE 50060 50070

USER rune
ENTRYPOINT ["rune-server"]
