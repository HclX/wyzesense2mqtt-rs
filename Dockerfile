# =============================================================================
# wyzesense2mqtt-rs Multi-Stage Docker Build
# =============================================================================
# Build:  docker build -t wyzesense2mqtt-rs .
# Run:    docker run --device /dev/hidraw0 -v ./config:/app/config wyzesense2mqtt-rs
#
# PUID/PGID: Set these environment variables to run as a specific user/group.
#   docker run -e PUID=1000 -e PGID=1000 ...
# =============================================================================

# --- Stage 1: Build ---
FROM rust:alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache pkgconfig musl-dev

# Cache dependency build by copying manifests first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo 'fn main() { println!("placeholder"); }' > src/main.rs && \
    echo 'pub fn lib() {}' > src/lib.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src

# Copy actual source and build
COPY src/ src/
RUN touch src/main.rs src/lib.rs && cargo build --release --bin wyzesense2mqtt-rs

# --- Stage 2: Runtime ---
FROM alpine:3.19

# Install runtime dependencies:
#   curl    - for health check
#   su-exec - for clean privilege drop (PUID/PGID support)
#   shadow  - for user/group modification tools (usermod, etc)
#   bash    - for the entrypoint script
RUN apk add --no-cache ca-certificates curl su-exec shadow bash && \
    # Create default user (will be remapped by entrypoint via PUID/PGID)
    groupadd -g 1000 wyzesense && \
    groupadd plugdev && \
    useradd -u 1000 -g wyzesense -G plugdev -d /app -s /sbin/nologin wyzesense && \
    mkdir -p /app/config /app/logs /app/state && \
    chown -R wyzesense:wyzesense /app

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/target/release/wyzesense2mqtt-rs /app/wyzesense2mqtt-rs

# Copy entrypoint and config template
COPY release/entrypoint.sh /app/entrypoint.sh
COPY release/config.yaml.template /app/config/config.yaml.template
RUN chmod +x /app/entrypoint.sh

# The config file, logs, and state are expected to be mounted as volumes
VOLUME ["/app/config", "/app/logs", "/app/state"]

# Web dashboard port
EXPOSE 8080

# Health check via web API
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8080/api/dongle || exit 1

# Run as root initially — entrypoint remaps to PUID/PGID and drops privileges
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--config", "/app/config/config.yaml"]
