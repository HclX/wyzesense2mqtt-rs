#!/bin/bash
set -e

# =============================================================================
# wyzesense2mqtt-rs Docker Entrypoint
# Supports PUID/PGID for runtime UID/GID mapping (like LinuxServer.io images)
# =============================================================================

PUID=${PUID:-1000}
PGID=${PGID:-1000}

# Only remap if running as root (normal Docker case)
if [ "$(id -u)" = "0" ]; then
    # Create/modify group
    if getent group wyzesense > /dev/null 2>&1; then
        groupmod -o -g "$PGID" wyzesense
    else
        groupadd -o -g "$PGID" wyzesense
    fi

    # Create/modify user
    if id wyzesense > /dev/null 2>&1; then
        usermod -o -u "$PUID" -g "$PGID" wyzesense
    else
        useradd -o -u "$PUID" -g "$PGID" -d /app -s /sbin/nologin wyzesense
    fi

    # Ensure app directories are owned by the runtime user
    chown -R wyzesense:wyzesense /app/config /app/logs /app/state

    echo "Starting wyzesense2mqtt-rs as UID=$PUID, GID=$PGID"

    # Drop privileges and exec the binary
    exec gosu wyzesense /app/wyzesense2mqtt-rs "$@"
else
    # Already running as non-root (e.g. Kubernetes with securityContext)
    exec /app/wyzesense2mqtt-rs "$@"
fi
