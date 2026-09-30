#!/bin/sh
set -eu

mkdir -p /app/data/secrets

if [ -n "${KALSHI_PRIVATE_KEY_PEM:-}" ]; then
  printf '%s\n' "$KALSHI_PRIVATE_KEY_PEM" > /app/data/secrets/kalshi_private.pem
  chmod 600 /app/data/secrets/kalshi_private.pem
  export KALSHI_PRIVATE_KEY_PATH=/app/data/secrets/kalshi_private.pem
fi

exec "$@"
