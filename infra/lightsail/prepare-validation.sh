#!/bin/sh
# Start an isolated stack without venue credentials or a trading journal.
set -eu
cd /home/ubuntu/prediction-bot
umask 077
if [ ! -f .env ]; then
  {
    printf 'POSTGRES_PASSWORD=%s\n' "$(openssl rand -hex 24)"
    printf 'TRADING_API_KEY=%s\n' "$(openssl rand -hex 32)"
    printf '%s\n' 'MARKET_WORKER_ENABLED=0' 'PREDICT_ONCHAIN_CANCEL_ENABLED=0' 'PREDICT_FILL_STUDY=0' 'HYPERLIQUID_ENABLED=false' 'MIGRATIONS_DIR=migrations'
  } > .env
fi
chmod 600 .env
docker load -i images.tar.gz
docker compose config --quiet
docker compose up -d
