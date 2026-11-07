#!/usr/bin/env bash
# Deploy a tested commit while retaining server credentials and data volumes.
set -euo pipefail
revision=${1:?Provide the full commit SHA}
[[ "$revision" =~ ^[0-9a-f]{40}$ ]] || exit 2
cd /home/ubuntu/prediction-bot
exec 9>.deploy.lock
flock -n 9
test -s .env
release="$PWD/releases/$revision"
trap 'rm -f "$release/images.tar.gz"' EXIT

# Refuse to restart a live submitter; the operator must pause and reconcile first.
curl --fail --silent --show-error http://127.0.0.1:8001/runtime/status | python3 -c '
import json, sys
state = json.load(sys.stdin)
if state.get("trading_enabled") is not False or state.get("active_executions") != 0:
    sys.exit("Pause trading and finish active executions before deployment.")
'
docker load -i "$release/images.tar.gz"
export API_IMAGE="prediction-backend:$revision"
export TRADING_IMAGE="$API_IMAGE"
export FRONTEND_IMAGE="prediction-frontend:$revision"
docker compose -f "$release/compose.yaml" --project-directory "$PWD" config --quiet
cp compose.yaml "$release/previous-compose.yaml"
if [ -f compose.override.yaml ]; then
  cp compose.override.yaml "$release/previous-compose.override.yaml"
fi
cp "$release/compose.yaml" compose.yaml
# Record only image references; .env (including its database password) is untouched.
printf 'services:\n  api:\n    image: %s\n  trading:\n    image: %s\n  frontend:\n    image: %s\n' "$API_IMAGE" "$TRADING_IMAGE" "$FRONTEND_IMAGE" > compose.override.yaml
docker compose up -d --no-deps --force-recreate api trading frontend
for attempt in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8000/health > /dev/null &&
     curl -fsS http://127.0.0.1:8001/health > /dev/null &&
     curl -fsS http://127.0.0.1:8080/ > /dev/null; then
    printf '%s\n' "$revision" > deployed-revision
    exit 0
  fi
  sleep 2
done
echo 'Health check failed. Inspect the deployment; no automatic database rollback is attempted.' >&2
exit 1
