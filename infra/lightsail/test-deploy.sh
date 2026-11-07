#!/usr/bin/env bash
# Exercise deployment guards and credential preservation without Docker or SSH.
set -euo pipefail
source_dir=$(cd "$(dirname "$0")" && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/bin" "$fixture/server/releases/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
export FIXTURE="$fixture"
export PATH="$fixture/bin:$PATH"
if ! command -v python3 > /dev/null; then
  : "${PYTHON:?Set PYTHON to a Python executable when python3 is unavailable}"
  printf '#!/usr/bin/env bash\nexec "$PYTHON" "$@"\n' > "$fixture/bin/python3"
fi
sed "s|/home/ubuntu/prediction-bot|$fixture/server|g" "$source_dir/deploy.sh" > "$fixture/deploy.sh"
cat > "$fixture/bin/curl" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == *runtime/status* ]]; then cat "$FIXTURE/status"; fi
EOF
cat > "$fixture/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FIXTURE/docker-calls"
EOF
# Git Bash lacks flock; this test checks deployment behavior, not OS locking.
printf '#!/usr/bin/env bash\nexit 0\n' > "$fixture/bin/flock"
chmod +x "$fixture/bin/"*
printf 'POSTGRES_PASSWORD=retain-this\n' > "$fixture/server/.env"
cp "$fixture/server/.env" "$fixture/original-env"
cp "$source_dir/compose.yaml" "$fixture/server/compose.yaml"
release="$fixture/server/releases/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
cp "$source_dir/compose.yaml" "$release/compose.yaml"
printf '{"trading_enabled":true,"active_executions":0}' > "$fixture/status"
if bash "$fixture/deploy.sh" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; then exit 1; fi
test ! -e "$fixture/docker-calls"
printf '{"trading_enabled":false,"active_executions":1}' > "$fixture/status"
if bash "$fixture/deploy.sh" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; then exit 1; fi
test ! -e "$fixture/docker-calls"
printf '{}' > "$fixture/status"
if bash "$fixture/deploy.sh" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; then exit 1; fi
test ! -e "$fixture/docker-calls"
printf '{"trading_enabled":false,"active_executions":0}' > "$fixture/status"
bash "$fixture/deploy.sh" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
cmp "$fixture/original-env" "$fixture/server/.env"
grep -q 'up -d --no-deps --force-recreate api trading frontend' "$fixture/docker-calls"
grep -q 'prediction-backend:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' "$fixture/server/compose.override.yaml"
test "$(cat "$fixture/server/deployed-revision")" = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
echo 'Deployment guard and credential preservation checks passed.'
