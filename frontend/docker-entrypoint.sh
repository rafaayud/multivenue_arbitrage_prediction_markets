#!/bin/sh
set -eu

wait_for_ipv4() {
  host=$1
  i=0
  while [ "$i" -lt 60 ]; do
    # Prefer DNS (Compose/Desktop); fall back to /etc/hosts for Service Connect.
    ip=$(getent ahostsv4 "$host" 2>/dev/null \
      | awk '{print $1; exit}')
    if [ -z "$ip" ]; then
      ip=$(grep -E "[[:space:]]${host}([[:space:]]|$)" /etc/hosts 2>/dev/null \
        | grep -v '2600:f0f0' \
        | awk '{print $1}' \
        | head -1)
    fi
    if [ -n "$ip" ]; then
      printf '%s' "$ip"
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "Service Connect IPv4 for ${host} not found via DNS or /etc/hosts:" >&2
  getent ahostsv4 "$host" >&2 || true
  cat /etc/hosts >&2 || true
  return 1
}

API_IP=$(wait_for_ipv4 api)
TRADING_IP=$(wait_for_ipv4 trading)

cat > /etc/nginx/conf.d/upstreams.conf <<EOF
upstream api_upstream {
    server ${API_IP}:8000;
}

upstream trading_upstream {
    server ${TRADING_IP}:8001;
}
EOF

nginx -t
exec nginx -g 'daemon off;'
