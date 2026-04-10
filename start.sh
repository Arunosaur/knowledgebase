#!/usr/bin/env bash
# Run: chmod +x start.sh stop.sh   (first time only)

set -e

echo "╔═══════════════════════════════════════════════╗"
echo "║         McLane WMS·IQ — One-Command Start    ║"
echo "╚═══════════════════════════════════════════════╝"

ROOT=$(cd "$(dirname "$0")" && pwd)

# check prerequisites
if command -v node >/dev/null 2>&1; then
  echo "✓ node $(node --version)"
else
  echo "✗ node not found"
  exit 1
fi

if command -v python3 >/dev/null 2>&1; then
  echo "✓ python3 $(python3 --version 2>/dev/null | awk '{print $2}')"
else
  echo "✗ python3 not found"
  exit 1
fi

if command -v sql >/dev/null 2>&1; then
  echo "✓ sql found"
else
  echo "✗ sql not found — ensure SQLcl is on PATH"
  exit 1
fi

if command -v ollama >/dev/null 2>&1; then
  echo "✓ ollama found"
else
  echo "✗ ollama not found — install from https://ollama.ai"
fi

if [ ! -f "$ROOT/config.json" ]; then
  echo "✗ config.json missing in $ROOT"; exit 1
fi

echo "ℹ Ensure VPN is connected before running live Oracle queries."

# start ollama if needed
if ! curl -s http://localhost:11434/api/tags >/dev/null 2>&1; then
  echo "Starting Ollama..."
  ollama serve &
  # wait for it
  for _ in {1..20}; do
    if curl -s http://localhost:11434/api/tags >/dev/null 2>&1; then
      echo "✓ Ollama running"
      break
    fi
    sleep 0.5
  done
else
  echo "✓ Ollama already running"
fi

# ensure model pulled
MODEL=$(node - <<'JS'
const fs=require('fs');
const cfg=JSON.parse(fs.readFileSync('config.json'));
console.log(cfg.bridge.defaultModel);
JS
)
if ! ollama list | grep -q "$MODEL"; then
  echo "Pulling model $MODEL..."
  ollama pull "$MODEL"
fi

# start semantic worker if needed
SEM_WORKER_PID=""
if curl -s http://127.0.0.1:3334/status >/dev/null 2>&1; then
  echo "✓ Semantic worker already running"
else
  if [ -d "$ROOT/semantic-worker" ]; then
    echo "Starting semantic worker..."
    PYTHON_BIN="python3"
    if [ ! -x "$ROOT/.venv/bin/python3" ]; then
      (cd "$ROOT" && python3 -m venv .venv >/dev/null 2>&1 || true)
    fi
    if [ -x "$ROOT/.venv/bin/python3" ]; then
      PYTHON_BIN="$ROOT/.venv/bin/python3"
      (cd "$ROOT/semantic-worker" && "$PYTHON_BIN" -m pip install flask requests -q >/dev/null 2>&1 || true)
    fi
    (cd "$ROOT/semantic-worker" && "$PYTHON_BIN" app.py >/tmp/semantic-worker.log 2>&1) &
    SEM_WORKER_PID=$!
    for _ in {1..20}; do
      if curl -s http://127.0.0.1:3334/status >/dev/null 2>&1; then
        echo "✓ Semantic worker running on 3334"
        break
      fi
      sleep 0.5
    done
  else
    echo "⚠ semantic-worker/ not found — semantic layer disabled"
  fi
fi

# kill existing bridge on port
PORT=$(node - <<'JS'
const fs=require('fs');
const cfg=JSON.parse(fs.readFileSync('config.json'));
console.log(cfg.bridge.port||3333);
JS
)

IP_INFO_JSON=$(node - <<'JS'
const os = require('os');

function isVmRangeIp(ip){
  return ip.startsWith('10.211.55.') || ip.startsWith('10.37.129.');
}

const interfaces = os.networkInterfaces();
const ips = [];
for (const name of Object.keys(interfaces || {})) {
  for (const entry of interfaces[name] || []) {
    const isIPv4 = typeof entry.family === 'string' ? entry.family === 'IPv4' : entry.family === 4;
    if (isIPv4 && !entry.internal) {
      ips.push(entry.address);
    }
  }
}

const preferred10 = ips.find(ip => ip.startsWith('10.') && !isVmRangeIp(ip));
const preferred172 = ips.find(ip => ip.startsWith('172.'));
const preferred192 = ips.find(ip => ip.startsWith('192.168.'));
const selectedIp = preferred10 || preferred172 || preferred192 || '127.0.0.1';

const entries = ips.map(ip => {
  let label = '(other)';
  if (ip.startsWith('192.168.')) label = '(home/wifi)';
  else if (ip.startsWith('10.') && isVmRangeIp(ip)) label = '(vm/interface)';
  else if (ip.startsWith('10.')) label = '(vpn)';
  else if (ip.startsWith('172.')) label = '(corp/alt)';
  return { ip, label, selected: ip === selectedIp };
});

console.log(JSON.stringify({ selectedIp, entries }));
JS
)

NETWORK_IP=$(printf '%s' "$IP_INFO_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin).get('selectedIp','127.0.0.1'))")
NETWORK_IP_LINES=$(IP_INFO_JSON="$IP_INFO_JSON" python3 - <<'PY'
import os, json

d = json.loads(os.environ.get('IP_INFO_JSON', '{}') or '{}')
s = d.get('selectedIp', '127.0.0.1')
entries = d.get('entries', [])
for e in entries:
  marker = ' ← use this one' if e.get('ip') == s and s != '127.0.0.1' else ''
  print(f"{e.get('ip', '')}  {e.get('label', '(other)')}{marker}")
PY
)

LOCAL_URL="http://localhost:$PORT"
NETWORK_URL="http://$NETWORK_IP:$PORT"
POWER_AUTOMATE_URL="http://$NETWORK_IP:$PORT/docs/upload"
BRIDGE_PID=""
BRIDGE_STARTED_BY_SCRIPT="false"
if pids=$(lsof -ti tcp:$PORT); then
  echo "Bridge already running on port $PORT (PID $pids). Kill and restart? [y/N]"
  read -r ans
  if [[ $ans =~ ^[Yy] ]]; then
    kill $pids || true
    BRIDGE_STARTED_BY_SCRIPT="true"
  else
    BRIDGE_PID=$(echo "$pids" | head -n1)
    echo "Using existing bridge (PID: $BRIDGE_PID)"
  fi
else
  BRIDGE_STARTED_BY_SCRIPT="true"
fi

# start bridge when needed
if [ "$BRIDGE_STARTED_BY_SCRIPT" = "true" ]; then
  node bridge.js --debug &
  BRIDGE_PID=$!
fi

# optional PostgreSQL + pgvector startup
POSTGRES_ENABLED=$(node - <<'JS'
const fs=require('fs');
const cfg=JSON.parse(fs.readFileSync('config.json'));
console.log(Boolean(cfg.bridge && cfg.bridge.postgresEnabled) ? 'true' : 'false');
JS
)

if [ "$POSTGRES_ENABLED" = "true" ] && command -v docker >/dev/null 2>&1; then
  echo "🐘 Starting PostgreSQL + pgvector..."
  docker compose up -d postgres >/dev/null 2>&1 || true
  for i in {1..6}; do
    if docker compose exec -T postgres pg_isready -U wmsiq >/dev/null 2>&1; then
      echo "✓ PostgreSQL + pgvector running (port 5432)"
      break
    fi
    echo "  Waiting for PostgreSQL... ($((i*5))s)"
    sleep 5
  done

  if [ "$GRAPH_SCAN_ON_START" = "true" ]; then
    echo "⏳ Schema graph building in background..."
    curl -s -X POST "http://localhost:$PORT/db/scan-schema" \
      -H "Content-Type: application/json" \
      -d '{"group":"manhattan-main","schemas":["MANH_CODE","SE_DM"]}' >/dev/null 2>&1 &
  fi
fi

# wait for health
for _ in {1..10}; do
  if curl -s http://localhost:$PORT/health >/dev/null 2>&1; then
    if [ "$BRIDGE_STARTED_BY_SCRIPT" = "true" ]; then
      echo "✓ Bridge running   http://localhost:$PORT  (PID: $BRIDGE_PID)"
    else
      echo "✓ Using existing bridge   http://localhost:$PORT  (PID: $BRIDGE_PID)"
    fi
    break
  fi
  sleep 0.5
done

if ! curl -s http://localhost:$PORT/health >/dev/null 2>&1; then
  echo "✗ Bridge failed to start. Check bridge logs in this terminal."
  exit 1
fi

open "http://localhost:$PORT" || xdg-open "http://localhost:$PORT" || true

echo
echo "╔══════════════════════════════════════════════════════════════════════╗"
printf "║  Local URL:  %-54s║\n" "$LOCAL_URL"
printf "║  Network URL: %-53s║\n" "$NETWORK_URL"
printf "║  Network IPs detected: %-44s║\n" ""
if [ -n "$NETWORK_IP_LINES" ]; then
  while IFS= read -r ip_line; do
    printf "║    %-62s║\n" "$ip_line"
  done <<EOF
$NETWORK_IP_LINES
EOF
else
  printf "║    %-62s║\n" "127.0.0.1  (loopback only)"
fi
printf "║  Power Automate URL: %-43s║\n" ""
printf "║    %-62s║\n" "$POWER_AUTOMATE_URL"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "WMS·IQ is ready"
echo "- App:            $LOCAL_URL"
if [ -f "$ROOT/public/dist/index.html" ]; then
  echo "⚛️  React UI available at $LOCAL_URL"
else
  echo "📄 Legacy UI at $LOCAL_URL (React build not found)"
fi
echo "- Bridge health:  $LOCAL_URL/health"
echo "- Semantic health: http://127.0.0.1:3334/status"
echo "- Bridge PID:     $BRIDGE_PID"
if [ -n "$SEM_WORKER_PID" ]; then
  echo "- Worker PID:     $SEM_WORKER_PID"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Press Ctrl+C to stop this session."

trap 'echo; echo "Bridge stopped. Ollama left running (stop with: killall ollama)"; if [ "$BRIDGE_STARTED_BY_SCRIPT" = "true" ] && [ -n "$BRIDGE_PID" ]; then kill $BRIDGE_PID 2>/dev/null; fi; if [ -n "$SEM_WORKER_PID" ]; then kill $SEM_WORKER_PID 2>/dev/null; fi; exit 0' SIGINT SIGTERM

# wait indefinitely
while true; do sleep 1; done
