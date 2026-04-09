#!/usr/bin/env bash
# Run: chmod +x start.sh stop.sh   (first time only)

PORT=$(node - <<'JS'
const fs=require('fs');
const cfg=JSON.parse(fs.readFileSync('config.json'));
console.log(cfg.bridge.port||3333);
JS
)

if pids=$(lsof -ti tcp:$PORT); then
  kill $pids && echo "Bridge stopped." || echo "Failed to stop bridge.";
else
  echo "Bridge was not running.";
fi

echo "Run 'killall ollama' to also stop Ollama."