#!/usr/bin/env node
// debug-mcp.js - standalone MCP handshake tester

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const cfgPath = path.join(__dirname, 'config.json');
let config;
try {
  config = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
} catch (e) {
  console.error('cannot read config.json:', e.message);
  process.exit(1);
}

const group = config.groups && config.groups[0];
if (!group) {
  console.error('no groups defined in config.json');
  process.exit(1);
}

const SQLCL_CMD = (config.bridge && config.bridge.sqlclCommand) || 'sql';
const SQLCL_ARGS = (config.bridge && config.bridge.sqlclArgs) || ['-R','2','-mcp'];
let responses = [];

const child = spawn(SQLCL_CMD, SQLCL_ARGS, { stdio: ['pipe','pipe','pipe'] });

child.stdout.on('data', d => {
  const lines = d.toString().split(/\r?\n/).filter(Boolean);
  lines.forEach(l => console.log('[STDOUT]', l));
});
child.stderr.on('data', d => {
  d.toString().split(/\r?\n/).filter(Boolean).forEach(l => console.error('[STDERR]', l));
});

let msgId = 1;
function send(obj) {
  console.log('[SEND]', JSON.stringify(obj));
  child.stdin.write(JSON.stringify(obj) + '\n');
}

// collect responses
child.stdout.on('data', d => {
  const lines = d.toString().split(/\r?\n/).filter(l => l.trim().startsWith('{'));
  lines.forEach(l => {
    try { const r = JSON.parse(l); responses.push(r);} catch(e){}
  });
});

function sleep(ms){return new Promise(r=>setTimeout(r,ms));}

(async()=>{
  send({ jsonrpc:'2.0', id: msgId++, method:'initialize',
         params:{ protocolVersion:'2024-11-05', capabilities:{},
                  clientInfo:{ name: config.bridge.mcpClientName||'oracle-kb-bridge', version:'1.0' } } });
  await sleep(500);
  send({ jsonrpc:'2.0', method:'notifications/initialized', params:{} });
  await sleep(500);
  send({ jsonrpc:'2.0', id: msgId++, method:'tools/call',
         params:{ name:'list-connections', arguments:{ mcp_client: config.bridge.mcpClientName, model: config.bridge.mcpModelName } }});
  await sleep(500);
  // issue connect and remember its id so we can wait for its response
  const connectId = msgId;
  send({ jsonrpc:'2.0', id: msgId++, method:'tools/call',
         params:{ name:'connect', arguments:{ connection_name: group.connectionName, mcp_client: config.bridge.mcpClientName, model: config.bridge.mcpModelName } }});

  // wait until we receive a response for the connect call (or timeout)
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) {
    if (responses.find(r => r.id === connectId)) break;
    await sleep(100);
  }

  send({ jsonrpc:'2.0', id: msgId++, method:'tools/call',
         params:{ name:'run-sql', arguments:{ sql: "SELECT 'MCP_OK' AS STATUS FROM DUAL", mcp_client: config.bridge.mcpClientName, model: config.bridge.mcpModelName } }});
  await sleep(500);
  send({ jsonrpc:'2.0', id: msgId++, method:'tools/call',
         params:{ name:'disconnect', arguments:{ mcp_client: config.bridge.mcpClientName, model: config.bridge.mcpModelName } }});

  // wait a few seconds then kill
  setTimeout(()=>{
    child.kill();
    // summarize
    console.log('\n=== SUMMARY ===');
    console.log('total responses:', responses.length);
    const has = (name)=>responses.find(r=>r.result && r.result.content && r.result.content[0].text && r.result.content[0].text.includes(name));
    console.log('list-connections:', has('')?'ok':'unknown');
    // find run-sql result
    const rs = responses.find(r=>r.result && r.result.content && r.result.content[0].text && r.result.content[0].text.includes('MCP_OK'));
    console.log('run-sql MCP_OK:', rs ? 'PASS' : 'FAIL');
    process.exit(rs ? 0 : 1);
  },8000);
})();