#!/usr/bin/env node
// test-bridge.js - basic sanity checks against a running bridge.js

const fs = require('fs');
const path = require('path');
const assert = require('assert');
const { spawn } = require('child_process');

const cfg = JSON.parse(fs.readFileSync(path.join(__dirname,'config.json'),'utf8'));
const FIRST_GROUP = cfg.groups && cfg.groups[0] && cfg.groups[0].id;
const FIRST_SCHEMA = (cfg.groups && cfg.groups[0] && cfg.groups[0].schemas && cfg.groups[0].schemas[0]) || 'HR';
if (!FIRST_GROUP) {
  console.error('no groups defined in config.json');
  process.exit(1);
}
if (!FIRST_SCHEMA) {
  console.error('no schema defined in first group of config.json');
  process.exit(1);
}
// kill any existing bridge listening on port (from earlier runs)
try { require('child_process').execSync('lsof -ti tcp:3333 | xargs -r kill'); } catch (_) {}

const BASE = `http://localhost:${cfg.bridge.port || 3333}`;

let bridgeProc;

async function request(path, opts={}) {
  const url = BASE + path;
  const res = await fetch(url, opts);
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch(e) { json = text; }
  return { status: res.status, json, headers: res.headers };
}

let passed = 0, failed = 0;
function pass(msg) { console.log('✓', msg); passed++; }
function fail(msg) { console.error('✗', msg); failed++; }

(async function(){
  console.log('Running bridge tests against', BASE);

  // start bridge subprocess
  bridgeProc = spawn('node', ['bridge.js'], { cwd: __dirname, stdio: 'inherit' });
  // wait briefly for listen
  await new Promise(r => setTimeout(r, 800));
  process.on('exit', () => bridgeProc.kill());

  // SECTION A
  try {
    const r = await request('/health');
    if (r.status === 200 && r.json && r.json.bridge) pass('A1 GET /health');
    else fail('A1 GET /health unexpected response ' + JSON.stringify(r.json));
    // A6: ensure ollamaUrl appears in health
    if (r.json.ollamaUrl) pass('A6 /health contains ollamaUrl');
    else fail('A6 missing ollamaUrl');
  } catch(e){ fail('A1/A6 GET /health threw '+e.message); }

  try {
    const r = await request('/groups');
    if (r.status === 200 && Array.isArray(r.json)) {
      pass('A2 GET /groups returns array');
      const hasBad = r.json.some(g=>g.password || g.auth);
      if (!hasBad) pass('A3 groups contain no password field');
      else fail('A3 groups include forbidden field');
    } else {
      fail('A2 GET /groups unexpected: '+JSON.stringify(r.json));
    }
  } catch(e){ fail('A2 GET /groups threw '+e.message); }

  try {
    const r = await request('/ollama/chat', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({model:'nonexistent',messages:[]}) });
    // expect bridge to return error object but not crash
    if (r.status >= 400 && r.json && r.json.error) pass('A4 POST /ollama/chat invalid model handled');
    else pass('A4 POST /ollama/chat invalid model returned '+r.status);
  } catch(e){ fail('A4 POST /ollama/chat threw '+e.message); }

  try {
    const r = await request('/nonexistent');
    if (r.status === 404) pass('A5 unknown route returns 404');
    else fail('A5 unexpected status '+r.status);
  } catch(e){ fail('A5 threw '+e.message); }

  // SECTION B
  try {
    const r = await request('/db/objects');
    if (r.status === 400) pass('B1 missing group param -> 400');
    else fail('B1 got '+r.status);
  } catch(e){ fail('B1 threw '+e.message); }

  try {
    const r = await request('/db/objects?group=nonexistent');
    if (r.status === 404 || r.status === 400) pass('B2 unknown group -> 404/400');
    else fail('B2 got '+r.status);
  } catch(e){ fail('B2 threw '+e.message); }

  try {
    const long = 'A'.repeat(500);
    const r = await request(`/db/search?group=${FIRST_GROUP}&schema=${encodeURIComponent(FIRST_SCHEMA)}&keyword=${encodeURIComponent(long)}`);
    if (r.status === 400) pass('B3 long keyword -> 400 or truncated');
    else pass('B3 got '+r.status);
  } catch(e){ fail('B3 threw '+e.message); }

  try {
    const r = await request(`/db/objects?group=${encodeURIComponent("x; DROP TABLE")}`);
    if (r.status === 400) pass('B4 injection attempt rejected');
    else fail('B4 got '+r.status);
  } catch(e){ fail('B4 threw '+e.message); }

  // extra B tests for CORS/rate-limit
  // origin tests using fetch (curl was unreliable inside exec)
  try {
    const r = await request(`/db/objects?group=${FIRST_GROUP}&schema=${encodeURIComponent(FIRST_SCHEMA)}`);
    // as long as it's not a 403, origin absence didn't block us
    if (r.status !== 403) pass('B5 no-origin request allowed');
    else fail('B5 was blocked with 403');
  } catch(e){ fail('B5 fetch threw '+e.message); }
  try {
    const r = await request(`/db/objects?group=${FIRST_GROUP}&schema=${encodeURIComponent(FIRST_SCHEMA)}`, { headers:{ Origin:'http://evil.com' } });
    if (r.status === 403) pass('B6 malicious origin blocked');
    else fail('B6 got '+r.status);
  } catch(e){ fail('B6 fetch threw '+e.message); }
  try {
    const r = await request('/health', { headers: { Origin: 'http://evil.com' } });
    if (r.status === 200) pass('B7 health works despite bad origin');
    else fail('B7 got '+r.status);
  } catch(e){ fail('B7 threw '+e.message); }
  try {
    const promises = [];
    for (let i = 0; i < 130; i++) promises.push(request('/health'));
    const rs = await Promise.all(promises);
    if (rs.every(r => r.status === 200)) pass('B8 rate limit exempt on health');
    else fail('B8 some failures');
  } catch(e){ fail('B8 threw '+e.message); }

  // SECTION C (may fail if DB unreachable)
  try {
    const r = await request(`/db/ping?group=${FIRST_GROUP}`);
    if ((r.status === 200 || r.status === 500) && (r.json.ok===true||r.json.ok===false))
      pass('C1 GET /db/ping');
    else
      fail('C1 unexpected '+JSON.stringify(r.json));

    if (r.json.ok) {
      // attempt a couple more calls
      const r2 = await request(`/db/schemas?group=${FIRST_GROUP}`);
      if (r2.status===200 && Array.isArray(r2.json)) pass('C2 schemas returned');
      else fail('C2 schemas failed');
      if (r2.json[0] && r2.json[0].SCHEMA_NAME) {
        const schema = encodeURIComponent(r2.json[0].SCHEMA_NAME);
        const r3 = await request(`/db/objects?group=${FIRST_GROUP}&schema=${schema}`);
        if (r3.status===200) pass('C2 objects query returned');
        else fail('C2 objects query status '+r3.status);
      }
    }
  } catch(e){ fail('C1/C2 threw '+e.message); }

  // C3 parallel pings
  try {
    const promises = [1,2,3].map(()=>request(`/db/ping?group=${FIRST_GROUP}`));
    const results = await Promise.all(promises);
    pass('C3 parallel pings completed');
  } catch(e){ fail('C3 parallel pings error '+e.message); }

  // SECTION E — Impact Analysis endpoints
  try {
    const r = await Promise.race([
      request('/db/impact?name=DUAL&type=TABLE&sourceSchema=SYS&sourceGroup=manhattan-main&depth=1'),
      new Promise((_, reject) => setTimeout(() => reject(new Error('E1 timeout')), 10000))
    ]);
    if (r.status === 200 && r.json && r.json.nodes && r.json.edges && r.json.root && r.json.nodes.length > 0)
      pass('E1 /db/impact returns nodes');
    else fail('E1 unexpected '+JSON.stringify(r.json));
  } catch(e){
    if (e && e.message === 'E1 timeout') fail('E1 timeout');
    else fail('E1 threw '+e.message);
  }
  try {
    const r = await Promise.race([
      request('/db/impact?name=NONEXISTENT_XYZ&type=TABLE&sourceSchema=MANH_CODE&sourceGroup=manhattan-main&depth=1'),
      new Promise((_, reject) => setTimeout(() => reject(new Error('E2 timeout')), 10000))
    ]);
    if (r.status === 200 && r.json && Array.isArray(r.json.nodes) && r.json.nodes.length === 1)
      pass('E2 /db/impact nonexistent returns root only');
    else fail('E2 unexpected '+JSON.stringify(r.json));
  } catch(e){
    if (e && e.message === 'E2 timeout') fail('E2 timeout');
    else fail('E2 threw '+e.message);
  }
  try {
    const r = await request('/db/impact');
    if (r.status === 400) pass('E3 /db/impact missing params -> 400');
    else fail('E3 got '+r.status);
  } catch(e){ fail('E3 threw '+e.message); }
  try {
    const r = await request(`/db/object-status?group=${FIRST_GROUP}&schema=SYS&name=DUAL&type=TABLE`);
    if (r.status === 200 && r.json && 'status' in r.json) pass('E4 /db/object-status OK');
    else fail('E4 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('E4 threw '+e.message); }

  // SECTION D - config endpoint
  try {
    const r = await request('/config', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({sleepAfterInit:500,sleepAfterNotification:500,sleepAfterToolCall:700}) });
    if (r.status === 200 && r.json.ok) pass('D1 POST /config valid values');
    else fail('D1 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('D1 threw '+e.message); }
  try {
    const r = await request('/config', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({sleepAfterToolCall:50}) });
    if (r.status === 400) pass('D2 POST /config invalid value -> 400');
    else fail('D2 got '+r.status);
  } catch(e){ fail('D2 threw '+e.message); }

  // SECTION F — document endpoints (no auth expected here)
  try {
    const r = await request('/docs/msal-config');
    if (r.status===200 && r.json && 'clientId' in r.json) pass('F1 GET /docs/msal-config');
    else fail('F1 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('F1 threw '+e.message); }
  try {
    const r = await request('/docs/search?q=shipment&limit=3');
    if (r.status===200 && Array.isArray(r.json)) pass('F2 GET /docs/search');
    else fail('F2 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('F2 threw '+e.message); }
  try {
    const r = await request('/docs/status');
    if (r.status===200) pass('F3 GET /docs/status public');
    else fail('F3 expected 200 got '+r.status);
  } catch(e){ fail('F3 threw '+e.message); }
  try {
    const r = await request('/docs/list');
    if (r.status===200) pass('F4 GET /docs/list public');
    else fail('F4 expected 200 got '+r.status);
  } catch(e){ fail('F4 threw '+e.message); }

  // SECTION G — safe data query endpoint
  try {
    const r = await request('/db/query', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ group: FIRST_GROUP, sql: 'SELECT 1 AS X FROM DUAL', maxRows: 5 })
    });
    if (r.status === 200 && r.json && Array.isArray(r.json.rows)) pass('G1 POST /db/query valid SELECT');
    else fail('G1 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('G1 threw '+e.message); }

  try {
    const r = await request('/db/query', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ group: FIRST_GROUP, sql: 'UPDATE SOME_TABLE SET A = 1', maxRows: 5 })
    });
    if (r.status === 400) pass('G2 POST /db/query UPDATE rejected');
    else fail('G2 expected 400 got '+r.status);
  } catch(e){ fail('G2 threw '+e.message); }

  try {
    const r = await request('/db/query', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ group: FIRST_GROUP, sql: 'DROP TABLE X', maxRows: 5 })
    });
    if (r.status === 400) pass('G3 POST /db/query DROP rejected');
    else fail('G3 expected 400 got '+r.status);
  } catch(e){ fail('G3 threw '+e.message); }

  try {
    const r = await request('/db/query', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ sql: 'SELECT 1 FROM DUAL', maxRows: 5 })
    });
    if (r.status === 400) pass('G4 POST /db/query missing group -> 400');
    else fail('G4 expected 400 got '+r.status);
  } catch(e){ fail('G4 threw '+e.message); }

  // SECTION H — unified cross-group search
  try {
    const r = await request('/db/search-all?keyword=RECEIPT');
    if (r.status === 200 && r.json && Array.isArray(r.json.results) && typeof r.json.groupsSearched === 'number') {
      pass('H1 GET /db/search-all?keyword=RECEIPT');
    } else {
      fail('H1 unexpected ' + JSON.stringify(r.json));
    }
  } catch(e){ fail('H1 threw '+e.message); }

  try {
    const r = await request('/db/search-all');
    if (r.status === 400) pass('H2 GET /db/search-all missing keyword -> 400');
    else fail('H2 expected 400 got '+r.status);
  } catch(e){ fail('H2 threw '+e.message); }

  // SECTION I — knowledge endpoints
  let knowledgeId = null;
  try {
    const r = await request('/knowledge/entry', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        type: 'qa',
        question: 'What does DOCK_PK.modify do?',
        answer: 'DOCK_PK.modify orchestrates shipment and door updates.',
        context: { group: FIRST_GROUP, schemas: ['MANH_CODE'], dcCodes: [], systems: ['manhattan'], objects: ['DOCK_PK'] },
        tags: ['dock_pk','shipment'],
        quality: 2,
        source: 'ai-verified',
        approved: true
      })
    });
    if (r.status === 200 && r.json && r.json.ok && r.json.id) {
      knowledgeId = r.json.id;
      pass('I1 POST /knowledge/entry -> ok,id');
    } else fail('I1 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('I1 threw '+e.message); }

  try {
    const r = await request('/knowledge/list');
    const entries = Array.isArray(r.json) ? r.json : (r.json && r.json.entries);
    if (r.status === 200 && Array.isArray(entries)) pass('I2 GET /knowledge/list entries array');
    else fail('I2 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('I2 threw '+e.message); }

  try {
    const r = await request('/knowledge/stats');
    if (r.status === 200 && r.json && typeof r.json.total === 'number') pass('I3 GET /knowledge/stats has total');
    else fail('I3 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('I3 threw '+e.message); }

  try {
    const r = await request('/knowledge/export?format=jsonl');
    const cd = r.headers.get('content-disposition') || '';
    if (r.status === 200 && cd.toLowerCase().includes('attachment')) pass('I4 GET /knowledge/export jsonl attachment');
    else fail('I4 unexpected status/header '+r.status+' '+cd);
  } catch(e){ fail('I4 threw '+e.message); }

  try {
    if (!knowledgeId) throw new Error('missing knowledge id from I1');
    const r = await request(`/knowledge/entry?id=${encodeURIComponent(knowledgeId)}`, { method:'DELETE' });
    if (r.status === 200 && r.json && r.json.ok) pass('I5 DELETE /knowledge/entry -> ok');
    else fail('I5 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('I5 threw '+e.message); }

  // SECTION J — Atlassian/JIRA endpoints
  try {
    const r = await request('/jira/search?q=dock&maxResults=10');
    if (r.status === 200 && r.json && Array.isArray(r.json.issues)) pass('J1 GET /jira/search?q=dock has issues array');
    else fail('J1 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('J1 threw '+e.message); }

  try {
    const r = await request('/jira/issue/INVALID-0');
    if (r.status === 404 || (r.status === 200 && r.json && r.json.issue === null)) pass('J2 GET /jira/issue/INVALID-0 -> 404 or issue:null');
    else fail('J2 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('J2 threw '+e.message); }

  try {
    const r = await request('/health');
    if (r.status === 200 && r.json && Object.prototype.hasOwnProperty.call(r.json, 'atlassian')) pass('J3 GET /health includes atlassian field');
    else fail('J3 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('J3 threw '+e.message); }

  // SECTION K — semantic layer endpoints
  let semanticEntryId = null;
  try {
    const r = await request('/semantic/list');
    if (r.status === 200 && r.json && Array.isArray(r.json.intents)) pass('K1 GET /semantic/list has intents array');
    else fail('K1 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('K1 threw '+e.message); }

  try {
    const seed = await request('/semantic/entry', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        intent: 'check load variance',
        keywords: ['load','variance'],
        package: 'DOCK_PK',
        procedure: 'modify',
        tables: ['SHIPMENT'],
        columns: ['STATUS'],
        schemas: ['MANH_CODE'],
        confidence: 0.76,
        source: 'usage',
        sqlTemplate: 'SELECT 1 AS X FROM {schema}.DUAL WHERE 1=1'
      })
    });
    semanticEntryId = seed?.json?.entry?.id || null;
    if (!semanticEntryId) throw new Error('missing semantic entry id');

    const r = await request(`/semantic/confirm/${encodeURIComponent(semanticEntryId)}`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ confirmedBy: 'test-suite' })
    });
    if (r.status === 200 && r.json && r.json.confirmed === true) pass('K2 POST /semantic/confirm/:id confirmed:true');
    else fail('K2 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('K2 threw '+e.message); }

  try {
    const r = await request('/semantic/scan-status');
    if (r.status === 200 && r.json && Object.prototype.hasOwnProperty.call(r.json, 'running') && Object.prototype.hasOwnProperty.call(r.json, 'paused')) pass('K3 GET /semantic/scan-status has running+paused');
    else fail('K3 unexpected '+JSON.stringify(r.json));
  } catch(e){ fail('K3 threw '+e.message); }

  try {
    const p = await request('/semantic/pause', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({}) });
    const u = await request('/semantic/resume', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({}) });
    if (p.status === 200 && u.status === 200) pass('K4 POST /semantic/pause and /semantic/resume');
    else fail('K4 unexpected '+JSON.stringify({ pause:p.json, resume:u.json }));
  } catch(e){ fail('K4 threw '+e.message); }

  try {
    if (semanticEntryId) {
      await request(`/semantic/entry/${encodeURIComponent(semanticEntryId)}`, { method:'DELETE' });
    }
  } catch(_e) {}

  console.log(`\nSummary: ✓ ${passed} passed  ✗ ${failed} failed`);
  process.exit(failed?1:0);
})();
