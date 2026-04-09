const fs = require('fs');
const path = require('path');

module.exports = function createSemanticRoutes(deps = {}) {
  const {
    fetch,
    workerBaseUrl = 'http://127.0.0.1:3334',
    triggerSemanticScan = null,
    ensureSemanticWorker = null,
    localIndexPath = path.join(process.cwd(), 'semantic-index', 'intents.json')
  } = deps;

  const fallbackState = {
    running: false,
    paused: false,
    progress: 0,
    lastScan: null,
    scanned: { groups: 0, docs: 0, tickets: 0 },
    pending: 0,
    confirmed: 0
  };

  function nowIso() {
    return new Date().toISOString();
  }

  function loadLocalIntents() {
    try {
      if (!fs.existsSync(localIndexPath)) {
        fs.mkdirSync(path.dirname(localIndexPath), { recursive: true });
        fs.writeFileSync(localIndexPath, '[]\n', 'utf8');
      }
      const raw = fs.readFileSync(localIndexPath, 'utf8');
      const parsed = JSON.parse(raw || '[]');
      return Array.isArray(parsed) ? parsed : [];
    } catch (_e) {
      return [];
    }
  }

  function saveLocalIntents(items) {
    fs.mkdirSync(path.dirname(localIndexPath), { recursive: true });
    fs.writeFileSync(localIndexPath, JSON.stringify(items || [], null, 2) + '\n', 'utf8');
  }

  function calcStats(items) {
    const bySource = {};
    const byConfidence = { high: 0, medium: 0, low: 0 };
    let confirmed = 0;
    for (const item of items) {
      const source = String(item?.source || 'unknown');
      bySource[source] = (bySource[source] || 0) + 1;
      const conf = Number(item?.confidence || 0);
      if (conf >= 0.8) byConfidence.high += 1;
      else if (conf >= 0.5) byConfidence.medium += 1;
      else byConfidence.low += 1;
      if (item?.confirmed) confirmed += 1;
    }
    return {
      total: items.length,
      bySource,
      byConfidence,
      confirmed,
      unconfirmed: Math.max(0, items.length - confirmed)
    };
  }

  async function readJsonBody(req) {
    return new Promise(resolve => {
      let body = '';
      req.on('data', chunk => { body += String(chunk || ''); });
      req.on('end', () => {
        if (!body.trim()) return resolve({});
        try { resolve(JSON.parse(body)); } catch (_e) { resolve({}); }
      });
      req.on('error', () => resolve({}));
    });
  }

  async function ensureWorkerReady() {
    if (typeof ensureSemanticWorker === 'function') {
      await ensureSemanticWorker();
    }
  }

  async function workerGet(pathname, queryObj = {}) {
    await ensureWorkerReady();
    const target = new URL(workerBaseUrl + pathname);
    Object.entries(queryObj || {}).forEach(([k, v]) => {
      if (v !== undefined && v !== null && String(v) !== '') target.searchParams.set(k, String(v));
    });
    const r = await fetch(target.toString());
    const text = await r.text();
    let json;
    try { json = text ? JSON.parse(text) : {}; } catch (_e) { json = { error: text || 'invalid json' }; }
    return { status: r.status, json };
  }

  async function workerSend(method, pathname, payload = {}) {
    await ensureWorkerReady();
    const r = await fetch(workerBaseUrl + pathname, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: method === 'GET' ? undefined : JSON.stringify(payload || {})
    });
    const text = await r.text();
    let json;
    try { json = text ? JSON.parse(text) : {}; } catch (_e) { json = { error: text || 'invalid json' }; }
    return { status: r.status, json };
  }

  async function workerOrFallback(res, workerOp, fallbackOp) {
    try {
      const out = await workerOp();
      res.writeHead(out.status || 200);
      res.end(JSON.stringify(out.json || {}));
    } catch (_e) {
      const out = await fallbackOp();
      res.writeHead(out.status || 200);
      res.end(JSON.stringify(out.json || {}));
    }
  }

  return async function handleSemanticRoute(req, res, pathname, query) {
    if (!pathname.startsWith('/semantic/')) return false;

    if (pathname === '/semantic/list' && req.method === 'GET') {
      await workerOrFallback(
        res,
        () => workerGet('/list'),
        async () => ({ status: 200, json: { intents: loadLocalIntents() } })
      );
      return true;
    }

    if (pathname === '/semantic/search' && req.method === 'GET') {
      const q = String(query.q || '').trim().toLowerCase();
      await workerOrFallback(
        res,
        () => workerGet('/search', { q }),
        async () => {
          const intents = loadLocalIntents();
          const filtered = !q ? intents : intents.filter(item => {
            const hay = [
              item.intent,
              item.package,
              item.procedure,
              ...(Array.isArray(item.keywords) ? item.keywords : []),
              ...(Array.isArray(item.tables) ? item.tables : [])
            ].join(' ').toLowerCase();
            return hay.includes(q);
          });
          return { status: 200, json: { intents: filtered.slice(0, 25), q } };
        }
      );
      return true;
    }

    if (pathname === '/semantic/stats' && req.method === 'GET') {
      await workerOrFallback(
        res,
        () => workerGet('/stats'),
        async () => ({ status: 200, json: calcStats(loadLocalIntents()) })
      );
      return true;
    }

    if ((pathname === '/semantic/scan-status' || pathname === '/semantic/status') && req.method === 'GET') {
      await workerOrFallback(
        res,
        () => workerGet('/status'),
        async () => ({ status: 200, json: fallbackState })
      );
      return true;
    }

    if ((pathname === '/semantic/auto-probe-status' || pathname === '/semantic/autoprobe-status') && req.method === 'GET') {
      await workerOrFallback(
        res,
        () => workerGet('/auto-probe/status'),
        async () => ({ status: 200, json: { running: false, queueDepth: 0, processedCount: 0, droppedDuplicates: 0 } })
      );
      return true;
    }

    if (pathname === '/semantic/pause' && req.method === 'POST') {
      await workerOrFallback(
        res,
        () => workerSend('POST', '/pause', {}),
        async () => {
          fallbackState.paused = true;
          fallbackState.running = false;
          return { status: 200, json: { ok: true, paused: true } };
        }
      );
      return true;
    }

    if (pathname === '/semantic/resume' && req.method === 'POST') {
      await workerOrFallback(
        res,
        () => workerSend('POST', '/resume', {}),
        async () => {
          fallbackState.paused = false;
          return { status: 200, json: { ok: true, paused: false } };
        }
      );
      return true;
    }

    if (pathname === '/semantic/scan' && req.method === 'POST') {
      const body = await readJsonBody(req);
      await workerOrFallback(
        res,
        async () => {
          const out = await workerSend('POST', '/scan', body || {});
          if (out?.json?.started === true && typeof triggerSemanticScan === 'function') {
            triggerSemanticScan({ manual: true, ...(body || {}) }).catch(() => {});
          }
          return out;
        },
        async () => {
          if (fallbackState.paused) {
            return { status: 200, json: { started: false, reason: 'paused' } };
          }
          if (fallbackState.running) {
            return {
              status: 200,
              json: {
                started: false,
                reason: 'scan-already-running',
                progress: Number(fallbackState.progress || 0)
              }
            };
          }
          fallbackState.running = true;
          fallbackState.paused = false;
          fallbackState.lastScan = nowIso();
          fallbackState.progress = 100;
          fallbackState.running = false;
          return { status: 200, json: { started: true, reason: 'scan-started' } };
        }
      );
      return true;
    }

    if (pathname === '/semantic/discover' && req.method === 'POST') {
      const body = await readJsonBody(req);
      await workerOrFallback(
        res,
        () => workerSend('POST', '/discover', body || {}),
        async () => ({ status: 200, json: { ok: true, discovered: 0 } })
      );
      return true;
    }

    if (pathname === '/semantic/auto-probe' && req.method === 'POST') {
      const body = await readJsonBody(req);
      await workerOrFallback(
        res,
        async () => {
          const out = await workerSend('POST', '/auto-probe', body || {});
          if ((out?.json?.queued === true || out?.json?.ok === true) && typeof triggerSemanticScan === 'function') {
            triggerSemanticScan({
              manual: true,
              reason: 'auto-probe',
              question: body?.question || '',
              groupId: body?.groupId || '',
              schemas: Array.isArray(body?.schemas) ? body.schemas : []
            }).catch(() => {});
          }
          return out;
        },
        async () => {
          if (typeof triggerSemanticScan === 'function') {
            triggerSemanticScan({
              manual: true,
              reason: 'auto-probe-fallback',
              question: body?.question || '',
              groupId: body?.groupId || '',
              schemas: Array.isArray(body?.schemas) ? body.schemas : []
            }).catch(() => {});
          }
          return {
            status: 202,
            json: {
              ok: true,
              queued: true,
              source: 'fallback',
              reason: body?.reason || 'insufficient-context'
            }
          };
        }
      );
      return true;
    }

    if (pathname === '/semantic/entry' && req.method === 'POST') {
      const body = await readJsonBody(req);
      await workerOrFallback(
        res,
        () => workerSend('POST', '/entry', body || {}),
        async () => {
          const item = body?.entry || body || {};
          const intents = loadLocalIntents();
          if (!item.id) {
            item.id = `si-${Math.floor(Date.now() / 1000)}-${Math.random().toString(36).slice(2, 6)}`;
          }
          item.updatedAt = nowIso();
          let replaced = false;
          const next = intents.map(cur => {
            if (cur.id === item.id) {
              replaced = true;
              return { ...cur, ...item };
            }
            return cur;
          });
          if (!replaced) next.push(item);
          saveLocalIntents(next);
          return { status: 200, json: { ok: true, entry: item } };
        }
      );
      return true;
    }

    if (pathname.startsWith('/semantic/entry/') && req.method === 'DELETE') {
      const id = decodeURIComponent(pathname.slice('/semantic/entry/'.length));
      await workerOrFallback(
        res,
        () => workerSend('DELETE', `/entry/${encodeURIComponent(id)}`, {}),
        async () => {
          const intents = loadLocalIntents();
          const kept = intents.filter(x => String(x.id) !== String(id));
          saveLocalIntents(kept);
          return { status: 200, json: { ok: true, deleted: intents.length - kept.length } };
        }
      );
      return true;
    }

    if (pathname.startsWith('/semantic/confirm/') && req.method === 'POST') {
      const id = decodeURIComponent(pathname.slice('/semantic/confirm/'.length));
      const body = await readJsonBody(req);
      await workerOrFallback(
        res,
        () => workerSend('POST', `/confirm/${encodeURIComponent(id)}`, body || {}),
        async () => {
          const intents = loadLocalIntents();
          let found = null;
          const next = intents.map(item => {
            if (String(item.id) !== String(id)) return item;
            found = {
              ...item,
              confirmed: true,
              confirmedBy: body?.confirmedBy || 'technical-user',
              source: 'confirmed',
              updatedAt: nowIso(),
              confidence: Math.max(Number(item.confidence || 0), 0.8)
            };
            return found;
          });
          if (!found) return { status: 404, json: { ok: false, error: 'not found' } };
          saveLocalIntents(next);
          return { status: 200, json: { ok: true, confirmed: true, entry: found } };
        }
      );
      return true;
    }

    if (pathname.startsWith('/semantic/usage/') && req.method === 'POST') {
      const id = decodeURIComponent(pathname.slice('/semantic/usage/'.length));
      const body = await readJsonBody(req);
      await workerOrFallback(
        res,
        () => workerSend('POST', `/usage/${encodeURIComponent(id)}`, body || {}),
        async () => {
          const direction = String(body?.direction || '').toLowerCase();
          const intents = loadLocalIntents();
          let found = null;
          const next = intents.map(item => {
            if (String(item.id) !== String(id)) return item;
            const conf = Number(item.confidence || 0);
            let newConf = conf;
            if (direction === 'up') newConf = Math.min(1, conf + 0.05);
            if (direction === 'down') newConf = Math.max(0, conf - 0.1);
            found = {
              ...item,
              confidence: newConf,
              usageCount: Number(item.usageCount || 0) + 1,
              lastUsed: nowIso(),
              updatedAt: nowIso()
            };
            if (!found.confirmed && direction === 'up' && found.usageCount >= 5) {
              found.confirmed = true;
              found.source = 'confirmed';
              found.confirmedBy = 'auto';
            }
            return found;
          });
          if (!found) return { status: 404, json: { ok: false, error: 'not found' } };
          saveLocalIntents(next);
          return { status: 200, json: { ok: true, entry: found } };
        }
      );
      return true;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'not found' }));
    return true;
  };
};
