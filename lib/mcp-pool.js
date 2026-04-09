const { setInterval } = require('timers');

module.exports = function createMcpPool(deps) {
  const {
    spawn,
    readline,
    config,
    SQLCL_CMD,
    SQLCL_ARGS,
    MCP_CLIENT,
    MCP_MODEL,
    debug
  } = deps;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const POOL_ENABLED = config.bridge && config.bridge.poolEnabled;
  const POOL_IDLE_TIMEOUT = (config.bridge && config.bridge.poolIdleTimeoutMs) || 300000;
  const POOL_MAX_QUEUE = (config.bridge && config.bridge.poolMaxQueueDepth) || 20;
  const TOOL_CALL_TIMEOUT = (config.bridge && config.bridge.toolCallTimeoutMs) || 15000;
  
  // Connection limits per DC/group
  const MAX_CONNECTIONS_PER_DC = (config.bridge && config.bridge.maxConcurrentConnectionsPerDC) || 5;
  const MAX_CONNECTIONS_CIGWMS = (config.bridge && config.bridge.maxConcurrentConnectionsCigWMS) || 5;
  const MAX_CONNECTIONS_WMSHUB = (config.bridge && config.bridge.maxConcurrentConnectionsWMSHub) || 5;

  const pool = new Map();
  const activeConnectionsPerGroup = new Map();
  const activeConnectionsPerDC = new Map();
  const parallelSemaphores = new Map(); // { groupId: { acquiring: 0, waiting: [] } }
  let lastRequest = null;

  // Semaphore for parallel mode: allows up to N concurrent operations
  function createSemaphore(maxConcurrent) {
    return {
      maxConcurrent,
      current: 0,
      waiting: []
    };
  }

  async function acquireSemaphore(semaphore) {
    if (semaphore.current < semaphore.maxConcurrent) {
      semaphore.current++;
      return;
    }
    // Wait in queue
    return new Promise(resolve => {
      semaphore.waiting.push(resolve);
    });
  }

  function releaseSemaphore(semaphore) {
    semaphore.current--;
    const nextWaiter = semaphore.waiting.shift();
    if (nextWaiter) {
      semaphore.current++;
      nextWaiter();
    }
  }

  function sendToEntry(entry, obj) {
    const text = JSON.stringify(obj);
    entry.child.stdin.write(text + '\n');
    if (debug) console.error('[MCP →]', text);
  }

  // Connection limit tracking functions
  function incrementActiveConnections(group, dcCode) {
    const groupKey = group.id;
    activeConnectionsPerGroup.set(groupKey, (activeConnectionsPerGroup.get(groupKey) || 0) + 1);
    
    if (dcCode) {
      activeConnectionsPerDC.set(dcCode, (activeConnectionsPerDC.get(dcCode) || 0) + 1);
    }
    
    return {
      groupActive: activeConnectionsPerGroup.get(groupKey),
      dcActive: dcCode ? activeConnectionsPerDC.get(dcCode) : 0
    };
  }

  function decrementActiveConnections(group, dcCode) {
    const groupKey = group.id;
    const current = (activeConnectionsPerGroup.get(groupKey) || 1) - 1;
    activeConnectionsPerGroup.set(groupKey, Math.max(0, current));
    
    if (dcCode) {
      const dcCurrent = (activeConnectionsPerDC.get(dcCode) || 1) - 1;
      activeConnectionsPerDC.set(dcCode, Math.max(0, dcCurrent));
    }
  }

  function getConnectionLimit(group) {
    // Determine which limit applies to this group
    // Manhattan groups use per-DC limits, others use group-wide limits
    if (group.id.includes('cigwms') || group.id === 'cigwms-prod') {
      return { type: 'group', limit: MAX_CONNECTIONS_CIGWMS };
    }
    if (group.id.includes('wmshub') || group.id === 'wmshub-prod') {
      return { type: 'group', limit: MAX_CONNECTIONS_WMSHUB };
    }
    // Manhattan groups (manhattan-main, manhattan-ck, manhattan-wk)
    return { type: 'per-dc', limit: MAX_CONNECTIONS_PER_DC };
  }

  async function checkConnectionLimit(group, dcCode) {
    const limitInfo = getConnectionLimit(group);
    
    if (limitInfo.type === 'group') {
      // CigWMS/WMSHUB: Check group-wide limit
      const groupActive = activeConnectionsPerGroup.get(group.id) || 0;
      if (groupActive >= limitInfo.limit) {
        const err = new Error(`Connection limit reached for group ${group.id} (${groupActive}/${limitInfo.limit})`);
        err.code = 'CONNECTION_LIMIT_EXCEEDED';
        err.retryAfter = 30;
        throw err;
      }
    } else if (limitInfo.type === 'per-dc') {
      // Manhattan groups: Check per-DC limit
      if (dcCode) {
        const dcActive = activeConnectionsPerDC.get(dcCode) || 0;
        if (dcActive >= limitInfo.limit) {
          const err = new Error(`Connection limit reached for DC ${dcCode} (${dcActive}/${limitInfo.limit})`);
          err.code = 'DC_CONNECTION_LIMIT_EXCEEDED';
          err.retryAfter = 30;
          throw err;
        }
      }
    }
  }

  function sendToEntry(entry, obj) {
    const text = JSON.stringify(obj);
    entry.child.stdin.write(text + '\n');
    if (debug) console.error('[MCP →]', text);
  }

  function parseCSVLine(line) {
    const result = [];
    let cur = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQuotes) {
        if (ch === '"') {
          if (i + 1 < line.length && line[i + 1] === '"') {
            cur += '"';
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          cur += ch;
        }
      } else {
        if (ch === '"') {
          inQuotes = true;
        } else if (ch === ',') {
          result.push(cur);
          cur = '';
        } else {
          cur += ch;
        }
      }
    }
    result.push(cur);
    return result;
  }

  function parseMCPResult(response) {
    const text = (response?.result?.content || [])
      .filter(c => c && c.type === 'text')
      .map(c => c.text || '')
      .join('\n');

    const statusLineRx = /^\d*\s*\(?\d+\s+rows?\s+selected\.?\)?\.?$/i;
    let lines = text.split('\n').map(l => l.trim()).filter(Boolean);
    lines = lines.filter(l => !statusLineRx.test(l));
    lines = lines.filter(l => !/^no rows selected\.?$/i.test(l));
    lines = lines.filter(l => !/^#+/.test(l));

    if (lines.length < 2) {
      console.error('[PARSE EMPTY] raw text was:', text.substring(0, 1000));
      return [];
    }

    const headers = parseCSVLine(lines[0]).map(h => h.trim().replace(/^"|"$/g, ''));
    if (!headers.length || headers.every(h => !h)) {
      console.error('[PARSE EMPTY] could not parse header from:', lines[0]);
      return [];
    }
    const dataLines = lines.slice(1).filter(line => !statusLineRx.test(line) && !/^no rows selected\.?$/i.test(line));

    return dataLines.map(line => {
      const cols = parseCSVLine(line);
      return Object.fromEntries(headers.map((h, i) => [h, cols[i] ?? '']));
    });
  }

  async function performCalls(entry, toolCalls) {
    entry.lastUsed = Date.now();
    const results = [];
    for (const tool of toolCalls) {
      const id = entry.nextMsgId++;
      const prom = new Promise((resolve, reject) => {
        entry.pending.set(id, resolve);
        setTimeout(() => {
          if (entry.pending.has(id)) {
            entry.pending.delete(id);
            reject(new Error('MCP tool timeout'));
          }
        }, TOOL_CALL_TIMEOUT);
      });
      sendToEntry(entry, {
        jsonrpc: '2.0',
        id,
        method: 'tools/call',
        params: {
          name: tool.name,
          arguments: { ...tool.arguments, mcp_client: MCP_CLIENT, model: MCP_MODEL }
        }
      });
      await sleep(config.bridge.sleepAfterToolCall || 600);
      const response = await prom;
      results.push(response);
    }
    return results;
  }

  async function createPoolEntry(group) {
    const child = spawn(SQLCL_CMD, SQLCL_ARGS, { stdio: ['pipe', 'pipe', 'pipe'] });
    const entry = {
      groupId: group.id,
      child,
      queue: [],
      processing: false,
      lastUsed: Date.now(),
      alive: true,
      nextMsgId: 1,
      pending: new Map(),
      dbUser: null
    };

    const rl = readline.createInterface({ input: child.stdout });
    rl.on('line', line => {
      if (line.trim()) {
        if (debug) console.error('[MCP ←]', line);
        try {
          const r = JSON.parse(line);
          if (r.id && entry.pending.has(r.id)) {
            entry.pending.get(r.id)(r);
            entry.pending.delete(r.id);
          }
        } catch (_e) {}
      }
    });
    child.on('close', () => {
      entry.alive = false;
    });

    sendToEntry(entry, {
      jsonrpc: '2.0',
      id: entry.nextMsgId++,
      method: 'initialize',
      params: {
        protocolVersion: '2024-11-05',
        capabilities: {},
        clientInfo: { name: MCP_CLIENT, version: '1.0' }
      }
    });
    await sleep(config.bridge.sleepAfterInit || 400);
    sendToEntry(entry, { jsonrpc: '2.0', method: 'notifications/initialized', params: {} });
    await sleep(config.bridge.sleepAfterNotification || 400);

    try {
      const bootstrapResults = await performCalls(entry, [
        { name: 'connect', arguments: { connection_name: group.connectionName } },
        { name: 'run-sql', arguments: { sql: `SELECT SYS_CONTEXT('USERENV','SESSION_USER') AS DB_USER FROM DUAL` } }
      ]);
      const rs = bootstrapResults && bootstrapResults.length >= 2 ? bootstrapResults[1] : null;
      if (rs && rs.result && !rs.result.isError) {
        const rows = parseMCPResult(rs);
        const user = rows && rows[0] ? String(rows[0].DB_USER || rows[0].db_user || '').trim() : '';
        entry.dbUser = user || null;
      }
    } catch (_e) {
      entry.dbUser = null;
    }

    return entry;
  }

  async function getPoolEntry(group) {
    let entry = pool.get(group.id);
    if (entry && entry.alive) {
      entry.lastUsed = Date.now();
      return entry;
    }
    entry = await createPoolEntry(group);
    pool.set(group.id, entry);
    return entry;
  }

  async function processQueue(entry, group) {
    if (entry.processing) return;
    entry.processing = true;
    while (entry.queue.length) {
      const job = entry.queue.shift();
      const { toolCalls, resolve, reject } = job;
      
      // Increment active connections
      const { groupActive, dcActive } = incrementActiveConnections(group);
      const limitInfo = getConnectionLimit(group);
      if (limitInfo.type === 'group') {
        console.error(`[POOL] Active connections for group ${group.id}: ${groupActive}/${limitInfo.limit}`);
      } else {
        console.error(`[POOL] Connection in use for group ${group.id} (per-DC limit: 5)`);
      }
      
      try {
        let attempt = 0;
        while (true) {
          try {
            const res = await performCalls(entry, toolCalls);
            resolve(res);
            break;
          } catch (err) {
            if (attempt === 0 && (!entry.alive || /not connected/i.test(err.message))) {
              pool.delete(group.id);
              entry = await getPoolEntry(group);
              attempt++;
              continue;
            }
            throw err;
          }
        }
      } catch (err) {
        reject(err);
      } finally {
        // Decrement active connections
        decrementActiveConnections(group);
        const remaining = activeConnectionsPerGroup.get(group.id) || 0;
        if (limitInfo.type === 'group') {
          console.error(`[POOL] Connection released for group ${group.id}, remaining: ${remaining}/${limitInfo.limit}`);
        } else {
          console.error(`[POOL] Connection released for group ${group.id} (per-DC limit: 5)`);
        }
      }
    }
    entry.processing = false;
  }

  async function enqueueToolCalls(group, toolCalls) {
    // Check connection limits before enqueueing
    // Only enforce group-level limits for CigWMS/WMSHUB
    // Manhattan groups rely on DC-level limits (checked at request handler level)
    const limitInfo = getConnectionLimit(group);
    
    if (limitInfo.type === 'group') {
      const groupActive = activeConnectionsPerGroup.get(group.id) || 0;
      if (groupActive >= limitInfo.limit) {
        const err = new Error(`Connection limit reached for group ${group.id} (${groupActive}/${limitInfo.limit})`);
        err.code = 'CONNECTION_LIMIT_EXCEEDED';
        err.httpStatus = 429;
        throw err;
      }
    }
    
    const entry = await getPoolEntry(group);
    if (entry.queue.length >= POOL_MAX_QUEUE) {
      const err = new Error('Queue full');
      err.code = 'QUEUE_FULL';
      throw err;
    }
    return new Promise((resolve, reject) => {
      entry.queue.push({ toolCalls, resolve, reject });
      processQueue(entry, group);
    });
  }

  async function enqueueToolCallsParallel(group, toolCalls, options = {}) {
    // Parallel mode: multiple concurrent operations on same group
    // Still respects connection limits
    const limitInfo = getConnectionLimit(group);
    const maxConcurrent = limitInfo.limit;
    
    if (limitInfo.type === 'group') {
      const groupActive = activeConnectionsPerGroup.get(group.id) || 0;
      if (groupActive >= limitInfo.limit) {
        const err = new Error(`Connection limit reached for group ${group.id} (${groupActive}/${limitInfo.limit})`);
        err.code = 'CONNECTION_LIMIT_EXCEEDED';
        err.httpStatus = 429;
        throw err;
      }
    }
    
    // Get or create semaphore for this group
    if (!parallelSemaphores.has(group.id)) {
      parallelSemaphores.set(group.id, createSemaphore(maxConcurrent));
    }
    const sem = parallelSemaphores.get(group.id);
    
    // Acquire semaphore slot
    await acquireSemaphore(sem);
    
    try {
      let entry = await getPoolEntry(group);
      const { groupActive, dcActive } = incrementActiveConnections(group);
      
      if (debug) {
        console.error(`[POOL-PARALLEL] Active connections for ${group.id}: ${groupActive}/${maxConcurrent}`);
      }
      
      try {
        let attempt = 0;
        let results = null;
        while (true) {
          try {
            results = await performCalls(entry, toolCalls);
            break;
          } catch (err) {
            if (attempt === 0 && (!entry.alive || /not connected/i.test(err.message))) {
              pool.delete(group.id);
              entry = await getPoolEntry(group);
              attempt++;
              continue;
            }
            throw err;
          }
        }
        return results;
      } finally {
        decrementActiveConnections(group);
        if (debug) {
          const remaining = activeConnectionsPerGroup.get(group.id) || 0;
          console.error(`[POOL-PARALLEL] Connection released for ${group.id}, remaining: ${remaining}/${maxConcurrent}`);
        }
      }
    } finally {
      releaseSemaphore(sem);
    }
  }

  async function runMCP_Spawn(toolCalls) {
    return new Promise((resolve, reject) => {
      const child = spawn(SQLCL_CMD, SQLCL_ARGS, { stdio: ['pipe', 'pipe', 'pipe'] });
      const rl = readline.createInterface({ input: child.stdout });
      const lines = [];
      const sent = [];
      let msgId = 1;

      rl.on('line', line => {
        if (line.trim()) {
          lines.push(line);
          if (debug) console.error('[MCP ←]', line);
        }
      });

      child.on('error', reject);
      child.on('close', () => {
        try {
          const responses = lines
            .filter(l => l.startsWith('{'))
            .map(l => JSON.parse(l))
            .filter(r => r.id && r.result);
          resolve({ responses, sent, received: lines });
        } catch (e) {
          reject(e);
        }
      });

      const send = obj => {
        const text = JSON.stringify(obj);
        sent.push(text);
        child.stdin.write(text + '\n');
        if (debug) console.error('[MCP →]', text);
      };

      (async () => {
        send({
          jsonrpc: '2.0',
          id: msgId++,
          method: 'initialize',
          params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: MCP_CLIENT, version: '1.0' } }
        });
        await sleep(300);
        send({ jsonrpc: '2.0', method: 'notifications/initialized', params: {} });
        await sleep(300);

        for (const tool of toolCalls) {
          send({
            jsonrpc: '2.0',
            id: msgId++,
            method: 'tools/call',
            params: { name: tool.name, arguments: { ...tool.arguments, mcp_client: MCP_CLIENT, model: MCP_MODEL } }
          });
          await sleep(500);
        }

        send({
          jsonrpc: '2.0',
          id: msgId++,
          method: 'tools/call',
          params: { name: 'disconnect', arguments: { mcp_client: MCP_CLIENT, model: MCP_MODEL } }
        });
        await sleep(200);
        child.stdin.end();
      })();
    });
  }

  async function runMCP(toolCalls, group, options = {}) {
    if (POOL_ENABLED && group) {
      if (options.parallel) {
        console.error('[POOL] enqueueing parallel request for', group.id);
        const arr = await enqueueToolCallsParallel(group, toolCalls, options);
        return { responses: arr, sent: [], received: [] };
      }
      console.error('[POOL] enqueueing FIFO request for', group.id);
      const arr = await enqueueToolCalls(group, toolCalls);
      return { responses: arr, sent: [], received: [] };
    }
    console.error('[POOL] spawning ad-hoc process');
    return runMCP_Spawn(toolCalls);
  }

  async function runSQL(group, sql, route, params) {
    const { connectionName } = group;
    const calls = (POOL_ENABLED && group)
      ? [
        { name: 'run-sql', arguments: { sql } }
      ]
      : [
        { name: 'connect', arguments: { connection_name: connectionName } },
        { name: 'run-sql', arguments: { sql } }
      ];
    const start = Date.now();
    if (debug) console.error('[REQ]', route, params);
    let parsedRows = 0;
    try {
      const { responses, sent, received } = await runMCP(calls, group);
      if (debug) console.error('[runSQL] received', responses.length, 'responses');
      const rs = responses.length >= 2 ? responses[1] : responses[responses.length - 1];
      if (!rs || !rs.result) {
        if (debug) {
          console.error('[runSQL] all responses (no usable result):', JSON.stringify(responses, null, 2));
        }
        throw new Error('run-sql did not return a valid result');
      }
      if (rs.result.isError) {
        throw new Error(rs.result.errorMessage || 'SQL execution error');
      }
      if (debug) {
        console.error('[runSQL] raw run-sql text:', rs.result.content[0].text.substring(0, 1000));
      }
      const rows = parseMCPResult(rs);
      parsedRows = rows.length;
      if (debug) console.error('[runSQL] parsed rows count:', parsedRows);
      if (debug) console.error('[RES]', 200, route, '→', parsedRows, 'rows in', Date.now() - start, 'ms');
      lastRequest = {
        route,
        params,
        mcpMessages: sent,
        mcpResponses: received,
        parsedRows,
        durationMs: Date.now() - start,
        error: null
      };
      return rows;
    } catch (err) {
      if (err.code === 'QUEUE_FULL' || err.message === 'Queue full') {
        const e2 = new Error('Queue full');
        e2.httpStatus = 503;
        err = e2;
      }
      if (debug) console.error('[RES]', err.httpStatus || 500, route, err.message);
      lastRequest = {
        route,
        params,
        mcpMessages: null,
        mcpResponses: null,
        parsedRows,
        durationMs: Date.now() - start,
        error: err.message
      };
      throw err;
    }
  }

  async function queryDB(group, sql, route = '/db/query', params = {}) {
    return runSQL(group, sql, route, params);
  }

  setInterval(() => {
    const now = Date.now();
    for (const [gid, entry] of pool) {
      if (!entry.alive || now - entry.lastUsed > POOL_IDLE_TIMEOUT) {
        try {
          sendToEntry(entry, {
            jsonrpc: '2.0',
            id: entry.nextMsgId++,
            method: 'tools/call',
            params: { name: 'disconnect', arguments: { mcp_client: MCP_CLIENT, model: MCP_MODEL } }
          });
        } catch (_e) {}
        entry.child.kill();
        pool.delete(gid);
      }
    }
  }, 60000);

  return {
    runSQL,
    queryDB,
    enqueueToolCallsParallel,
    getLastRequest: () => lastRequest || {},
    getPoolSnapshot: () => {
      const info = {
        timestamp: new Date().toISOString(),
        connectionLimits: {
          manhattan: {
            type: 'per-dc',
            limit: MAX_CONNECTIONS_PER_DC,
            description: `5 connections per DC (9 DCs in main = ~45 max, 3 DCs in ck = 15 max, 1 DC in wk = 5 max)`
          },
          cigwms: {
            type: 'group-wide',
            limit: MAX_CONNECTIONS_CIGWMS,
            description: '5 connections total'
          },
          wmshub: {
            type: 'group-wide',
            limit: MAX_CONNECTIONS_WMSHUB,
            description: '5 connections total'
          }
        },
        activeConnections: {}
      };
      
      // Group stats
      for (const [groupId, count] of activeConnectionsPerGroup) {
        if (!info.activeConnections[groupId]) {
          info.activeConnections[groupId] = {};
        }
        const limitInfo = getConnectionLimit({ id: groupId });
        info.activeConnections[groupId].active = count;
        info.activeConnections[groupId].limitType = limitInfo.type;
        info.activeConnections[groupId].limit = limitInfo.limit;
        info.activeConnections[groupId].available = Math.max(0, limitInfo.limit - count);
      }
      
      // DC stats
      for (const [dcCode, count] of activeConnectionsPerDC) {
        if (!info.activeConnections[`dc-${dcCode}`]) {
          info.activeConnections[`dc-${dcCode}`] = {};
        }
        info.activeConnections[`dc-${dcCode}`].active = count;
        info.activeConnections[`dc-${dcCode}`].limitType = 'per-dc';
        info.activeConnections[`dc-${dcCode}`].limit = MAX_CONNECTIONS_PER_DC;
        info.activeConnections[`dc-${dcCode}`].available = Math.max(0, MAX_CONNECTIONS_PER_DC - count);
      }
      
      // Pool entry stats
      for (const [id, entry] of pool) {
        if (!info.activeConnections[id]) {
          info.activeConnections[id] = {};
        }
        info.activeConnections[id].poolEntry = {
          alive: entry.alive,
          queueDepth: entry.queue ? entry.queue.length : 0,
          lastUsedAgo: Date.now() - (entry.lastUsed || 0),
          dbUser: entry.dbUser || null
        };
      }
      
      return info;
    },
    getDbUser: (groupId) => {
      const entry = pool.get(groupId);
      return entry && entry.alive ? (entry.dbUser || null) : null;
    }
  };
};