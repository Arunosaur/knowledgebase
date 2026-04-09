module.exports = function createDbRoutes(deps) {
  const {
    config,
    withTimeout,
    runSQL,
    TOOL_CALL_TIMEOUT,
    validGroup,
    validIdentifier,
    validType,
    sanitizeKeyword,
    graphStore
  } = deps;

  const QUERY_MAX_ROWS = Number((config.bridge && config.bridge.queryMaxRows) || 50);
  const QUERY_HARD_MAX_ROWS = Number((config.bridge && config.bridge.queryHardMaxRows) || 500);
  const IMPACT_MAX_NODES = Number((config.bridge && config.bridge.impactMaxNodes) || 200);
  const EMBED_BATCH = 10;

  // Graph store state
  const scanRunning = new Map(); // groupId -> { startTime, timestamp }
  const lastScanTime = new Map(); // groupId -> ISO timestamp
  const objectCountsByGroup = new Map(); // groupId -> count

  function validateDbParams(params) {
    if (params.group && !validGroup(params.group)) throw new Error('Invalid parameter: group');
    if (params.schema && !validIdentifier(params.schema)) throw new Error('Invalid parameter: schema');
    if (params.table && !validIdentifier(params.table)) throw new Error('Invalid parameter: table');
    if (params.name && !validIdentifier(params.name)) throw new Error('Invalid parameter: name');
    if (params.type && !validType(params.type)) throw new Error('Invalid parameter: type');
    if (params.keyword) params.keyword = sanitizeKeyword(params.keyword);
  }

  return async function handleDbRoute(req, res, pathname, query) {
    if (!pathname.startsWith('/db/')) return false;

    let groupId;
    let group;
    if (
      pathname !== '/db/impact'
      && pathname !== '/db/query'
      && pathname !== '/db/search-all'
      && pathname !== '/db/scan-schema'
      && pathname !== '/db/scan-status'
      && pathname !== '/db/find-table'
      && pathname !== '/db/find-columns'
    ) {
      groupId = query.group;
      group = config.groups.find(g => g.id === groupId);
      if (!group) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'invalid group' }));
        return true;
      }
    }

    const run = async sql => {
      const rows = await withTimeout(runSQL(group, sql, pathname, query));
      res.end(JSON.stringify(rows));
    };

    if (pathname === '/db/query' && req.method === 'POST') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', async () => {
        try {
          const payload = body ? JSON.parse(body) : {};
          const groupIdFromBody = payload.group;
          const rawSql = String(payload.sql || '');
          let maxRows = Number(payload.maxRows);
          if (!groupIdFromBody || !validGroup(groupIdFromBody)) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'group is required' }));
            return;
          }
          const targetGroup = config.groups.find(g => g.id === groupIdFromBody);
          if (!targetGroup) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'invalid group' }));
            return;
          }
          if (!rawSql.trim()) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'sql is required' }));
            return;
          }
          if (!Number.isFinite(maxRows)) maxRows = QUERY_MAX_ROWS;
          maxRows = Math.max(1, Math.min(QUERY_HARD_MAX_ROWS, Math.floor(maxRows)));

          const strippedForValidation = rawSql
            .replace(/\/\*[\s\S]*?\*\//g, ' ')
            .replace(/--.*$/gm, ' ')
            .trim();
          const firstWord = (strippedForValidation.match(/^([a-zA-Z]+)/) || [])[1] || '';
          if (!/^select$/i.test(firstWord)) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'Only SELECT statements are allowed' }));
            return;
          }
          if (/\b(insert|update|delete|drop|create|alter|truncate|execute|exec)\b/i.test(strippedForValidation)) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'Only SELECT statements are allowed' }));
            return;
          }

          let finalSql = rawSql.trim().replace(/;\s*$/, '');
          if (!/fetch\s+first\s+\d+\s+rows?\s+only/i.test(finalSql)) {
            finalSql = `${finalSql} FETCH FIRST ${maxRows} ROWS ONLY`;
          }
          finalSql = `/* LLM in use is bridge */\n${finalSql}`;

          const rows = await withTimeout(runSQL(targetGroup, finalSql, pathname, { group: groupIdFromBody }), TOOL_CALL_TIMEOUT);
          const columns = rows.length ? Object.keys(rows[0]) : [];
          const rowCount = rows.length;
          const fetchMatch = finalSql.match(/fetch\s+first\s+(\d+)\s+rows?\s+only/i);
          const cappedRows = fetchMatch ? parseInt(fetchMatch[1], 10) : maxRows;
          const truncated = Number.isFinite(cappedRows) ? rowCount >= cappedRows : false;
          res.end(JSON.stringify({ rows, columns, rowCount, truncated }));
        } catch (e) {
          if (e.code === 'CONNECTION_LIMIT_EXCEEDED' || e.code === 'DC_CONNECTION_LIMIT_EXCEEDED') {
            res.writeHead(429, { 'Retry-After': String(e.retryAfter || 30) });
            res.end(JSON.stringify({ error: e.message, code: e.code, retryAfter: e.retryAfter || 30 }));
          } else {
            res.writeHead(400);
            res.end(JSON.stringify({ error: e.message }));
          }
        }
      });
      return true;
    }

    if (pathname === '/db/schemas' && req.method === 'GET') {
      if (group.schemas && group.schemas.length > 0) {
        res.end(JSON.stringify(group.schemas.map(s => ({ SCHEMA_NAME: s }))));
        return true;
      }
      const sql = `/* LLM in use is bridge */\nSELECT DISTINCT OWNER AS SCHEMA_NAME FROM DBA_OBJECTS\nWHERE OBJECT_TYPE IN ('TABLE','PACKAGE','PROCEDURE','FUNCTION','VIEW','TRIGGER')\n  AND OWNER NOT IN ('SYS','SYSTEM','DBSNMP','OUTLN','XDB','APPQOSSYS','CTXSYS',\n    'DVSYS','EXFSYS','LBACSYS','MDSYS','OJVMSYS','OLAPSYS','ORDDATA','ORDSYS','WMSYS','APEX_PUBLIC_USER')\nORDER BY 1`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/debug-objects' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const sql = `/* LLM in use is bridge */\nSELECT OBJECT_NAME, OBJECT_TYPE, STATUS FROM DBA_OBJECTS\nWHERE OWNER = UPPER('${schema}')\n  AND ROWNUM < 20\nORDER BY OBJECT_TYPE, OBJECT_NAME`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/objects' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const sql = `/* LLM in use is bridge */\n      SELECT OBJECT_NAME, OBJECT_TYPE, STATUS FROM DBA_OBJECTS\nWHERE OWNER = UPPER('${schema}')\n  AND OBJECT_TYPE IN ('TABLE','PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION',\n                'VIEW','TRIGGER','SEQUENCE','INDEX','SYNONYM','TYPE',\n                'TYPE BODY','DATABASE LINK','MATERIALIZED VIEW',\n                'JAVA CLASS','QUEUE')\nORDER BY OBJECT_TYPE, OBJECT_NAME`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/columns' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const table = query.table || '';
      const sql = `/* LLM in use is bridge */\nSELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE,\n       NULLABLE, DATA_DEFAULT, COLUMN_ID\nFROM DBA_TAB_COLUMNS\nWHERE OWNER = UPPER('${schema}') AND TABLE_NAME = UPPER('${table}')\nORDER BY COLUMN_ID`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/constraints' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const table = query.table || '';
      const sql = `/* LLM in use is bridge */\nSELECT c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, c.STATUS,\n       cc.COLUMN_NAME, c.R_CONSTRAINT_NAME\nFROM DBA_CONSTRAINTS c\nJOIN DBA_CONS_COLUMNS cc ON c.OWNER=cc.OWNER AND c.CONSTRAINT_NAME=cc.CONSTRAINT_NAME\nWHERE c.OWNER = UPPER('${schema}') AND c.TABLE_NAME = UPPER('${table}')\nORDER BY c.CONSTRAINT_TYPE, cc.POSITION`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/source' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const name = query.name || '';
      const type = query.type || '';
      const sql = `/* LLM in use is bridge */\nSELECT TEXT FROM DBA_SOURCE\nWHERE OWNER = UPPER('${schema}') AND NAME = UPPER('${name}') AND TYPE = UPPER('${type}')\nORDER BY LINE`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/test-source' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const name = query.name || '';
      try {
        const sql = `/* LLM in use is bridge */\nSELECT COUNT(*) AS LINE_COUNT FROM DBA_SOURCE\nWHERE OWNER = UPPER('${schema}') AND NAME = UPPER('${name}')`;
        const rows = await withTimeout(runSQL(group, sql, pathname, query));
        const count = rows[0]?.LINE_COUNT || 0;
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          schema,
          name,
          lineCount: count,
          canAccessDbaSource: count > 0,
          message: count > 0 ? `✅ SQLcl MCP found ${count} lines in DBA_SOURCE for ${schema}.${name}` : `⚠️ No rows found in DBA_SOURCE for ${schema}.${name} - check name spelling or schema access`
        }));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'DBA_SOURCE query failed via SQLcl MCP', message: String(e && e.message ? e.message : e), diagnosis: 'Check if -R 2 restrict level or connection context blocks DBA_SOURCE access' }));
      }
      return true;
    }

    if (pathname === '/db/dependencies' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const name = query.name || '';
      const sql = `/* LLM in use is bridge */\nSELECT REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE, DEPENDENCY_TYPE\nFROM DBA_DEPENDENCIES\nWHERE OWNER = UPPER('${schema}') AND NAME = UPPER('${name}')\nORDER BY REFERENCED_TYPE, REFERENCED_NAME`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/dependants' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const name = query.name || '';
      const sql = `/* LLM in use is bridge */\nSELECT OWNER, NAME, TYPE FROM DBA_DEPENDENCIES\nWHERE REFERENCED_OWNER = UPPER('${schema}') AND REFERENCED_NAME = UPPER('${name}')\nORDER BY TYPE, NAME`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/search' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const keyword = query.keyword || '';
      const sql = `/* LLM in use is bridge */\nSELECT DISTINCT NAME, TYPE FROM DBA_SOURCE\nWHERE OWNER = UPPER('${schema}') AND UPPER(TEXT) LIKE UPPER('%${keyword}%')\nORDER BY TYPE, NAME`;
      await run(sql);
      return true;
    }

    if (pathname === '/db/search-all' && req.method === 'GET') {
      const keyword = String(query.keyword || '').trim();
      if (!keyword) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'keyword is required' }));
        return true;
      }

      const started = Date.now();
      const deadline = started + 30000;
      const timedOut = [];
      const allResults = [];

      const groupTasks = config.groups.map(async g => {
        const timeLeft = deadline - Date.now();
        if (timeLeft <= 0) {
          timedOut.push(g.id);
          return;
        }
        const perGroupTask = (async () => {
          const schemas = Array.isArray(g.schemas) ? g.schemas : [];
          const schemaTasks = schemas.map(async schema => {
            const sql = `/* LLM in use is bridge */\nSELECT DISTINCT NAME, TYPE FROM DBA_SOURCE\nWHERE OWNER = UPPER('${schema}') AND UPPER(TEXT) LIKE UPPER('%${keyword}%')\nORDER BY TYPE, NAME`;
            try {
              const rows = await withTimeout(runSQL(g, sql, pathname, { group: g.id, schema, keyword }), Math.max(1000, deadline - Date.now()));
              return rows.map(r => ({
                group: g.id,
                groupName: g.name,
                groupColor: g.color,
                schema,
                name: r.NAME || r.name || '',
                type: r.TYPE || r.type || ''
              })).filter(x => x.name && x.type);
            } catch (_e) {
              return [];
            }
          });
          const perSchema = await Promise.all(schemaTasks);
          perSchema.forEach(arr => allResults.push(...arr));
        })();

        try {
          await withTimeout(perGroupTask, Math.max(1000, timeLeft));
        } catch (_e) {
          timedOut.push(g.id);
        }
      });

      await Promise.allSettled(groupTasks);

      const dedup = new Map();
      for (const r of allResults) {
        const key = `${String(r.group).toUpperCase()}::${String(r.name).toUpperCase()}::${String(r.type).toUpperCase()}`;
        if (!dedup.has(key)) {
          dedup.set(key, { ...r, schemas: [r.schema] });
        } else {
          const cur = dedup.get(key);
          if (!cur.schemas.includes(r.schema)) cur.schemas.push(r.schema);
        }
      }

      const results = Array.from(dedup.values()).map(r => ({
        group: r.group,
        groupName: r.groupName,
        groupColor: r.groupColor,
        schema: r.schema,
        name: r.name,
        type: r.type,
        foundInSchemas: r.schemas,
        foundInSchemaCount: r.schemas.length,
        note: r.schemas.length > 1 ? `found in ${r.schemas.length} schemas` : ''
      }));

      res.end(JSON.stringify({ results, queryMs: Date.now() - started, groupsSearched: config.groups.length, totalResults: results.length, timedOut }));
      return true;
    }

    if (pathname === '/db/object-status' && req.method === 'GET') {
      try { validateDbParams(query); } catch (e) { res.writeHead(400); res.end(JSON.stringify({ error: e.message })); return true; }
      const schema = query.schema || '';
      const name = query.name || '';
      const type = query.type || '';
      const sql = `/* LLM in use is bridge */\nSELECT STATUS, LAST_DDL_TIME FROM DBA_OBJECTS\nWHERE OWNER = UPPER('${schema}') AND OBJECT_NAME = UPPER('${name}') AND OBJECT_TYPE = UPPER('${type}')`;
      const rows = await withTimeout(runSQL(group, sql, pathname, query));
      const row = rows[0] || {};
      res.end(JSON.stringify({ status: row.STATUS || 'N/A', lastDdlTime: row.LAST_DDL_TIME || null }));
      return true;
    }

    if (pathname === '/db/impact' && req.method === 'GET') {
      const srcName = query.name;
      const srcType = query.type;
      const srcSchema = query.sourceSchema;
      const srcGroup = query.sourceGroup;
      if (!srcName || !srcType || !srcSchema || !srcGroup) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Missing required parameter' }));
        return true;
      }
      if (!validIdentifier(srcName) || !validType(srcType) || !validIdentifier(srcSchema) || !validGroup(srcGroup)) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid parameter' }));
        return true;
      }
      let depth = Number.parseInt(query.depth, 10);
      if (!Number.isFinite(depth)) depth = 3;
      if (depth < 1) depth = 1;
      if (depth > 5) depth = 5;

      const start = Date.now();
      const warnings = [];
      const maxNodes = IMPACT_MAX_NODES;
      const nodesMap = new Map();
      const edges = [];

      const makeId = (grp, schema, name) => `${(grp || '').toUpperCase()}::${(schema || '').toUpperCase()}::${(name || '').toUpperCase()}`;
      const getStatus = async (grp, schema, name, type) => {
        try {
          const sql = `/* LLM in use is bridge */\nSELECT STATUS, LAST_DDL_TIME FROM DBA_OBJECTS\nWHERE OWNER = UPPER('${schema}') AND OBJECT_NAME = UPPER('${name}') AND OBJECT_TYPE = UPPER('${type}')`;
          const rows = await withTimeout(runSQL(config.groups.find(g => g.id === grp), sql, '/db/object-status', {}));
          const r = rows[0] || {};
          return { status: r.STATUS || 'N/A', lastDdlTime: r.LAST_DDL_TIME || null };
        } catch (e) {
          warnings.push(`⚠ ${grp} connection error: ${e.message}`);
          return { status: 'UNKNOWN', lastDdlTime: null };
        }
      };
      const queryDeps = async (grp, schema, name) => {
        try {
          const sql = `/* LLM in use is bridge */\nSELECT REFERENCED_OWNER AS OWNER, REFERENCED_NAME AS NAME, REFERENCED_TYPE AS TYPE FROM DBA_DEPENDENCIES\nWHERE OWNER = UPPER('${schema}') AND NAME = UPPER('${name}')\nORDER BY REFERENCED_TYPE, REFERENCED_NAME`;
          return await withTimeout(runSQL(config.groups.find(g => g.id === grp), sql, '/db/dependencies', {}));
        } catch (e) { warnings.push(`⚠ ${grp} deps error: ${e.message}`); return []; }
      };
      const queryDependants = async (grp, schema, name) => {
        try {
          const sql = `/* LLM in use is bridge */\nSELECT OWNER, NAME, TYPE FROM DBA_DEPENDENCIES\nWHERE REFERENCED_OWNER = UPPER('${schema}') AND REFERENCED_NAME = UPPER('${name}')\nORDER BY TYPE, NAME`;
          return await withTimeout(runSQL(config.groups.find(g => g.id === grp), sql, '/db/dependants', {}));
        } catch (e) { warnings.push(`⚠ ${grp} dependants error: ${e.message}`); return []; }
      };

      if (depth === 1) {
        const rootId = makeId(srcGroup, srcSchema, srcName);
        const grpObj = config.groups.find(g => g.id === srcGroup) || {};
        const statusInfo = await getStatus(srcGroup, srcSchema, srcName, srcType);
        nodesMap.set(rootId, {
          id: rootId,
          name: srcName,
          type: srcType,
          schema: srcSchema,
          group: srcGroup,
          groupName: grpObj.name || srcGroup,
          groupColor: grpObj.color || '#888',
          status: statusInfo.status,
          depth: 0
        });

        const depsRows = await queryDeps(srcGroup, srcSchema, srcName);
        for (const r of depsRows) {
          if (nodesMap.size >= maxNodes) break;
          const owner = r.OWNER || '';
          const nm = r.NAME || '';
          const tp = r.TYPE || '';
          if (!owner || !nm) continue;
          const nid = makeId(srcGroup, owner, nm);
          if (!nodesMap.has(nid)) {
            nodesMap.set(nid, {
              id: nid,
              name: nm,
              type: tp,
              schema: owner,
              group: srcGroup,
              groupName: grpObj.name || srcGroup,
              groupColor: grpObj.color || '#888',
              status: 'UNKNOWN',
              depth: 1
            });
          }
          edges.push({ from: rootId, to: nid, kind: 'DEPENDS_ON' });
        }

        const nodes = Array.from(nodesMap.values());
        const crossSchemaEdges = edges.filter(e => {
          const a = nodesMap.get(e.from);
          const b = nodesMap.get(e.to);
          if (!a || !b) return false;
          return a.group !== b.group || a.schema !== b.schema;
        });

        const response = {
          root: { name: srcName, type: srcType, schema: srcSchema, group: srcGroup },
          nodes,
          edges,
          crossSchemaEdges,
          truncated: nodesMap.size >= maxNodes,
          queryMs: Date.now() - start
        };
        if (warnings.length) response.warnings = warnings;
        res.end(JSON.stringify(response));
        return true;
      }

      const queue = [{ group: srcGroup, schema: srcSchema, name: srcName, type: srcType, depth: 0 }];
      while (queue.length && nodesMap.size < maxNodes) {
        const cur = queue.shift();
        const id = makeId(cur.group, cur.schema, cur.name);
        if (nodesMap.has(id)) continue;
        const grpObj = config.groups.find(g => g.id === cur.group) || {};
        const statusInfo = await getStatus(cur.group, cur.schema, cur.name, cur.type);
        nodesMap.set(id, {
          id,
          name: cur.name,
          type: cur.type,
          schema: cur.schema,
          group: cur.group,
          groupName: grpObj.name || cur.group,
          groupColor: grpObj.color || '#888',
          status: statusInfo.status,
          depth: cur.depth
        });

        if (cur.depth < depth) {
          const depsRows = await queryDeps(cur.group, cur.schema, cur.name);
          for (const r of depsRows) {
            const owner = r.OWNER || '';
            const nm = r.NAME || '';
            const tp = r.TYPE || '';
            if (!owner || !nm) continue;
            const nid = makeId(cur.group, owner, nm);
            edges.push({ from: id, to: nid, kind: 'DEPENDS_ON' });
            queue.push({ group: cur.group, schema: owner, name: nm, type: tp, depth: cur.depth + 1 });
          }

          const dnts = await queryDependants(cur.group, cur.schema, cur.name);
          for (const r of dnts) {
            const owner = r.OWNER || '';
            const nm = r.NAME || '';
            const tp = r.TYPE || '';
            if (!owner || !nm) continue;
            const nid = makeId(cur.group, owner, nm);
            edges.push({ from: id, to: nid, kind: 'USED_BY' });
            queue.push({ group: cur.group, schema: owner, name: nm, type: tp, depth: cur.depth + 1 });
          }
        }
      }

      const nodes = Array.from(nodesMap.values());
      const crossSchemaEdges = edges.filter(e => {
        const a = nodesMap.get(e.from);
        const b = nodesMap.get(e.to);
        if (!a || !b) return false;
        return a.group !== b.group || a.schema !== b.schema;
      });

      const response = {
        root: { name: srcName, type: srcType, schema: srcSchema, group: srcGroup },
        nodes,
        edges,
        crossSchemaEdges,
        truncated: nodesMap.size >= maxNodes,
        queryMs: Date.now() - start
      };
      if (warnings.length) response.warnings = warnings;
      res.end(JSON.stringify(response));
      return true;
    }

    // New AGE-based endpoint for graph traversal
    if (pathname === '/db/impact-graph' && req.method === 'GET') {
      const srcName = query.name;
      const srcSchema = query.schema;
      const srcGroup = query.group;
      if (!srcName || !srcSchema || !srcGroup) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Missing required parameter' }));
        return true;
      }
      if (!validIdentifier(srcName) || !validIdentifier(srcSchema) || !validGroup(srcGroup)) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid parameter' }));
        return true;
      }

      let depth = Number.parseInt(query.depth, 10);
      if (!Number.isFinite(depth)) depth = 3;
      if (depth < 1) depth = 1;
      if (depth > 5) depth = 5;

      const srcType = query.type || '';

      // Try AGE-based traversal
      if (graphStore && graphStore.traverseImpact) {
        try {
          const objectId = `${srcGroup}::${srcSchema}::${srcName}::${srcType}`;
          const result = await graphStore.traverseImpact(objectId, query.direction || 'both', depth);
          const response = {
            root: { name: srcName, type: srcType, schema: srcSchema, group: srcGroup },
            nodes: result.nodes || [],
            edges: result.edges || [],
            crossSchemaEdges: [],
            truncated: result.truncated || false,
            queryMs: result.queryMs || 0,
            graphBacked: true,
            error: result.error || null
          };
          res.end(JSON.stringify(response));
          return true;
        } catch (err) {
          console.warn('[API] AGE traversal error, falling back to BFS:', err.message.slice(0, 100));
        }
      }

      // Fallback to BFS like /db/impact
      res.writeHead(501);
      res.end(JSON.stringify({ error: 'AGE not available, use /db/impact endpoint' }));
      return true;
    }

    if (pathname === '/db/ping' && req.method === 'GET') {
      const sql = 'SELECT 1 AS OK FROM DUAL';
      try {
        await withTimeout(runSQL(group, sql, pathname, query));
        res.end(JSON.stringify({ ok: true, group: groupId, connectionName: group.connectionName }));
      } catch (e) {
        res.writeHead(500);
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
      return true;
    }

    if (pathname === '/db/whoami' && req.method === 'GET') {
      const sql = `SELECT SYS_CONTEXT('USERENV','SESSION_USER') AS DB_USER,\n       SYS_CONTEXT('USERENV','DB_NAME') AS DB_NAME\nFROM DUAL`;
      try {
        const rows = await withTimeout(runSQL(group, sql, pathname, query));
        const row = rows && rows[0] ? rows[0] : {};
        const dbUser = row.DB_USER || row.db_user || null;
        const dbName = row.DB_NAME || row.db_name || null;
        res.end(JSON.stringify({ dbUser, dbName }));
      } catch (e) {
        res.writeHead(500);
        res.end(JSON.stringify({ error: e.message }));
      }
      return true;
    }

    // ===== Knowledge Graph Endpoints (PROMPT 32/33) =====

    if (pathname === '/db/scan-schema' && req.method === 'POST') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', async () => {
        try {
          const payload = body ? JSON.parse(body) : {};
          const scanGroupId = payload.group;
          const schemas = Array.isArray(payload.schemas) ? payload.schemas : [];
          const includeSource = payload.includeSource !== false;
          const includeColumns = payload.includeColumns !== false;

          if (!scanGroupId || !validGroup(scanGroupId)) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'invalid group' }));
            return;
          }

          if (!graphStore) {
            res.writeHead(503);
            res.end(JSON.stringify({ error: 'PostgreSQL not enabled' }));
            return;
          }

          if (schemas.length === 0) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'schemas array required' }));
            return;
          }

          if (scanRunning.has(scanGroupId)) {
            res.writeHead(409);
            res.end(JSON.stringify({ error: 'scan already running for this group' }));
            return;
          }

          // Respond 202 immediately, run in background
          res.writeHead(202);
          res.end(JSON.stringify({ status: 'scanning in background' }));

          // Background scan
          (async () => {
            scanRunning.set(scanGroupId, { startTime: Date.now() });
            let totalObjects = 0, totalColumns = 0, totalSources = 0, totalDependencies = 0;
            const errors = [];

            try {
              const targetGroup = config.groups.find(g => g.id === scanGroupId);
              if (!targetGroup) throw new Error('group not found');

              for (const schema of schemas) {
                if (!validIdentifier(schema)) continue;

                try {
                  // Scan objects
                  const objSql = `SELECT OBJECT_NAME, OBJECT_TYPE, STATUS FROM DBA_OBJECTS WHERE OWNER = '${schema}' AND OBJECT_TYPE NOT IN ('SYNONYM','GRANT','INDEX') ORDER BY OBJECT_TYPE, OBJECT_NAME`;
                  const objRows = await withTimeout(runSQL(targetGroup, objSql, '/db/scan-schema', {})) || [];

                  for (let i = 0; i < objRows.length; i += EMBED_BATCH) {
                    const batch = objRows.slice(i, i + EMBED_BATCH);
                    await Promise.all(batch.map(async (obj) => {
                      const objName = obj.OBJECT_NAME || obj.object_name || '';
                      const objType = obj.OBJECT_TYPE || obj.object_type || '';
                      const objStatus = obj.STATUS || obj.status || '';
                      if (!objName) return;

                      const embText = `${objType} ${schema}.${objName}`;
                      const embedding = await graphStore.generateEmbedding(embText, config.bridge?.ollamaUrl);
                      const objId = await graphStore.upsertObject(scanGroupId, schema, objName, objType, objStatus, null, embedding);

                      // Upsert into AGE graph
                      if (graphStore.upsertAgeVertex) {
                        await graphStore.upsertAgeVertex(scanGroupId, schema, objName, objType, objStatus).catch(() => {});
                      }

                      if (objType === 'TABLE' && includeColumns && objId) {
                        const colSql = `SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, COLUMN_ID, DATA_LENGTH FROM DBA_TAB_COLUMNS WHERE OWNER = '${schema}' AND TABLE_NAME = '${objName}' ORDER BY COLUMN_ID`;
                        const colRows = await withTimeout(runSQL(targetGroup, colSql, '/db/scan-schema', {}));
                        if (colRows && Array.isArray(colRows)) {
                          const colsToInsert = [];
                          for (const col of colRows) {
                            const colName = col.COLUMN_NAME || col.column_name || '';
                            const dataType = col.DATA_TYPE || col.data_type || '';
                            const nullable = col.NULLABLE !== 'N' && col.nullable !== 'N';
                            const colId = col.COLUMN_ID || col.column_id;
                            const dataLen = col.DATA_LENGTH || col.data_length;

                            const colEmbText = `${objName}.${colName} ${dataType}`;
                            const colEmbed = await graphStore.generateEmbedding(colEmbText, config.bridge?.ollamaUrl);

                            colsToInsert.push({
                              column_name: colName,
                              data_type: dataType,
                              nullable,
                              column_id: colId,
                              data_length: dataLen,
                              embedding: colEmbed
                            });
                          }
                          await graphStore.upsertColumns(objId, colsToInsert);
                          totalColumns += colsToInsert.length;
                        }
                      }

                      if (includeSource && objId && ['PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION', 'TRIGGER'].includes(objType)) {
                        const srcSql = `SELECT TEXT FROM DBA_SOURCE WHERE OWNER = '${schema}' AND NAME = '${objName}' AND TYPE = '${objType}' ORDER BY LINE`;
                        const srcRows = await withTimeout(runSQL(targetGroup, srcSql, '/db/scan-schema', {}));
                        if (srcRows && Array.isArray(srcRows)) {
                          const srcText = srcRows.map(r => r.TEXT || r.text || '').join('\n');
                          if (srcText.trim()) {
                            const srcEmbText = srcText.slice(0, 2000);
                            const srcEmbed = await graphStore.generateEmbedding(srcEmbText, config.bridge?.ollamaUrl);
                            await graphStore.upsertSource(objId, srcText, srcEmbed);
                            totalSources++;
                          }
                        }
                      }

                      totalObjects++;
                      if (totalObjects % 100 === 0) {
                        console.log(`[GRAPH] ${scanGroupId}/${schema}: ${totalObjects} objects scanned...`);
                      }
                    }));
                  }

                  // Scan dependencies
                  if (totalObjects > 0) {
                    const depSql = `SELECT NAME, TYPE, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE FROM DBA_DEPENDENCIES WHERE OWNER = '${schema}'`;
                    const depRows = await withTimeout(runSQL(targetGroup, depSql, '/db/scan-schema', {}));
                    if (depRows && Array.isArray(depRows)) {
                      for (const dep of depRows) {
                        const fromName = dep.NAME || dep.name || '';
                        const fromType = dep.TYPE || dep.type || '';
                        const toSchema = dep.REFERENCED_OWNER || dep.referenced_owner || '';
                        const toName = dep.REFERENCED_NAME || dep.referenced_name || '';
                        const toType = dep.REFERENCED_TYPE || dep.referenced_type || '';
                        if (!fromName || !fromType || !toSchema || !toName || !toType) continue;
                        const fromId = await graphStore.getObjectId(scanGroupId, schema, fromName, fromType);
                        const toId = await graphStore.getObjectId(scanGroupId, toSchema, toName, toType);
                        if (!fromId || !toId) continue;
                        await graphStore.upsertDependency(fromId, toId, 'REFERENCE');

                        // Upsert edge into AGE graph
                        if (graphStore.upsertAgeEdge) {
                          const fromAgeId = `${scanGroupId}::${schema}::${fromName}::${fromType}`;
                          const toAgeId = `${scanGroupId}::${toSchema}::${toName}::${toType}`;
                          await graphStore.upsertAgeEdge(fromAgeId, toAgeId, 'REFERENCE').catch(() => {});
                        }

                        totalDependencies++;
                      }
                    }
                  }
                } catch (schemaErr) {
                  errors.push(`Schema ${schema}: ${schemaErr.message.slice(0, 100)}`);
                }
              }

              lastScanTime.set(scanGroupId, new Date().toISOString());
              objectCountsByGroup.set(scanGroupId, totalObjects);
              console.log(`[GRAPH] ✓ ${scanGroupId}: ${totalObjects} objects, ${totalColumns} columns, ${totalDependencies} deps (${Date.now() - scanRunning.get(scanGroupId).startTime}ms)`);
            } catch (err) {
              console.error('[GRAPH] Scan failed:', err.message.slice(0, 200));
              errors.push(err.message.slice(0, 200));
            } finally {
              scanRunning.delete(scanGroupId);
            }
          })();
        } catch (e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message.slice(0, 200) }));
        }
      });
      return true;
    }

    if (pathname === '/db/scan-status' && req.method === 'GET') {
      try {
        const running = scanRunning.size > 0;
        const byGroup = {};
        for (const [gid, cnt] of objectCountsByGroup) {
          byGroup[gid] = cnt;
        }
        res.end(JSON.stringify({
          running,
          lastScan: lastScanTime.size > 0 ? Array.from(lastScanTime.values())[0] : null,
          objectsByGroup: byGroup
        }));
      } catch (e) {
        res.writeHead(500);
        res.end(JSON.stringify({ error: e.message }));
      }
      return true;
    }

    if (pathname === '/db/find-table' && req.method === 'GET') {
      try {
        const q = query.q || '';
        const findGroup = query.group || '';
        const findSchema = query.schema || '';

        if (!q || !findGroup) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: 'q and group required' }));
          return;
        }

        const targetGroup = config.groups.find(g => g.id === findGroup);
        if (!targetGroup) {
          res.end(JSON.stringify([]));
          return true;
        }

        if (graphStore && await graphStore.isConnected(config?.bridge?.postgresUrl)) {
          const results = await graphStore.findTableByQuery(q, findGroup, findSchema || null, 10, config.bridge?.ollamaUrl);
          res.end(JSON.stringify(results));
          return true;
        }

        const likeSql = `SELECT OBJECT_NAME AS object_name, OWNER AS schema_name, OBJECT_TYPE AS object_type, STATUS AS status
          FROM DBA_OBJECTS
          WHERE OBJECT_TYPE = 'TABLE'
            AND OWNER = '${String(findSchema || '').toUpperCase().replace(/'/g, "''")}'
            AND OBJECT_NAME LIKE '%${String(q).toUpperCase().replace(/'/g, "''")}%'
          ORDER BY OBJECT_NAME
          FETCH FIRST 10 ROWS ONLY`;
        const fallbackRows = await withTimeout(runSQL(targetGroup, likeSql, pathname, query));
        res.end(JSON.stringify(Array.isArray(fallbackRows) ? fallbackRows.map(row => ({
          object_name: row.object_name || row.OBJECT_NAME,
          schema_name: row.schema_name || row.SCHEMA_NAME || row.OWNER,
          object_type: row.object_type || row.OBJECT_TYPE,
          status: row.status || row.STATUS,
          similarity: '0.50'
        })) : []));
      } catch (e) {
        res.end(JSON.stringify([]));
      }
      return true;
    }

    if (pathname === '/db/find-columns' && req.method === 'GET') {
      try {
        const q = query.q || '';
        const objId = parseInt(query.objectId, 10);

        if (!q || !Number.isFinite(objId)) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: 'q and objectId required' }));
          return;
        }

        if (!graphStore || !(await graphStore.isConnected(config?.bridge?.postgresUrl))) {
          res.end(JSON.stringify([]));
          return true;
        }

        const results = await graphStore.findColumnsByQuery(q, objId, 10, config.bridge?.ollamaUrl);
        res.end(JSON.stringify(results));
      } catch (e) {
        res.writeHead(500);
        res.end(JSON.stringify({ error: e.message }));
      }
      return true;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'not found' }));
    return true;
  };
};