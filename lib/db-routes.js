module.exports = function createDbRoutes(deps) {
  const {
    config,
    withTimeout,
    runSQL,
    TOOL_CALL_TIMEOUT,
    validGroup,
    validIdentifier,
    validType,
    sanitizeKeyword
  } = deps;

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
    if (pathname !== '/db/impact' && pathname !== '/db/query' && pathname !== '/db/search-all') {
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
          if (!Number.isFinite(maxRows)) maxRows = 50;
          maxRows = Math.max(1, Math.min(500, Math.floor(maxRows)));

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
      const maxNodes = 200;
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

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'not found' }));
    return true;
  };
};