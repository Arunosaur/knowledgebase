module.exports = function createKnowledgeRoutes(deps) {
  const {
    config,
    fs,
    readKnowledgeEntries,
    applyKnowledgeFilters,
    knowledgePathById,
    generateKnowledgeId,
    knowledgeSearch,
    knowledgeStats,
    boolQuery,
    graphStore
  } = deps;

  function normalizeText(value) {
    return String(value || '').replace(/\s+/g, ' ').trim();
  }

  function normalizeList(list, toUpper = false) {
    const values = Array.isArray(list) ? list : [];
    return values
      .map(v => normalizeText(v))
      .filter(Boolean)
      .map(v => (toUpper ? v.toUpperCase() : v.toLowerCase()))
      .sort();
  }

  function makeEntrySignature(entry) {
    const context = entry?.context || {};
    const signature = {
      type: normalizeText(entry?.type || 'qa').toLowerCase(),
      question: normalizeText(entry?.question || '').toLowerCase(),
      answer: normalizeText(entry?.answer || '').toLowerCase(),
      context: {
        group: normalizeText(context?.group || '').toLowerCase(),
        schemas: normalizeList(context?.schemas),
        dcCodes: normalizeList(context?.dcCodes, true),
        systems: normalizeList(context?.systems),
        objects: normalizeList(context?.objects),
        jiraIssues: normalizeList(context?.jiraIssues, true)
      },
      tags: normalizeList(entry?.tags)
    };
    return JSON.stringify(signature);
  }

  return async function handleKnowledgeRoute(req, res, pathname, query) {
    if (!pathname.startsWith('/knowledge/')) return false;

    if (pathname === '/knowledge/list' && req.method === 'GET') {
      const all = readKnowledgeEntries();
      const filtered = applyKnowledgeFilters(all, query);
      const limit = Math.max(1, Math.min(200, parseInt(query.limit, 10) || 50));
      const offset = Math.max(0, parseInt(query.offset, 10) || 0);
      const paged = filtered.slice(offset, offset + limit);
      res.end(JSON.stringify({ entries: paged, total: all.length, filtered: filtered.length }));
      return true;
    }

    if (pathname === '/knowledge/entry' && req.method === 'GET') {
      const id = String(query.id || '').trim();
      if (!id) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'id is required' }));
        return true;
      }
      const filePath = knowledgePathById(id);
      if (!fs.existsSync(filePath)) {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'not found' }));
        return true;
      }
      res.end(fs.readFileSync(filePath, 'utf8'));
      return true;
    }

    if (pathname === '/knowledge/entry' && req.method === 'POST') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => {
        try {
          const payload = body ? JSON.parse(body) : {};
          const proposedEntry = {
            type: payload.type || 'qa',
            question: payload.question || '',
            answer: payload.answer || '',
            context: payload.context || {},
            tags: Array.isArray(payload.tags) ? payload.tags : []
          };
          const proposedSignature = makeEntrySignature(proposedEntry);
          const existing = readKnowledgeEntries().find(e => makeEntrySignature(e) === proposedSignature);
          if (existing) {
            res.end(JSON.stringify({ ok: true, id: existing.id, duplicate: true }));
            return;
          }
          const id = payload.id ? String(payload.id) : generateKnowledgeId();
          const nowIso = new Date().toISOString();
          const entry = {
            id,
            type: payload.type || 'qa',
            question: payload.question || '',
            answer: payload.answer || '',
            context: payload.context || {},
            tags: Array.isArray(payload.tags) ? payload.tags : [],
            quality: Number(payload.quality) || 1,
            source: payload.source || 'ai-generated',
            capturedBy: payload.capturedBy || '',
            capturedAt: payload.capturedAt || nowIso,
            updatedAt: nowIso,
            approved: typeof payload.approved === 'boolean' ? payload.approved : false,
            notes: payload.notes || ''
          };
          fs.writeFileSync(knowledgePathById(id), JSON.stringify(entry, null, 2));
          res.end(JSON.stringify({ ok: true, id }));
        } catch (e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return true;
    }

    if (pathname === '/knowledge/entry' && req.method === 'PUT') {
      const id = String(query.id || '').trim();
      if (!id) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'id is required' }));
        return true;
      }
      const filePath = knowledgePathById(id);
      if (!fs.existsSync(filePath)) {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'not found' }));
        return true;
      }
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => {
        try {
          const patch = body ? JSON.parse(body) : {};
          const current = JSON.parse(fs.readFileSync(filePath, 'utf8'));
          const merged = { ...current, ...patch, id: current.id, updatedAt: new Date().toISOString() };
          fs.writeFileSync(filePath, JSON.stringify(merged, null, 2));
          res.end(JSON.stringify({ ok: true, id }));
        } catch (e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return true;
    }

    if (pathname === '/knowledge/entry' && req.method === 'DELETE') {
      const id = String(query.id || '').trim();
      if (!id) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'id is required' }));
        return true;
      }
      const filePath = knowledgePathById(id);
      if (!fs.existsSync(filePath)) {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'not found' }));
        return true;
      }
      fs.unlinkSync(filePath);
      res.end(JSON.stringify({ ok: true, id }));
      return true;
    }

    if (pathname === '/knowledge/search' && req.method === 'GET') {
      const q = String(query.q || '').trim();
      const limit = Math.max(1, Math.min(50, parseInt(query.limit, 10) || 5));

      // Try pgvector semantic search first if postgres connected
      if (graphStore && q) {
        try {
          const postgresUrl = config?.bridge?.postgresUrl;
          if (await graphStore.isConnected(postgresUrl)) {
            const vectorResults = await graphStore.semanticKnowledgeSearch(q, limit, config?.bridge?.ollamaUrl);
            if (vectorResults && vectorResults.length > 0) {
              res.end(JSON.stringify(vectorResults));
              return true;
            }
          }
        } catch (err) {
          // Fall through to JSON search on error
        }
      }

      // Fall back to keyword search
      const results = knowledgeSearch(q, limit);
      res.end(JSON.stringify(results));
      return true;
    }

    if (pathname === '/knowledge/stats' && req.method === 'GET') {
      const all = readKnowledgeEntries();
      res.end(JSON.stringify(knowledgeStats(all)));
      return true;
    }

    if (pathname === '/knowledge/export' && req.method === 'GET') {
      const format = String(query.format || 'jsonl').toLowerCase();
      const qualityMin = Number(query.quality);
      const approvedOnly = boolQuery(query.approved);
      const typeFilter = String(query.type || '').trim().toLowerCase();
      const systemFilter = String(query.system || '').trim().toLowerCase();
      const dcFilter = String(query.dc || '').trim().toUpperCase();

      const all = readKnowledgeEntries();
      const filtered = all.filter(e => {
        if (Number.isFinite(qualityMin) && Number(e.quality) < qualityMin) return false;
        if (approvedOnly !== null && Boolean(e.approved) !== approvedOnly) return false;
        if (typeFilter && String(e.type || '').toLowerCase() !== typeFilter) return false;
        if (systemFilter) {
          const systems = Array.isArray(e?.context?.systems) ? e.context.systems.map(x => String(x).toLowerCase()) : [];
          if (!systems.includes(systemFilter)) return false;
        }
        if (dcFilter) {
          const dcs = Array.isArray(e?.context?.dcCodes) ? e.context.dcCodes.map(x => String(x).toUpperCase()) : [];
          if (!dcs.includes(dcFilter)) return false;
        }
        return true;
      });

      const dateTag = new Date().toISOString().slice(0, 10).replace(/-/g, '');
      let ext = 'jsonl';
      if (format === 'raw') ext = 'json';
      const filename = `wmsiq-knowledge-${dateTag}.${ext}`;
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);

      if (!filtered.length) {
        const emptyBody = format === 'raw' ? '[]\n' : '# WMS·IQ knowledge export: no matching entries\n';
        res.setHeader('Content-Type', format === 'raw' ? 'application/json' : 'application/x-ndjson');
        res.end(emptyBody);
        return true;
      }

      if (format === 'raw') {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(filtered, null, 2));
        return true;
      }

      const lines = filtered.map(e => {
        if (format === 'alpaca') {
          const ctxParts = [];
          const dcs = Array.isArray(e?.context?.dcCodes) ? e.context.dcCodes : [];
          const systems = Array.isArray(e?.context?.systems) ? e.context.systems : [];
          if (dcs.length) ctxParts.push(`DCs: ${dcs.join(', ')}`);
          if (systems.length) ctxParts.push(`Systems: ${systems.join(', ')}`);
          const contextLine = ctxParts.length ? `Context: McLane WMS knowledge. ${ctxParts.join(' | ')}` : 'Context: McLane WMS knowledge.';
          return JSON.stringify({ instruction: e.question || '', input: contextLine, output: e.answer || '' });
        }
        return JSON.stringify({ instruction: e.question || '', input: '', output: e.answer || '' });
      });
      res.setHeader('Content-Type', 'application/x-ndjson');
      res.end(lines.join('\n') + '\n');
      return true;
    }

    if (pathname === '/knowledge/migrate-to-graph' && req.method === 'POST') {
      // Migrate all knowledge entries to PostgreSQL pgvector
      if (!graphStore || !(await graphStore.isConnected(config?.bridge?.postgresUrl))) {
        res.writeHead(200);
        res.end(JSON.stringify({ skipped: true, reason: 'PostgreSQL not enabled' }));
        return true;
      }

      // Respond 202 immediately, run in background
      res.writeHead(202);
      res.end(JSON.stringify({ status: 'migrating in background' }));

      // Background migration
      (async () => {
        let migrated = 0;
        const errors = [];

        try {
          const all = readKnowledgeEntries();
          console.log(`[GRAPH] Migrating ${all.length} knowledge entries...`);

          for (const entry of all) {
            try {
              const id = entry.id || generateKnowledgeId();
              const question = entry.question || '';
              const answer = entry.answer || '';
              const fullText = `${question}\n${answer}`;
              const tags = entry.tags || [];
              const quality = entry.quality || 1;
              const approved = entry.approved || false;
              const capturedBy = entry.capturedBy || '';
              const jiraIssues = entry.jiraIssues || [];

              // Generate embedding
              const embedding = await graphStore.generateEmbedding(fullText, config?.bridge?.ollamaUrl);

              // Upsert to graph
              await graphStore.upsertKnowledgeEntry(
                id,
                question,
                answer,
                tags,
                quality,
                approved,
                capturedBy,
                jiraIssues,
                embedding
              );

              migrated++;
              if (migrated % 20 === 0) {
                console.log(`[GRAPH] Migrated ${migrated}/${all.length} knowledge entries...`);
              }
            } catch (err) {
              errors.push({ id: entry.id, error: err.message.slice(0, 100) });
            }
          }

          console.log(`[GRAPH] ✓ Knowledge migration complete: ${migrated} entries`);
        } catch (err) {
          console.error('[GRAPH] Knowledge migration failed:', err.message);
          errors.push({ error: err.message.slice(0, 200) });
        }
      })();

      return true;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'not found' }));
    return true;
  };
};