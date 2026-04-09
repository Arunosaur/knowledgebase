const path = require('path');
const os = require('os');
const fs = require('fs');

module.exports = function createDocsRoutes(deps) {
  const {
    config,
    SP_SITE_URL,
    MSAL_CLIENT_ID,
    MSAL_TENANT_ID,
    DOCS_INDEX_DIR,
    DOCS_SYNC_CONCURRENCY,
    UPLOAD_TOKEN,
    syncJobs,
    getSiteId,
    fetchGraph,
    sanitizeFileName,
    extractFileText,
    chunkTextSentenceAware,
    preprocessDocText,
    getChunkParams,
    listIndexedDocs,
    docsSearch,
    deleteIndex,
    sleep,
    fetch
  } = deps;

  function getDocChunkParams(extension) {
    const ext = String(extension || '').toLowerCase().replace(/^\./, '');
    if (ext === 'docx' || ext === 'pdf') {
      return { size: 2500, overlap: 300 };
    }
    return getChunkParams(ext, 0);
  }

  function sanitizeStoredText(text) {
    return String(text || '')
      .replace(/\u0000/g, '')
      .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '');
  }

  function writeValidatedJson(filePath, value) {
    const jsonStr = JSON.stringify(value, null, 2);
    JSON.parse(jsonStr);
    const tmpPath = `${filePath}.tmp`;
    fs.writeFileSync(tmpPath, jsonStr, 'utf8');
    fs.renameSync(tmpPath, filePath);
    const verify = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    if (!verify || !Array.isArray(verify.chunks)) {
      throw new Error('Write verification failed');
    }
  }

  function normalizeExtractionResult(result, fallbackMode = 'provided-text') {
    if (result && typeof result === 'object' && !Array.isArray(result)) {
      return {
        text: sanitizeStoredText(result.text || ''),
        mode: String(result.mode || fallbackMode)
      };
    }
    return {
      text: sanitizeStoredText(result || ''),
      mode: fallbackMode
    };
  }

  return async function handleDocsRoute(req, res, pathname, query) {
    if (!pathname.startsWith('/docs/')) return false;

    if (pathname === '/docs/msal-config' && req.method === 'GET') {
      res.end(JSON.stringify({ clientId: MSAL_CLIENT_ID, tenantId: MSAL_TENANT_ID, sharepointSiteUrl: SP_SITE_URL }));
      return true;
    }

    if (pathname === '/docs/upload-token' && req.method === 'GET') {
      res.end(JSON.stringify({ token: UPLOAD_TOKEN }));
      return true;
    }

    if (pathname === '/docs/upload' && req.method === 'POST') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', async () => {
        try {
          const authh = req.headers.authorization || '';
          const bearer = authh.startsWith('Bearer ') ? authh.slice(7).trim() : '';
          const headerToken = String(req.headers['x-upload-token'] || '');
          const queryToken = String(query.token || '');
          const suppliedToken = headerToken || bearer || queryToken;
          if (UPLOAD_TOKEN && suppliedToken !== UPLOAD_TOKEN) {
            res.writeHead(401);
            res.end(JSON.stringify({ error: 'Invalid upload token' }));
            return;
          }

          const payload = body ? JSON.parse(body) : {};
          const filename = String(payload.filename || '').trim();
          const content = String(payload.content || '').trim();
          const mimeType = String(payload.mimeType || '').trim();
          const webUrl = String(payload.webUrl || '').trim();
          const lastModified = String(payload.lastModified || new Date().toISOString()).trim();
          const groupId = String(payload.group || '').trim();
          const site = String(payload.site || '').trim();
          const providedText = String(payload.text || '');
          const requireText = !!payload.requireText;

          if (!filename || !content || !groupId) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'filename, content (base64), and group are required' }));
            return;
          }
          if (!config.groups.some(g => g.id === groupId)) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'invalid group' }));
            return;
          }

          const safeName = sanitizeFileName(filename);
          const extension = path.extname(filename || safeName).slice(1).toLowerCase();
          const tempPath = path.join(os.tmpdir(), `wmsiq-${Date.now()}-${Math.random().toString(36).slice(2, 8)}-${safeName}`);
          const binary = Buffer.from(content, 'base64');
          fs.writeFileSync(tempPath, binary);
          console.log(`[docs/upload] filename=${filename} extension=${extension} tempPath=${tempPath} exists=${fs.existsSync(tempPath)} bytes=${binary.length}`);

          let text = '';
          let extractionMode = 'provided-text';
          try {
            const normalizedExtension = String(extension || '').toLowerCase();
            const shouldExtractServerSide = ['docx', 'pdf', 'txt', 'md', 'xlsx'].includes(normalizedExtension);
            console.log(`[docs/upload] extraction dispatch extension=${normalizedExtension} serverSide=${shouldExtractServerSide}`);
            if (shouldExtractServerSide) {
              if (!fs.existsSync(tempPath)) {
                throw new Error('Temp file missing before extraction');
              }
              const extracted = normalizeExtractionResult(await extractFileText(tempPath, normalizedExtension), 'server-extract');
              text = extracted.text;
              extractionMode = extracted.mode;
              console.log(`[docs/upload] extraction complete mode=${extractionMode} chars=${text.length}`);
            } else if (providedText && providedText.trim()) {
              text = sanitizeStoredText(providedText);
              extractionMode = 'provided-text';
            }
          } finally {
            try { fs.unlinkSync(tempPath); } catch (_e) {}
          }

          if ((!text || !text.trim()) && requireText) {
            res.writeHead(422);
            res.end(JSON.stringify({ error: 'No extractable text for file', fileName: filename }));
            return;
          }

          if (!text || !text.trim()) {
            text = `indexed file ${safeName}`;
            extractionMode = 'placeholder';
          }

          const wordCount = text.trim().split(/\s+/).filter(Boolean).length;
          const ext = extension || path.extname(safeName).slice(1).toLowerCase();
          const { size: chunkSize, overlap: chunkOverlap } = getDocChunkParams(ext);
          const cleanedText = preprocessDocText(text);
          const title = filename.replace(/\.[^/.]+$/, '');
          const docContext = `[Document: ${title}]\n\n`;
          const rawChunks = chunkTextSentenceAware(cleanedText, chunkSize, chunkOverlap);
          const chunks = rawChunks.map(chunk => ({ ...chunk, text: docContext + chunk.text }));
          console.log(`[docs/upload] chunking extension=${ext} wordCount=${wordCount} chunkSize=${chunkSize} overlap=${chunkOverlap} chunkCount=${chunks.length}`);
          const outName = `${groupId}-${safeName}.json`;
          const outPath = path.join(DOCS_INDEX_DIR, outName);
          const sourcePath = String(payload.sourcePath || payload.originalPath || '').trim();
          const doc = {
            fileId: `${groupId}-${safeName}`,
            fileName: filename,
            webUrl,
            mimeType,
            extension: extension ? `.${extension}` : '',
            group: groupId,
            site,
            lastModified,
            syncedAt: new Date().toISOString(),
            sourcePath,
            title: filename.replace(/\.[^/.]+$/, ''),
            fullText: text,
            extractionMode,
            chunks,
            chunkCount: chunks.length,
            wordCount,
            byteSize: binary.length
          };
          writeValidatedJson(outPath, doc);

          res.end(JSON.stringify({ ok: true, fileName: filename, chunkCount: chunks.length, wordCount, extractionMode }));
        } catch (e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return true;
    }

    if (pathname === '/docs/status' && req.method === 'GET') {
      const files = fs.readdirSync(DOCS_INDEX_DIR).filter(f => f.endsWith('.json'));
      res.end(JSON.stringify({
        connected: true,
        mode: 'power-automate-upload',
        siteUrl: SP_SITE_URL,
        indexedFiles: files.length,
        lastSyncAt: null,
        syncInProgress: Array.from(syncJobs.values()).some(j => j.status === 'running')
      }));
      return true;
    }

    if (pathname === '/docs/list' && req.method === 'GET') {
      res.end(JSON.stringify(listIndexedDocs()));
      return true;
    }

    if (pathname === '/docs/search' && req.method === 'GET') {
      const q = query.q || '';
      const lim = parseInt(query.limit, 10) || 10;
      res.end(JSON.stringify(docsSearch(q, lim)));
      return true;
    }

    if (pathname === '/docs/validate' && req.method === 'GET') {
      const files = fs.readdirSync(DOCS_INDEX_DIR).filter(f => f.endsWith('.json'));
      let valid = 0;
      const invalid = [];
      for (const fileName of files) {
        const filePath = path.join(DOCS_INDEX_DIR, fileName);
        try {
          JSON.parse(fs.readFileSync(filePath, 'utf8'));
          valid++;
        } catch (_e) {
          invalid.push(fileName);
        }
      }
      res.end(JSON.stringify({ valid, invalid }));
      return true;
    }

    if (pathname === '/docs/remove' && req.method === 'DELETE') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => {
        try {
          const payload = body ? JSON.parse(body) : {};
          const filename = String(payload.filename || '').trim();
          const groupId = String(payload.group || '').trim();
          const site = String(payload.site || '').trim();
          if (!filename || !groupId) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'filename and group are required' }));
            return;
          }
          if (!config.groups.some(g => g.id === groupId)) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'invalid group' }));
            return;
          }
          const safeName = sanitizeFileName(filename);
          const outName = `${groupId}-${safeName}.json`;
          const outPath = path.join(DOCS_INDEX_DIR, outName);
          let deleted = false;
          if (fs.existsSync(outPath)) {
            fs.unlinkSync(outPath);
            deleted = true;
          }
          res.end(JSON.stringify({ ok: true, fileName: filename, group: groupId, site, deleted }));
        } catch (e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return true;
    }

    if (pathname === '/docs/index' && req.method === 'DELETE') {
      const fid = query.fileId;
      if (fid && deleteIndex(fid)) res.end(JSON.stringify({ ok: true, fileId: fid }));
      else {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'not found' }));
      }
      return true;
    }

    if (pathname === '/docs/reindex' && req.method === 'POST') {
      const files = fs.readdirSync(DOCS_INDEX_DIR).filter(f => f.endsWith('.json') && !f.startsWith('jira-'));
      let reindexed = 0;
      let skipped = 0;
      const errors = [];
      console.log(`[docs/reindex] starting ${files.length} files`);
      for (const f of files) {
        const filePath = path.join(DOCS_INDEX_DIR, f);
        try {
          const doc = JSON.parse(fs.readFileSync(filePath, 'utf8'));
          const ext = (doc.extension || '').replace(/^\./, '').toLowerCase();
          const sourcePath = String(doc.sourcePath || doc.originalPath || '').trim();
          let rawText = sanitizeStoredText(doc.fullText || '');
          let extractionMode = String(doc.extractionMode || 'provided-text');
          if (sourcePath && fs.existsSync(sourcePath)) {
            const extracted = normalizeExtractionResult(await extractFileText(sourcePath, ext), extractionMode);
            if (extracted.text.trim()) {
              rawText = extracted.text;
              extractionMode = extracted.mode;
            }
          }
          if (!rawText.trim()) {
            skipped++;
            console.log(`[docs/reindex] skipped ${f} (no text)`);
            continue;
          }
          const wordCount = rawText.trim().split(/\s+/).filter(Boolean).length;
          const { size: chunkSize, overlap: chunkOverlap } = getDocChunkParams(ext);
          const cleanedText = preprocessDocText(rawText);
          const title = doc.title || doc.fileName || f;
          const docContext = `[Document: ${title}]\n\n`;
          const rawChunks = chunkTextSentenceAware(cleanedText, chunkSize, chunkOverlap);
          const chunks = rawChunks.map(chunk => ({ ...chunk, text: docContext + chunk.text }));
          doc.chunks = chunks;
          doc.chunkCount = chunks.length;
          doc.wordCount = wordCount;
          doc.fullText = rawText;
          doc.extractionMode = extractionMode;
          doc.reindexedAt = new Date().toISOString();
          writeValidatedJson(filePath, doc);
          reindexed++;
          console.log(`[docs/reindex] reindexed ${f} chunks=${chunks.length}`);
        } catch (e) {
          errors.push({ file: f, error: e.message });
          console.log(`[docs/reindex] error ${f}: ${e.message}`);
        }
      }
      console.log(`[docs/reindex] complete reindexed=${reindexed} skipped=${skipped} errors=${errors.length}`);
      res.end(JSON.stringify({ reindexed, skipped, errors }));
      return true;
    }

    if (pathname === '/docs/sync-status' && req.method === 'GET') {
      const jid = query.jobId;
      const job = syncJobs.get(jid);
      if (!job) {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'unknown job' }));
        return true;
      }
      res.end(JSON.stringify({ jobId: jid, status: job.status, total: job.total, done: job.done, failed: job.failed, log: job.log }));
      return true;
    }

    if (pathname === '/docs/sync' && req.method === 'POST') {
      const authh = req.headers.authorization || '';
      const token = authh.startsWith('Bearer ') ? authh.slice(7).trim() : null;
      if (!token) {
        res.writeHead(401);
        res.end(JSON.stringify({ error: 'Token required' }));
        return true;
      }
      let body = '';
      req.on('data', c => body += c);
      req.on('end', async () => {
        let ids;
        try { ids = body ? JSON.parse(body).fileIds : null; } catch (_e) { ids = null; }
        const jobId = 'sync-' + Date.now();
        const job = { status: 'running', total: 0, done: 0, failed: 0, log: [] };
        syncJobs.set(jobId, job);
        res.end(JSON.stringify({ jobId, status: 'started', fileCount: ids ? ids.length : 0 }));
        (async () => {
          try {
            const log = msg => { job.log.push({ message: msg, ts: new Date().toISOString() }); };
            let files = [];
            if (ids && ids.length) files = ids;
            else {
              const siteId = await getSiteId(token);
              const j = await fetchGraph(token, `/sites/${siteId}/drive/root/children`);
              files = j.value.map(i => i.id);
            }
            job.total = files.length;
            const sem = { count: 0, queue: [] };
            const acquire = () => new Promise(r => { if (sem.count < DOCS_SYNC_CONCURRENCY) { sem.count++; r(); } else sem.queue.push(r); });
            const release = () => { sem.count--; if (sem.queue.length) sem.queue.shift()(); };
            for (const fid of files) {
              await acquire();
              (async fidInner => {
                try {
                  const siteId = await getSiteId(token);
                  const cont = await fetch(`https://graph.microsoft.com/v1.0/sites/${siteId}/drive/items/${fidInner}/content`, { headers: { Authorization: `Bearer ${token}` } });
                  const tmp = path.join(os.tmpdir(), fidInner);
                  const out = fs.createWriteStream(tmp);
                  await new Promise((resolve, reject) => { cont.body.pipe(out).on('finish', resolve).on('error', reject); });
                  const text = `indexed file ${fidInner}`;
                  const idx = { fileId: fidInner, fileName: fidInner, webUrl: '', mimeType: '', extension: '', lastModified: new Date().toISOString(), syncedAt: new Date().toISOString(), title: fidInner, fullText: text, chunks: [{ chunkIndex: 0, startChar: 0, text }], wordCount: text.split(/\s+/).length, chunkCount: 1 };
                  const fname = `${idx.fileId}-${Date.now()}.json`;
                  idx.fullText = sanitizeStoredText(idx.fullText);
                  writeValidatedJson(path.join(DOCS_INDEX_DIR, fname), idx);
                  job.done++;
                  log(`Indexed ${fidInner}`);
                } catch (e) {
                  job.failed++;
                  log(`fail ${fidInner}: ${e.message}`);
                }
                release();
              })(fid);
            }
            while (sem.count > 0) await sleep(200);
            job.status = 'complete';
          } catch (e) {
            job.status = 'error';
            job.log.push({ message: e.message });
          }
        })();
      });
      return true;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'not found' }));
    return true;
  };
};