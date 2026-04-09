module.exports = function createOllamaRoutes(deps) {
  const { OLLAMA, fetch } = deps;

  function extractResponseText(payload) {
    if (!payload || typeof payload !== 'object') return '';
    if (typeof payload.response === 'string') return payload.response;
    if (typeof payload.completion === 'string') return payload.completion;
    if (typeof payload?.message?.content === 'string') return payload.message.content;
    if (Array.isArray(payload?.choices) && typeof payload.choices[0]?.message?.content === 'string') {
      return payload.choices[0].message.content;
    }
    return '';
  }

  return async function handleOllamaRoute(req, res, pathname) {
    if (pathname === '/ollama/models' && req.method === 'GET') {
      const resp = await fetch(`${OLLAMA}/api/tags`);
      const text = await resp.text();
      const contentType = resp.headers.get('content-type') || 'application/json';
      res.writeHead(resp.status, { 'Content-Type': contentType });
      res.end(text);
      return true;
    }

    if (pathname === '/ollama/chat' && req.method === 'POST') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', async () => {
        try {
          let payload = {};
          if (body && body.trim()) {
            try {
              payload = JSON.parse(body);
            } catch (_e) {
              res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
              res.end(JSON.stringify({ error: 'Invalid JSON body' }));
              return;
            }
          }
          if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
            payload = {};
          }
          payload.stream = false;

          const resp = await fetch(`${OLLAMA}/api/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          const text = await resp.text();
          const parsed = (() => {
            try { return JSON.parse(text); } catch (_e) { return null; }
          })();

          if (!resp.ok) {
            const detail = typeof parsed?.error === 'string'
              ? parsed.error
              : text;
            res.writeHead(resp.status, { 'Content-Type': 'application/json; charset=utf-8' });
            res.end(JSON.stringify({ error: 'Ollama proxy error', detail }));
            return;
          }

          const responseText = extractResponseText(parsed);
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ response: responseText }));
        } catch (e) {
          res.writeHead(502, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ error: 'Ollama proxy error', detail: String(e && e.message ? e.message : e) }));
        }
      });
      return true;
    }

    return false;
  };
};