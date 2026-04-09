const fs = require('fs');
const path = require('path');

module.exports = function createJiraRoutes(deps) {
  const {
    atlassianConfigured,
    atlassianSearchIssuesDetailed,
    atlassianListProjectsDetailed,
    atlassianGetIssue,
    jqlEscape,
    getAtlassianProjectKeys,
    config,
    DOCS_INDEX_DIR,
    UPLOAD_TOKEN,
    sanitizeFileName,
    chunkTextSentenceAware
  } = deps;

  const DEFAULT_STOP_WORDS = [
    'are','there','any','is','the','a','an','in','on',
    'at','to','for','of','and','or','what','how','why',
    'when','where','who','which','be','been','being',
    'have','has','had','do','does','did','will','would',
    'could','should','may','might','can','with','about',
    'issues','issue','related','problem','problems',
    'error','errors','please','help','find','show','me',
    'tell','give','list','get','check','look',
    'solving','solve','solution','fixing','fix','fixed',
    'addressing','address','handling','handle','handled',
    'finding','searching','search','looking',
    'need','needs','needed','using','use','used','making',
    'make','getting','trying','try','working','work',
    'many','were',
    'assigned','assignment','assignee','owner','owned',
    'group','last','latest','top','hour','hours','hr','hrs'
  ];

  const configuredStopWords = Array.isArray(config?.bridge?.jiraStopWords)
    ? config.bridge.jiraStopWords
    : DEFAULT_STOP_WORDS;
  const STOP_WORDS = new Set(configuredStopWords.map(w => String(w || '').trim().toLowerCase()).filter(Boolean));
  const JIRA_MAX_TERMS = Math.max(1, Number(config?.bridge?.jiraMaxTerms) || 4);
  const JIRA_MAX_RESULTS = Math.max(1, Number(config?.bridge?.jiraMaxResults) || 10);

  const TERM_ALIASES = {
    wmshub: ['wmshub', 'ems'],
    ems: ['ems', 'wmshub'],
    manhattan: ['manhattan', 'manh'],
    bluegrass: ['bluegrass', 'wk'],
    ck: ['ck', 'otsego', 'columbus', 'st louis'],
    opcms: ['opcms', 'op cms', 'opcigwms', 'opcig', 'op cigwms', 'op cig', 'cms', 'cigwms', 'cig management system', 'order processing'],
    opcigwms: ['opcms', 'op cms', 'opcigwms', 'opcig', 'op cigwms', 'op cig', 'cms', 'cigwms', 'cig management system', 'order processing'],
    opcig: ['opcms', 'op cms', 'opcigwms', 'opcig', 'op cigwms', 'op cig', 'cms', 'cigwms', 'cig management system', 'order processing'],
    cigwms: ['opcms', 'op cms', 'opcigwms', 'opcig', 'op cigwms', 'op cig', 'cms', 'cigwms', 'cig management system', 'order processing'],
    cms: ['opcms', 'op cms', 'opcigwms', 'opcig', 'op cigwms', 'op cig', 'cms', 'cigwms', 'cig management system', 'order processing']
  };

  const RESPONSIBLE_TEAM_FIELD = '"Responsible Team[User Picker (single user)]"';

  const RESPONSIBLE_TEAM_IDS = {
    wmshub: '712020:044f7a5f-a66b-46cb-99fd-f451b5845379',
    opcms: '712020:910e4d3a-947f-4e35-b1ea-e9354466e8a2'
  };

  function isOpcmsFamilyQuery(queryText) {
    const q = String(queryText || '').toLowerCase();
    const compact = q.replace(/[^a-z0-9]+/g, '');
    if (
      compact.includes('opcms') ||
      compact.includes('opcigwms') ||
      compact.includes('opcig') ||
      compact.includes('cigwms') ||
      compact.includes('cigmanagementsystem') ||
      compact.includes('orderprocessing')
    ) {
      return true;
    }
    return /\bop\s*cms\b|\bop\s*cig\s*wms\b|\bcig\s*management\s*system\b|\border\s*processing\b/.test(q);
  }

  // Issue types that are project-management containers, not operational incidents.
  // Applied by default, but disabled when user explicitly asks for stories/epics/tasks.
  const ISSUE_TYPE_EXCLUSION = 'issueType NOT IN (Story, Epic, Project, Initiative, "Sub-task", "Technical task")';
  const PROJECT_ITEM_INCLUSION = 'issueType IN (Story, Epic, Task, "Sub-task", "Technical task", Bug, Improvement, Spike)';

  function wantsProjectItems(queryText) {
    const q = String(queryText || '').toLowerCase();
    return /\b(story|stories|epic|epics|task|tasks|project\s*container|project\s*item|project\s*items)\b/.test(q);
  }

  function extractMeaningfulTerms(rawQuery) {
    const queryText = String(rawQuery || '').trim();
    if (!queryText) return [];
    const exactIssueKeys = queryText.toUpperCase().match(/\b[A-Z][A-Z0-9]+-\d+\b/g) || [];
    if (exactIssueKeys.length) {
      return [...new Set(exactIssueKeys)].slice(0, JIRA_MAX_TERMS);
    }
    if (isOpcmsFamilyQuery(queryText)) {
      return ['opcms'];
    }
    const rawTerms = queryText.match(/[A-Za-z0-9_\-]+/g) || [];
    const terms = [];
    const seen = new Set();
    for (const token of rawTerms) {
      const term = String(token || '').trim();
      const termLower = term.toLowerCase();
      // Never strip codes that look like extension or ticket IDs.
      if (/^[A-Z]{1,4}[-_]?\d+$/i.test(term)) {
        if (seen.has(termLower)) continue;
        seen.add(termLower);
        terms.push(term);
        if (terms.length >= JIRA_MAX_TERMS) break;
        continue;
      }
      if (term.length < 3) continue;
      if (STOP_WORDS.has(termLower)) continue;
      if (seen.has(termLower)) continue;
      seen.add(termLower);
      terms.push(term);
      if (terms.length >= JIRA_MAX_TERMS) break;
    }
    return terms;
  }

  function buildTermClause(term) {
    const key = String(term || '').toLowerCase();
    const aliases = TERM_ALIASES[key];
    if (!Array.isArray(aliases) || !aliases.length) {
      return `textfields ~ "${jqlEscape(term)}*"`;
    }
    const parts = aliases
      .map(a => String(a || '').trim())
      .filter(Boolean)
      .map(a => `textfields ~ "${jqlEscape(a)}*"`);
    if (!parts.length) {
      return `textfields ~ "${jqlEscape(term)}*"`;
    }
    return parts.length === 1 ? parts[0] : `(${parts.join(' OR ')})`;
  }

  function looksLikeJql(queryText) {
    const q = String(queryText || '').trim();
    if (!q) return false;
    if (/\bORDER\s+BY\b/i.test(q)) return true;
    if (/\b(AND|OR|NOT)\b/i.test(q) && /[=<>]/.test(q)) return true;
    if (/\b(?:project|status|resolution|created|updated|labels|assignee|category)\b\s*(?:=|!=|~|IN\s*\()/i.test(q)) return true;
    return false;
  }

  function inferCreatedDateClause(queryText) {
    const q = String(queryText || '').toLowerCase();
    if (!q) return '';

    if (/\btoday\b/.test(q)) return 'created >= startOfDay()';
    if (/\bthis\s+morning\b/.test(q)) return 'created >= startOfDay()';
    if (/\bthis\s+week\b/.test(q)) return 'created >= startOfWeek()';
    if (/\bthis\s+month\b/.test(q)) return 'created >= startOfMonth()';
    if (/\bthis\s+year\b/.test(q)) return 'created >= startOfYear()';

    const rel = q.match(/\b(?:last|past)\s+(\d{1,4})\s*(day|days|week|weeks|month|months|year|years)\b/);
    if (!rel) return '';

    const amount = Math.max(1, Math.min(3650, parseInt(rel[1], 10) || 0));
    const unit = rel[2];
    let days = amount;
    if (unit.startsWith('week')) days = amount * 7;
    if (unit.startsWith('month')) days = amount * 30;
    if (unit.startsWith('year')) days = amount * 365;
    return `created >= -${days}d`;
  }

  function inferUpdatedDateClause(queryText) {
    const q = String(queryText || '').toLowerCase();
    if (!q) return '';

    if (/\bthis\s+morning\b/.test(q) || /\btoday\b/.test(q)) {
      return 'updated >= startOfDay()';
    }

    const hourRel = q.match(/\b(?:last|past)\s+(\d{1,3})\s*(hour|hours|hr|hrs)\b/);
    if (hourRel) {
      const hours = Math.max(1, Math.min(720, parseInt(hourRel[1], 10) || 0));
      return `updated >= -${hours}h`;
    }

    const dayRel = q.match(/\b(?:last|past)\s+(\d{1,4})\s*(day|days|week|weeks|month|months|year|years)\b/);
    if (!dayRel) return '';

    const amount = Math.max(1, Math.min(3650, parseInt(dayRel[1], 10) || 0));
    const unit = dayRel[2];
    let days = amount;
    if (unit.startsWith('week')) days = amount * 7;
    if (unit.startsWith('month')) days = amount * 30;
    if (unit.startsWith('year')) days = amount * 365;
    return `updated >= -${days}d`;
  }

  function inferJiraFilters(queryText) {
    const q = String(queryText || '').toLowerCase();
    const safeClauses = [];
    const optionalClauses = [];
    const projectIntent = wantsProjectItems(q);
    const hasIssueIntent = /\b(ticket|tickets|issue|issues|incident|incidents|problem|problems|failure|failures|failed|failing|resolution|resolved|alert|job|jobs|control-?m)\b/.test(q);
    const hasOwnershipIntent = /\b(assigned|assignee|owner|responsible\s+team|team|addressed\s+by|handled\s+by|worked\s+by|group)\b/.test(q);

    const createdClause = inferCreatedDateClause(q);
    const updatedClause = inferUpdatedDateClause(q);
    if (createdClause) safeClauses.push(createdClause);
    if (updatedClause) safeClauses.push(updatedClause);

    // Default behavior: exclude planning artifacts for operational issue searches.
    // If the user explicitly asks for stories/epics/tasks, include project item types instead.
    if (projectIntent) {
      safeClauses.push(PROJECT_ITEM_INCLUSION);
    } else {
      safeClauses.push(ISSUE_TYPE_EXCLUSION);
    }

    if (/\b(service\s*desk|help\s*desk|support\s*ticket|incident)\b/.test(q)) {
      safeClauses.push('category = "Service Desk"');
    }

    const unresolvedRequested = /\b(unresolved|open|pending|not\s+resolved|not\s+closed)\b/.test(q);
    const resolutionRequested = /\bresolution\b/.test(q);
    const resolvedRequested = (/\b(resolved|closed|done|completed)\b/.test(q) || resolutionRequested) && !unresolvedRequested;

    if (unresolvedRequested) {
      safeClauses.push('resolution = Unresolved');
    }
    if (resolvedRequested) {
      safeClauses.push('status = Resolved');
    }

    if (/\b(exclude\s+time\s+entry|without\s+time\s+entry|not\s+time\s+entry)\b/.test(q) ||
        /\bwmshub[_\s-]*time[_\s-]*entry[_\s-]*only\b/.test(q)) {
      safeClauses.push('(labels != WMSHUB_TIME_ENTRY_ONLY OR labels IS EMPTY)');
    }

    if (hasOwnershipIntent) {
      if (/\bwmshub\b|\bems\b/.test(q) && hasIssueIntent) {
        // Try Responsible Team field first (most precise); text fallback is handled via candidate ordering
        safeClauses.push('category = "Service Desk"');
        optionalClauses.push(`${RESPONSIBLE_TEAM_FIELD} = ${RESPONSIBLE_TEAM_IDS.wmshub}`);
      }
      if (isOpcmsFamilyQuery(q) && hasIssueIntent) {
        safeClauses.push('category = "Service Desk"');
        optionalClauses.push(`${RESPONSIBLE_TEAM_FIELD} = ${RESPONSIBLE_TEAM_IDS.opcms}`);
      }
    }

    // Detect recency keywords: "last issue", "latest ticket", "last ticket for X", etc.
    const hasRecencyKeyword = /\b(last|latest)\b/.test(q);
    const hasTicketOrIssueKeyword = /\b(ticket|issue|incident|problem)\b/.test(q);
    
    // Add default 90-day window if:
    // 1. Asking for "last/latest" + ownership (e.g., "last issue assigned to wmshub")
    // 2. Asking for "last/latest" + ticket/issue keyword (e.g., "last ticket for MD")
    if (hasRecencyKeyword && !createdClause && (hasOwnershipIntent || hasTicketOrIssueKeyword)) {
      safeClauses.push('updated >= -90d');
    }

    // Use "updated DESC" for recency, ensures most recently touched issues appear first
    const orderBy = /\b(created|newly\s+created|opened\s+recently)\b/.test(q)
      ? 'created DESC'
      : 'updated DESC';

    return { safeClauses, optionalClauses, orderBy };
  }

  function extractProjectKeyHints(queryText) {
    const input = String(queryText || '');
    const upper = input.toUpperCase();
    const hints = new Set();
    const issueKeys = upper.match(/\b([A-Z][A-Z0-9]{1,9})-\d+\b/g) || [];
    issueKeys.forEach(v => hints.add(String(v).split('-')[0]));

    const storyStyle = [...input.matchAll(/\b([A-Za-z][A-Za-z0-9]{1,9})\s+(?:stories|story|epics|epic|tasks|task)\b/gi)]
      .map(m => String(m[1] || '').toUpperCase());
    const projectStyle = [...input.matchAll(/\b(?:project\s+)?([A-Za-z][A-Za-z0-9]{1,9})\s+project\b/gi)]
      .map(m => String(m[1] || '').toUpperCase());

    const blocked = new Set(['WHAT','WHICH','SHOW','LIST','OPEN','TOP','GROUP','TEAM']);
    [...storyStyle, ...projectStyle]
      .filter(Boolean)
      .filter(v => !blocked.has(v))
      .forEach(v => hints.add(v));
    return [...hints].slice(0, JIRA_MAX_TERMS);
  }

  function buildSearchJqlCandidates(rawQuery) {
    const queryText = String(rawQuery || '').trim();
    if (!queryText) return [];
    const meaningfulTerms = extractMeaningfulTerms(queryText);
    const explicitIssueKeys = queryText.toUpperCase().match(/\b[A-Z][A-Z0-9]+-\d+\b/g) || [];
    const projectKeyHints = extractProjectKeyHints(queryText);
    const projectIntent = wantsProjectItems(queryText);

    const projectKeys = typeof getAtlassianProjectKeys === 'function'
      ? getAtlassianProjectKeys()
      : [];
    const keyFilter = Array.isArray(projectKeys) && projectKeys.length
      ? `project IN (${projectKeys.join(',')}) AND `
      : '';

    let termClause = '';
    if (meaningfulTerms.length >= 1) {
      termClause = meaningfulTerms
        .map(buildTermClause)
        .join(' AND ');
    } else {
      const phrase = queryText.replace(/\s+/g, ' ').replace(/[?!.,;:]+$/g, '').trim();
      const escapedPhrase = jqlEscape(phrase || queryText);
      termClause = `textfields ~ "${escapedPhrase}*"`;
    }

    const { safeClauses, optionalClauses, orderBy } = inferJiraFilters(queryText);
    const lowerQ = queryText.toLowerCase();
    const hasOwnershipIntent = /\b(assigned|assignee|owner|responsible\s+team|team|group|handled\s+by|worked\s+by)\b/.test(lowerQ);
    const hasNamedTarget = /\b(opcms|wmshub|ems|manhattan|bluegrass|ck|otsego|columbus|st\s*louis)\b/.test(lowerQ)
      || isOpcmsFamilyQuery(lowerQ);
    const strictOwnershipQuery = hasOwnershipIntent && hasNamedTarget;
    const candidates = [];
    const seen = new Set();

    function addCandidate(jql) {
      const value = String(jql || '').trim();
      if (!value || seen.has(value)) return;
      seen.add(value);
      candidates.push(value);
    }

    if (explicitIssueKeys.length) {
      addCandidate(`${keyFilter}issueKey IN (${[...new Set(explicitIssueKeys)].join(',')}) ORDER BY updated DESC`);
    }

    if (projectIntent) {
      const mentionedProjects = [...new Set(projectKeyHints.filter(token => /^[A-Z][A-Z0-9]{1,9}$/.test(token)))];
      const openRequested = /\b(open|opened|unresolved|active|in progress|requested|to do)\b/i.test(queryText);
      const itemClauses = [];
      if (/\bstor(y|ies)\b/i.test(queryText)) itemClauses.push('issueType = Story');
      if (/\bepic(s)?\b/i.test(queryText)) itemClauses.push('issueType = Epic');
      if (/\btask(s)?\b/i.test(queryText)) itemClauses.push('issueType IN (Task, "Sub-task", "Technical task", Bug, Improvement, Spike)');
      const itemClause = itemClauses.length ? `(${itemClauses.join(' OR ')})` : PROJECT_ITEM_INCLUSION;
      const statusClause = openRequested ? 'statusCategory != Done' : '';
      mentionedProjects.slice(0, JIRA_MAX_TERMS).forEach(projectKey => {
        const clauses = [`project = ${projectKey}`, itemClause].concat(statusClause ? [statusClause] : []);
        addCandidate(`${clauses.join(' AND ')} ORDER BY updated DESC`);
      });
    }

    if (looksLikeJql(queryText)) {
      const hasOrderBy = /\bORDER\s+BY\b/i.test(queryText);
      const direct = `${keyFilter}${queryText}${hasOrderBy ? '' : ' ORDER BY updated DESC'}`;
      addCandidate(direct);
    }

    if (safeClauses.length || optionalClauses.length) {
      const withOptional = [...safeClauses, ...optionalClauses];
      if (withOptional.length) {
        if (strictOwnershipQuery) {
          // For ownership/team questions, exact team/category/time filters should lead,
          // otherwise term matching can undercount valid tickets.
          addCandidate(`${keyFilter}${withOptional.join(' AND ')} ORDER BY ${orderBy}`);
          addCandidate(`${keyFilter}${withOptional.join(' AND ')} AND ${termClause} ORDER BY ${orderBy}`);
        } else {
          addCandidate(`${keyFilter}${withOptional.join(' AND ')} AND ${termClause} ORDER BY ${orderBy}`);
          addCandidate(`${keyFilter}${withOptional.join(' AND ')} ORDER BY ${orderBy}`);
        }
      }
      if (safeClauses.length && !strictOwnershipQuery) {
        addCandidate(`${keyFilter}${safeClauses.join(' AND ')} AND ${termClause} ORDER BY ${orderBy}`);
        addCandidate(`${keyFilter}${safeClauses.join(' AND ')} ORDER BY ${orderBy}`);
      }
    }

    // For strict ownership queries, never relax to broad text-only fallbacks,
    // otherwise unrelated recent tickets can leak in.
    if (!strictOwnershipQuery) {
      addCandidate(`${keyFilter}${ISSUE_TYPE_EXCLUSION} AND ${termClause} ORDER BY updated DESC`);
      addCandidate(`${keyFilter}${termClause} ORDER BY updated DESC`);
    }

    return candidates;
  }

  function parseJsonBody(req) {
    return new Promise(resolve => {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => {
        try {
          resolve(body ? JSON.parse(body) : {});
        } catch (_e) {
          resolve({});
        }
      });
      req.on('error', () => resolve({}));
    });
  }

  function getUploadToken(req, query) {
    const authh = req.headers.authorization || '';
    const bearer = authh.startsWith('Bearer ') ? authh.slice(7).trim() : '';
    const headerToken = String(req.headers['x-upload-token'] || '');
    const queryToken = String((query && query.token) || '');
    return headerToken || bearer || queryToken;
  }

  function isResolvedOrClosed(status) {
    const s = String(status || '').trim().toLowerCase();
    return s === 'resolved' || s === 'closed';
  }

  function asTextArray(value) {
    if (!value) return [];
    if (Array.isArray(value)) return value.map(v => String(v || '').trim()).filter(Boolean);
    return String(value || '')
      .split(',')
      .map(v => v.trim())
      .filter(Boolean);
  }

  function buildTicketText(ticket) {
    const comments = Array.isArray(ticket.comments) ? ticket.comments : [];
    const commentText = comments
      .map(c => {
        if (typeof c === 'string') return c.trim();
        if (!c || typeof c !== 'object') return '';
        const who = String(c.author || c.displayName || '').trim();
        const body = String(c.body || c.text || '').trim();
        const created = String(c.created || c.updated || '').trim();
        const prefix = [who, created].filter(Boolean).join(' · ');
        return [prefix, body].filter(Boolean).join('\n');
      })
      .filter(Boolean)
      .join('\n\n');

    const labels = asTextArray(ticket.labels).join(', ');
    const components = asTextArray(ticket.components).join(', ');

    const sections = [
      `JIRA Key: ${ticket.key || ''}`,
      `Summary: ${ticket.summary || ''}`,
      `Status: ${ticket.status || ''}`,
      `Resolution: ${ticket.resolution || ''}`,
      `Project: ${ticket.project || ''}`,
      `Type: ${ticket.type || ''}`,
      `Priority: ${ticket.priority || ''}`,
      `Assignee: ${ticket.assignee || ''}`,
      `Reporter: ${ticket.reporter || ''}`,
      `Updated: ${ticket.updated || ''}`,
      `URL: ${ticket.url || ''}`,
      labels ? `Labels: ${labels}` : '',
      components ? `Components: ${components}` : '',
      '',
      'Description:',
      String(ticket.description || '').trim(),
      '',
      'Comments:',
      commentText || '(no comments)'
    ].filter(line => line !== '');

    return sections.join('\n');
  }

  return async function handleJiraRoute(req, res, pathname, query) {
    if (pathname === '/jira/upload-token' && req.method === 'GET') {
      res.end(JSON.stringify({ token: UPLOAD_TOKEN || '' }));
      return true;
    }

    if (pathname === '/jira/upload' && req.method === 'POST') {
      const suppliedToken = getUploadToken(req, query);
      if (UPLOAD_TOKEN && suppliedToken !== UPLOAD_TOKEN) {
        res.writeHead(401);
        res.end(JSON.stringify({ error: 'Invalid upload token' }));
        return true;
      }

      const payload = await parseJsonBody(req);
      const ticket = payload.ticket && typeof payload.ticket === 'object'
        ? payload.ticket
        : payload;

      const key = String(ticket.key || ticket.issueKey || '').trim().toUpperCase();
      const summary = String(ticket.summary || '').trim();
      const status = String(ticket.status || '').trim();
      if (!key || !summary || !status) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'key, summary, and status are required' }));
        return true;
      }

      if (!isResolvedOrClosed(status)) {
        res.end(JSON.stringify({
          ok: true,
          ingested: false,
          reason: 'status-not-resolved-or-closed',
          status
        }));
        return true;
      }

      const groupId = String(payload.group || ticket.group || ((config.groups || [])[0]?.id || 'manhattan-main')).trim();
      const safeKey = sanitizeFileName ? sanitizeFileName(key) : key.replace(/[^A-Z0-9_-]/gi, '_');
      const fileId = `jira-${safeKey}`;
      const filename = `${fileId}.json`;

      const normalizedTicket = {
        key,
        summary,
        status,
        resolution: String(ticket.resolution || '').trim(),
        project: String(ticket.project || '').trim(),
        type: String(ticket.type || '').trim(),
        priority: String(ticket.priority || '').trim(),
        assignee: String(ticket.assignee || '').trim(),
        reporter: String(ticket.reporter || '').trim(),
        updated: String(ticket.updated || new Date().toISOString()).trim(),
        url: String(ticket.url || '').trim(),
        description: String(ticket.description || '').trim(),
        labels: asTextArray(ticket.labels),
        components: asTextArray(ticket.components),
        comments: Array.isArray(ticket.comments) ? ticket.comments : []
      };

      const fullText = buildTicketText(normalizedTicket);
      const chunker = typeof chunkTextSentenceAware === 'function'
        ? chunkTextSentenceAware
        : (text => [{ chunkIndex: 0, startChar: 0, text }]);
      const chunks = chunker(fullText, 800, 100);
      const wordCount = fullText.trim().split(/\s+/).filter(Boolean).length;

      const doc = {
        fileId,
        fileName: `${key}.txt`,
        webUrl: normalizedTicket.url,
        mimeType: 'text/plain',
        extension: '.txt',
        group: groupId,
        site: 'jira',
        lastModified: normalizedTicket.updated,
        syncedAt: new Date().toISOString(),
        title: `[${key}] ${summary}`,
        source: 'jira-power-automate',
        jira: normalizedTicket,
        fullText,
        chunks,
        chunkCount: chunks.length,
        wordCount,
        byteSize: Buffer.byteLength(fullText, 'utf8')
      };

      fs.mkdirSync(DOCS_INDEX_DIR, { recursive: true });
      fs.writeFileSync(path.join(DOCS_INDEX_DIR, filename), JSON.stringify(doc, null, 2));

      res.end(JSON.stringify({
        ok: true,
        ingested: true,
        key,
        status,
        fileId,
        chunkCount: chunks.length,
        wordCount
      }));
      return true;
    }

    if (pathname === '/jira/search' && req.method === 'GET') {
      const qRaw = String(query.q || '');
      let q = qRaw;
      try {
        q = decodeURIComponent(qRaw.replace(/\+/g, ' '));
      } catch (_e) {
        q = qRaw.replace(/\+/g, ' ');
      }
      q = String(q || '').trim();
      const maxResults = Math.max(1, Math.min(50, parseInt(query.maxResults, 10) || JIRA_MAX_RESULTS));
      if (!atlassianConfigured() || !q) {
        res.end(JSON.stringify({ issues: [] }));
        return true;
      }
      const jqlCandidates = buildSearchJqlCandidates(q);
      const searchResult = await atlassianSearchIssuesDetailed(q, maxResults, {
        jqlCandidates
      });
      const issues = searchResult.issues || [];
      res.end(JSON.stringify({ issues: issues.map(i => ({
        key: i.key,
        summary: i.summary,
        type: i.type,
        status: i.status,
        resolution: i.resolution,
        priority: i.priority,
        assignee: i.assignee,
        updated: i.updated,
        url: i.url,
        description: i.description
      })) }));
      return true;
    }

    if (pathname === '/jira/projects' && req.method === 'GET') {
      if (!atlassianConfigured()) {
        res.end(JSON.stringify({ projects: [] }));
        return true;
      }
      const projectsResult = await atlassianListProjectsDetailed();
      const projects = projectsResult.projects || [];
      res.end(JSON.stringify({ projects }));
      return true;
    }

    if (pathname.startsWith('/jira/issue/') && req.method === 'GET') {
      const issueKey = decodeURIComponent(pathname.slice('/jira/issue/'.length)).trim().toUpperCase();
      if (!issueKey || !/^[A-Z][A-Z0-9]+-\d+$/.test(issueKey)) {
        res.writeHead(404);
        res.end(JSON.stringify({ issue: null }));
        return true;
      }
      if (!atlassianConfigured()) {
        res.writeHead(404);
        res.end(JSON.stringify({ issue: null }));
        return true;
      }
      const issue = await atlassianGetIssue(issueKey);
      if (!issue) {
        res.writeHead(404);
        res.end(JSON.stringify({ issue: null }));
        return true;
      }
      res.end(JSON.stringify({ issue }));
      return true;
    }

    return false;
  };
};