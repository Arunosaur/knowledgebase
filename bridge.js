#!/usr/bin/env node
// bridge.js - Oracle Knowledge Base bridge using SQLCL MCP
// zero npm dependencies (built-in modules only)

const http = require('http');
const https = require('https');
const { spawn, spawnSync } = require('child_process');
const url = require('url');
const fs = require('fs');
const path = require('path');
const os = require('os');
const readline = require('readline');
const mammoth = require('mammoth');
const createMcpPool = require('./lib/mcp-pool');
const createDbRoutes = require('./lib/db-routes');
const createDocsRoutes = require('./lib/docs-routes');
const createKnowledgeRoutes = require('./lib/knowledge-routes');
const createJiraRoutes = require('./lib/jira-routes');
const createOllamaRoutes = require('./lib/ollama-routes');
const createSemanticRoutes = require('./lib/semantic-routes');
const graphStore = require('./lib/graph-store');

process.on('unhandledRejection', (reason) => {
  console.error('[BRIDGE] Unhandled rejection:', reason);
});

// load config.json from same directory
const cfgPath = path.join(__dirname, 'config.json');
let config;
try {
  config = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
} catch (e) {
  console.error('Failed to read config.json:', e.message);
  process.exit(1);
}

const PORT = (config.bridge && config.bridge.port) || 3333;
const OLLAMA = (config.bridge && config.bridge.ollamaUrl) || 'http://localhost:11434';
const DEFAULT_MODEL = (config.bridge && config.bridge.defaultModel) || 'llama3';
const SQLCL_CMD = (config.bridge && config.bridge.sqlclCommand) || 'sql';
const SQLCL_ARGS = (config.bridge && config.bridge.sqlclArgs) || ['-R','2','-mcp'];
const MCP_CLIENT = (config.bridge && config.bridge.mcpClientName) || 'oracle-kb-bridge';
const MCP_MODEL  = (config.bridge && config.bridge.mcpModelName) || 'bridge';

const debug = process.argv.includes('--debug');
let lastRequest = null;

graphStore.setConfig({
  postgresUrl: config.bridge && config.bridge.postgresUrl,
  ollamaUrl: OLLAMA
});

// security settings from config
const ALLOWED_ORIGINS = (config.bridge && config.bridge.allowedOrigins) || [];
const AUTH_TOKEN = (config.bridge && config.bridge.authToken) || '';
const RATE_LIMIT = (config.bridge && config.bridge.rateLimitPerMinute) || 120;

// sharepoint / docs index configuration
const MSAL_CLIENT_ID = (config.bridge && config.bridge.msalClientId) || '';
const MSAL_TENANT_ID = (config.bridge && config.bridge.msalTenantId) || 'common';
const SP_SITE_URL   = (config.bridge && config.bridge.sharepointSiteUrl) || '';
const DOCS_INDEX_DIR = (config.bridge && config.bridge.docsIndexDir) || './docs-index';
const KNOWLEDGE_INDEX_DIR = (config.bridge && config.bridge.knowledgeIndexDir) || './knowledge-index';
const WHISPER_MODEL = (config.bridge && config.bridge.whisperModel) || 'base';
const DOCS_SYNC_CONCURRENCY = (config.bridge && config.bridge.docsSyncConcurrency) || 2;
const UPLOAD_TOKEN = (config.bridge && config.bridge.uploadToken) || '';
const SEMANTIC_WORKER_PORT = Number((config.bridge && config.bridge.semanticWorkerPort) || process.env.SEMANTIC_PORT || 3334);
const SEMANTIC_WORKER_HOST = (config.bridge && config.bridge.semanticWorkerHost) || '127.0.0.1';
const SEMANTIC_WORKER_URL = (config.bridge && config.bridge.semanticWorkerUrl) || `http://${SEMANTIC_WORKER_HOST}:${SEMANTIC_WORKER_PORT}`;
const SEMANTIC_INDEX_DIR = (config.bridge && config.bridge.semanticIndexDir) || './semantic-index';
const DEFAULT_CHUNK_SIZE = Number((config.bridge && config.bridge.defaultChunkSize) || 800);
const DEFAULT_CHUNK_OVERLAP = Number((config.bridge && config.bridge.defaultChunkOverlap) || 100);
const DOCS_CHUNK_SIZE = Number((config.bridge && config.bridge.docsChunkSize) || 2500);
const DOCS_CHUNK_OVERLAP = Number((config.bridge && config.bridge.docsChunkOverlap) || 300);

function listNonLoopbackIPv4() {
  const out = [];
  const interfaces = os.networkInterfaces();
  for (const iface of Object.keys(interfaces || {})) {
    const entries = interfaces[iface] || [];
    for (const entry of entries) {
      const isIPv4 = typeof entry.family === 'string' ? entry.family === 'IPv4' : entry.family === 4;
      if (isIPv4 && !entry.internal) out.push(entry.address);
    }
  }
  return out;
}

function isVmRangeIp(ip) {
  return ip.startsWith('10.211.55.') || ip.startsWith('10.37.129.');
}

function getPreferredIPv4() {
  const ips = listNonLoopbackIPv4();
  const ten = ips.find(ip => ip.startsWith('10.') && !isVmRangeIp(ip));
  if (ten) return ten;
  const oneSevenTwo = ips.find(ip => ip.startsWith('172.'));
  if (oneSevenTwo) return oneSevenTwo;
  const oneNineTwo = ips.find(ip => ip.startsWith('192.168.'));
  if (oneNineTwo) return oneNineTwo;
  return '127.0.0.1';
}

// ensure docs index directory exists
try { fs.mkdirSync(DOCS_INDEX_DIR, { recursive: true }); } catch(e) { console.error('could not create docs index dir', e.message); }
try { fs.mkdirSync(KNOWLEDGE_INDEX_DIR, { recursive: true }); } catch(e) { console.error('could not create knowledge index dir', e.message); }
try { fs.mkdirSync(SEMANTIC_INDEX_DIR, { recursive: true }); } catch(e) { console.error('could not create semantic index dir', e.message); }
try {
  const intentsPath = path.join(SEMANTIC_INDEX_DIR, 'intents.json');
  if (!fs.existsSync(intentsPath)) fs.writeFileSync(intentsPath, '[]\n', 'utf8');
} catch(e) { console.error('could not initialize semantic intents file', e.message); }

// in‑memory sync job tracker
const syncJobs = new Map();

let ATLASSIAN_ENABLED = !!(config.bridge && config.bridge.atlassianEnabled);
let ATLASSIAN_PROJECT_KEYS = Array.isArray(config.bridge && config.bridge.atlassianProjectKeys)
  ? config.bridge.atlassianProjectKeys.map(k => String(k || '').trim().toUpperCase()).filter(Boolean)
  : [];
let ATLASSIAN_EMAIL = String((config.bridge && config.bridge.atlassianEmail) || '').trim();
let ATLASSIAN_DOMAIN = String((config.bridge && config.bridge.atlassianDomain) || '').trim().replace(/^https?:\/\//i, '').replace(/\/$/, '');
let ATLASSIAN_TOKEN = String((config.bridge && config.bridge.atlassianToken) || '').trim();
let atlassianHealthCache = { ok: false, checkedAt: 0 };

// cache for siteId derived from SP_SITE_URL
let cachedSiteId = null;

async function fetchGraph(token, path) {
  const r = await fetch('https://graph.microsoft.com/v1.0' + path, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!r.ok) throw new Error(`Graph API ${path} failed: ${r.status}`);
  return r.json();
}

async function getSiteId(token) {
  if (cachedSiteId) return cachedSiteId;
  if (!SP_SITE_URL) throw new Error('sharepointSiteUrl not configured');
  const u = new URL(SP_SITE_URL);
  const hostname = u.hostname;
  const path = u.pathname.replace(/\/$/, '');
  try {
    const j = await fetchGraph(token, `/sites/${hostname}:${path}`);
    cachedSiteId = j.id;
    return cachedSiteId;
  } catch(e) {
    // fallback search
    const esc = encodeURIComponent(SP_SITE_URL);
    const j = await fetchGraph(token, `/sites?search=${esc}`);
    if (j.value && j.value[0] && j.value[0].id) {
      cachedSiteId = j.value[0].id;
      return cachedSiteId;
    }
    throw e;
  }
}

// simple indexing search: scan JSON files in DOCS_INDEX_DIR
function docsSearch(query, limit=5) {
  const terms = query.trim().toLowerCase().split(/\s+/);
  const results = [];
  const files = fs.readdirSync(DOCS_INDEX_DIR).filter(f=>f.endsWith('.json'));
  for (const f of files) {
    try {
      const data = JSON.parse(fs.readFileSync(path.join(DOCS_INDEX_DIR,f),'utf8'));
      for (let i=0;i<data.chunks.length;i++){
        const txt = data.chunks[i].text.toLowerCase();
        let score = 0;
        terms.forEach(t=>{ if(txt.includes(t)) score++; });
        if(score>0){
          results.push({fileId:data.fileId,fileName:data.fileName,webUrl:data.webUrl,chunkIndex:i,text:data.chunks[i].text,score});
        }
      }
    } catch(e){}
  }
  results.sort((a,b)=>b.score-a.score);
  return results.slice(0,limit);
}

// extract text from a downloaded file using CLI tools or naive parsing
async function extractFileText(filePath, extension){
  const ext = String(extension || '').toLowerCase().replace(/^\./, '');
  switch(ext){
    case 'docx': {
      console.log(`[docs/extract] mode=mammoth extension=${ext} file=${filePath} exists=${fs.existsSync(filePath)}`);
      await fs.promises.access(filePath, fs.constants.R_OK);
      const result = await mammoth.extractRawText({ path: filePath });
      return { text: String(result?.value || ''), mode: 'mammoth' };
    }
    case 'pdf': {
      return new Promise((res,rej)=>{
        const p = spawn('pdftotext',[filePath,'-']);
        let out=''; p.stdout.on('data',d=>out+=d);
        p.on('close',()=>res({ text: out, mode: 'pdftotext' }));
        p.on('error',e=>rej(e));
      });
    }
    case 'txt':
    case 'md':
      return { text: fs.readFileSync(filePath,'utf8'), mode: 'read-file' };
    case 'xlsx': {
      // unzip sharedStrings and worksheets
      let out='';
      try{
        const s = spawnSync('unzip',['-p',filePath,'xl/sharedStrings.xml']);
        out += s.stdout.toString().replace(/<t>([^<]+)<\/t>/g,'$1\n');
        const w = spawnSync('unzip',['-p',filePath,'xl/worksheets/*.xml']);
        out += w.stdout.toString().replace(/<v>([^<]+)<\/v>/g,'$1\n');
      }catch(e){ }
      return { text: out, mode: 'xlsx-xml' };
    }
    case 'pptx': {
      let out='';
      try{
        const s = spawnSync('unzip',['-p',filePath,'ppt/slides/*.xml']);
        out += s.stdout.toString().replace(/<a:t>([^<]+)<\/a:t>/g,'$1\n');
      }catch(e){}
      return { text: out, mode: 'pptx-xml' };
    }
    case 'mp4':
    case 'mov':
      return new Promise((res,rej)=>{
        const tmpOut = path.join(os.tmpdir(), path.basename(filePath)+'.txt');
        const p = spawn('whisper',[filePath,'--model',WHISPER_MODEL,'--output_format','txt','--output_dir',os.tmpdir()]);
        p.on('close',()=>{
          try{ const t = fs.readFileSync(tmpOut,'utf8'); res({ text: t, mode: 'whisper' }); }catch(_){res({ text: '', mode: 'whisper' });}
        });
        p.on('error',e=>res({ text: '', mode: 'whisper' }));
      });
    case 'youtube': {
      const urls = fs.readFileSync(filePath,'utf8').split(/\r?\n/).filter(Boolean);
      let combined='';
      for(const u of urls){
        try{
          const tmpAudio = path.join(os.tmpdir(), 'yt-'+Date.now()+'.mp3');
          spawnSync('yt-dlp',['-x','--audio-format','mp3','-o',tmpAudio,u]);
          const t = await extractFileText(tmpAudio,'mp4');
          combined += String(t?.text || '') + '\n';
        }catch(e){}
      }
      return { text: combined, mode: 'youtube' };
    }
    default: return { text: '', mode: 'provided-text' };
  }
}

// delete index entry for a file
function deleteIndex(fileId){
  const fname = fs.readdirSync(DOCS_INDEX_DIR).find(f=>f.startsWith(fileId+'-'));
  if(fname){ fs.unlinkSync(path.join(DOCS_INDEX_DIR,fname)); return true; }
  return false;
}

function sanitizeFileName(name) {
  return String(name || 'document')
    .replace(/[\\/]/g, '-')
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '') || 'document';
}

function preprocessDocText(text) {
  let cleanedText = String(text || '').replace(/\r\n/g, '\n');
  cleanedText = cleanedText.replace(/PAGEREF\s+_Toc\d+\s+\\h\s+\d+/g, '');
  cleanedText = cleanedText.replace(/TOC\s+\\o\s+"[^"]*"\s+\\h\s+\\z\s+\\u/g, '');
  cleanedText = cleanedText.replace(/^\s*(-?\d+\s+){2,}\d+\s*/gm, '');
  cleanedText = cleanedText.replace(/\\[A-Za-z]+/g, '');
  cleanedText = cleanedText.replace(/DATE\s+\\@[^\n\\]*/g, '');
  cleanedText = cleanedText.replace(/TIME\s+\\@[^\n\\]*/g, '');
  cleanedText = cleanedText.replace(/PAGE\s+PAGE\s+\d+\s+of\s+NUMPAGES\s+\d+/g, '');

  const looksLikeTocEntry = line => {
    const trimmed = String(line || '').trim();
    if (!trimmed) return false;
    const digitSpaceRatio = ((trimmed.match(/[\d\s]/g) || []).length) / Math.max(trimmed.length, 1);
    const alphaTokens = trimmed.match(/[A-Za-z][A-Za-z\-]{1,}/g) || [];
    const numberTokens = trimmed.match(/\b\d+(?:\.\d+)*\b/g) || [];
    const sectionTitleish = alphaTokens.length > 0 && alphaTokens.every(token => /^[A-Z][a-z]+(?:-[A-Za-z]+)?$/.test(token) || /^(EX\d+|EX)$/i.test(token) || /^(Contents|Overview|Summary|History|Revision|Introduction|Enhancement|Conversion|Configuration|Guide|Scope|Purpose|Requirements|Assumptions)$/i.test(token));
    const looksLikeContentsBody = /^contents\b/i.test(trimmed) && numberTokens.length >= 2;
    const hasVerb = /\b(is|are|was|were|provides?|enables?)\b/i.test(trimmed);
    const repeatingSectionPattern = numberTokens.length >= 3 && !/[.!?]/.test(trimmed) && !hasVerb;
    const longIndexLine = numberTokens.length >= 5 && alphaTokens.length >= 5 && !hasVerb;
    return looksLikeContentsBody || repeatingSectionPattern || longIndexLine || (digitSpaceRatio > 0.6 && sectionTitleish);
  };

  const stripInlineTocBlob = value => {
    const source = String(value || '');
    const stripped = source
      .replace(/\bContents\b(?:\s+\d+(?:\.\d+)*\s+[A-Za-z][A-Za-z0-9–\-()/:, ]{0,120}){4,}/i, ' ')
      .replace(/\s{2,}/g, ' ')
      .trim();
    return stripped || source.trim();
  };

  const trimToNarrativeStart = value => {
    const source = String(value || '').trim();
    if (!source) return source;
    const verbRe = /\b(is|are|was|were|provides?|enables?)\b/gi;
    let m;
    let chosenIndex = -1;
    while ((m = verbRe.exec(source))) {
      const idx = m.index;
      if (idx < 180) continue;
      const prev = source.slice(Math.max(0, idx - 200), idx);
      const around = source.slice(Math.max(0, idx - 240), idx + 120);
      const nums = (prev.match(/\b\d+(?:\.\d+)*\b/g) || []).length;
      const letters = (prev.match(/[A-Za-z]/g) || []).length;
      const bad = /\b(contents|toc|pageref)\b/i.test(around);
      if (!bad && nums <= 1 && letters >= 80) {
        chosenIndex = idx;
        break;
      }
    }
    if (chosenIndex > 500) {
      const sentenceStart = source.lastIndexOf('. ', chosenIndex);
      const start = sentenceStart >= 0 ? sentenceStart + 2 : Math.max(0, chosenIndex - 120);
      return source.slice(start).trim();
    }
    return source;
  };

  const lines = cleanedText.split('\n');
  const rawFallback = trimToNarrativeStart(stripInlineTocBlob(cleanedText
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim()));
  const filteredLines = lines.filter(line => {
    const trimmed = line.trim();
    if (!trimmed) return true;
    if ((trimmed.match(/PAGEREF/gi) || []).length * 'PAGEREF'.length / Math.max(trimmed.length, 1) > 0.5) return false;
    if (/^[\d\s]{10,}$/.test(trimmed)) return false;
    if (looksLikeTocEntry(trimmed)) return false;
    if (/\bcontents\b/i.test(trimmed) && (trimmed.match(/\b\d+(?:\.\d+)*\b/g) || []).length >= 2) return false;
    return true;
  });
  const fallbackText = filteredLines
    .join('\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  const safeFallback = fallbackText || rawFallback;

  const buildParagraphs = linesList => {
    const paragraphs = [];
    let bucket = [];
    for (const line of linesList) {
      const trimmed = String(line || '').trim();
      if (!trimmed) {
        if (bucket.length) {
          paragraphs.push(bucket.join(' ').replace(/\s+/g, ' ').trim());
          bucket = [];
        }
        continue;
      }
      bucket.push(trimmed);
    }
    if (bucket.length) {
      paragraphs.push(bucket.join(' ').replace(/\s+/g, ' ').trim());
    }
    return paragraphs.filter(Boolean);
  };

  const paragraphs = buildParagraphs(filteredLines);
  const isMeaningfulStart = paragraph => {
    const p = String(paragraph || '').trim();
    return p.length > 100
      && !/^\d/.test(p)
      && /\b(is|are|was|were|provides?|enables?)\b/i.test(p);
  };
  let startIndex = paragraphs.findIndex(isMeaningfulStart);

  if (startIndex < 0) {
    const lineStartIndex = filteredLines.findIndex((_, i) => {
      const first = String(filteredLines[i] || '').trim();
      if (!first || /^\d/.test(first) || looksLikeTocEntry(first)) return false;
      const window = filteredLines.slice(i, i + 6).join(' ').replace(/\s+/g, ' ').trim();
      return window.length > 100 && /\b(is|are|was|were|provides?|enables?)\b/i.test(window);
    });
    if (lineStartIndex >= 0) {
      const lineParagraphs = buildParagraphs(filteredLines.slice(lineStartIndex));
      const lineBased = lineParagraphs
        .join('\n\n')
        .replace(/[ \t]+\n/g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim();
      return lineBased || safeFallback;
    }
  }

  const startTrimmed = startIndex > 0 ? paragraphs.slice(startIndex) : paragraphs;
  const finalText = startTrimmed
    .join('\n\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return trimToNarrativeStart(stripInlineTocBlob(finalText || safeFallback));
}

function getChunkParams(extension, wordCount) {
  const ext = (extension || '').toLowerCase().replace(/^\./, '');
  if (ext === 'pdf' || ext === 'docx') {
    return { size: DOCS_CHUNK_SIZE, overlap: DOCS_CHUNK_OVERLAP };
  }
  if (ext === 'txt' || ext === 'md') {
    return { size: 1000, overlap: 150 };
  }
  return { size: DEFAULT_CHUNK_SIZE, overlap: DEFAULT_CHUNK_OVERLAP };
}

function chunkTextSentenceAware(text, maxChunkSize = 2000, overlap = 250) {
  const clean = String(text || '').replace(/\r\n/g, '\n');
  if (!clean.trim()) return [];
  const chunks = [];
  const splitLongParagraph = (paragraph) => {
    const normalized = String(paragraph || '').trim();
    if (!normalized) return [];
    if (normalized.length <= maxChunkSize) return [normalized];
    const parts = [];
    let remaining = normalized;
    const minBreak = Math.floor(maxChunkSize * 0.6);
    while (remaining.length > maxChunkSize) {
      let splitAt = remaining.lastIndexOf('\n', maxChunkSize);
      if (splitAt < minBreak) {
        const sentenceBreak = Math.max(
          remaining.lastIndexOf('. ', maxChunkSize),
          remaining.lastIndexOf('! ', maxChunkSize),
          remaining.lastIndexOf('? ', maxChunkSize)
        );
        if (sentenceBreak >= minBreak) splitAt = sentenceBreak + 1;
      }
      if (splitAt < minBreak) splitAt = remaining.lastIndexOf(' ', maxChunkSize);
      if (splitAt < minBreak) splitAt = maxChunkSize;
      const part = remaining.slice(0, splitAt).trim();
      if (part) parts.push(part);
      remaining = remaining.slice(splitAt).trimStart();
    }
    if (remaining.trim()) parts.push(remaining.trim());
    return parts;
  };
  const paragraphs = clean
    .split(/\n\n+/)
    .flatMap(splitLongParagraph)
    .filter(Boolean);
  let currentChunk = '';
  let currentStart = 0;
  let charPos = 0;
  for (const para of paragraphs) {
    const paraWithNewline = para + '\n\n';
    if (currentChunk.length + paraWithNewline.length > maxChunkSize && currentChunk.length > 0) {
      chunks.push({ chunkIndex: chunks.length, startChar: currentStart, text: currentChunk.trim() });
      const overlapText = currentChunk.slice(-overlap);
      currentStart = charPos - overlapText.length;
      currentChunk = overlapText + paraWithNewline;
    } else {
      currentChunk += paraWithNewline;
    }
    charPos += paraWithNewline.length;
  }
  if (currentChunk.trim().length > 0) {
    chunks.push({ chunkIndex: chunks.length, startChar: currentStart, text: currentChunk.trim() });
  }
  return chunks;
}

function listIndexedDocs() {
  const files = fs.readdirSync(DOCS_INDEX_DIR).filter(f => f.endsWith('.json'));
  const docs = [];
  for (const f of files) {
    try {
      const full = path.join(DOCS_INDEX_DIR, f);
      const data = JSON.parse(fs.readFileSync(full, 'utf8'));
      docs.push({
        id: data.fileId || f.replace(/\.json$/,''),
        name: data.fileName || f,
        webUrl: data.webUrl || '',
        size: Number(data.byteSize || 0),
        lastModified: data.lastModified || data.syncedAt || '',
        mimeType: data.mimeType || '',
        extension: data.extension || path.extname(data.fileName || '').toLowerCase(),
        indexed: true,
        indexedAt: data.syncedAt || ''
      });
    } catch (_e) {}
  }
  docs.sort((a,b)=>new Date(b.lastModified || 0) - new Date(a.lastModified || 0));
  return docs;
}

function countJsonFiles(dir) {
  try {
    return fs.readdirSync(dir).filter(f => f.endsWith('.json')).length;
  } catch (_e) {
    return 0;
  }
}

function boolQuery(v) {
  if (v === undefined || v === null || v === '') return null;
  if (String(v).toLowerCase() === 'true') return true;
  if (String(v).toLowerCase() === 'false') return false;
  return null;
}

function normalizeProjectKeys(keys) {
  if (!Array.isArray(keys)) return [];
  return keys.map(k => String(k || '').trim().toUpperCase()).filter(Boolean);
}

function shouldIncludeIssueByProject(issueKey) {
  if (!ATLASSIAN_PROJECT_KEYS.length) return true;
  const key = String(issueKey || '').toUpperCase();
  const project = key.includes('-') ? key.split('-')[0] : key;
  return ATLASSIAN_PROJECT_KEYS.includes(project);
}

function extractText(v) {
  if (typeof v === 'string') return v;
  if (!v) return '';
  if (Array.isArray(v)) return v.map(extractText).filter(Boolean).join('\n');
  if (typeof v === 'object') {
    if (typeof v.text === 'string') return v.text;
    if (typeof v.value === 'string') return v.value;
    if (Array.isArray(v.content)) return v.content.map(extractText).filter(Boolean).join('\n');
  }
  return '';
}

function normalizeJiraIssue(raw) {
  const fields = raw?.fields || {};
  const key = raw?.key || raw?.issueKey || raw?.id || '';
  const summary = fields.summary || raw?.summary || '';
  const issueType = fields.issuetype?.name || raw?.type || raw?.issueType || '';
  const status = fields.status?.name || raw?.status || '';
  const priority = fields.priority?.name || raw?.priority || '';
  const assignee = fields.assignee?.displayName || fields.assignee?.name || raw?.assignee || '';
  const updated = fields.updated || raw?.updated || '';
  const description = extractText(fields.description || raw?.description || '');
  const url = atlassianBrowseUrl(key) || raw?.url || raw?.self || '';
  const comments = Array.isArray(fields.comment?.comments)
    ? fields.comment.comments.map(c => ({ author: c?.author?.displayName || '', body: extractText(c?.body || ''), created: c?.created || '' }))
    : (Array.isArray(raw?.comments) ? raw.comments : []);
  return {
    key,
    summary,
    type: issueType,
    status,
    priority,
    assignee,
    updated,
    url,
    description,
    resolution: fields.resolution?.name || raw?.resolution || '',
    labels: Array.isArray(fields.labels) ? fields.labels : (Array.isArray(raw?.labels) ? raw.labels : []),
    components: Array.isArray(fields.components) ? fields.components.map(c => c?.name).filter(Boolean) : (Array.isArray(raw?.components) ? raw.components : []),
    comments
  };
}

function firstArrayCandidate(obj, keys) {
  for (const key of keys) {
    if (Array.isArray(obj?.[key])) return obj[key];
  }
  return null;
}

function atlassianConfigured() {
  return !!(ATLASSIAN_ENABLED && ATLASSIAN_EMAIL && ATLASSIAN_DOMAIN && ATLASSIAN_TOKEN);
}

function atlassianBrowseUrl(issueKey) {
  const key = String(issueKey || '').toUpperCase();
  if (!key || !ATLASSIAN_DOMAIN) return '';
  return `https://${ATLASSIAN_DOMAIN}/browse/${encodeURIComponent(key)}`;
}

function atlassianAuthHeader() {
  const raw = `${ATLASSIAN_EMAIL}:${ATLASSIAN_TOKEN}`;
  return `Basic ${Buffer.from(raw).toString('base64')}`;
}

function jqlEscape(text) {
  return String(text || '').replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

async function atlassianRestGet(pathname, params = {}, timeoutMs = 4000) {
  if (!atlassianConfigured()) return null;
  const detailed = await atlassianRestGetDetailed(pathname, params, timeoutMs);
  return detailed && detailed.ok ? detailed.json : null;
}

async function atlassianRestGetDetailed(pathname, params = {}, timeoutMs = 4000) {
  if (!atlassianConfigured()) return { ok: false, status: 0, url: '', bodyPreview: '', json: null, error: 'Not configured' };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const target = new URL(`https://${ATLASSIAN_DOMAIN}${pathname}`);
    for (const [k, v] of Object.entries(params || {})) {
      if (v !== undefined && v !== null && v !== '') target.searchParams.set(k, String(v));
    }
    const requestUrl = target.toString();
    const resp = await fetch(requestUrl, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
        'Authorization': atlassianAuthHeader()
      },
      signal: controller.signal
    });
    const bodyText = await resp.text();
    let parsed = null;
    try { parsed = bodyText ? JSON.parse(bodyText) : null; } catch (_e) { parsed = null; }
    return {
      ok: resp.ok,
      status: resp.status,
      url: requestUrl,
      bodyPreview: String(bodyText || '').slice(0, 200),
      json: parsed,
      error: null
    };
  } catch (_e) {
    return {
      ok: false,
      status: 0,
      url: `https://${ATLASSIAN_DOMAIN}${pathname}`,
      bodyPreview: '',
      json: null,
      error: String(_e && _e.message ? _e.message : _e)
    };
  } finally {
    clearTimeout(timer);
  }
}

async function atlassianSearchIssues(queryText, maxResults = 10) {
  const out = await atlassianSearchIssuesDetailed(queryText, maxResults);
  return out.issues;
}

async function atlassianSearchIssuesDetailed(queryText, maxResults = 10, options = {}) {
  if (!atlassianConfigured()) return { issues: [], attempts: [] };
  const decodedQuery = String(queryText || '').replace(/\+/g, ' ').trim();
  const terms = Array.from(new Set(
    decodedQuery
      .split(/\s+/)
      .map(t => t.trim())
      .filter(Boolean)
  ));
  const escapedTerms = terms.map(t => jqlEscape(t));
  const q = jqlEscape(decodedQuery);
  const keyFilter = ATLASSIAN_PROJECT_KEYS.length ? `project IN (${ATLASSIAN_PROJECT_KEYS.join(',')}) AND ` : '';
  const textAndClause = escapedTerms.length
    ? escapedTerms.map(t => `text ~ "${t}"`).join(' AND ')
    : `text ~ "${q}"`;
  const summaryOrClause = escapedTerms.length
    ? escapedTerms.map(t => `summary ~ "${t}"`).join(' OR ')
    : `summary ~ "${q}"`;
  const defaultJqlCandidates = [
    `${keyFilter}${textAndClause} ORDER BY updated DESC`,
    `${keyFilter}${summaryOrClause} ORDER BY updated DESC`,
    `${keyFilter}text ~ "${q}" ORDER BY updated DESC`,
    `${keyFilter}summary ~ "${q}" ORDER BY updated DESC`
  ];
  const jqlCandidates = Array.isArray(options.jqlCandidates) && options.jqlCandidates.length
    ? options.jqlCandidates
    : defaultJqlCandidates;
  const fields = 'summary,status,issuetype,priority,assignee,updated,description,comment,resolution,labels,components';
  const max = Math.max(1, Math.min(50, Number(maxResults) || 10));
  const endpoints = ['/rest/api/3/search/jql', '/rest/api/3/search', '/rest/api/3/issue/search'];
  const attempts = [];
  let lastEmptyResult = null;

  for (const endpoint of endpoints) {
    for (const jql of jqlCandidates) {
      const detailed = await atlassianRestGetDetailed(endpoint, { jql, maxResults: max, fields }, 5000);
      const result = detailed.json;
      const arr = firstArrayCandidate(result, ['issues', 'results', 'items']) || (Array.isArray(result) ? result : null);
      const totalHits = Number(result?.total) || (Array.isArray(arr) ? arr.length : 0);
      attempts.push({ endpoint, jql, totalHits, ...detailed });
      if (Array.isArray(arr)) {
        const issues = arr.map(normalizeJiraIssue).filter(i => i.key && shouldIncludeIssueByProject(i.key)).slice(0, max);
        if (issues.length > 0) {
          return { issues, attempts };
        }
        lastEmptyResult = { issues, attempts };
      }
    }
  }
  return lastEmptyResult || { issues: [], attempts };
}

async function atlassianGetIssue(issueKey) {
  if (!atlassianConfigured() || !issueKey) return null;
  const key = String(issueKey || '').trim().toUpperCase();
  const fields = 'summary,status,issuetype,priority,assignee,updated,description,comment,resolution,labels,components';
  const result = await atlassianRestGet(`/rest/api/3/issue/${encodeURIComponent(key)}`, { fields }, 5000);
  if (!result) return null;
  const normalized = normalizeJiraIssue(result);
  if (normalized.key && shouldIncludeIssueByProject(normalized.key)) return normalized;
  return null;
}

async function atlassianListProjects() {
  const out = await atlassianListProjectsDetailed();
  return out.projects;
}

async function atlassianListProjectsDetailed() {
  if (!atlassianConfigured()) return { projects: [], attempts: [] };
  const attempts = [];
  const endpoints = ['/rest/api/3/project/search', '/rest/api/3/project'];
  for (const endpoint of endpoints) {
    const detailed = await atlassianRestGetDetailed(endpoint, { maxResults: 1000 }, 5000);
    attempts.push({ endpoint, ...detailed });
    const result = detailed.json;
    const arr = Array.isArray(result) ? result : (Array.isArray(result?.values) ? result.values : []);
    if (Array.isArray(arr)) {
      const projects = arr.map(p => ({ key: String(p?.key || '').toUpperCase(), name: p?.name || '', id: p?.id || '' })).filter(p => p.key);
      return { projects, attempts };
    }
  }
  return { projects: [], attempts };
}

async function checkAtlassianHealth(force = false) {
  const now = Date.now();
  if (!atlassianConfigured()) return false;
  if (!force && (now - atlassianHealthCache.checkedAt) < 30000) return atlassianHealthCache.ok;
  const probe = await atlassianRestGet('/rest/api/3/myself', {}, 3000);
  atlassianHealthCache = { ok: !!probe, checkedAt: now };
  return atlassianHealthCache.ok;
}

function generateKnowledgeId() {
  return `ke-${Math.floor(Date.now() / 1000)}-${Math.random().toString(36).slice(2,5)}`;
}

function knowledgePathById(id) {
  return path.join(KNOWLEDGE_INDEX_DIR, `${String(id || '').replace(/[^a-zA-Z0-9_-]/g, '')}.json`);
}

function readKnowledgeEntries() {
  const files = fs.readdirSync(KNOWLEDGE_INDEX_DIR).filter(f => f.endsWith('.json'));
  const entries = [];
  for (const f of files) {
    try {
      const full = path.join(KNOWLEDGE_INDEX_DIR, f);
      const data = JSON.parse(fs.readFileSync(full, 'utf8'));
      if (data && data.id) entries.push(data);
    } catch (_e) {}
  }
  entries.sort((a,b)=>String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')));
  return entries;
}

function applyKnowledgeFilters(entries, queryParams = {}) {
  const type = String(queryParams.type || '').trim().toLowerCase();
  const tags = String(queryParams.tags || '').trim().toLowerCase();
  const quality = queryParams.quality !== undefined ? Number(queryParams.quality) : null;
  const approved = boolQuery(queryParams.approved);
  const dcCode = String(queryParams.dcCode || '').trim().toUpperCase();

  const tagList = tags ? tags.split(',').map(t=>t.trim()).filter(Boolean) : [];

  return entries.filter(e => {
    if (type && String(e.type || '').toLowerCase() !== type) return false;
    if (Number.isFinite(quality) && Number(e.quality) !== quality) return false;
    if (approved !== null && Boolean(e.approved) !== approved) return false;
    if (dcCode) {
      const dcs = Array.isArray(e?.context?.dcCodes) ? e.context.dcCodes.map(x=>String(x).toUpperCase()) : [];
      if (!dcs.includes(dcCode)) return false;
    }
    if (tagList.length) {
      const itemTags = Array.isArray(e.tags) ? e.tags.map(t=>String(t).toLowerCase()) : [];
      if (!tagList.every(t => itemTags.includes(t))) return false;
    }
    return true;
  });
}

function knowledgeSearch(queryText, limit = 5) {
  const q = String(queryText || '').trim().toLowerCase();
  if (!q) return [];
  const terms = q.split(/\s+/).filter(Boolean);
  const entries = readKnowledgeEntries();
  const scored = [];
  for (const e of entries) {
    const hay = [e.question || '', e.answer || '', Array.isArray(e.tags) ? e.tags.join(' ') : '']
      .join(' ')
      .toLowerCase();
    let score = 0;
    for (const t of terms) {
      const matches = hay.split(t).length - 1;
      score += matches;
    }
    if (score > 0) {
      scored.push({
        id: e.id,
        type: e.type,
        question: e.question,
        answer: e.answer,
        tags: e.tags || [],
        context: e.context || {},
        quality: e.quality,
        approved: !!e.approved,
        score
      });
    }
  }
  scored.sort((a,b)=>b.score-a.score);
  return scored.slice(0, Math.max(1, Math.min(50, Number(limit) || 5)));
}

function knowledgeStats(entries) {
  const byType = {};
  const byQuality = {};
  const bySystem = {};
  const byDC = {};
  let approved = 0;
  let readyForTraining = 0;

  for (const e of entries) {
    const type = e.type || 'unknown';
    byType[type] = (byType[type] || 0) + 1;

    const q = Number(e.quality) || 0;
    byQuality[q] = (byQuality[q] || 0) + 1;

    const systems = Array.isArray(e?.context?.systems) ? e.context.systems : [];
    systems.forEach(s => { bySystem[s] = (bySystem[s] || 0) + 1; });

    const dcs = Array.isArray(e?.context?.dcCodes) ? e.context.dcCodes : [];
    dcs.forEach(dc => { byDC[dc] = (byDC[dc] || 0) + 1; });

    if (e.approved) approved++;
    if (e.approved && q >= 2) readyForTraining++;
  }

  return {
    total: entries.length,
    byType,
    byQuality,
    bySystem,
    byDC,
    approved,
    readyForTraining
  };
}

if (!AUTH_TOKEN) {
  console.error('⚠  Auth disabled — set config.bridge.authToken to restrict access');
}

// in-memory rate limiter: ip -> { count, windowStart }
const rateMap = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of rateMap) {
    if (now - entry.windowStart > 60000) rateMap.delete(ip);
  }
}, 300000); // prune every 5 minutes

// helpers for parameter validation
function validGroup(id) {
  return /^[a-zA-Z0-9_-]+$/.test(id);
}
function validIdentifier(name) {
  return /^[a-zA-Z0-9_$#]+$/.test(name);
}
function validType(t) {
  return [
    'TABLE','VIEW','PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION','TRIGGER','SEQUENCE','INDEX'
  ].includes(t.toUpperCase());
}
function sanitizeKeyword(k) {
  let s = k.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
  if (s.length > 200) s = s.slice(0,200);
  return s;
}

// utility: sleep ms
const sleep = ms => new Promise(r => setTimeout(r, ms));

// pooling configuration values
const POOL_ENABLED        = config.bridge && config.bridge.poolEnabled;
const POOL_IDLE_TIMEOUT   = (config.bridge && config.bridge.poolIdleTimeoutMs) || 300000;
const POOL_MAX_QUEUE      = (config.bridge && config.bridge.poolMaxQueueDepth) || 20;
const TOOL_CALL_TIMEOUT   = (config.bridge && config.bridge.toolCallTimeoutMs) || 15000;

// connection pool map: groupId -> entry
const pool = new Map();

// helper to send raw JSON-RPC object to an entry
function sendToEntry(entry, obj) {
  const text = JSON.stringify(obj);
  entry.child.stdin.write(text + '\n');
  if (debug) console.error('[MCP →]', text);
}

// create and initialize a new MCP client process for a group
async function createPoolEntry(group) {
  const child = spawn(SQLCL_CMD, SQLCL_ARGS, { stdio: ['pipe','pipe','pipe'] });
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
      } catch(e) {}
    }
  });
  child.on('close', () => {
    entry.alive = false;
  });

  sendToEntry(entry, { jsonrpc:'2.0', id: entry.nextMsgId++, method:'initialize',
            params:{ protocolVersion:'2024-11-05', capabilities:{},
                     clientInfo:{ name: MCP_CLIENT, version:'1.0' } } });
  await sleep(config.bridge.sleepAfterInit || 400);
  sendToEntry(entry, { jsonrpc:'2.0', method:'notifications/initialized', params:{} });
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
  } catch (e) {
    entry.dbUser = null;
  }

  return entry;
}

// obtain a pool entry, creating one if necessary
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

// process queued requests for an entry
async function processQueue(entry, group) {
  if (entry.processing) return;
  entry.processing = true;
  while (entry.queue.length) {
    const job = entry.queue.shift();
    const { toolCalls, resolve, reject } = job;
    try {
      let attempt = 0;
      while (true) {
        try {
          const res = await performCalls(entry, toolCalls);
          resolve(res);
          break;
        } catch (err) {
          // if child died or not connected, respawn and retry once
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
    }
  }
  entry.processing = false;
}

// enqueue calls for a group entry
async function enqueueToolCalls(group, toolCalls) {
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

// execute tool calls on a given entry, sequentially
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
      } catch {}
      entry.child.kill();
      pool.delete(gid);
    }
  }
}, 60000);

// original spawn-per-request implementation kept for fallback
async function runMCP_Spawn(toolCalls) {
  return new Promise((resolve, reject) => {
    const child = spawn(SQLCL_CMD, SQLCL_ARGS, { stdio: ['pipe','pipe','pipe'] });
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
      } catch (e) { reject(e); }
    });

    const send = obj => {
      const text = JSON.stringify(obj);
      sent.push(text);
      child.stdin.write(text + '\n');
      if (debug) console.error('[MCP →]', text);
    };

    (async () => {
      // handshake
      send({ jsonrpc:'2.0', id: msgId++, method:'initialize',
             params:{ protocolVersion:'2024-11-05', capabilities:{},
                      clientInfo:{ name: MCP_CLIENT, version:'1.0' } } });
      await sleep(300);
      send({ jsonrpc:'2.0', method:'notifications/initialized', params:{} });
      await sleep(300);

      for (const tool of toolCalls) {
        send({ jsonrpc:'2.0', id: msgId++, method:'tools/call',
               params:{ name: tool.name,
                        arguments: { ...tool.arguments,
                                     mcp_client: MCP_CLIENT,
                                     model: MCP_MODEL } } });
        await sleep(500);
      }

      // disconnect
      send({ jsonrpc:'2.0', id: msgId++, method:'tools/call',
             params:{ name:'disconnect',
                      arguments:{ mcp_client: MCP_CLIENT, model: MCP_MODEL } } });
      await sleep(200);
      child.stdin.end();
    })();
  });
}

// new runMCP chooses pooled or spawn depending on config and group
async function runMCP(toolCalls, group) {
  if (POOL_ENABLED && group) {
    console.error('[POOL] enqueueing request for', group.id);
    const arr = await enqueueToolCalls(group, toolCalls);
    // normalize to spawn output shape
    return { responses: arr, sent: [], received: [] };
  }
  console.error('[POOL] spawning ad-hoc process');
  return runMCP_Spawn(toolCalls);
}

// lightweight CSV parser that understands quoted fields and preserves
// empty values ("","foo" => ["","foo"]).
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
          i++; // skip escaped quote
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

// parse a single run-sql response into array of objects
function parseMCPResult(response) {
  const text = (response?.result?.content || [])
    .filter(c => c && c.type === 'text')
    .map(c => c.text || '')
    .join('\n');

  const statusLineRx = /^\d*\s*\(?\d+\s+rows?\s+selected\.?\)?\.?$/i;
  let lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  // Remove SQLcl status output before locating/processing CSV content.
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

// helper to perform a SQL query via MCP and return array of objects
async function runSQL(group, sql, route, params) {
  const start = Date.now();
  if (debug) console.error('[REQ]', route, params);
  let parsedRows = 0;
  try {
    const rows = await modularPool.runSQL(group, sql, route, params);
    parsedRows = Array.isArray(rows) ? rows.length : 0;
    if (debug) console.error('[RES]', 200, route, '→', parsedRows, 'rows in', Date.now()-start, 'ms');
    const poolLast = (typeof modularPool.getLastRequest === 'function') ? modularPool.getLastRequest() : null;
    lastRequest = poolLast && Object.keys(poolLast).length
      ? poolLast
      : { route, params, mcpMessages: null, mcpResponses: null, parsedRows, durationMs: Date.now()-start, error: null };
    return rows;
  } catch (err) {
    if (err.code === 'QUEUE_FULL' || err.message === 'Queue full') {
      const e2 = new Error('Queue full');
      e2.httpStatus = 503;
      err = e2;
    }
    if (debug) console.error('[RES]', err.httpStatus||500, route, err.message);
    const poolLast = (typeof modularPool.getLastRequest === 'function') ? modularPool.getLastRequest() : null;
    lastRequest = poolLast && Object.keys(poolLast).length
      ? poolLast
      : { route, params, mcpMessages: null, mcpResponses: null, parsedRows, durationMs: Date.now()-start, error: err.message };
    throw err;
  }
}

// error handling wrapper with 15s timeout
async function withTimeout(promise, ms = TOOL_CALL_TIMEOUT) {
  let timer;
  const timeout = new Promise((_, rej) => timer = setTimeout(() => rej(new Error('timeout')), ms));
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

const modularPool = createMcpPool({
  spawn,
  readline,
  config,
  SQLCL_CMD,
  SQLCL_ARGS,
  MCP_CLIENT,
  MCP_MODEL,
  debug
});

let semanticScanRunning = false;
let semanticWorkerBooting = false;

async function fetchJsonWithTimeout(target, opts = {}, timeoutMs = 4000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const r = await fetch(target, { ...opts, signal: controller.signal });
    const txt = await r.text();
    let json;
    try { json = txt ? JSON.parse(txt) : {}; } catch (_e) { json = {}; }
    return { ok: r.ok, status: r.status, json };
  } finally {
    clearTimeout(timer);
  }
}

async function isSemanticWorkerRunning() {
  try {
    const r = await fetchJsonWithTimeout(`${SEMANTIC_WORKER_URL}/status`, {}, 2000);
    return !!(r.ok && r.json && Object.prototype.hasOwnProperty.call(r.json, 'paused'));
  } catch (_e) {
    return false;
  }
}

async function startSemanticWorkerIfNeeded() {
  if (await isSemanticWorkerRunning()) return true;
  if (semanticWorkerBooting) return false;
  semanticWorkerBooting = true;
  try {
    const workerDir = path.join(__dirname, 'semantic-worker');
    const workerApp = path.join(workerDir, 'app.py');
    const venvDir = path.join(__dirname, '.venv');
    const venvPython = path.join(venvDir, 'bin', 'python3');
    if (!fs.existsSync(workerApp)) {
      console.error('[SEMANTIC] semantic-worker/app.py not found; semantic layer disabled');
      return false;
    }

    let pythonCmd = 'python3';
    try {
      if (fs.existsSync(venvPython)) {
        pythonCmd = venvPython;
      } else {
        const mkVenv = spawnSync('python3', ['-m', 'venv', venvDir], { stdio: 'ignore' });
        if ((mkVenv.status || 1) === 0 && fs.existsSync(venvPython)) {
          pythonCmd = venvPython;
        }
      }

      const check = spawnSync(pythonCmd, ['-c', 'import flask,requests'], { stdio: 'ignore' });
      if ((check.status || 1) !== 0 && pythonCmd === venvPython) {
        console.log('[SEMANTIC] installing python packages into .venv (flask, requests)...');
        spawnSync(pythonCmd, ['-m', 'pip', 'install', 'flask', 'requests', '-q'], { stdio: 'ignore' });
      }
    } catch (_e) {}

    const child = spawn(pythonCmd, ['app.py'], {
      cwd: workerDir,
      stdio: 'ignore',
      detached: true,
      env: {
        ...process.env,
        OLLAMA_URL: OLLAMA,
        OLLAMA_MODEL: DEFAULT_MODEL
      }
    });
    child.unref();

    const deadline = Date.now() + 12000;
    while (Date.now() < deadline) {
      if (await isSemanticWorkerRunning()) {
        console.log('[SEMANTIC] worker online at', SEMANTIC_WORKER_URL);
        return true;
      }
      await sleep(500);
    }
    console.error('[SEMANTIC] worker failed to start in time');
    return false;
  } catch (e) {
    console.error('[SEMANTIC] failed to start worker:', e.message);
    return false;
  } finally {
    semanticWorkerBooting = false;
  }
}

function sanitizeSqlName(value) {
  return String(value || '').replace(/[^a-zA-Z0-9_$#]/g, '').toUpperCase();
}

async function queryDB(group, sql, route = '/semantic/scan') {
  return withTimeout(modularPool.runSQL(group, sql, route, { group: group.id }), TOOL_CALL_TIMEOUT);
}

async function discoverFromPackageSource(group, schemaName, packageName) {
  const schema = sanitizeSqlName(schemaName);
  const pkg = sanitizeSqlName(packageName);
  if (!schema || !pkg) return 0;
  const sql = `/* LLM in use is bridge */\nSELECT TEXT FROM DBA_SOURCE\nWHERE OWNER = UPPER('${schema}') AND NAME = UPPER('${pkg}') AND TYPE = 'PACKAGE BODY'\nORDER BY LINE`;
  let rows = [];
  try {
    rows = await queryDB(group, sql);
  } catch (_e) {
    return 0;
  }
  const source = (rows || []).map(r => r.TEXT || r.text || '').join('');
  if (!source.trim()) return 0;

  try {
    const resp = await fetchJsonWithTimeout(`${SEMANTIC_WORKER_URL}/discover`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        group: group.id,
        schema,
        package: pkg,
        source
      })
    }, 60000);
    const discovered = Number(resp.json && resp.json.discovered) || 0;
    return discovered;
  } catch (_e) {
    return 0;
  }
}

async function scanSemanticTables(group, schema) {
  // Scan key business tables to infer schema patterns + column meanings
  const safeSchema = sanitizeSqlName(schema);
  if (!safeSchema) return 0;
  
  // Find key tables in schema (WAVE*, LOAD*, SHIPMENT*, etc.)
  const keyTablesSql = `/* LLM in use is bridge */
SELECT OBJECT_NAME FROM DBA_OBJECTS
WHERE OWNER = UPPER('${safeSchema}') AND OBJECT_TYPE = 'TABLE'
  AND (OBJECT_NAME LIKE 'WAVE%' OR OBJECT_NAME LIKE 'LOAD%' OR OBJECT_NAME LIKE 'SHIPMENT%' 
       OR OBJECT_NAME LIKE 'INVENTORY%' OR OBJECT_NAME LIKE 'DOCK%' OR OBJECT_NAME LIKE 'RECEIPT%'
       OR OBJECT_NAME LIKE 'CONTAINER%' OR OBJECT_NAME LIKE 'CYCLE%' OR OBJECT_NAME LIKE 'PICK%'
       OR OBJECT_NAME LIKE 'PACK%' OR OBJECT_NAME LIKE 'VARIANCE%')
ORDER BY OBJECT_NAME FETCH FIRST 6 ROWS ONLY`;
  
  let tables = [];
  try {
    const rows = await queryDB(group, keyTablesSql);
    tables = (rows || []).map(r => String(r.OBJECT_NAME || '').trim()).filter(Boolean);
  } catch (_e) {
    return 0;
  }
  
  let discovered = 0;
  for (const table of tables) {
    const safeTable = sanitizeSqlName(table);
    if (!safeTable) continue;
    
    // Get columns for this table
    const colsSql = `/* LLM in use is bridge */
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_ID FROM DBA_TAB_COLUMNS
WHERE OWNER = UPPER('${safeSchema}') AND TABLE_NAME = UPPER('${safeTable}')
ORDER BY COLUMN_ID FETCH FIRST 50 ROWS ONLY`;
    
    let columns = [];
    try {
      const colRows = await queryDB(group, colsSql);
      columns = Array.isArray(colRows) ? colRows : [];
    } catch (_e) {
      continue;
    }
    
    // Send to semantic worker for analysis
    try {
      const resp = await fetchJsonWithTimeout(`${SEMANTIC_WORKER_URL}/analyze-table`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          schema: safeSchema,
          table: safeTable,
          columns: columns.map(c => ({
            COLUMN_NAME: c.COLUMN_NAME || c.column_name || '',
            DATA_TYPE: c.DATA_TYPE || c.data_type || ''
          }))
        })
      }, 30000);
      const intentsCount = Number(resp.json && resp.json.discovered) || 0;
      discovered += intentsCount;
    } catch (_e) {
      // continue on table analysis failure
    }
  }
  
  return discovered;
}

async function scanSemanticGroup(group) {
  const schemas = Array.isArray(group.schemas) ? group.schemas : [];
  const prioritySchemas = ['MANH_CODE', 'FRAMEWORK'];
  const orderedSchemas = [
    ...prioritySchemas.filter(s => schemas.includes(s)),
    ...schemas.filter(s => !prioritySchemas.includes(s))
  ].slice(0, 2);

  let discovered = 0;
  let scannedSchemas = 0;
  for (const schema of orderedSchemas) {
    scannedSchemas += 1;
    const safeSchema = sanitizeSqlName(schema);
    if (!safeSchema) continue;
    
    // Phase 1: Package source analysis
    const listSql = `/* LLM in use is bridge */\nSELECT OBJECT_NAME FROM DBA_OBJECTS\nWHERE OWNER = UPPER('${safeSchema}') AND OBJECT_TYPE = 'PACKAGE'\nORDER BY OBJECT_NAME FETCH FIRST 4 ROWS ONLY`;
    let packages = [];
    try {
      const rows = await queryDB(group, listSql);
      packages = (rows || []).map(r => String(r.OBJECT_NAME || '').trim()).filter(Boolean);
    } catch (_e) {
      packages = [];
    }
    for (const pkg of packages) {
      const count = await discoverFromPackageSource(group, safeSchema, pkg);
      discovered += count;
    }
    
    // Phase 2: Table schema analysis (NEW)
    const tableCount = await scanSemanticTables(group, schema);
    discovered += tableCount;
  }
  return { discovered, scannedSchemas };
}

async function runSemanticScan(trigger = {}) {
  if (semanticScanRunning) return false;
  if (!(await startSemanticWorkerIfNeeded())) return false;

  if (!trigger || trigger.manual !== true) {
    const snapshot = modularPool.getPoolSnapshot ? modularPool.getPoolSnapshot() : {};
    const totalQueue = Object.values(snapshot || {}).reduce((sum, entry) => sum + (Number(entry.queueDepth) || 0), 0);
    if (totalQueue > 0) return false;
  }

  semanticScanRunning = true;
  try {
    const prodGroups = (config.groups || []).filter(g => (g.env || 'prod') === 'prod');
    const ordered = [...prodGroups].sort((a, b) => {
      const score = g => {
        const schemas = Array.isArray(g.schemas) ? g.schemas : [];
        let s = 0;
        if (schemas.includes('MANH_CODE')) s += 2;
        if (schemas.includes('FRAMEWORK')) s += 1;
        return s;
      };
      return score(b) - score(a);
    });

    let groupsScanned = 0;
    let discovered = 0;
    const scannedSchemas = [];
    for (let i = 0; i < ordered.length; i += 2) {
      const chunk = ordered.slice(i, i + 2);
      const results = await Promise.all(chunk.map(async g => {
        const out = await scanSemanticGroup(g);
        return { groupId: g.id, ...out };
      }));
      groupsScanned += results.length;
      results.forEach(r => {
        discovered += r.discovered;
        scannedSchemas.push(r.groupId);
      });
    }

    await fetchJsonWithTimeout(`${SEMANTIC_WORKER_URL}/scan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        groupsScanned,
        docsScanned: 0,
        ticketsScanned: 0,
        pending: 0,
        schemas: scannedSchemas,
        discovered
      })
    }, 10000);

    return true;
  } catch (e) {
    console.error('[SEMANTIC] scan failed:', e.message);
    return false;
  } finally {
    semanticScanRunning = false;
  }
}

const handleSemanticRoute = createSemanticRoutes({
  fetch,
  workerBaseUrl: SEMANTIC_WORKER_URL,
  ensureSemanticWorker: startSemanticWorkerIfNeeded,
  localIndexPath: path.join(SEMANTIC_INDEX_DIR, 'intents.json'),
  triggerSemanticScan: async (payload = {}) => runSemanticScan({ manual: true, ...payload }),
  config,
  graphStore
  });
const handleJiraRoute = createJiraRoutes({
  atlassianConfigured,
  atlassianSearchIssuesDetailed,
  atlassianListProjectsDetailed,
  atlassianGetIssue,
  jqlEscape,
  getAtlassianProjectKeys: () => ATLASSIAN_PROJECT_KEYS,
  config,
  DOCS_INDEX_DIR,
  UPLOAD_TOKEN,
  sanitizeFileName,
  chunkTextSentenceAware
});
const handleOllamaRoute = createOllamaRoutes({ OLLAMA, fetch });
const handleKnowledgeRoute = createKnowledgeRoutes({
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
});
const handleDocsRoute = createDocsRoutes({
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
  fetch,
  graphStore
});
const handleDbRoute = createDbRoutes({
  config,
  withTimeout,
  runSQL: modularPool.runSQL,
  TOOL_CALL_TIMEOUT,
  validGroup,
  validIdentifier,
  validType,
  sanitizeKeyword,
  graphStore
});

// http server
const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;
  const query = parsed.query;
  if (debug) console.error('[REQ]', req.method, pathname + (parsed.search||''));

  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');

  const _writeHead = res.writeHead;
  res.writeHead = function patchedWriteHead(statusCode, reasonPhrase, headers) {
    let rp = reasonPhrase;
    let h = headers;
    if (typeof rp === 'object' && rp !== null) {
      h = rp;
      rp = undefined;
    }
    if (h && typeof h === 'object') {
      const ctKey = Object.keys(h).find(k => k.toLowerCase() === 'content-type');
      if (ctKey && /^application\/json\b/i.test(String(h[ctKey])) && !/charset=/i.test(String(h[ctKey]))) {
        h[ctKey] = 'application/json; charset=utf-8';
      }
    }
    if (!h) {
      const existing = res.getHeader('Content-Type');
      if (existing && /^application\/json\b/i.test(String(existing)) && !/charset=/i.test(String(existing))) {
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
      }
    }
    return _writeHead.call(this, statusCode, rp, h);
  };

  // CORS origin check
  const origin = req.headers.origin;
  if (origin) {
    if (pathname === '/health') {
      // always allow health, don't block on origin
      res.setHeader('Access-Control-Allow-Origin', origin);
    } else if (ALLOWED_ORIGINS.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
    } else {
      res.writeHead(403);
      return res.end(JSON.stringify({ error: 'Origin not allowed' }));
    }
  } else {
    // no origin header (curl, node fetch) → allow
    res.setHeader('Access-Control-Allow-Origin', '*');
  }

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  // rate limiting (except health)
  if (pathname !== '/health') {
    const ip = req.socket.remoteAddress || 'unknown';
    const now = Date.now();
    const entry = rateMap.get(ip) || { count: 0, windowStart: now };
    if (now - entry.windowStart > 60000) {
      entry.count = 0;
      entry.windowStart = now;
    }
    entry.count++;
    rateMap.set(ip, entry);
    if (entry.count > RATE_LIMIT) {
      const retryAfter = entry.windowStart + 60000 - now;
      res.writeHead(429);
      return res.end(JSON.stringify({ error: 'Rate limit exceeded', retryAfterMs: retryAfter }));
    }
  }

  // token auth (public endpoints: /health, /pool-status, /groups, /myip, /msal-browser.min.js, GET /config)
  const isPublicConfigGet = pathname === '/config' && req.method === 'GET';
  const isPublicKnowledgeSearch = pathname === '/knowledge/search' && req.method === 'GET';
  if (AUTH_TOKEN && !['/','/health','/pool-status','/groups','/myip','/msal-browser.min.js'].includes(pathname) && !isPublicConfigGet && !isPublicKnowledgeSearch) {
    let token = null;
    const authh = req.headers.authorization;
    if (authh && authh.startsWith('Bearer ')) token = authh.slice(7).trim();
    if (!token && query.token) token = query.token;
    if (token !== AUTH_TOKEN) {
      res.writeHead(401);
      return res.end(JSON.stringify({ error: 'Unauthorized' }));
    }
  }

  // parameter validation helper closure -- used in db routes
  function validateDbParams(params) {
    if (params.group && !validGroup(params.group)) {
      throw new Error('Invalid parameter: group');
    }
    if (params.schema && !validIdentifier(params.schema)) {
      throw new Error('Invalid parameter: schema');
    }
    if (params.table && !validIdentifier(params.table)) {
      throw new Error('Invalid parameter: table');
    }
    if (params.name && !validIdentifier(params.name)) {
      throw new Error('Invalid parameter: name');
    }
    if (params.type && !validType(params.type)) {
      throw new Error('Invalid parameter: type');
    }
    if (params.keyword) {
      params.keyword = sanitizeKeyword(params.keyword);
    }
  }



  try {
    const reactIndex = path.join(__dirname, 'public', 'dist', 'index.html');
    const hasReactBuild = fs.existsSync(reactIndex);
    const apiPrefixes = [
      '/health', '/pool-status', '/myip', '/semantic/', '/jira/', '/ollama/', '/knowledge/', '/docs/', '/db/', '/groups', '/config', '/debug/', '/msal-browser.min.js'
    ];

    const isApiPath = apiPrefixes.some(prefix => pathname === prefix || pathname.startsWith(prefix));

    if (req.method === 'GET' && hasReactBuild) {
      const distRelative = pathname.replace(/^\/+/, '');
      const distCandidate = path.join(__dirname, 'public', 'dist', distRelative);
      if (distRelative && fs.existsSync(distCandidate) && fs.statSync(distCandidate).isFile()) {
        const ext = path.extname(distCandidate).toLowerCase();
        const mime = {
          '.js': 'application/javascript; charset=utf-8',
          '.css': 'text/css; charset=utf-8',
          '.json': 'application/json; charset=utf-8',
          '.svg': 'image/svg+xml',
          '.png': 'image/png',
          '.jpg': 'image/jpeg',
          '.jpeg': 'image/jpeg',
          '.ico': 'image/x-icon',
          '.map': 'application/json; charset=utf-8',
        }[ext] || 'application/octet-stream';
        res.writeHead(200, { 'Content-Type': mime });
        return fs.createReadStream(distCandidate).pipe(res);
      }

      if (!isApiPath && (pathname === '/' || pathname === '/index.html')) {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        return fs.createReadStream(reactIndex).pipe(res);
      }
    }

    if (pathname === '/' && req.method === 'GET') {
      const publicIndex = path.join(__dirname, 'public', 'index.html');
      const fallbackIndex = path.join(__dirname, 'knowledge-base.html');
      const filePath = fs.existsSync(publicIndex) ? publicIndex : fallbackIndex;
      if (!fs.existsSync(filePath)) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: 'index.html not found' }));
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return fs.createReadStream(filePath).pipe(res);
    }

    if (debug && pathname === '/debug/last-request' && req.method === 'GET') {
      return res.end(JSON.stringify(modularPool.getLastRequest() || lastRequest || {}));
    }
    if (pathname === '/msal-browser.min.js' && req.method === 'GET') {
      const msalPath = path.join(__dirname, 'msal-browser.min.js');
      if (!fs.existsSync(msalPath)) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: 'msal-browser.min.js not found' }));
      }
      res.writeHead(200, { 'Content-Type': 'application/javascript' });
      return fs.createReadStream(msalPath).pipe(res);
    }
    if (pathname === '/health' && req.method === 'GET') {
      // simple health check, verify Ollama is reachable and returns valid JSON
      let ollamaStatus = false;
      try {
        const resp = await fetch(`${OLLAMA}/api/tags`);
        if (resp.ok) {
          await resp.json();
          ollamaStatus = true;
        }
      } catch {}
      const atlassian = await checkAtlassianHealth(false);
      const health = {
        bridge: true,
        ollama: ollamaStatus,
        atlassian,
        ollamaUrl: config.bridge.ollamaUrl,
        model: DEFAULT_MODEL,
        groups: config.groups.length,
        postgres: false,
        graphObjects: 0,
        graphReady: false,
        age: false,
        ageGraph: null,
        ageVertices: 0
      };
      if (config.bridge && config.bridge.poolEnabled) {
        health.pool = modularPool.getPoolSnapshot();
      }
      // Add postgres graph connectivity info
      if (postgresEnabled && postgresUrl) {
        const graphInfo = await graphStore.getConnectivityInfo(postgresUrl);
        health.postgres = graphInfo.postgres;
        health.graphObjects = graphInfo.graphObjects;
        health.graphReady = graphInfo.graphReady;
        // Add AGE status from graphStore
        if (graphInfo.age !== undefined) {
          health.age = graphInfo.age;
          health.ageGraph = graphInfo.ageGraph || null;
          health.ageVertices = graphInfo.ageVertices || 0;
        }
      }
      res.end(JSON.stringify(health));

    } else if (pathname === '/pool-status' && req.method === 'GET') {
      // Connection pool status endpoint
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.end(JSON.stringify(modularPool.getPoolSnapshot(), null, 2));

    } else if (pathname === '/myip' && req.method === 'GET') {
      const ip = getPreferredIPv4();
      res.end(JSON.stringify({
        ip,
        port: PORT,
        uploadUrl: `http://${ip}:${PORT}/docs/upload`,
        jiraUploadUrl: `http://${ip}:${PORT}/jira/upload`
      }));

    } else if (await handleSemanticRoute(req, res, pathname, query)) {
      return;
    } else if (await handleJiraRoute(req, res, pathname, query)) {
      return;
    } else if (await handleOllamaRoute(req, res, pathname)) {
      return;
    } else if (await handleKnowledgeRoute(req, res, pathname, query)) {
      return;
    } else if (await handleDocsRoute(req, res, pathname, query)) {
      return;
    } else if (await handleDbRoute(req, res, pathname, query)) {
      return;

    } else if (pathname === '/groups' && req.method === 'GET') {
      const out = [];
      for (const g of config.groups) {
        const dbUser = modularPool.getDbUser(g.id);
        const { id, name, description, icon, color, schemas, readOnly, env } = g;
        out.push({ id, name, description, icon, color, schemas, readOnly, env: env || 'prod', dbUser: dbUser || null });
      }
      res.end(JSON.stringify(out));

    } else if (pathname === '/config' && req.method === 'GET') {
      // frontend bootstrap config (safe subset)
      res.end(JSON.stringify({
        bridge: {
          port: PORT,
          semanticWorkerPort: SEMANTIC_WORKER_PORT,
          ollamaUrl: OLLAMA,
          defaultModel: DEFAULT_MODEL,
          docsChunkSize: DOCS_CHUNK_SIZE,
          docsChunkOverlap: DOCS_CHUNK_OVERLAP,
          docsMaxResults: Number((config.bridge && config.bridge.docsMaxResults) || 10),
          answerWordCap: Number((config.bridge && config.bridge.answerWordCap) || 200),
          qaContextCharLimit: Number((config.bridge && config.bridge.qaContextCharLimit) || 12000),
          jiraMaxResults: Number((config.bridge && config.bridge.jiraMaxResults) || 10),
          jiraMaxTerms: Number((config.bridge && config.bridge.jiraMaxTerms) || 4),
          atlassianEnabled: ATLASSIAN_ENABLED,
          atlassianDomain: ATLASSIAN_DOMAIN,
          atlassianEmail: ATLASSIAN_EMAIL,
          atlassianProjectKeys: ATLASSIAN_PROJECT_KEYS,
          atlassianTokenSet: !!ATLASSIAN_TOKEN,
          msalClientId: MSAL_CLIENT_ID,
          msalTenantId: MSAL_TENANT_ID,
          sharepointSiteUrl: SP_SITE_URL
        },
        groups: config.groups || [],
        distributionCenters: config.distributionCenters || []
      }));

    } else if (pathname === '/config' && req.method === 'POST') {
      // runtime tuning of MCP sleep delays
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => {
        try {
          const obj = JSON.parse(body);
          const applied = {};
          const validateInt = (v,min,max,name) => {
            if (typeof v !== 'number' || !Number.isInteger(v)) throw new Error(`${name} must be an integer`);
            if (v < min || v > max) throw new Error(`${name} must be ${min}–${max}`);
          };
          if ('sleepAfterInit' in obj) {
            validateInt(obj.sleepAfterInit,100,2000,'sleepAfterInit');
            config.bridge.sleepAfterInit = obj.sleepAfterInit;
            applied.sleepAfterInit = obj.sleepAfterInit;
          }
          if ('sleepAfterNotification' in obj) {
            validateInt(obj.sleepAfterNotification,100,2000,'sleepAfterNotification');
            config.bridge.sleepAfterNotification = obj.sleepAfterNotification;
            applied.sleepAfterNotification = obj.sleepAfterNotification;
          }
          if ('sleepAfterToolCall' in obj) {
            validateInt(obj.sleepAfterToolCall,200,3000,'sleepAfterToolCall');
            config.bridge.sleepAfterToolCall = obj.sleepAfterToolCall;
            applied.sleepAfterToolCall = obj.sleepAfterToolCall;
          }
          if ('atlassianEnabled' in obj) {
            if (typeof obj.atlassianEnabled !== 'boolean') throw new Error('atlassianEnabled must be boolean');
            ATLASSIAN_ENABLED = obj.atlassianEnabled;
            config.bridge.atlassianEnabled = ATLASSIAN_ENABLED;
            applied.atlassianEnabled = ATLASSIAN_ENABLED;
            atlassianHealthCache = { ok: false, checkedAt: 0 };
          }
          if ('atlassianDomain' in obj) {
            if (typeof obj.atlassianDomain !== 'string') throw new Error('atlassianDomain must be string');
            ATLASSIAN_DOMAIN = String(obj.atlassianDomain || '').trim().replace(/^https?:\/\//i, '').replace(/\/$/, '');
            config.bridge.atlassianDomain = ATLASSIAN_DOMAIN;
            applied.atlassianDomain = ATLASSIAN_DOMAIN;
            atlassianHealthCache = { ok: false, checkedAt: 0 };
          }
          if ('atlassianEmail' in obj) {
            if (typeof obj.atlassianEmail !== 'string') throw new Error('atlassianEmail must be string');
            ATLASSIAN_EMAIL = String(obj.atlassianEmail || '').trim();
            config.bridge.atlassianEmail = ATLASSIAN_EMAIL;
            applied.atlassianEmail = ATLASSIAN_EMAIL;
            atlassianHealthCache = { ok: false, checkedAt: 0 };
          }
          if ('atlassianProjectKeys' in obj) {
            let keys = [];
            if (Array.isArray(obj.atlassianProjectKeys)) {
              keys = obj.atlassianProjectKeys;
            } else if (typeof obj.atlassianProjectKeys === 'string') {
              keys = obj.atlassianProjectKeys.split(',').map(s => s.trim()).filter(Boolean);
            } else {
              throw new Error('atlassianProjectKeys must be array or comma-separated string');
            }
            ATLASSIAN_PROJECT_KEYS = normalizeProjectKeys(keys);
            config.bridge.atlassianProjectKeys = ATLASSIAN_PROJECT_KEYS;
            applied.atlassianProjectKeys = ATLASSIAN_PROJECT_KEYS;
            atlassianHealthCache = { ok: false, checkedAt: 0 };
          }
          if ('atlassianToken' in obj) {
            if (typeof obj.atlassianToken !== 'string') throw new Error('atlassianToken must be string');
            ATLASSIAN_TOKEN = String(obj.atlassianToken || '').trim();
            config.bridge.atlassianToken = ATLASSIAN_TOKEN;
            applied.atlassianTokenSet = !!ATLASSIAN_TOKEN;
            atlassianHealthCache = { ok: false, checkedAt: 0 };
          }
          res.end(JSON.stringify({ ok: true, applied }));
        } catch (e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
    } else {
      res.writeHead(404);
      res.end(JSON.stringify({ error: 'not found' }));
    }
  } catch (err) {
    const status = err.httpStatus || 500;
    res.writeHead(status);
    res.end(JSON.stringify({ error: err.message }));
  }
});

// ===== Initialize PostgreSQL Knowledge Graph (PROMPT 32/33) =====
const postgresEnabled = config.bridge && config.bridge.postgresEnabled;
const postgresUrl = config.bridge && config.bridge.postgresUrl;

if (postgresEnabled && postgresUrl) {
  (async () => {
    try {
      await graphStore.initSchema(postgresUrl);
    } catch (err) {
      console.error('[GRAPH] Schema init failed:', err.message);
    }
  })();
}

server.listen(PORT, () => {
  const docsCount = countJsonFiles(DOCS_INDEX_DIR);
  const knowledgeCount = countJsonFiles(KNOWLEDGE_INDEX_DIR);
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║         Oracle Knowledge Base — Bridge Server            ║');
  console.log('╠══════════════════════════════════════════════════════════╣');
  console.log(`║  Bridge  : http://localhost:${PORT}`.padEnd(58) + '║');
  console.log(`║  Ollama  : ${OLLAMA}`.padEnd(58) + '║');
  console.log(`║  📚 Docs: ${docsCount} files | 🧠 Knowledge: ${knowledgeCount} entries`.padEnd(58) + '║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log('Groups loaded:');
  config.groups.forEach(g => {
    console.log(`  ${g.icon || '🗄'} ${g.name}  →  "${g.connectionName || ''}"`);
  });

  startSemanticWorkerIfNeeded().then(ok => {
    if (ok) {
      setTimeout(() => { runSemanticScan({ manual: false, reason: 'startup' }).catch(() => {}); }, 7000);
      setInterval(() => {
        runSemanticScan({ manual: false, reason: 'interval' }).catch(() => {});
      }, 5 * 60 * 1000);
    }
  }).catch(() => {});
});
