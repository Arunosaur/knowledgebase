# McLane WMS·IQ — GitHub Copilot Instructions (v17)

> Place this file at `.github/copilot-instructions.md` in your project root.
> Copilot will use it automatically as workspace context in VS Code.

---

## 🏁 Project Status

| Prompt | Feature | Status | Tests |
|--------|---------|--------|-------|
| 1 | `config.json` — real groups (MANP, OPCIGP, OMSP) | ✅ Done | — |
| 2 | `bridge.js` — MCP JSON-RPC client, all `/db/*` endpoints | ✅ Done | A1–A5 |
| 3 | `knowledge-base.html` — group picker + main app | ✅ Done | — |
| 4 | Connection health check UI on group picker cards | ✅ Done | — |
| 5 | Offline snapshot export (throttled, cancellable) | ✅ Done | — |
| 6 | ERD diagram view (SVG, spring layout, pan/zoom) | ✅ Done | — |
| 7 | `debug-mcp.js` — standalone MCP handshake tester | ✅ Done | — |
| 8 | `--debug` flag + `/debug/last-request` endpoint | ✅ Done | — |
| 9 | MCP connection pool (FIFO queue, idle timeout, reconnect) | ✅ Done | C3 |
| 10 | Security: CORS allowlist, token auth, rate limiting, validation | ✅ Done | B1–B8 |
| 11 | UX polish: skeletons, 503/429/401 handling, export modal | ✅ Done | — |
| 12 | Settings panel + `POST /config` endpoint | ✅ Done | D1–D2 |
| 13 | `start.sh` / `stop.sh` + `test-bridge.js` | ✅ Done | all |
| 14 | Documentation export — PDF with AI descriptions & ERD | ✅ Done | A6 |
| 15 | Cross-schema impact analysis — graph, trees, process flow, export | ✅ Done | E1–E4 |
| 16 | SharePoint ingestion — OAuth, Graph API, text extraction, local index | ✅ Done | F1–F4 |
| 17 | Document-aware Q&A + Docs library tab | ✅ Done | — |
| 18 | Rebrand to McLane WMS·IQ + two-mode entry (Ask / Explore) | ✅ Done | — |
| 19 | DC-aware business user Q&A + data-level query execution | ✅ Done | G1–G4 |
| 20 | Power Automate document ingestion via POST /docs/upload | ✅ Done | — |
| 21 | Unified cross-group search | ✅ Done | H1–H2 |
| 22 | Schema diff / change detection | ✅ Done | — |
| 23 | UT DC activation (September 2026) | 🔜 Future | — |
| 24 | Knowledge capture system + fine-tuning dataset export | ✅ Done | I1–I5 |
| 25 | Schema context injection — source-grounded Q&A | ✅ Done | — |
| 26 | Connected user display (whoami) on group cards + settings | ✅ Done | — |
| 27 | Production guardrail + env classification + Compare UX | ✅ Done | — |
| 28 | Atlassian JIRA integration — live search, Q&A pills, knowledge capture | ✅ Done | J1–J3 |
| 28b | Lightweight modularization + static file server | ✅ Done | — |
| 29 | Self-building semantic layer — background AI discovery | ✅ Done | K1–K4 |
| 30 | Power Automate JIRA→Knowledge pipeline | ✅ Done | — |
| 31 | Demo polish — dashboard, sample questions, answer quality, demo mode | ✅ Done | UI1–UI3 |

**Test suite:** `node test-bridge.js` → **42/42 passing** (A1–A6, B1–B8, C1–C3, D1–D2, E1–E4, F1–F4, G1–G4, H1–H2, I1–I5, J1–J3)
> ✅ E1 now passes consistently — was using SYS.DUAL (infinite BFS) now uses DOCK_PK depth=1 (deterministic)
> ℹ️ App served at `http://localhost:3333`. `file://` still works as fallback.
> ℹ️ JIRA search uses `textfields ~ "term*"` JQL with stop word filtering — max 4 meaningful terms, AND-joined.

**Live DBs:** All 5 groups confirmed connected over VPN as ASRAJAG
**Atlassian:** Connected as arun.rajagopalan@mclaneco.com @ mclane.atlassian.net ✅

---

## ✅ Confirmed Environment Facts

| Item | Value | Notes |
|------|-------|-------|
| SQLCL command | `sql` | On `$PATH` on macOS |
| MCP args | `["-R", "2", "-mcp"]` | Restrict level 2, MCP mode |
| Bridge port | `3333` | Default, no conflicts |
| Ollama URL | `http://localhost:11434` | Local, default port |
| Ollama model | `llama3:latest` (8B Q4_0) | Confirmed working |
| Other models | `mistral`, `phi`, `tinyllama` | Selectable in Q&A model dropdown |
| Ollama health check | Uses `/api/tags` not `/api/models` | `/api/models` does not exist |
| Ollama response format | NDJSON streaming by default | Must use `stream: false` in ALL /ollama/chat calls |
| Ollama timeout | 60s AbortController | Prevents Q&A hang on large prompts |
| Connection: Manhattan Main | `MANP` | ASRAJAG @ MANP — 9 DCs, env: prod |
| Connection: Manhattan CK | `MAN002P` | ASRAJAG @ MAN002P — C1 C2 C3, env: prod |
| Connection: Manhattan WK | `MAN001P` | ASRAJAG @ MAN001P — DC490, env: prod |
| Connection: WMSHUB | `OMSP` | ASRAJAG @ OMSP — all 13 DCs, env: prod |
| Connection: CIG WMS / OP | `OPCIGP` | ASRAJAG @ OPCIGP — all DCs, env: prod |
| Connected user | ASRAJAG | Same user across ALL 5 databases |
| DBA_SOURCE access | All 5 DBs: ✅ full | Confirmed via queryDB() |
| Critical rule | Always use queryDB() | Raw runMCP() without connect returns [] |
| Network path | On-prem Oracle over VPN | Sub-500ms response times confirmed |
| Sleep: after initialize | `400ms` | Do not reduce — VPN latency sensitive |
| Sleep: after notification | `400ms` | Do not reduce |
| Sleep: after each tool call | `600ms` | Critical: connect must finish before run-sql |
| Test suite | test-bridge.js | 41/41 passing |
| Pool shape | queue-based FIFO, not busy-flag | Concurrent requests serialise per group |
| OS | macOS | `start.sh` uses `open` to launch browser |
| MSAL | Blocked by McLane Azure AD (AADSTS65002) | Use Power Automate POST /docs/upload instead |
| Oracle views | Use `DBA_*` not `ALL_*` | ASRAJAG has DBA privileges in all databases |
| LAST_DDL_TIME | Removed from /db/objects | Caused CSV parse failures — do NOT add back |
| SQLcl footer filter | Strip "N rows selected." variants | parseMCPResult() must filter these |
| JSON encoding | UTF-8 | `Content-Type: application/json; charset=utf-8` on all responses |
| POST /db/query | SELECT-only, max 50 rows | Rejects all DDL/DML |
| /db/source | Use queryDB(), fetch PACKAGE BODY | Raw runMCP() returns [] |
| /db/whoami | Returns dbUser + dbName | queryDB() with SYS_CONTEXT |
| POST /docs/upload | Verified working end-to-end | base64→text→chunk→docs-index JSON |
| /docs/search | Verified working | Returns chunks with score, fileId, text |
| /docs/status | Public endpoint (no auth) | Power Automate mode — returns 200 |
| /docs/list | Public endpoint (no auth) | Power Automate mode — returns 200 |
| GET /db/search-all | Verified working | Fans out to all 5 groups, dedupes |
| Group env field | All 5 current groups: "env": "prod" | Future UAT/test: "env": "uat" or "env": "test" |
| Production guardrail | Q&A + data queries: prod only | Compare modal is the ONLY exempt feature |
| Atlassian domain | mclane.atlassian.net | Confirmed working |
| Atlassian email | arun.rajagopalan@mclaneco.com | Confirmed working |
| Atlassian auth | Basic auth (email:apiToken) | NOT MCP OAuth — REST API only |
| Atlassian health | GET /rest/api/3/myself → 200 | Returns atlassian:true in /health |
| JIRA projects | Manhattan, Help Desk Support, Change Management + more | atlassianProjectKeys:[] = all |
| JIRA JQL pattern | `textfields ~ "term*" AND textfields ~ "term2*"` | Stop words filtered, max 4 terms, wildcards added |
| JIRA stop words | are,there,any,is,the,a,to,for,issues,related... | Full list in lib/jira-routes.js STOP_WORDS set |
| JIRA prompt cap | 12,000 chars total context | Schema+docs+knowledge+JIRA truncated proportionally |
| User personas | Business (Ask mode) vs Technical (Explore Ask AI) | Different AI prompt rules per persona |

---

## 🏢 Project Overview

**McLane WMS·IQ** is a unified knowledge and intelligence platform for the McLane WMS ecosystem.

- **Product name:** McLane WMS·IQ
- **Short form:** WMS·IQ
- **Tagline:** Navigate the McLane WMS landscape
- **Scope:** WMS only — NOT Financials, ERP, eCommerce, MDM, Data Intelligence

### Two Entry Modes
1. **💬 Ask a Question** — business user, DC-aware, plain English answers ✅
2. **🔧 Explore Systems** — technical, schema browser, ERD, impact analysis ✅

### User Persona Rules
| | Business User (Ask mode) | Technical User (Explore Ask AI) |
|---|---|---|
| JIRA results | Plain English summaries, no ticket keys | Full keys, summaries, status |
| DB results | Business impact only, no schema names | Raw data, row counts, schema names |
| INVALID objects | Never shown | Shown with full detail |
| Schema names | Never mentioned | Full technical context |
| Ticket keys | Hidden — "a known issue was reported" | Shown e.g. SDHD-2562205 |
| Answer tone | Plain business English | Technical detail |

---

## 🗄️ Database Connections & Groups (5 Groups)

```
MANP      → Manhattan (Main)      — FE MD MK MN MY MZ NE SE FS06 + MANH/MANH_CODE  [env: prod]
MAN002P   → Manhattan (CK)        — C1 C2 C3 + MANH/MANH_CODE                       [env: prod]
MAN001P   → Manhattan (Bluegrass) — MAN490 + MANH/MANH_CODE                          [env: prod]
OMSP      → WMSHUB                — WMSHUB WMSHUB_CODE EMS EMS_CODE                  [env: prod]
OPCIGP    → CIG WMS / OP          — CIGWMS CIGWMS_CODE OP OP_CODE MCLANE MCLANE_CODE FRAMEWORK [env: prod]
```

All connections: **ASRAJAG** as Oracle session user.

### Group config shape
```json
{
  "id": "manhattan-main",
  "name": "Manhattan (Main)",
  "db": "MANP",
  "env": "prod",
  "color": "#3fb950",
  "schemas": ["FE_DM", "FE_MDA", "...", "MANH", "MANH_CODE"]
}
```

---

## 🏭 Distribution Centers

### 13 Manhattan + WMSHUB DCs

| DC_ID | Code | Name | Type | DB | DM Schema | MDA Schema |
|-------|------|------|------|----|-----------|------------|
| 606 | 06 | Lakeland 606 | **food-service** | MANP | FS06_DM | FS06_MDA |
| 290 | FE | McLane Ocala | grocery | MANP | FE_DM | FE_MDA |
| 160 | MD | Dothan | grocery | MANP | MD_DM | MD_MDA |
| 360 | MK | Cumberland | grocery | MANP | MK_DM | MK_MDA |
| 460 | MN | Minnesota | grocery | MANP | MN_DM | MN_MDA |
| 260 | MY | NE/Concord | grocery | MANP | MY_DM | MY_MDA |
| 450 | MZ | Mid-Atlantic | grocery | MANP | MZ_DM | MZ_MDA |
| 800 | NE | Northeast | grocery | MANP | NE_DM | NE_MDA |
| 400 | SE | Southeast | grocery | MANP | SE_DM | SE_MDA |
| 421 | C1 | CK Otsego | grocery | MAN002P | C1_DM | C1_MDA |
| 431 | C2 | CK St Louis | grocery | MAN002P | C2_DM | C2_MDA |
| 411 | C3 | CK Columbus | grocery | MAN002P | C3_DM | C3_MDA |
| 490 | WK | McLane Bluegrass | grocery | MAN001P | MAN490_DM | MAN490_MDA |

### 19 Non-Manhattan DCs (CIG WMS / OP only — filter by LEG_DIV_ID)

| Code | Name | Code | Name |
|------|------|------|------|
| GA | MIW GA | MW | Western |
| GM | McLane Interstate | NC | Carolina |
| HP | High Plains | NT | North Texas |
| ME | Suneast | NW | Northwest |
| MG | McLane Ohio | PA | McLane PA |
| MI | Midwest | SO | Southern |
| MO | McLane Ozark | SW | Southwest |
| MP | Pacific | SZ | So. Calif. |
| MS | Sunwest | WJ | New Jersey |
| UT | Salt Lake City *(Manhattan Sept 2026)* | | |

---

## ⚠️ Critical Architecture Notes

### #1 — Always use queryDB() never raw runMCP()
```javascript
// WRONG — returns [] even when data exists
const responses = await runMCP([{ name: 'run-sql', arguments: { sql } }]);
// RIGHT
const rows = await queryDB(group, sql);
```

### #2 — DBA_* views for all queries
```
DBA_OBJECTS  DBA_TAB_COLUMNS  DBA_SOURCE
DBA_CONSTRAINTS  DBA_CONS_COLUMNS  DBA_DEPENDENCIES
```

### #3 — Ollama stream: false always + 60s timeout
```javascript
const controller = new AbortController();
const timeoutId = setTimeout(() => controller.abort(), 60000);
const response = await fetch(BRIDGE + '/ollama/chat', {
  method: 'POST', signal: controller.signal,
  body: JSON.stringify({ model, messages, stream: false })
});
clearTimeout(timeoutId);
```

### #4 — Ollama health uses /api/tags
`ollama: true` = reachable, not model-specific.

### #5 — verifyOllamaReady()
```javascript
async function verifyOllamaReady() {
  try {
    const r = await fetch(BRIDGE + '/health');
    const h = await r.json();
    return h.ollama === true;
  } catch(e) { return false; }
}
```

### #6 — JSON UTF-8 encoding
```javascript
res.setHeader('Content-Type', 'application/json; charset=utf-8');
```

### #7 — Source context injection
- Fetch PACKAGE BODY (not PACKAGE) for implementation
- enrichQAContext() auto-fetches objects via queryDB() if empty
- Shape B assembly: `data.map(r => Object.values(r)[0] || '').join('')`
- Inject 80,000+ chars into system prompt with anti-hallucination rules

### #8 — autoTag() DC false positive prevention
```javascript
if (textRaw.includes(' ' + dc.code + ' ') ||
    textRaw.includes(' ' + dc.code + ',') ||
    text.includes(dc.name.toLowerCase())) {
  detectedDCs.push(dc.code);
}
```

### #9 — AI hallucination prevention
- Always inject /db/source (PACKAGE BODY) into system prompt
- Always inject only real JIRA issues from jiraResults.issues array
- Never render ticket keys not present in jiraResults
- Explicit prompt rule: "Only reference JIRA ticket keys that appear in the above list. Do NOT invent ticket numbers."
- When context is empty (0 JIRA + 0 knowledge hits): respond with "I don't have enough information" — never invent

### #10 — Knowledge index data model
```json
{
  "id": "ke-{timestamp}-{random}",
  "type": "qa|process|integration|dc-specific|troubleshooting",
  "question": "...", "answer": "...",
  "context": {
    "group": "manhattan-main", "schemas": ["MANH_CODE"],
    "dcCodes": [], "systems": ["manhattan"], "objects": ["DOCK_PK"],
    "jiraIssues": ["MANH-2284", "SDHD-2562205"]
  },
  "tags": ["dock_pk", "shipment", "manhattan"],
  "quality": 3, "source": "ai-verified",
  "capturedBy": "asrajag", "approved": true
}
```
Quality: 1=needs review, 2=good, 3=gold standard
jiraIssues: optional — auto-populated from JIRA results when saving Q&A answer

### #11 — Cross-group search (PROMPT 21)
```javascript
// GET /db/search-all?keyword=<k>
// Fans out to all 5 groups in parallel, 30s timeout
// Dedupes by group+name+type
// Returns: { results, queryMs, groupsSearched, totalResults, timedOut }
```

### #12 — Production guardrail ⚠️ CRITICAL
Every group has `"env"` field. Bridge exposes it in `GET /groups`.
- Q&A, DC resolver, POST /db/query: **ONLY** `env === "prod"` groups
- Compare modal: exempt — may use any group
- Non-prod selected in Compare: show "⚠ includes non-production databases"
- Ask mode bootstrap: shows "⏳ Connecting to WMS·IQ..." while groups load
```javascript
const prodGroups = config.groups.filter(g => g.env === 'prod');
```

### #13 — JIRA integration (PROMPT 28)
- Uses Atlassian REST API with Basic auth (NOT MCP OAuth)
- Auth: `Buffer.from(email + ":" + token).toString("base64")`
- Health check: GET /rest/api/3/myself → 200 = atlassian:true
- Search: GET /rest/api/3/issue/search with JQL
- Max 5 issues injected into prompt, summary+key+status only
- Full description NOT injected unless user asks about specific ticket key
- Total prompt cap: 12,000 chars (schema+docs+knowledge+JIRA proportional)
- JIRA results verified before rendering — never show keys not in jiraResults

### #14 — User persona rules (PROMPT 28)
Two personas based on entry mode:
- **Business** (Ask a Question): plain English, no schema names, no ticket keys, no INVALID objects, translate everything to business impact
- **Technical** (Explore → Ask AI): full technical detail, schema names, ticket keys, INVALID status, raw data
Inject persona into every system prompt before context.

### #15 — Q&A context assembly order
For every Q&A question, run ALL of these in parallel:
1. GET /knowledge/search → knowledge hits (🧠)
2. GET /docs/search → document chunks (📄)
3. GET /jira/search → JIRA tickets (🎫) — only if atlassianEnabled
4. POST /db/query → live DB findings (🗄️) — only if issue/problem keywords + prod group resolved
Then assemble system prompt with all results, capped at 12,000 chars.
Show pills: 🧠 N knowledge · 📄 N docs · 🎫 N tickets · 🗄️ N DB results

### #16 — Classifier rules (PROMPT 28 fix)
classifyQuery() intent detection:
- Issue/problem guard: "issues", "problem", "error", "failing", "what happened", "why is", "how does" → always route to Q&A, never Explore
- Schema routing ONLY on explicit technical terms: "table", "column", "package", "procedure", "index", "view", "constraint", "trigger", "synonym"
- Cross-DC phrases: "across all DCs", "all distribution centers", "every DC" → cigwms-prod then wmshub-prod
- Default fallback: Q&A mode (never Explore)
- Ask mode NEVER navigates to Explore mode — safety hardened

---

## 🗺️ Architecture

```
knowledge-base.html  (file:// in browser)
        │
        ▼
  Mode Selector
  ├── 💬 Ask a Question  [BUSINESS PERSONA]
  │     ⏳ Connecting... while groups load
  │     → DC resolver → classifier → routing
  │     → ONLY env="prod" groups ← GUARDRAIL
  │     → Parallel context fetch:
  │         /knowledge/search  → 🧠 pill
  │         /docs/search       → 📄 pill
  │         /jira/search       → 🎫 pill (if enabled)
  │         /db/query (issues) → 🗄️ pill (if issue keywords)
  │     → enrichQAContext() → PACKAGE BODY source
  │     → System prompt: BUSINESS persona rules
  │       + 12,000 char cap on all context
  │     → Ollama (stream:false, 60s timeout)
  │     → Answer in plain English
  │     → Related tickets section (plain English, no keys)
  │     → Live data section (business impact language)
  │     → [💾 Save] → knowledge entry + jiraIssues captured
  │
  └── 🔧 Explore Systems → Group Picker
        → Main App [TECHNICAL PERSONA]
        Tabs: ⚡Impact | Home | 💬Ask AI | 📚Docs | 🧠Knowledge
                    │
        Global search: [🔍 This group] [🔍 All systems]
        ⟷ Compare → background diff job
          Source/Target: Group + Schema + Type
          Status bar: stage + % + cancel
          History: 🕐 session-only, download/retry/delete
                    │
                    ▼
bridge.js  (http://localhost:3333)
        │
        ├── GET /groups          → includes env field
        ├── GET /db/whoami       → SYS_CONTEXT user
        ├── GET /db/search       → single group search
        ├── GET /db/search-all   → fan-out all 5 groups
        ├── POST /db/query       → SELECT-only safety gate
        ├── GET /jira/search     → Atlassian REST API + Basic auth
        ├── GET /jira/issue/:key → full issue detail
        ├── GET /jira/projects   → project list for settings
        ├── POST /docs/upload    → Power Automate ingestion
        │   docs-index/          → document chunks
        ├── /knowledge/*         → CRUD + export
        │   knowledge-index/     → training data + jiraIssues
        └── POST /ollama/*       → stream:false
                │
                ▼
          Oracle (5 DBs — all ASRAJAG, all env:prod)
          MANP / MAN002P / MAN001P / OMSP / OPCIGP
          +
          Atlassian REST API (mclane.atlassian.net)
          Basic auth: arun.rajagopalan@mclaneco.com
```

---

## 📋 Current config.json

```json
{
  "bridge": {
    "port": 3333,
    "ollamaUrl": "http://localhost:11434",
    "defaultModel": "llama3",
    "sqlclCommand": "sql",
    "sqlclArgs": ["-R", "2", "-mcp"],
    "mcpClientName": "oracle-kb-bridge",
    "mcpModelName": "bridge",
    "poolEnabled": true,
    "poolIdleTimeoutMs": 300000,
    "poolMaxQueueDepth": 20,
    "toolCallTimeoutMs": 15000,
    "sleepAfterInit": 400,
    "sleepAfterNotification": 400,
    "sleepAfterToolCall": 600,
    "rateLimitPerMinute": 120,
    "allowedOrigins": ["null", "file://", "http://localhost:3333"],
    "authToken": "",
    "atlassianEnabled": true,
    "atlassianDomain": "mclane.atlassian.net",
    "atlassianEmail": "arun.rajagopalan@mclaneco.com",
    "atlassianToken": "<set>",
    "atlassianProjectKeys": [],
    "docsIndexDir": "./docs-index",
    "uploadToken": ""
  },
  "distributionCenters": [ "... 33 entries ..." ],
  "groups": [
    { "id": "manhattan-main", "env": "prod", ... },
    { "id": "manhattan-ck",   "env": "prod", ... },
    { "id": "manhattan-wk",   "env": "prod", ... },
    { "id": "wmshub-prod",    "env": "prod", ... },
    { "id": "cigwms-prod",    "env": "prod", ... }
  ]
}
```

## 🗺️ POC vs Production Roadmap

### Current Phase — POC (MacBook, single user)
Goal: demonstrate value to McLane stakeholders.
Keep changes minimal and safe. Do NOT introduce
TypeScript, React, PostgreSQL, or Docker until
after stakeholder approval.

```
✅ PROMPTS 1–28   Core features complete
🔜 PROMPT 28b    Modularization + static server (lightweight)
🔜 PROMPT 29     Self-building semantic layer (Python + JSON)
🔜 PROMPT 30     Power Automate JIRA pipeline
🔜 PROMPT 31     Demo polish — one-command start, sample questions
─────────────────────────────────────────────
SHOW TO McLane IT / Management
─────────────────────────────────────────────
```

### Post-Approval Phase — Production (on-prem VM or cloud)
Only after stakeholder sign-off. IT involvement required.

```
PROMPT 32   TypeScript migration (bridge.js → lib/*.ts)
PROMPT 33   PostgreSQL + pgvector (replaces all JSON files)
PROMPT 34   Docker Compose (frontend + bridge + worker + postgres + ollama)
PROMPT 35   React + TypeScript frontend
PROMPT 36   LDAP authentication (multi-user, McLane AD)
            Note: Azure AD SSO requires IT approval (AADSTS65002 blocks direct)
            LDAP is the proven fallback — already used in another McLane POC
PROMPT 37   Production hardening (rate limits, audit log, backup)
```

### Deployment Architecture (post-approval target)
```
┌─────────────────────────────────────────────────────┐
│                   Docker Compose                     │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │  frontend   │  │ bridge (API) │  │  semantic  │ │
│  │  Nginx      │  │ TypeScript   │  │  worker    │ │
│  │  React app  │──│ Node.js      │  │  Python    │ │
│  │  port 80    │  │ port 3333    │  │  port 3334 │ │
│  └─────────────┘  └──────┬───────┘  └─────┬──────┘ │
│                          │                │         │
│  ┌───────────────────────▼────────────────▼──────┐  │
│  │  PostgreSQL + pgvector  (port 5432)           │  │
│  │  Replaces: docs-index/ knowledge-index/       │  │
│  │            semantic-index/ JSON files         │  │
│  └───────────────────────────────────────────────┘  │
│  ┌─────────────────────────────────────────────┐    │
│  │  Ollama (port 11434) — GPU preferred        │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
         │ VPN / McLane Network
         ▼
   Oracle on-prem (5 DBs) + mclane.atlassian.net
```

### Technology Decisions (locked)
| Layer | POC (now) | Production (later) |
|---|---|---|
| Bridge | Node.js vanilla JS | TypeScript |
| Frontend | Single HTML file | React + TypeScript |
| Semantic engine | Python Flask | Python + pgvector |
| Storage | JSON files on disk | PostgreSQL |
| Container | None | Docker Compose |
| Auth | None (local only) | LDAP → Azure AD |
| LLM local | Ollama llama3 8B | Ollama (GPU server) |
| LLM cloud | Claude API | Claude API |
| Process mgr | None | PM2 → Rust (future) |

### Rust consideration
Rust is NOT introduced in POC phase. Revisit for:
- SQLcl MCP process manager (child process management)
- High-throughput semantic diff engine
Only after production architecture is stable.

---

---

### PROMPT 29 — Self-Building Semantic Layer 🔜 NEXT

```
Read .github/copilot-instructions.md in full before writing anything.

Build a background AI discovery process that automatically learns 
business intent → table/procedure mappings from existing sources
and refines them through usage and technical user confirmation.

━━━ SEMANTIC INDEX ━━━
New file: semantic-index/intents.json
Shape per entry:
{
  "id": "si-{timestamp}-{random}",
  "intent": "check if load is waved",
  "keywords": ["waved", "wave status", "load waved"],
  "package": "WAVE_PK",
  "procedure": "check_wave_status",
  "tables": ["WAVE_HDR", "WAVE_DTL"],
  "columns": ["WAVE_STATUS", "LOAD_NBR"],
  "schemas": ["SE_DM", "FE_DM"],
  "dcSpecific": false,
  "confidence": 0.87,
  "source": "package-analysis|sharepoint|jira|usage|confirmed",
  "confirmed": false,
  "confirmedBy": null,
  "usageCount": 0,
  "lastUsed": null,
  "sqlTemplate": "SELECT WAVE_STATUS FROM {schema}.WAVE_HDR WHERE LOAD_NBR = {input}"
}

━━━ BRIDGE ENDPOINTS ━━━
GET  /semantic/list          → all intents
GET  /semantic/search?q=     → match by keyword
POST /semantic/entry         → create/update entry
DELETE /semantic/entry/:id   → remove entry
GET  /semantic/stats         → counts by source/confidence/confirmed
POST /semantic/confirm/:id   → mark confirmed, set confirmedBy

━━━ BACKGROUND DISCOVERY ENGINE ━━━
Runs in bridge.js as a background worker (non-blocking).

Sources scanned in order:
1. Oracle packages/procedures (all prod groups, MANH_CODE + FRAMEWORK first)
   - Read PACKAGE BODY source via queryDB()
   - Send to Ollama: extract business intent, tables, columns, procedures
   - Ollama prompt: "Given this PL/SQL source, list all business operations
     this code performs. For each: intent in plain English, tables read/written,
     key columns, input parameters. Return JSON only."
2. docs-index/ — SharePoint documents as they are added
3. knowledge-index/ — existing confirmed Q&A entries
4. JIRA resolved tickets — via GET /jira/search?status=resolved

Scheduling:
- Runs on bridge startup (low priority, after pool init)
- Runs when CPU is idle (check every 5 minutes, skip if active queries)
- Manual trigger: POST /semantic/scan
- User can PAUSE via POST /semantic/pause and RESUME via POST /semantic/resume
- Scans run in parallel: max 2 concurrent group scans
- VPN-aware: if a group times out, skip and retry next cycle

Progress tracking:
- GET /semantic/scan-status → { running, paused, progress, lastScan,
    scanned: {groups, docs, tickets}, pending: N, confirmed: N }

━━━ Q&A INTEGRATION ━━━
Before routing any Q&A question:
1. GET /semantic/search?q=<question>
2. If confirmed match (confidence > 0.8, confirmed:true):
   - Run sqlTemplate via POST /db/query
   - Inject result as primary context
   - Business user: plain English answer from real data
   - Technical user: show SQL + raw result
3. If unconfirmed match (confidence > 0.5, confirmed:false):
   - Use it but prepend caveat:
     Business: "Based on my best understanding of the system..."
     Technical: "⚠ Unconfirmed mapping (confidence: 87%)"
4. If no match: fall through to existing JIRA+knowledge+schema flow

━━━ NEW TAB — 🔬 Semantic (Explore Systems only) ━━━
Tab shows:
  Left panel: list of all semantic intents
    - Filter: all | confirmed | unconfirmed | by source
    - Each entry: intent text + confidence dot + confirmed badge
    - 🟢 confirmed  🟡 unconfirmed  🔴 low confidence
  Right panel: intent detail editor
    - Intent text, keywords, package, procedure, tables, columns
    - SQL template with test button (runs live query)
    - [✓ Confirm] [✗ Reject] [✏ Edit] buttons
    - Usage count + last used

Confirmation UX:
  - Badge on 🔬 Semantic tab when unconfirmed items exist: 🔬 12
  - Periodic notification (once per session, not more):
    "12 semantic mappings await your review"
  - Technical user can confirm/reject/edit in the tab
  - Confirmed mappings immediately used for business Q&A

Scan status panel (top of Semantic tab):
  [● Scanning...  MANH_CODE 47%  ⏸ Pause]
  or
  [⏸ Paused  Resume ▶]
  Last scan: 13 Mar 2026 18:30 · 247 intents discovered · 34 confirmed

━━━ USAGE REFINEMENT ━━━
After every Q&A answer that used a semantic mapping:
  - Increment usageCount on the matched intent
  - If user clicks 👍: boost confidence += 0.05 (max 1.0)
  - If user clicks 👎: reduce confidence -= 0.1, flag for review
  - After 5 👍 on unconfirmed: auto-promote to confirmed

━━━ SETUP.md ━━━
Add section: Semantic Layer
  - What it does
  - How to pause/resume scan
  - How to confirm mappings in the 🔬 Semantic tab
  - How confidence scores work

━━━ test-bridge.js section K ━━━
K1: GET /semantic/list → 200, has intents array
K2: POST /semantic/confirm/:id → 200, confirmed:true
K3: GET /semantic/scan-status → 200, has running + paused fields
K4: POST /semantic/pause → 200; POST /semantic/resume → 200

Do not change test sections A–J.
```

---

### PROMPT 30 — Power Automate JIRA→Knowledge Pipeline ✅
```
POST /jira/upload endpoint in lib/jira-routes.js
GET /jira/upload-token → { token: "" } (empty = no auth)

Upload behavior:
  Resolved or Closed → ingest into docs-index as chunks
    source: "jira-power-automate"
    fileId: "jira-{key}"
    Returns: { ok, ingested:true, key, chunkCount, wordCount }
  Non-resolved → skip
    Returns: { ok, ingested:false, reason:"status-not-resolved-or-closed" }

Docs tab UI additions:
  🎫 JIRA Power Automate Sync card alongside Power Automate card
    Last ingested: filename — date
    Total JIRA docs: N files in docs-index
    Upload token: ● Set / ○ Not set
    [↗ View in Docs] → filters library to jira-power-automate source
    Clear filter button to return to all docs

Power Automate flow (PROMPT 30):
  Trigger: JIRA issue status → Resolved/Done
  Action: POST http://[mac-ip]:3333/jira/upload
  Body: { key, summary, status, description, resolution,
          project, assignee, updated, url }

Verified: SDHD-2624364 ingested, shows in Docs library ✅

Semantic scanner improvements (added alongside PROMPT 30):
  Auto-pause after full scan:
    paused=True, pauseReason="auto-paused after full scan"
    Status bar: "⏸ Auto-paused after full scan [▶ Rescan]"
    Distinct amber styling vs manual pause
    resume clears pauseReason
  Bulk confirm in Semantic tab:
    [✓ Confirm all ≥] [90% ▾] toolbar button
    Threshold options: 70%, 80%, 90%, 95%, 100%
    Dialog: "Confirm N intents with confidence ≥ X%?"
    Live progress: "Confirming N intents... i/N"
    Toast: "✅ N intents confirmed"
    Badge updates immediately

Verified: 118 intents discovered, bulk confirm at 90%
  reduced unconfirmed badge to 40 ✅
  CIG WMS / OP FRAMEWORK schema scanned ✅
```

---

### PROMPT 31 — Demo Polish ✅
```
Target audience: McLane IT Management
Goal: impress with breadth across all 33 DCs + 5 groups

start.sh improvements:
  - Prerequisite checks: node, python3, sql, ollama, VPN
  - Starts semantic worker (.venv) + bridge
  - Waits for /health → bridge:true
  - Prints clean startup summary with all service status
  - chmod +x fix: always set execute permissions

Home tab dashboard (WMS Intelligence Overview):
  - Live stats: 5 groups, 32 DCs, 43 schemas, 204,500 objects
    856 packages, 23,705 source units, docs, knowledge, intents
  - Recent activity: last 3 knowledge entries + last 3 JIRA tickets
  - 🏛️ Governance quick actions:
    [Find INVALID objects] [Compare schema drift]
    [Run impact analysis] [Export full documentation]
  - [🔄 Refresh] button with spinner

Sample questions — randomized 4 from pool each load:
  Business: wave processing, variance tickets, dock assignment,
    receiving process, WMSHUB integration, load sequence,
    shipment delays, dock scheduling
  Technical (Explore Ask AI): package dependencies, INVALID
    objects, cross-group schemas, impact analysis

Answer quality improvements:
  - Filler phrase stripping: removes "I'm happy to help",
    "Great question", "Certainly", "As an AI" etc
  - 200 word cap + [Read more] toggle
  - Source attribution: "Answer based on: 🎫 N · 🧠 N · 📄 N"
  - Direct one-sentence opening for business answers
  - DC list formatting for multi-DC answers

Friendly error messages:
  "fetch failed" → "Cannot reach database. Check VPN."
  "No response generated" → "Not enough info found. Try rephrasing."
  "401" → "Session expired. Please refresh."
  "503" → "System busy. Try again shortly."

Demo mode (Settings toggle):
  - Page title: "WMS·IQ (Demo)"
  - Yellow banner: "🎭 Demo mode active — changes will not be saved"
  - Disables: knowledge save, semantic confirm/reject,
    POST /db/query writes
  - All read operations work normally
  - Settings X button works regardless of demo mode state
  - Banner appears immediately on toggle

Auto-routing for DC data queries (NO user choice dialogs):
  Business user asks about a DC → system decides automatically:
    Manhattan DCs → dc.dmSchema in dc.manhattanGroup
    CIG DCs → cigwms-prod with LEG_DIV_ID filter
    No DC → manhattan-main MANH default
  Internal system names (Manhattan, WMSHUB, SE_DM) 
  NEVER shown to business users

Date-aware shipment queries with automatic fallback:
  "today" → "this week" → "this month" → all recent
  Shows fallback note when widening window
  ORDER BY date DESC, FETCH FIRST 5 ROWS ONLY

Verified UI smoke tests (live Oracle data):
  UI1: "Show me today's shipments for SE DC"
    → Fallback to this week: Shipments 68787-68792 ✅
  UI2: "What was the last shipment for MD this week?"
    → Shipments 67954-67958, March 14 2026 ✅
  UI3: "How many shipments did NE DC have this month?"
    → Shipments 125126-125130, March 13-14 2026 ✅
  No internal system names exposed in any response ✅
```

---

---

## 📜 Prompt History (v1–v28)

> Preserved as rebuild kit. Do NOT re-run unless regenerating from scratch.

### PROMPT 1–26 — See v12 instructions for full history ✅

### PROMPT 27 — Production Guardrail + env Classification + Compare UX ✅
```
- "env": "prod" on all 5 groups in config.json
- Bridge exposes env in GET /groups (bridge.js:952-959)
- Q&A, DC resolver, POST /db/query: only env="prod" groups
- Compare modal exempt — shows ⚠ warning for non-prod
- Background diff job: modal closes immediately, status bar at bottom
  Stages: 0–25% fetch source → 25–50% fetch target →
          50–75% compare → 75–99% build report
- Large object hint: shows count if 8000+ objects
- Success: green bar + toast + 60s auto-dismiss
- Failure: red bar + Retry (reopens modal with prior selections)
- One job at a time guard
- Download: Blob URL + hidden <a> — no browser permission prompt
- Diff History: 🕐 button, session-only, download/retry/delete
  "History is cleared on page reload. Download reports to save them."
- Filename: diff-{source-group}-vs-{target-group}-{YYYYMMDD}.html
- Verified: MANH_CODE manhattan-main vs manhattan-ck → 24 dropped objects ✅
```

### PROMPT 28 — Atlassian JIRA Integration ✅
```
Atlassian REST API with Basic auth (NOT MCP OAuth — blocked).
Auth: Buffer.from(email + ":" + token).toString("base64")
Domain: mclane.atlassian.net
Email: arun.rajagopalan@mclaneco.com
Token: stored in config.json as atlassianToken

Bridge endpoints added:
  GET /jira/search?q=&maxResults=5 — JQL text search
  GET /jira/issue/:key — full issue detail
  GET /jira/projects — project list (uses /rest/api/3/project/search)
  GET /health — now includes atlassian:true/false
  GET/POST /config — exposes atlassianDomain, atlassianEmail, atlassianToken

JIRA projects accessible: Manhattan, Help Desk Support, 
  Change Management + many more (atlassianProjectKeys:[] = all)

Q&A integration:
  - Runs in parallel with /docs/search and /knowledge/search
  - Max 5 issues, key+summary+status+type injected into prompt
  - Full description NOT injected (prevents prompt bloat/hangs)
  - Total context cap: 12,000 chars proportional truncation
  - Ollama 60s AbortController timeout added
  - Anti-hallucination: only render ticket keys in jiraResults.issues
  - 🎫 pill: "🎫 N tickets found"
  - Related tickets section below answer with ↗ Open in JIRA links

User persona rules:
  Business (Ask mode): plain English, no ticket keys shown,
    "a known issue was reported" not "SDHD-2562205"
  Technical (Explore Ask AI): full keys, summaries, raw detail

Knowledge capture:
  - jiraIssues array saved in knowledge entry context
  - 🎫 N badge in Knowledge list for entries with JIRA links
  - Editable JIRA field in Knowledge editor with clickable links
  - Export includes jiraIssues; Alpaca adds "Related JIRA issues: ..."
  - 🎫 JIRA filter in export panel: Any / Has JIRA links / No JIRA links

Settings panel Atlassian section:
  - Domain, Email, API Token (masked), Project keys multi-select
  - Test Connection button → checks /health
  - Token:● Set indicator

Classifier fixes (Ask mode never navigates to Explore):
  - "sequence", "issues", "problem", "error" → Q&A not schema
  - Schema routing only on explicit technical terms
  - Cross-DC phrases → cigwms-prod/wmshub-prod
  - Default fallback: Q&A mode

DB enrichment for issue queries:
  - When issue/problem keywords detected + prod group resolved:
    runs INVALID object probe + shipment/load indicator query
  - Results shown as 🗄️ pill + Live data section
  - Business user: business impact language only
  - Technical user: raw data + schema names
  - runIssueDbQueries() self-gates internally

SETUP.md updated with:
  - Atlassian JIRA Integration section
  - Domain/Email/Token setup instructions
  - Power Automate JIRA→Knowledge note (PROMPT 30, future)
```

---

## 🔧 Troubleshooting

| Symptom | Fix |
|---------|-----|
| "Bridge offline" | Run `node bridge.js`; check `/health` |
| "Ollama offline" | verifyOllamaReady() checks h.ollama === true |
| Ollama called wrong endpoint | Must use `/api/tags` not `/api/models` |
| Ollama hangs on large prompt | 60s AbortController timeout — check context cap |
| AI hallucinated ticket numbers | jiraResults not injected — check sendQA() parallel fetch |
| 🎫 pill not showing | Check jiraHits in scope at pill render time |
| atlassian:false in health | Check domain/email/token all set in config.json |
| JIRA search returns empty | Check JQL syntax — use text~"keyword" with spaces |
| JIRA projects returns empty | Endpoint must be /rest/api/3/project/search not /project |
| Ask mode shows "No production groups" | groups bootstrap timing — loadGroups() must complete first |
| Ask mode switches to Explore | Classifier bug — check classifyQuery() issue guard terms |
| AI gives hallucinated answers | Source not injected — check enrichQAContext() |
| Any /db/* returns [] | Using raw runMCP() — use queryDB() |
| Schema shows (0) objects | Must use DBA_OBJECTS not ALL_OBJECTS |
| Emoji garbled | Missing charset: Content-Type: application/json; charset=utf-8 |
| SharePoint AADSTS65002 | Use Power Automate instead |
| POST /docs/upload error | Check base64 encoding and file extension |
| E1 troubleshooting | E1 fixed — SYS.DUAL with depth=1. Never remove depth param or E1 hangs forever |
| Q&A hits wrong database | Check env field — all prod groups must have "env":"prod" |

---

## 📁 Project File Structure

```
knowledgeBase/
├── .github/
│   └── copilot-instructions.md   ← this file (v16)
├── bridge.js                     ← thin entry point + HTTP server
├── lib/                          ← ✅ created by PROMPT 28b
│   ├── mcp-pool.js               ← MCP pool + queryDB()
│   ├── db-routes.js              ← /db/* handlers
│   ├── docs-routes.js            ← /docs/* handlers
│   ├── knowledge-routes.js       ← /knowledge/* handlers
│   ├── jira-routes.js            ← /jira/* + JQL builder
│   ├── ollama-routes.js          ← /ollama/* handlers
│   └── semantic-routes.js        ← /semantic/* proxy to worker
├── public/                       ← ✅ created by PROMPT 28b
│   └── index.html                ← frontend (sections labelled)
├── semantic-worker/              ← ✅ created by PROMPT 29
│   ├── app.py                    ← Python Flask semantic engine
│   └── .venv/                    ← Python virtualenv (auto-created)
├── config.json                   ← 5 groups + Atlassian config
├── debug-mcp.js                  ← MCP handshake tester
├── test-bridge.js                ← 46/46 passing (A–K)
├── msal-browser.min.js           ← MSAL local (CDN blocked)
├── start.sh / stop.sh            ← startup/shutdown scripts
├── SETUP.md                      ← setup + Power Automate + JIRA
│                                   + Semantic Layer section
├── docs-index/                   ← document chunks
├── knowledge-index/              ← institutional knowledge
└── semantic-index/               ← ✅ created by PROMPT 29
    └── intents.json              ← 66+ auto-discovered intents
```
