# McLane WMS·IQ — GitHub Copilot Instructions (v18)

> Place this file at `.github/copilot-instructions.md` in your project root.
> Copilot will use it automatically as workspace context in VS Code.

---

## 🏁 Project Status

| Prompt | Feature | Status | Tests |
|--------|---------|--------|-------|
| 1 | `config.json` — 5 groups (MANP, MAN002P, MAN001P, OMSP, OPCIGP) | ✅ Done | — |
| 2 | `bridge.js` — MCP JSON-RPC client, all `/db/*` endpoints | ✅ Done | A1–A5 |
| 3 | `public/index.html` — group picker + main app | ✅ Done | — |
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
| 32 | SQL abstraction layer + remove all hard-coding | 🔜 Next | — |
| 33 | PostgreSQL + pgvector semantic search | 🔜 Next | — |
| 34 | Auto-detect Oracle code changes via LAST_DDL_TIME polling | 🔜 Future | — |
| 35 | Claude API for Q&A answers | 🔜 Future | — |
| 36 | LDAP authentication (multi-user) | 🔜 Future | — |
| 37 | Docker Compose multi-container deployment | 🔜 Future | — |

**Test suite:** `node test-bridge.js` → **46/46 passing** (A1–A6, B1–B8, C1–C3, D1–D2, E1–E4, F1–F4, G1–G4, H1–H2, I1–I5, J1–J3, K1–K4)
> ✅ 46/46 zero failures
> ✅ E1 uses SYS.DUAL with depth=1 — DUAL guaranteed forever, depth=1 prevents infinite BFS
> ℹ️ App served at `http://localhost:3333`. `file://` still works as fallback.
> ℹ️ JIRA search uses `textfields ~ "term*"` JQL with stop word filtering.
> ℹ️ Semantic worker runs on port 3334 (Python Flask). Bridge launches it on startup via .venv.

**Live DBs:** All 5 groups confirmed connected over VPN as ASRAJAG
**Atlassian:** Connected as arun.rajagopalan@mclaneco.com @ mclane.atlassian.net
**Documents:** 226 documents indexed, reindexed with improved chunking (2500/300)
**Semantic:** 118+ intents auto-discovered from MANH_CODE + FRAMEWORK
**Knowledge:** 4+ entries, gold standard quality

---

## ✅ Confirmed Environment Facts

| Item | Value | Notes |
|------|-------|-------|
| SQLCL command | `sql` | On `$PATH` on macOS |
| MCP args | `["-R", "2", "-mcp"]` | Restrict level 2, MCP mode |
| Bridge port | `3333` | Default, no conflicts |
| Semantic worker port | `3334` | Python Flask, launched by bridge |
| Ollama URL | `http://localhost:11434` | Local, default port |
| Ollama model | `llama3:latest` (8B) | Confirmed working |
| Embedding model | `nomic-embed-text:latest` | 274MB, needed for pgvector |
| Other models | `mistral`, `phi`, `tinyllama` | Can be removed to free RAM |
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
| Pool shape | queue-based FIFO, not busy-flag | Concurrent requests serialise per group |
| OS | macOS | 32GB RAM |
| MSAL | Blocked by McLane Azure AD (AADSTS65002) | Use Power Automate POST /docs/upload instead |
| Oracle views | Use `DBA_*` not `ALL_*` | ASRAJAG has DBA privileges in all databases |
| LAST_DDL_TIME | Removed from /db/objects | Caused CSV parse failures — do NOT add back |
| SQLcl footer filter | Strip "N rows selected." variants | parseMCPResult() must filter these |
| JSON encoding | UTF-8 | `Content-Type: application/json; charset=utf-8` on all responses |
| POST /db/query | SELECT-only, max 50 rows | Rejects all DDL/DML |
| /db/impact depth | Optional, default 3, max 5 | depth=1 for E1/E2 tests (fast path) |
| E1 test object | SYS.DUAL depth=1 | DUAL in SYS is guaranteed forever |
| Atlassian domain | mclane.atlassian.net | Confirmed working |
| Atlassian email | arun.rajagopalan@mclaneco.com | Confirmed working |
| Atlassian auth | Basic auth (email:apiToken) | NOT MCP OAuth — REST API only |
| JIRA JQL | `textfields ~ "term*" AND textfields ~ "term2*"` | Stop words filtered, max 4 terms |
| JIRA stop words | Full list in lib/jira-routes.js STOP_WORDS | Includes solving, fixing, handling etc |
| Doc chunk size | 2500 chars, 300 overlap | Upgraded from 800/100 — better for technical docs |
| Doc search results | 10 results | Upgraded from 5 |
| Doc preprocessing | preprocessDocText() strips TOC/PAGEREF noise | Word field codes, layout numbers removed |
| Doc context prefix | `[Document: {title}]` prepended to each chunk | AI knows which document it is reading |
| Doc reindex | POST /docs/reindex | Re-processes all existing docs without re-upload |
| Semantic auto-pause | After full scan: paused=true | User must manually Rescan |
| Semantic bulk confirm | ✓ Confirm all ≥ N% | Threshold: 70/80/90/95/100% |
| JIRA upload | POST /jira/upload | Resolved/Closed only → docs-index |
| User personas | Business (Ask mode) vs Technical (Explore Ask AI) | Different AI prompt rules per persona |
| Demo mode | Settings toggle, yellow banner, write guards | All reads work, writes blocked |
| Auto DC routing | No user choice dialogs ever | System decides Manhattan vs CIG automatically |
| Date fallback | today→week→month→all | Shows fallback note when widening window |
| Shipment query | Live Oracle via POST /db/query | Real shipment IDs confirmed from SE_DM, MD_DM, NE_DM |
| App entry point | `http://localhost:3333` | Static server added in 28b |
| Group env field | All 5 current groups: "env": "prod" | Future UAT/test: "env": "uat" or "env": "test" |
| Production guardrail | Q&A + data queries: prod only | Compare modal is the ONLY exempt feature |
| SQLcl MCP connection | connectionName field in groups | e.g. "connectionName": "MANP" — do NOT rename |
| Groups readOnly | All 5 groups have "readOnly": true | WMS·IQ never writes to Oracle |
| Total DCs | 32 active + 1 pending (UT) | UT active:false, goLive: 2026-09 |
| WMSHUB schemas | WMSHUB WMSHUB_CODE EM EM_CODE | EM not EMS — verified from config.json |
| MANH_CODE location | All 3 Manhattan DBs | MANP + MAN002P + MAN001P each have own instance |
| CIG-only DCs | 19 DCs with manhattanGroup: null | Route to cigwms-prod only, no Manhattan schema |
| config jiraStopWords | Full list in config.json | Moved from code to config ✅ |
| config semanticConfidence | High: 0.8, Medium: 0.5 | In config.json ✅ |
| config docsChunkSize | 2500 chars, 300 overlap | In config.json ✅ |

---

## 🏢 Project Overview

**McLane WMS·IQ** is a unified knowledge and intelligence platform for the McLane WMS ecosystem.
Currently migrating from **Manhattan WMOS 2019** to **Manhattan Active WM**.

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
| System names | Never (no "Manhattan", "WMSHUB") | Full detail |
| Answer tone | Plain business English, max 200 words | Technical detail |

---

## 🗄️ Database Connections & Groups (5 Groups)

All connections use **SQLcl MCP via queryDB()** — never change this.
Connection names match SQLcl named connections on macOS.

```
manhattan-main  connectionName: MANP      env: prod  color: #3fb950
  Per-DC:  FS06_DM FS06_MDA  FE_DM FE_MDA  MD_DM MD_MDA
           MK_DM MK_MDA  MN_DM MN_MDA  MY_DM MY_MDA
           MZ_DM MZ_MDA  NE_DM NE_MDA  SE_DM SE_MDA
  Shared:  MANH  MANH_CODE
  DCs:     06(FS06) FE MD MK MN MY MZ NE SE
  UT_DM/UT_MDA: defined but active:false until Sept 2026

manhattan-ck    connectionName: MAN002P   env: prod  color: #58a6ff
  Per-DC:  C1_DM C1_MDA  C2_DM C2_MDA  C3_DM C3_MDA
  Shared:  MANH  MANH_CODE
  DCs:     C1(Otsego) C2(St Louis) C3(Columbus)

manhattan-wk    connectionName: MAN001P   env: prod  color: #bc8cff
  Per-DC:  MAN490_DM  MAN490_MDA
  Shared:  MANH  MANH_CODE
  DCs:     WK(Bluegrass DC490)

wmshub-prod     connectionName: OMSP      env: prod  color: #39d0d8
  Schemas: WMSHUB  WMSHUB_CODE  EM  EM_CODE
  ⚠️ EM and EM_CODE — NOT EMS/EMS_CODE

cigwms-prod     connectionName: OPCIGP    env: prod  color: #d29922
  Schemas: CIGWMS  CIGWMS_CODE  OP  OP_CODE  MCLANE  MCLANE_CODE  FRAMEWORK
  DCs:     GA GM HP ME MG MI MO MP MS MW NC NT NW PA SO SW SZ WJ
           (CIG WMS only — manhattanGroup: null for these DCs)
```

> ⚠️ MANH and MANH_CODE exist in ALL THREE Manhattan databases independently.
> Always specify group when querying MANH_CODE — it is not unique to manhattan-main.

> ⚠️ Groups use `connectionName` field (not `db`) — e.g. "connectionName": "MANP"
> This matches the SQLcl named connection. Do NOT rename this field.

> ℹ️ All groups have `"readOnly": true` — no DDL/DML via WMS·IQ
> ℹ️ Total DCs: 32 active + 1 pending (UT, active:false, goLive: 2026-09)
> ℹ️ 19 CIG-only DCs have manhattanGroup: null — route to cigwms-prod only

### Key Source Code Schemas
- `MANH_CODE` — in ALL 3 Manhattan DBs (MANP, MAN002P, MAN001P)
  Contains: DOCK_PK, DC_SCHEMA_PK, LOG_PK, LOCATION_PK, TIMER, DOCK_WRAPPER_PK
- `FRAMEWORK` (cigwms-prod) — LOGS (874 spec + 1231 body lines)
- `OP` (cigwms-prod) — CIG WMS operational packages
- `MCLANE` (cigwms-prod) — McLane-specific packages
- `WMSHUB_CODE` (wmshub-prod) — WMSHUB packages
- `EM_CODE` (wmshub-prod) — EM packages

### Active WM — Future Architecture (do not implement yet)
Manhattan Active WM will provide read-only PostgreSQL replicated tables.
The existing SQLcl MCP Oracle connections are NOT affected.
When Active WM replicas are available, add a NEW group:
  { "id": "active-wm-prod", "engine": "postgres",
    "connectionString": "...", "replicaOnly": true, "env": "prod" }
This requires a separate PostgreSQL adapter — future work.
Until then: all 5 groups connect via SQLcl MCP as today.

---

## 🏭 Distribution Centers

### 13 Manhattan + WMSHUB DCs

| DC_ID | Code | Name | Type | DB | DM Schema | MDA Schema |
|-------|------|------|------|----|-----------|------------|
| 606 | 06 | Lakeland 606 | food-service | MANP | FS06_DM | FS06_MDA |
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

### #4 — AI hallucination prevention
- Always inject /db/source (PACKAGE BODY) into system prompt
- Always inject only real JIRA issues from jiraResults.issues array
- Never render ticket keys not present in jiraResults
- Explicit prompt rule: "Only reference JIRA ticket keys that appear in the above list. Do NOT invent ticket numbers."
- When context is empty (0 JIRA + 0 knowledge hits): "I don't have enough information" — never invent

### #5 — Production guardrail ⚠️ CRITICAL
Every group has `"env"` field. Bridge exposes it in `GET /groups`.
- Q&A, DC resolver, POST /db/query: **ONLY** `env === "prod"` groups
- Compare modal: exempt — may use any group, shows ⚠ warning for non-prod
```javascript
const prodGroups = config.groups.filter(g => g.env === 'prod');
```

### #6 — SQL abstraction (PROMPT 32 — in progress)
All SQL queries must live in `lib/sql-catalog.js`.
No SQL strings anywhere else in the codebase.
Table names must reference `config.tableAliases` for Active WM migration safety.

### #7 — Document pipeline
```
Upload → extract text (mammoth/.docx, pdftotext/.pdf)
       → preprocessDocText() strips TOC/PAGEREF/Word noise
       → find content start (skip TOC, start at first narrative paragraph)
       → chunkTextSentenceAware(text, 2500, 300)  ← paragraph boundary splits
       → prepend [Document: {title}] to each chunk
       → write to docs-index/{group}-{filename}.json (atomic write)
```
Re-index without re-upload: `POST /docs/reindex` → `{"reindexed":226}`

### #8 — JIRA JQL pattern
```javascript
// Extract meaningful terms, strip stop words, max 4 terms
// Build: textfields ~ "MZIC6101*" AND textfields ~ "VARIANCE*"
// Never strip extension codes: EX01, EX33, SDN-215 etc
if (/^[A-Z]{1,4}[-_]?\d+$/i.test(term)) keepAlways = true;
```

### #9 — Q&A context assembly (parallel, every question)
```
1. GET /semantic/search?q=  → semantic intent match (first — highest priority)
2. GET /knowledge/search?q= → knowledge hits (🧠)
3. GET /docs/search?q=      → document chunks (📄) — 10 results
4. GET /jira/search?q=      → JIRA tickets (🎫) — if atlassianEnabled
5. POST /db/query           → live DB findings (🗄️) — if issue keywords + prod group
Total context capped at 12,000 chars (complete chunks, never truncated mid-chunk)
```

### #10 — Classifier rules (Ask mode never navigates to Explore)
- Issue/problem guard: "issues", "problem", "error", "failing" → always Q&A
- Schema routing ONLY on: "table", "column", "package", "procedure", "index", "view"
- Cross-DC phrases → cigwms-prod then wmshub-prod
- Default fallback: Q&A mode
- Extension codes (EX01, EX33) are NEVER stop words

### #11 — Hard-coding policy ⚠️
**NO hard-coded values anywhere in lib/*.js or public/index.html.**
All configurable values must be in config.json and loaded at startup.
This includes: chunk sizes, thresholds, row limits, table names, model names,
port numbers, stop words, schema names, SQL queries.
See PROMPT 32 for the complete list.

### #12 — Manhattan Active WM migration safety
All SQL table names reference `config.tableAliases`:
```javascript
const t = config.tableAliases;
// Use: `FROM ${schema}.${t.SHIPMENT_HDR}`
// Not: `FROM ${schema}.SHIPMENT_HDR`
```
When Active WM renames tables, only config.json changes — zero code changes.

---

## 🗺️ Architecture

```
public/index.html  (served at http://localhost:3333)
        │
        ▼
  Mode Selector
  ├── 💬 Ask a Question  [BUSINESS PERSONA]
  │     ⏳ Connecting... while groups load (groupsLoadPromise)
  │     → DC resolver → classifier → routing
  │     → ONLY env="prod" groups ← GUARDRAIL
  │     → Parallel context: semantic + knowledge + docs + JIRA + DB
  │     → System prompt: BUSINESS persona rules + 12,000 char cap
  │     → Ollama (stream:false, 60s timeout) → plain English answer
  │     → Pills: 🧠 🎫 📄 🗄️
  │     → Related JIRA tickets (plain English, no ticket keys shown)
  │     → Date-aware shipment queries: today→week→month→all fallback
  │     → [💾 Save] → knowledge entry + jiraIssues captured
  │
  └── 🔧 Explore Systems → Group Picker
        → Main App [TECHNICAL PERSONA]
        Tabs: ⚡Impact | Home | 💬Ask AI | 📚Docs | 🧠Knowledge | 🔬Semantic
                    │
        Home tab: WMS Intelligence Overview dashboard
          Stats: groups, DCs, schemas, objects, packages,
                 source units, docs, knowledge, intents, JIRA
          🏛️ Governance: INVALID objects, Compare, Impact, Export docs
                    │
        Global search: [🔍 This group] [🔍 All systems]
        ⟷ Compare → background diff job (Blob URL download, no browser prompt)
          Source/Target: Group + Schema + Type (Live DB | Snapshot)
          Status bar: stage + % + cancel ✕ + auto-pause after scan
          History: 🕐 session-only, download/retry/delete
                    │
        🔬 Semantic tab:
          Left: intent list with 🟢🟡🔴 confidence dots
          Right: detail editor + SQL template test + ✓ Confirm / ✗ Reject
          Bulk confirm: ✓ Confirm all ≥ N% (70/80/90/95/100%)
          Scan status: auto-pauses after full scan
                    │
                    ▼
bridge.js  (http://localhost:3333) — thin entry point
        │
        ├── lib/mcp-pool.js       → MCP pool + queryDB()
        ├── lib/db-routes.js      → /db/* handlers
        ├── lib/docs-routes.js    → /docs/* handlers
        ├── lib/knowledge-routes.js → /knowledge/* handlers
        ├── lib/jira-routes.js    → /jira/* + JQL builder + /jira/upload
        ├── lib/ollama-routes.js  → /ollama/* handlers
        ├── lib/semantic-routes.js → /semantic/* proxy to Python worker
        └── lib/sql-catalog.js    → ALL SQL queries (PROMPT 32)
                │
                ▼
        Oracle (5 DBs — all ASRAJAG, all env:prod)
        MANP / MAN002P / MAN001P / OMSP / OPCIGP
        +
        Atlassian REST API (mclane.atlassian.net)
        +
        semantic-worker/app.py (port 3334, Python Flask)
        +
        PostgreSQL + pgvector (port 5432, Docker) ← PROMPT 33
```

---

## 📋 Current config.json structure

```json
{
  "bridge": {
    "port": 3333,
    "ollamaUrl": "http://localhost:11434",
    "defaultModel": "llama3",
    "sqlclCommand": "sql",
    "sqlclArgs": ["-R", "2", "-mcp"],
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
    "atlassianToken": "<set — do not commit>",
    "atlassianProjectKeys": [],
    "docsIndexDir": "./docs-index",
    "docsChunkSize": 2500,
    "docsChunkOverlap": 300,
    "docsMaxSearchResults": 10,
    "docsContextBudget": 12000,
    "uploadToken": "",
    "postgresEnabled": false,
    "postgresUrl": "postgresql://wmsiq:wmsiq@localhost:5432/wmsiq",
    "oracleChangePollingEnabled": false,
    "oracleChangePollingIntervalMinutes": 15,
    "oracleChangePollingSchemas": ["MANH_CODE", "FRAMEWORK"],
    "claudeApiEnabled": false,
    "claudeApiModel": "claude-sonnet-4-6",
    "claudeApiForQA": false
  },
  "queryLimits": {
    "defaultRowLimit": 50,
    "impactAnalysisNodeCap": 200,
    "impactAnalysisDefaultDepth": 3,
    "impactAnalysisMaxDepth": 5,
    "shipmentQueryLimit": 5,
    "jiraSearchLimit": 10,
    "semanticSearchLimit": 5
  },
  "schemaDefaults": {
    "codeSchemas": ["MANH_CODE", "FRAMEWORK"],
    "sharedSchemas": ["MANH", "WMSHUB", "WMSHUB_CODE"],
    "excludedObjectTypes": ["INDEX","SYNONYM","GRANT","JAVA CLASS","SCHEDULE","TRIGGER"],
    "excludedNamePatterns": ["%$%"]
  },
  "shipmentTables": {
    "headerTable": "SHIPMENT_HDR",
    "detailTable": "SHIPMENT_DTL",
    "dateColumns": ["CREATION_DATE","CREATE_DATE","CREATED_DATE","INSERT_DATE","INSR_DATE","SHIP_DATE"],
    "idColumns": ["TC_SHIPMENT_ID","SHIPMENT_ID","SHIPMENT_NBR"]
  },
  "tableAliases": {
    "SHIPMENT_HDR": "SHIPMENT_HDR",
    "SHIPMENT_DTL": "SHIPMENT_DTL",
    "TASK_HDR": "TASK_HDR",
    "TASK_DTL": "TASK_DTL",
    "LPN": "LPN",
    "LOCN_HDR": "LOCN_HDR",
    "ITEM_CBO": "ITEM_CBO",
    "WAVE_HDR": "WAVE_HDR",
    "WAVE_DTL": "WAVE_DTL"
  },
  "semantic": {
    "workerUrl": "http://localhost:3334",
    "confirmedThreshold": 0.8,
    "unconfirmedThreshold": 0.5,
    "scanIntervalMinutes": 60,
    "maxParallelScans": 2
  },
  "qa": {
    "ollamaTimeoutMs": 60000,
    "maxContextChars": 12000,
    "businessMaxWords": 200
  },
  "distributionCenters": [ "... 33 entries ..." ],
  "groups": [
    { "id": "manhattan-main", "env": "prod", "db": "MANP", ... },
    { "id": "manhattan-ck",   "env": "prod", "db": "MAN002P", ... },
    { "id": "manhattan-wk",   "env": "prod", "db": "MAN001P", ... },
    { "id": "wmshub-prod",    "env": "prod", "db": "OMSP", ... },
    { "id": "cigwms-prod",    "env": "prod", "db": "OPCIGP", ... }
  ]
}
```

---

## 🚀 Pending Prompts

---

### PROMPT 32 — SQL Abstraction Layer + Remove All Hard-Coding 🔜 NEXT

```
Read .github/copilot-instructions.md in full before writing anything.

The codebase has hard-coded SQL, thresholds, table names, and
configuration values scattered throughout lib/*.js and public/index.html.
This is a critical fix before Active WM migration.

PART 1 — Create lib/sql-catalog.js:
All SQL queries must move here. No SQL string anywhere else.
Use config.tableAliases for all table names.

module.exports = function(config) {
  const t = config.tableAliases || {};
  const lim = config.queryLimits || {};
  return {
    listObjects: (schema) => `
      SELECT OBJECT_NAME, OBJECT_TYPE, STATUS
      FROM DBA_OBJECTS
      WHERE OWNER = '${schema}'
      AND OBJECT_TYPE NOT IN (${
        (config.schemaDefaults?.excludedObjectTypes || [])
          .map(t => `'${t}'`).join(',')
      })
      AND OBJECT_NAME NOT LIKE '%$%'
      ORDER BY OBJECT_TYPE, OBJECT_NAME`,

    listColumns: (schema, table) => `
      SELECT COLUMN_NAME, DATA_TYPE, NULLABLE,
             DATA_LENGTH, COLUMN_ID
      FROM DBA_TAB_COLUMNS
      WHERE OWNER = '${schema}'
      AND TABLE_NAME = '${table}'
      ORDER BY COLUMN_ID`,

    getSource: (schema, name, type) => `
      SELECT TEXT FROM DBA_SOURCE
      WHERE OWNER = '${schema}'
      AND NAME = '${name}'
      AND TYPE = '${type}'
      ORDER BY LINE`,

    getDependencies: (schema, name) => `
      SELECT OWNER, NAME, TYPE,
             REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
      FROM DBA_DEPENDENCIES
      WHERE (OWNER = '${schema}' AND NAME = '${name}')
      OR (REFERENCED_OWNER = '${schema}'
          AND REFERENCED_NAME = '${name}')`,

    getInvalidObjects: (schema) => `
      SELECT OBJECT_NAME, OBJECT_TYPE, STATUS
      FROM DBA_OBJECTS
      WHERE OWNER = '${schema}'
      AND STATUS = 'INVALID'
      ORDER BY OBJECT_TYPE, OBJECT_NAME`,

    getChangedObjects: (schema, since) => `
      SELECT OBJECT_NAME, OBJECT_TYPE, LAST_DDL_TIME
      FROM DBA_OBJECTS
      WHERE OWNER = '${schema}'
      AND LAST_DDL_TIME > TIMESTAMP '${since}'
      AND OBJECT_TYPE IN ('PACKAGE','PACKAGE BODY',
        'PROCEDURE','FUNCTION')
      ORDER BY LAST_DDL_TIME DESC`,

    getRecentShipments: (schema, dateCol, limit) => `
      SELECT * FROM ${schema}.${t.SHIPMENT_HDR || 'SHIPMENT_HDR'}
      WHERE TRUNC(${dateCol}) >= TRUNC(SYSDATE) - 7
      ORDER BY ${dateCol} DESC
      FETCH FIRST ${limit || lim.shipmentQueryLimit || 5} ROWS ONLY`,

    getShipmentColumns: (schema) => `
      SELECT COLUMN_NAME, DATA_TYPE
      FROM DBA_TAB_COLUMNS
      WHERE OWNER = '${schema}'
      AND TABLE_NAME = '${t.SHIPMENT_HDR || 'SHIPMENT_HDR'}'
      AND COLUMN_NAME IN (${
        (config.shipmentTables?.dateColumns || [])
          .concat(config.shipmentTables?.idColumns || [])
          .map(c => `'${c}'`).join(',')
      })
      ORDER BY COLUMN_ID`,

    getWhoami: () => `
      SELECT SYS_CONTEXT('USERENV','SESSION_USER') AS DB_USER,
             SYS_CONTEXT('USERENV','DB_NAME') AS DB_NAME
      FROM DUAL`,
  };
};

PART 2 — Add to config.json all missing values:
  queryLimits, schemaDefaults, shipmentTables, tableAliases,
  semantic, qa sections (see config.json structure above).
  Move every hard-coded value from code to config.

PART 3 — Update all lib/*.js to use sql-catalog.js:
  const sqlCatalog = require('./sql-catalog')(config);
  Replace every inline SQL string with sqlCatalog.methodName()

PART 4 — Update public/index.html:
  Replace const BRIDGE = 'http://localhost:3333'
  with: fetch /config on startup, use config.bridge.appUrl
        or default to window.location.origin
  Move all hard-coded thresholds to read from /config response

PART 5 — Verify:
  node test-bridge.js → still 46/46 passing
  grep -r "FETCH FIRST\|DBA_OBJECTS\|SHIPMENT_HDR" lib/
  → zero results (all in sql-catalog.js only)
  grep -r "localhost:3333\|localhost:3334" public/
  → zero results (loaded from config)

Do NOT change test-bridge.js test logic.
```

---

### PROMPT 33 — PostgreSQL + pgvector Semantic Search 🔜 NEXT (after 32)

```
Read .github/copilot-instructions.md in full before writing anything.

POC phase — Docker PostgreSQL on MacBook.
nomic-embed-text already installed in Ollama.
Goal: replace keyword /docs/search with semantic similarity search.

PART 1 — docker-compose.yml in project root:
  version: '3.8'
  services:
    postgres:
      image: ankane/pgvector:latest
      environment:
        POSTGRES_DB: wmsiq
        POSTGRES_USER: wmsiq
        POSTGRES_PASSWORD: wmsiq
      ports:
        - "5432:5432"
      volumes:
        - ./pgdata:/var/lib/postgresql/data
      healthcheck:
        test: ["CMD-SHELL", "pg_isready -U wmsiq"]
        interval: 5s
        timeout: 5s
        retries: 10

PART 2 — lib/vector-store.js:
  Uses 'pg' npm package (add to package.json)
  
  initSchema() — creates tables:
    CREATE EXTENSION IF NOT EXISTS vector;
    CREATE TABLE IF NOT EXISTS doc_chunks (
      id SERIAL PRIMARY KEY,
      file_id TEXT NOT NULL,
      file_name TEXT,
      title TEXT,
      group_id TEXT,
      site TEXT,
      source TEXT,
      chunk_index INTEGER,
      chunk_text TEXT,
      embedding vector(768),
      last_modified TIMESTAMPTZ,
      synced_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(file_id, chunk_index)
    );
    CREATE INDEX IF NOT EXISTS doc_chunks_embedding_idx
      ON doc_chunks USING ivfflat (embedding vector_cosine_ops)
      WITH (lists = 100);

  generateEmbedding(text):
    POST http://localhost:11434/api/embeddings
    { model: "nomic-embed-text", prompt: text }
    Returns float[] (768 dimensions)
    Cache embeddings in memory for duplicate chunks

  upsertChunk(chunk):
    INSERT ... ON CONFLICT (file_id, chunk_index) DO UPDATE

  semanticSearch(queryText, groupId, limit=10):
    embedding = await generateEmbedding(queryText)
    SELECT chunk_text, file_name, title, group_id,
      1 - (embedding <=> $1::vector) AS similarity
    FROM doc_chunks
    WHERE ($2::text IS NULL OR group_id = $2)
    ORDER BY embedding <=> $1::vector
    LIMIT $3

  deleteByFileId(fileId):
    DELETE FROM doc_chunks WHERE file_id = $1

PART 3 — POST /docs/migrate-to-vector endpoint:
  Reads all docs-index/*.json files
  For each chunk: generateEmbedding(chunk.text)
  Upserts into PostgreSQL
  Returns { migrated: N, chunks: N, errors: [] }
  Progress: console.log every 10 files
  Note: 226 files × avg 10 chunks × embedding time ~0.5s = ~18 minutes

PART 4 — Hybrid search in lib/docs-routes.js:
  GET /docs/search:
    if (config.bridge.postgresEnabled && vectorStore.isConnected()):
      return await vectorStore.semanticSearch(q, group, limit)
    else:
      return existingKeywordSearch(q, group, limit)  ← zero regression risk

PART 5 — Also migrate knowledge-index to vector:
  CREATE TABLE IF NOT EXISTS knowledge_entries (
    id TEXT PRIMARY KEY,
    question TEXT,
    answer TEXT,
    tags TEXT[],
    quality INTEGER,
    embedding vector(768),
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
  GET /knowledge/search → semantic search if postgres enabled

PART 6 — Health + startup:
  GET /health → add "postgres": true/false
  start.sh: if postgresEnabled and docker installed:
    docker compose up -d postgres
    Wait for healthcheck to pass
    Print: ✓ PostgreSQL + pgvector running (port 5432)

PART 7 — SETUP.md — add PostgreSQL section:
  ## PostgreSQL + pgvector Setup
  Requires Docker Desktop.
  1. docker compose up -d postgres
  2. curl -X POST http://localhost:3333/docs/migrate-to-vector
  Takes ~20 minutes for 226 documents.
  After migration, set postgresEnabled: true in config.json.

Do NOT change test-bridge.js.
Do NOT break existing keyword search fallback.
```

---

### PROMPT 34 — Auto-Detect Oracle Code Changes 🔜 FUTURE

```
Read .github/copilot-instructions.md before writing anything.

When PL/SQL packages change in production Oracle, WMS·IQ should
automatically detect the change and update its semantic intent mappings.

Config fields (already in config.json):
  oracleChangePollingEnabled: false
  oracleChangePollingIntervalMinutes: 15
  oracleChangePollingSchemas: ["MANH_CODE", "FRAMEWORK"]

In bridge.js startup:
  If oracleChangePollingEnabled:
    setInterval(pollForChanges, intervalMs)

pollForChanges():
  lastCheckTime stored in memory (initialized to startup time)
  For each schema in oracleChangePollingSchemas:
    rows = await queryDB(group, sqlCatalog.getChangedObjects(schema, lastCheckTime))
    For each changed object:
      Log: "[ORACLE CHANGE] DOCK_PK (PACKAGE BODY) modified at ..."
      POST http://localhost:3334/discover-object
        { schema, objectName, objectType, group }
  lastCheckTime = new Date().toISOString()

New bridge endpoint:
  GET /db/changes?since=<ISO8601>&group=<id>
  Returns objects changed since given timestamp
  Uses sqlCatalog.getChangedObjects()

GET /health → add:
  "oracleChangePolling": {
    "enabled": bool,
    "lastCheck": ISO8601,
    "changesDetected": N
  }

In semantic-worker/app.py — add POST /discover-object:
  Receives { schema, objectName, objectType, group }
  Fetches source via Oracle MCP (calls bridge /db/source)
  Runs Ollama intent extraction on just that object
  Updates existing intent if found, adds new if not
  Returns { updated: N, added: N }

Do NOT change test-bridge.js.
```

---

### PROMPT 35 — Claude API for Q&A Answers 🔜 FUTURE

```
Read .github/copilot-instructions.md before writing anything.

Switch Q&A answers from Ollama to Claude API for better quality.
Claude API already used for PDF export (claude-sonnet-4-6).

Config fields (already in config.json):
  claudeApiEnabled: false
  claudeApiModel: "claude-sonnet-4-6"
  claudeApiForQA: false
  claudeApiFallbackToOllama: true

In lib/ollama-routes.js:
  POST /ollama/chat:
    If request body has useClaudeApi:true AND config.claudeApiEnabled
       AND config.claudeApiForQA:
      Call Claude API:
        POST https://api.anthropic.com/v1/messages
        Headers:
          x-api-key: process.env.ANTHROPIC_API_KEY or config.claudeApiKey
          anthropic-version: 2023-06-01
          anthropic-dangerous-direct-browser-access: true
        Body: { model: config.claudeApiModel, max_tokens: 2048, messages }
      Return same shape: { response: text }
    Else: existing Ollama path unchanged

In public/index.html sendQA():
  If claudeApiEnabled AND claudeApiForQA:
    Add useClaudeApi: true to POST /ollama/chat body

Settings panel — add AI Provider section:
  AI Provider: [Ollama ●] [Claude API ○]
  (Claude API option only shown if ANTHROPIC_API_KEY detected in /health)

GET /health → add "claudeApi": true/false
  true if ANTHROPIC_API_KEY set and reachable

Cost: ~$0.003 per Q&A question (negligible for internal use)
Quality improvement: dramatic on complex multi-document WMS questions

Do NOT change test-bridge.js.
```

---

### PROMPT 36 — LDAP Authentication (Multi-User) 🔜 FUTURE (post-demo)

```
After stakeholder approval and server deployment.
Use LDAP/Active Directory — already proven in another McLane POC.
Azure AD SSO requires IT approval (AADSTS65002 blocks direct access).

Add to config.json:
  "ldapEnabled": false,
  "ldapUrl": "ldap://mclaneco.com",
  "ldapBaseDn": "DC=mclaneco,DC=com",
  "ldapBindDn": "",
  "ldapBindPassword": "",
  "sessionSecret": "",
  "sessionTimeoutMinutes": 480

POST /auth/login → LDAP bind with user credentials
GET /auth/logout → clear session
All /db/*, /docs/*, /knowledge/* → require valid session
Knowledge entries → capturedBy: real username from LDAP
Semantic confirmations → confirmedBy: real username

Do NOT implement until server deployment is planned.
```

---

### PROMPT 37 — Docker Compose Multi-Container 🔜 FUTURE (post-demo)

```
After stakeholder approval.
Full production-ready Docker Compose setup.

Services:
  frontend  — Nginx serving public/ (port 80/443)
  bridge    — Node.js TypeScript (port 3333, internal)
  worker    — Python semantic worker (port 3334, internal)
  postgres  — PostgreSQL + pgvector (port 5432, internal)
  ollama    — Ollama LLM (port 11434, internal, GPU preferred)

Network:
  All services on internal Docker network
  Only frontend exposed externally
  Bridge connects to on-prem Oracle via VPN/McLane network

Environment variables (not config.json) for secrets:
  ATLASSIAN_TOKEN, ANTHROPIC_API_KEY, LDAP_BIND_PASSWORD,
  POSTGRES_PASSWORD, SESSION_SECRET

Do NOT implement until server is provisioned by IT.
```

---

### PROMPT 23 — UT DC Activation (September 2026) 🔜 FUTURE

```
Config-only change when McLane Salt Lake City (UT, DC 210)
goes live on Manhattan in September 2026.
1. config.json distributionCenters: set "active": true for UT
2. config.json manhattan-main schemas: add "UT_DM", "UT_MDA"
3. Verify schemas exist via DBA_OBJECTS
4. Restart bridge.js — zero code changes needed
5. Update this file: UT row → "✅ Live"
```

---

## 🔧 Troubleshooting

| Symptom | Fix |
|---------|-----|
| "Bridge offline" | Run `node bridge.js`; check `/health` |
| "Ollama offline" | verifyOllamaReady() checks h.ollama === true |
| Ollama hangs | 60s AbortController timeout — check context cap |
| AI hallucinated ticket numbers | jiraResults not injected — check sendQA() |
| 🎫 pill not showing | Check jiraHits in scope at pill render |
| atlassian:false in health | Check domain/email/token all set in config.json |
| JIRA search returns empty | textfields~ JQL — check stop word filtering |
| JIRA extension code stripped | EX01/EX33 must never be stop words |
| Ask mode shows "No prod groups" | groupsLoadPromise timing — loadGroups() must complete |
| Ask mode switches to Explore | Classifier bug — check issue guard terms |
| Doc chunks still TOC garbage | preprocessDocText() — run POST /docs/reindex |
| chunkCount: 1 for large doc | extractionMode check — mammoth must run for .docx |
| JSON truncated in docs-index | Atomic write fix — write to .tmp then rename |
| Any /db/* returns [] | Using raw runMCP() — always use queryDB() |
| Schema shows (0) objects | Must use DBA_OBJECTS not ALL_OBJECTS |
| E1 hangs forever | depth=1 parameter missing — never remove it |
| E1 fails intermittently | Was SYS.DUAL infinite BFS — now fixed with depth=1 |
| Q&A hits wrong database | Check env field — all prod groups must have "env":"prod" |
| Shipment query returns empty | Date fallback chain — today→week→month→all |
| Hard-coded SQL breaks Active WM | Must use sql-catalog.js + tableAliases — see PROMPT 32 |
| start.sh permission denied | chmod +x start.sh stop.sh |
| ngrok blocked by McLane SSL | Use phone hotspot for ngrok, or use 10.98.215.99 directly |
| Power Automate folder shows "No items" | SharePoint API permissions — use recurrence trigger |

---

## 📁 Project File Structure

```
knowledgeBase/
├── .github/
│   └── copilot-instructions.md   ← this file (v18)
├── bridge.js                     ← thin entry point + HTTP server
├── lib/                          ← modular route handlers
│   ├── mcp-pool.js               ← MCP pool + queryDB()
│   ├── db-routes.js              ← /db/* handlers
│   ├── docs-routes.js            ← /docs/* + preprocessDocText + chunking
│   ├── knowledge-routes.js       ← /knowledge/* handlers
│   ├── jira-routes.js            ← /jira/* + JQL builder + stop words
│   ├── ollama-routes.js          ← /ollama/* + Claude API (PROMPT 35)
│   ├── semantic-routes.js        ← /semantic/* proxy to Python worker
│   ├── sql-catalog.js            ← ALL SQL queries (PROMPT 32) ← CREATE THIS
│   └── vector-store.js           ← pgvector client (PROMPT 33) ← CREATE THIS
├── public/
│   └── index.html                ← WMS·IQ single-file frontend
├── semantic-worker/
│   ├── app.py                    ← Python Flask semantic engine
│   └── .venv/                    ← Python virtualenv (auto-created)
├── OP/                           ← Oracle PL/SQL source code
├── config.json                   ← ⚠️ in .gitignore — never commit
├── config.example.json           ← Safe template — commit this
├── docker-compose.yml            ← PostgreSQL (PROMPT 33) ← CREATE THIS
├── .gitignore                    ← node_modules, config.json, docs-index etc
├── debug-mcp.js                  ← MCP handshake tester
├── test-bridge.js                ← 46/46 passing (A–K)
├── bulk_upload_docs.py           ← Python bulk upload script
├── start.sh / stop.sh            ← startup/shutdown (chmod +x required)
├── SETUP.md                      ← setup + Power Automate + JIRA + Semantic
├── docs-index/                   ← 226 documents (in .gitignore)
├── knowledge-index/              ← institutional knowledge (in .gitignore)
└── semantic-index/               ← 118+ intents (in .gitignore)
    └── intents.json
```

---

## 📊 POC vs Production Roadmap

### Current Phase — POC (MacBook, single user ASRAJAG)
```
✅ PROMPTS 1–31   Complete — demo ready
🔜 PROMPT 32     SQL abstraction + hard-coding cleanup ← DO NEXT
🔜 PROMPT 33     PostgreSQL + pgvector ← major quality improvement
─────────────────────────────────────────────────────
SHOW TO McLane IT / Management
─────────────────────────────────────────────────────
```

### Post-Approval Phase
```
PROMPT 34   Oracle change detection (LAST_DDL_TIME polling)
PROMPT 35   Claude API for Q&A (better answers)
PROMPT 36   LDAP authentication (multi-user)
PROMPT 37   Docker Compose (full production deployment)
```

### Technology Decisions
| Layer | POC (now) | Production (later) |
|---|---|---|
| Bridge | Node.js vanilla JS | TypeScript |
| Frontend | Single HTML file | React + TypeScript |
| Semantic engine | Python Flask | Python + pgvector |
| Storage | JSON files + PostgreSQL | PostgreSQL only |
| Container | Docker (postgres only) | Full Docker Compose |
| Auth | None (local only) | LDAP → Azure AD |
| LLM local | Ollama llama3 8B | Ollama (GPU server) |
| LLM cloud | Claude API (optional) | Claude API (primary) |

### Rust consideration
Not in POC phase. Revisit post-production for SQLcl MCP process manager.
Bottleneck is Oracle VPN latency and Ollama inference, not CPU/memory.
