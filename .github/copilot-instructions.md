# McLane WMS·IQ — GitHub Copilot Instructions (v20)

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
| 32/33 | Oracle Schema Knowledge Graph + pgvector | ✅ Done | L1–L4 |
| 34 | Auto-detect Oracle code changes via LAST_DDL_TIME polling | 🔜 Future | — |
| 35 | Apache AGE graph database — dependency analysis | ✅ Done | M1–M3 |
| 36 | React + TypeScript frontend migration + conversational Ask mode | 🔜 Next | — |
| 37 | LDAP authentication (multi-user) | 🔜 Future | — |
| 38 | Docker Compose multi-container deployment | 🔜 Future | — |

**Test suite:** `node test-bridge.js` → **53/53 passing** (A1–A6, B1–B8, C1–C3, D1–D2, E1–E4, F1–F4, G1–G4, H1–H2, I1–I5, J1–J3, K1–K4, L1–L4, M1–M3)
> ✅ 53/53 zero failures
> ✅ E1 uses SYS.DUAL with depth=1 — DUAL guaranteed forever, depth=1 prevents infinite BFS
> ℹ️ App served at `http://localhost:3333`.
> ℹ️ Semantic worker runs on port 3334 (Python Flask). Bridge launches it on startup via .venv.
> ℹ️ PostgreSQL + pgvector running on port 5432 via Colima + docker-compose
> ℹ️ Embedding model: mxbai-embed-large (1024 dims) — nomic-embed-text was broken/corrupted

**Live DBs:** All 5 groups confirmed connected over VPN as ASRAJAG
**Atlassian:** Connected as arun.rajagopalan@mclaneco.com @ mclane.atlassian.net
**Documents:** 222 documents indexed (226 total — 4 PDFs failed pdftotext extraction), migrated to pgvector
**Semantic:** 76 confirmed intents migrated to pgvector
**Knowledge:** Rebuilt organically — save good Q&A answers to rebuild
**Graph:** 8,802+ Oracle objects scanned (manhattan-main: MANH, MANH_CODE, SE_DM, SE_MDA)

---

## ✅ Confirmed Environment Facts

| Item | Value | Notes |
|------|-------|-------|
| SQLCL command | `sql` | On `$PATH` on macOS — do NOT change |
| MCP args | `["-R", "2", "-mcp"]` | Restrict level 2, MCP mode |
| Bridge port | `3333` | Default, no conflicts |
| Semantic worker port | `3334` | Python Flask, launched by bridge |
| PostgreSQL port | `5432` | Colima Docker, wmsiq/wmsiq/wmsiq |
| Ollama URL | `http://localhost:11434` | Local, default port |
| Ollama model | `qwen2.5:14b` | Upgraded from llama3 — better WMS answers |
| Available models | `gemma4:26b`, `qwen2.5:14b`, `llama3`, `phi`, `mistral`, `tinyllama` | |
| Embedding model | `mxbai-embed-large` | 1024 dimensions — confirmed working |
| ⚠️ nomic-embed-text | BROKEN — returns identical vectors | Do NOT use — repull does not fix it |
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
| OS | macOS | 32GB RAM, Apple Silicon (arm64) |
| Docker runtime | Colima (not Docker Desktop) | Work laptop — no Docker Desktop allowed |
| Docker image | `pgvector/pgvector:pg16` | arm64 image pulled via crane from outside corporate network |
| docker-compose command | `docker-compose` (hyphenated) | New `docker compose` plugin not installed |
| MSAL | Blocked by McLane Azure AD (AADSTS65002) | Use Power Automate POST /docs/upload instead |
| Oracle views | Use `DBA_*` not `ALL_*` | ASRAJAG has DBA privileges in all databases |
| SQLcl footer filter | Strip "N rows selected." variants | parseMCPResult() must filter these — "076 rows selected." appears as ghost object |
| JSON encoding | UTF-8 | `Content-Type: application/json; charset=utf-8` on all responses |
| POST /db/query | SELECT-only, max 50 rows | Rejects all DDL/DML |
| /db/impact depth | Optional, default 3, max 5 | depth=1 for E1/E2 tests (fast path) |
| E1 test object | SYS.DUAL depth=1 | DUAL in SYS is guaranteed forever |
| Atlassian domain | mclane.atlassian.net | Confirmed working |
| Atlassian email | arun.rajagopalan@mclaneco.com | Confirmed working |
| Atlassian auth | Basic auth (email:apiToken) | NOT MCP OAuth — REST API only |
| JIRA JQL | `textfields ~ "term*" AND textfields ~ "term2*"` | Stop words filtered, max 4 terms |
| Doc chunk size | 2500 chars, 300 overlap | Better for technical docs |
| Doc search results | 10 results | config: docsMaxResults |
| Doc preprocessing | preprocessDocText() strips TOC/PAGEREF noise | Word field codes, layout numbers removed |
| Doc context prefix | `[Document: {title}]` prepended to each chunk | AI knows which document it is reading |
| Doc reindex | POST /docs/reindex | Re-processes all existing docs without re-upload |
| Doc upload | bulk_upload_docs.py --dir --group --extensions docx,pdf,txt,xlsx | PDFs need: brew install poppler |
| Semantic auto-pause | After full scan: paused=true | User must manually Rescan |
| JIRA upload | POST /jira/upload | Resolved/Closed only → docs-index |
| User personas | Business (Ask mode) vs Technical (Explore Ask AI) | Different AI prompt rules per persona |
| Demo mode | Settings toggle, yellow banner, write guards | All reads work, writes blocked |
| Auto DC routing | No user choice dialogs ever | System decides Manhattan vs CIG automatically |
| Date fallback | today→week→month→all | Shows fallback note when widening window |
| Production guardrail | Q&A + data queries: prod only | Compare modal is the ONLY exempt feature |
| SQLcl MCP connection | connectionName field in groups | e.g. "connectionName": "MANP" — do NOT rename |
| Groups readOnly | All 5 groups have "readOnly": true | WMS·IQ never writes to Oracle |
| Total DCs | 32 active + 1 pending (UT) | UT active:false, goLive: 2026-09 |
| WMSHUB schemas | WMSHUB WMSHUB_CODE EM EM_CODE | EM not EMS — verified from config.json |
| MANH_CODE location | All 3 Manhattan DBs | MANP + MAN002P + MAN001P each have own instance |
| CIG-only DCs | 19 DCs with manhattanGroup: null | Route to cigwms-prod only, no Manhattan schema |
| SE_DM total objects | 7,077 scanned (2,126 tables confirmed match Oracle) | Full scan complete |
| Vector dimensions | 1024 | mxbai-embed-large — all tables use vector(1024) |
| Query enrichment | findTableByQuery enriches: "TABLE SCHEMA.OBJECTNAME" | Must match scan embedding format |
| Column enrichment | findColumnsByQuery enriches: "TABLENAME.COLUMNNAME" | Looks up table name from objectId |
| Hybrid doc search | Extension codes → title ILIKE; else → pgvector semantic | EX01/EX33/SDN-215 all resolved by title |
| Extension code variants | EX01, EX1, EX 01, extension 01, ext01, SDN-215 | extractExtensionCode() normalises all variants |
| hasAuthoritativeSourceContext bug | FIXED: removed guard from doc/knowledge/jira blocks | Docs were silently excluded when schema source present |
| config.json | in .gitignore — never commit | Copy from config.example.json on fresh clone |
| pgdata/ | in .gitignore — Docker volume data | Delete to reset PostgreSQL |

---

## 🏢 Project Overview

**McLane WMS·IQ** is a unified knowledge and intelligence platform for the McLane WMS ecosystem.
Currently migrating from **Manhattan WMOS 2019** to **Manhattan Active WM**.

- **Product name:** McLane WMS·IQ
- **Short form:** WMS·IQ
- **Tagline:** Navigate the McLane WMS landscape
- **Scope:** WMS only — NOT Financials, ERP, eCommerce, MDM, Data Intelligence
- **Active WM note:** Manhattan provides read-only PostgreSQL replicated tables, not direct DB access. All current Oracle connections via SQLcl MCP are unaffected.

### Two Entry Modes
1. **💬 Ask a Question** — business user, DC-aware, plain English answers ✅
2. **🔧 Explore Systems** — technical, schema browser, ERD, impact analysis ✅

### User Persona Rules
| | Business User (Ask mode) | Technical User (Explore Ask AI) |
|---|---|---|
| JIRA results | Plain English, no ticket keys | Full keys, summaries, status |
| DB results | Business impact only | Raw data, row counts, schema names |
| INVALID objects | Never shown | Shown with full detail |
| Schema/system names | Never mentioned | Full technical context |
| Answer length | Max 200 words | No limit |

---

## 🗄️ Database Connections & Groups (5 Groups)

All connections use **SQLcl MCP via queryDB()** — do NOT change this mechanism.
Connection names match SQLcl named connections on macOS.

```
manhattan-main  connectionName: MANP      env: prod  color: #3fb950
  Per-DC:  FS06_DM FS06_MDA  FE_DM FE_MDA  MD_DM MD_MDA
           MK_DM MK_MDA  MN_DM MN_MDA  MY_DM MY_MDA
           MZ_DM MZ_MDA  NE_DM NE_MDA  SE_DM SE_MDA
  Shared:  MANH  MANH_CODE
  DCs:     06(FS06) FE MD MK MN MY MZ NE SE
  Pending: UT_DM UT_MDA (active:false, goLive: 2026-09)

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
  DCs(CIG-only, manhattanGroup:null):
    GA GM HP ME MG MI MO MP MS MW NC NT NW PA SO SW SZ WJ
```

> ⚠️ MANH and MANH_CODE exist in ALL THREE Manhattan databases independently.
> Always specify group when querying — MANH_CODE is not unique to manhattan-main.
> ⚠️ Groups use `connectionName` field — do NOT rename to `db`.
> ℹ️ 19 CIG-only DCs have manhattanGroup: null — route to cigwms-prod only.

### Key Source Code Schemas
- `MANH_CODE` — ALL 3 Manhattan DBs: DOCK_PK, DC_SCHEMA_PK, LOG_PK, LOCATION_PK, TIMER, DOCK_WRAPPER_PK
- `FRAMEWORK` (cigwms-prod) — LOGS (874 spec + 1231 body lines)
- `OP` (cigwms-prod) — CIG WMS operational packages
- `MCLANE` (cigwms-prod) — McLane-specific packages
- `WMSHUB_CODE` (wmshub-prod) — WMSHUB packages
- `EM_CODE` (wmshub-prod) — EM packages

### Verified Oracle Table Names (SE_DM — confirmed live)
```
⚠️ These are the ACTUAL table names — not assumed names:
SHIPMENT          ← NOT SHIPMENT_HDR
WAVE              ← NOT WAVE_HDR
PICK_LOCN_HDR     ← NOT LOCN_HDR
TASK_HDR  TASK_DTL  LPN  LPN_DETAIL  OUTPT_LPN
ITEM_CBO  SHIPMENT_STATUS  SHIPMENT_EVENT
```
> These are discovered dynamically via /db/find-table — NEVER hardcode table names.

---

## 🏭 Distribution Centers

### 13 Manhattan DCs (active)

| DC_ID | Code | Name | Type | Group | DM Schema | MDA Schema |
|-------|------|------|------|-------|-----------|------------|
| 606 | 06 | Lakeland 606 | food-service | manhattan-main | FS06_DM | FS06_MDA |
| 290 | FE | McLane Ocala | grocery | manhattan-main | FE_DM | FE_MDA |
| 160 | MD | Dothan | grocery | manhattan-main | MD_DM | MD_MDA |
| 360 | MK | Cumberland | grocery | manhattan-main | MK_DM | MK_MDA |
| 460 | MN | Minnesota | grocery | manhattan-main | MN_DM | MN_MDA |
| 260 | MY | NE/Concord | grocery | manhattan-main | MY_DM | MY_MDA |
| 450 | MZ | Mid-Atlantic | grocery | manhattan-main | MZ_DM | MZ_MDA |
| 800 | NE | Northeast | grocery | manhattan-main | NE_DM | NE_MDA |
| 400 | SE | Southeast | grocery | manhattan-main | SE_DM | SE_MDA |
| 421 | C1 | CK Otsego | grocery | manhattan-ck | C1_DM | C1_MDA |
| 431 | C2 | CK St Louis | grocery | manhattan-ck | C2_DM | C2_MDA |
| 411 | C3 | CK Columbus | grocery | manhattan-ck | C3_DM | C3_MDA |
| 490 | WK | McLane Bluegrass | grocery | manhattan-wk | MAN490_DM | MAN490_MDA |

### 1 Pending Manhattan DC
| 210 | UT | McLane Salt Lake City | grocery | manhattan-main | UT_DM | UT_MDA | active:false, goLive:2026-09 |

### 19 CIG WMS-only DCs (manhattanGroup: null)
GA GM HP ME MG MI MO MP MS MW NC NT NW PA SO SW SZ WJ
All route to cigwms-prod with LEG_DIV_ID filter.

---

## ⚠️ Critical Architecture Notes

### #1 — Always use queryDB() never raw runMCP()
```javascript
// WRONG — returns [] even when data exists
const responses = await runMCP([{ name: 'run-sql', arguments: { sql } }]);
// RIGHT
const rows = await queryDB(group, sql);
```

### #2 — DBA_* views for all Oracle queries
```
DBA_OBJECTS  DBA_TAB_COLUMNS  DBA_SOURCE
DBA_CONSTRAINTS  DBA_CONS_COLUMNS  DBA_DEPENDENCIES
DBA_TABLES  DBA_PROCEDURES
```
Use DBA_* not ALL_* — ASRAJAG has DBA privileges in all 5 databases.

### #3 — Ollama always stream:false + 60s timeout
```javascript
const controller = new AbortController();
setTimeout(() => controller.abort(), 60000);
fetch(BRIDGE + '/ollama/chat', {
  method: 'POST', signal: controller.signal,
  body: JSON.stringify({ model, messages, stream: false })
});
```

### #4 — AI hallucination prevention
- Inject /db/source (PACKAGE BODY) into system prompt
- Only inject real JIRA keys from jiraResults.issues
- Empty context → "I don't have enough information" — never invent
- Extension codes (EX01, EX33) are NEVER stop words
- ⚠️ hasAuthoritativeSourceContext must NOT suppress docs/knowledge/jira blocks

### #5 — Production guardrail ⚠️ CRITICAL
```javascript
const prodGroups = config.groups.filter(g => g.env === 'prod');
// Q&A, DC resolver, POST /db/query: prod only
// Compare modal: exempt — may use any group
```

### #6 — No hardcoded table names ⚠️
Never hardcode Oracle table names anywhere in code.
Use the knowledge graph: GET /db/find-table?q=shipment&schema=SE_DM
The graph discovers actual names from DBA_TABLES at runtime.
Verified actual names: SHIPMENT (not SHIPMENT_HDR), WAVE (not WAVE_HDR),
PICK_LOCN_HDR (not LOCN_HDR) — see environment facts table.

### #7 — Document pipeline
```
Upload → mammoth(.docx) / pdftotext(.pdf) / readFileSync(.txt)
       → preprocessDocText() strips TOC/PAGEREF/Word noise
       → find content start (skip TOC)
       → chunkTextSentenceAware(text, 2500, 300)
       → prepend [Document: {title}] to each chunk
       → atomic write to docs-index/{group}-{name}.json
Re-index: POST /docs/reindex → {"reindexed":222}
```

### #8 — JIRA JQL keyword extraction
```javascript
// Strip stop words (from config.jiraStopWords)
// Keep extension codes: EX01, EX33, SDN-215 etc
if (/^[A-Z]{1,4}[-_]?\d+$/i.test(term)) keepAlways = true;
// Build: textfields ~ "MZIC6101*" AND textfields ~ "VARIANCE*"
// Max 4 terms (config: jiraMaxTerms)
```

### #9 — Q&A context assembly (parallel)
```
1. GET /semantic/search   → semantic intent (highest priority)
2. GET /knowledge/search  → knowledge hits 🧠
3. GET /docs/search       → document chunks 📄 (hybrid: code→title, else→pgvector)
4. GET /jira/search       → JIRA tickets 🎫
5. POST /db/query         → live DB 🗄️ (issue keywords only)
Total capped at 12,000 chars — complete chunks never truncated
⚠️ NEVER suppress docs/knowledge/jira with hasAuthoritativeSourceContext guard
```

### #10 — Hybrid document search (lib/docs-routes.js)
```javascript
// extractExtensionCode() normalises all user variants:
// "EX01" | "EX1" | "EX 01" | "extension 01" | "ext01" | "SDN-215" → canonical
// Step 1: if code detected → searchDocChunksByTitle(code) → title ILIKE %EX01%
// Step 2: if no title match → searchDocChunksByText(code) → chunk_text ILIKE %EX01%
// Step 3: no code → semanticDocSearch() → pgvector similarity
// Step 4: postgres down → docsSearch() keyword fallback
```

### #11 — Knowledge graph embedding rules ⚠️
```javascript
// Embedding model: mxbai-embed-large (1024 dims) — NEVER change back to nomic
// Schema objects embedded as: "TABLE SE_DM.SHIPMENT" or "PACKAGE MANH_CODE.DOCK_PK"
// findTableByQuery enriches query: `TABLE ${schema}.${queryText.toUpperCase()}`
// findColumnsByQuery looks up tableName from objectId, enriches: "TABLENAME.COLNAME"
// Embedding cache: max 5000 entries, LRU eviction
// Null embedding → ILIKE fallback — NEVER crash
// ivfflat indexes: need >100 rows — wrapped in try/catch
```

### #12 — Active WM migration safety
Existing SQLcl MCP Oracle connections unchanged.
When Active WM PostgreSQL replicas available:
add new group with engine:postgres, replicaOnly:true.
Graph automatically learns new schemas when scanned.

---

## 🗺️ Architecture

```
public/index.html  (http://localhost:3333) ← PROMPT 36: migrating to React+TS
        │
  Mode Selector
  ├── 💬 Ask a Question [BUSINESS PERSONA]
  │     → Parallel context: semantic + knowledge + docs(hybrid) + JIRA + DB
  │     → Business persona rules, 200 word cap
  │     → Date-aware shipment queries with fallback chain
  │     → Table names from knowledge graph (no hardcoding)
  │     → PROMPT 36: conversational history + context length bar
  │
  └── 🔧 Explore Systems [TECHNICAL PERSONA]
        Tabs: ⚡Impact | Home | 💬Ask AI | 📚Docs | 🧠Knowledge | 🔬Semantic
        Home: WMS Intelligence Overview + Governance quick actions
        🔬 Semantic: intent list, confirm/reject, bulk confirm, scan status
        ⟷ Compare: background diff job, history panel

bridge.js  (http://localhost:3333)
        │
        ├── lib/mcp-pool.js        → MCP pool + queryDB() [NEVER CHANGE]
        ├── lib/db-routes.js       → /db/* + /db/find-table + /db/find-columns
        │                             + /db/scan-schema + /db/scan-status
        ├── lib/docs-routes.js     → /docs/* + hybrid search + migrate-to-graph
        ├── lib/knowledge-routes.js → /knowledge/*
        ├── lib/jira-routes.js     → /jira/* + JQL builder
        ├── lib/ollama-routes.js   → /ollama/*
        ├── lib/semantic-routes.js → /semantic/* proxy to Python worker
        └── lib/graph-store.js     → PostgreSQL + pgvector (1024 dims, mxbai)
                │
                ▼
        Oracle (5 DBs via SQLcl MCP — ASRAJAG, all env:prod)
        MANP / MAN002P / MAN001P / OMSP / OPCIGP
        +
        Atlassian REST API (mclane.atlassian.net)
        +
        semantic-worker/app.py (port 3334)
        +
        PostgreSQL + pgvector port 5432 (Colima Docker)
          Tables: schema_objects, schema_columns, schema_source
                  schema_dependencies, doc_chunks
                  knowledge_entries, semantic_intents
          All vector(1024) — mxbai-embed-large
```

---

## 📋 Current config.json (key fields)

```json
{
  "bridge": {
    "port": 3333,
    "semanticWorkerPort": 3334,
    "ollamaUrl": "http://localhost:11434",
    "defaultModel": "qwen2.5:14b",
    "sqlclCommand": "sql",
    "sqlclArgs": ["-R", "2", "-mcp"],
    "poolEnabled": true,
    "poolIdleTimeoutMs": 300000,
    "poolMaxQueueDepth": 20,
    "toolCallTimeoutMs": 15000,
    "rateLimitPerMinute": 120,
    "allowedOrigins": ["null", "file://", "http://localhost:3333"],
    "authToken": "",
    "docsChunkSize": 2500,
    "docsChunkOverlap": 300,
    "docsMaxResults": 10,
    "answerWordCap": 200,
    "qaContextCharLimit": 12000,
    "jiraMaxResults": 10,
    "jiraMaxTerms": 4,
    "jiraStopWords": ["... full list in config.json ..."],
    "semanticConfidenceHigh": 0.8,
    "semanticConfidenceMedium": 0.5,
    "atlassianEnabled": true,
    "atlassianDomain": "mclane.atlassian.net",
    "atlassianEmail": "arun.rajagopalan@mclaneco.com",
    "atlassianToken": "<set — never commit>",
    "uploadToken": "",
    "postgresEnabled": true,
    "postgresUrl": "postgresql://wmsiq:wmsiq@localhost:5432/wmsiq",
    "graphScanEnabled": true,
    "graphScanIncludeSource": true,
    "graphScanIncludeColumns": true
  },
  "graphScanSchemas": {
    "manhattan-main": ["MANH_CODE", "MANH", "SE_DM", "SE_MDA"],
    "cigwms-prod": ["FRAMEWORK", "OP", "MCLANE", "CIGWMS"],
    "wmshub-prod": ["WMSHUB_CODE", "EM_CODE"]
  },
  "distributionCenters": ["... 32 active + 1 pending ..."],
  "groups": [
    { "id": "manhattan-main", "connectionName": "MANP", "env": "prod", "readOnly": true },
    { "id": "manhattan-ck",   "connectionName": "MAN002P", "env": "prod", "readOnly": true },
    { "id": "manhattan-wk",   "connectionName": "MAN001P", "env": "prod", "readOnly": true },
    { "id": "wmshub-prod",    "connectionName": "OMSP", "env": "prod", "readOnly": true },
    { "id": "cigwms-prod",    "connectionName": "OPCIGP", "env": "prod", "readOnly": true }
  ]
}
```

---

## 🚀 Pending Prompts

---

### PROMPT 34 — Auto-Detect Oracle Code Changes 🔜 FUTURE

```
Poll DBA_OBJECTS.LAST_DDL_TIME every N minutes.
When packages change → trigger semantic re-scan for that object.
Config: oracleChangePollingEnabled, oracleChangePollingIntervalMinutes,
        oracleChangePollingSchemas.
Endpoint: GET /db/changes?since=<ISO8601>&group=<id>
Health: add oracleChangePolling status object.
Do NOT implement until PROMPT 35/36 are stable.
```

---

### PROMPT 35 — Apache AGE Graph Database 🔜 NEXT

```
Read .github/copilot-instructions.md in full before writing anything.

Add Apache AGE (graph extension) to the existing PostgreSQL container.
AGE runs inside PostgreSQL alongside pgvector — same container, same port.
Replace the BFS impact analysis in lib/db-routes.js with Cypher graph traversal.

━━━ PART 1 — Docker image upgrade ━━━

The current docker-compose.yml uses pgvector/pgvector:pg16.
AGE requires a custom image that has BOTH pgvector AND AGE.

Create Dockerfile.postgres in project root:
  FROM pgvector/pgvector:pg16
  RUN apt-get update && apt-get install -y \
      build-essential postgresql-server-dev-16 \
      libreadline-dev zlib1g-dev \
    && git clone https://github.com/apache/age.git /tmp/age \
    && cd /tmp/age && git checkout PG16 \
    && make && make install \
    && rm -rf /tmp/age \
    && apt-get remove -y build-essential postgresql-server-dev-16 \
    && apt-get autoclean

Update docker-compose.yml:
  build: { context: ., dockerfile: Dockerfile.postgres }
  # Remove: image: pgvector/pgvector:pg16
  Add to healthcheck env: PGPASSWORD=wmsiq

━━━ PART 2 — initSchema() additions in lib/graph-store.js ━━━

After CREATE EXTENSION IF NOT EXISTS vector, add:
  CREATE EXTENSION IF NOT EXISTS age;
  LOAD 'age';
  SET search_path = ag_catalog, "$user", public;

Create the WMS dependency graph:
  SELECT * FROM ag_catalog.create_graph('wms_dependencies')
  ON CONFLICT DO NOTHING;

Vertex labels:
  SELECT * FROM ag_catalog.create_vlabel('wms_dependencies', 'OracleObject')
  ON CONFLICT DO NOTHING;

Edge labels:
  SELECT * FROM ag_catalog.create_elabel('wms_dependencies', 'DEPENDS_ON')
  ON CONFLICT DO NOTHING;
  SELECT * FROM ag_catalog.create_elabel('wms_dependencies', 'USED_BY')
  ON CONFLICT DO NOTHING;

━━━ PART 3 — lib/graph-store.js new functions ━━━

upsertAgeVertex(groupId, schemaName, objectName, objectType, status):
  Upsert vertex in AGE graph:
  SELECT * FROM cypher('wms_dependencies', $$
    MERGE (o:OracleObject {
      id: $id,
      groupId: $groupId,
      schema: $schema,
      name: $name,
      type: $type,
      status: $status
    })
    RETURN o
  $$, $params) AS (o agtype);
  id = "{groupId}::{schemaName}::{objectName}::{objectType}"

upsertAgeEdge(fromId, toId, edgeType, dependencyType):
  SELECT * FROM cypher('wms_dependencies', $$
    MATCH (a:OracleObject {id: $fromId}),
          (b:OracleObject {id: $toId})
    MERGE (a)-[r:DEPENDS_ON {type: $depType}]->(b)
    RETURN r
  $$, $params) AS (r agtype);

traverseImpact(objectId, direction, maxDepth):
  direction: 'outbound' (what this depends on) or 'inbound' (what uses this)
  SELECT * FROM cypher('wms_dependencies', $$
    MATCH path = (start:OracleObject {id: $id})-[:DEPENDS_ON*1..$depth]-(related)
    RETURN related, length(path) as depth
    LIMIT 200
  $$, $params) AS (related agtype, depth agtype);

━━━ PART 4 — Update POST /db/scan-schema in lib/db-routes.js ━━━

After upserting schema_objects and schema_dependencies into PostgreSQL:
  Also upsert into AGE graph:
  await graphStore.upsertAgeVertex(scanGroupId, schema, objName, objType, status)
  
After scanning DBA_DEPENDENCIES:
  await graphStore.upsertAgeEdge(fromId, toId, 'DEPENDS_ON', dep.DEPENDENCY_TYPE)

━━━ PART 5 — New endpoint GET /db/impact-graph ━━━

Replace or supplement existing /db/impact BFS with AGE Cypher traversal:
GET /db/impact-graph?name=&schema=&group=&depth=3&direction=both

Uses graphStore.traverseImpact() for fast graph traversal.
Returns same shape as /db/impact for UI compatibility:
{ root, nodes, edges, crossSchemaEdges, truncated, queryMs }

Falls back to existing BFS /db/impact if AGE not available.
⚠️ Do NOT remove existing /db/impact — keep both endpoints.

━━━ PART 6 — GET /health additions ━━━

Add:
  "age": true/false,
  "ageGraph": "wms_dependencies",
  "ageVertices": N

━━━ TESTS — add section M to test-bridge.js ━━━

M1: GET /health → has age field (true or false)
M2: GET /db/impact-graph?name=DUAL&schema=SYS&group=manhattan-main&depth=1
    → 200, has nodes array
M3: POST /db/scan-schema with AGE enabled → vertices appear in AGE graph

Do NOT change test sections A–L.
Do NOT remove existing /db/impact endpoint.
AGE unavailable → graceful fallback to BFS, do NOT crash.
```

---

### PROMPT 36 — React + TypeScript Frontend + Conversational Ask Mode 🔜 NEXT

```
Read .github/copilot-instructions.md in full before writing anything.

Migrate public/index.html (single ~4000 line file) to a proper
React + TypeScript + Vite application. The bridge.js server is unchanged —
only the frontend changes. Output still goes to public/ which bridge serves.

━━━ TECH STACK ━━━

  Vite + React 18 + TypeScript
  Tailwind CSS (keep existing dark theme + color palette)
  No Next.js — bridge.js is already the server
  Build output: public/dist/ (bridge serves index.html from there)
  Dev: vite dev server proxies /api/* to localhost:3333

━━━ COMPONENT STRUCTURE ━━━

src/
  main.tsx                    ← React entry point
  App.tsx                     ← mode selector (Ask / Explore)
  
  types/
    config.ts                 ← Group, DC, BridgeConfig, HealthStatus
    qa.ts                     ← QAMessage, DocHit, KnowledgeHit, JiraHit
    graph.ts                  ← SchemaObject, ImpactNode, ImpactEdge
    semantic.ts               ← SemanticIntent, ConfidenceLevel
  
  api/
    bridge.ts                 ← typed fetch wrappers for ALL bridge endpoints
    useHealth.ts              ← polling hook for /health
  
  hooks/
    useConversation.ts        ← conversation history, context window tracking
    useBridge.ts              ← bridge config loader
    useGroups.ts              ← group loading + active group state
  
  components/
    AskMode/
      AskMode.tsx             ← main Ask mode container
      ChatThread.tsx          ← conversation history display
      MessageBubble.tsx       ← single Q&A message with pills
      QuestionInput.tsx       ← input bar + Ask button
      ContextBar.tsx          ← context length progress bar (see below)
      SampleQuestions.tsx     ← suggested question chips
      LearningHint.tsx        ← optional hint field
    
    ExploreMode/
      ExploreMode.tsx         ← group picker → main explore app
      GroupCard.tsx           ← group picker card with health indicator
      SchemaExplorer.tsx      ← schema browser tab
      ImpactAnalysis.tsx      ← ⚡Impact tab
      ErdDiagram.tsx          ← ERD SVG viewer
    
    tabs/
      AskAITab.tsx            ← 💬Ask AI tab in Explore mode
      DocsTab.tsx             ← 📚Docs library tab
      KnowledgeTab.tsx        ← 🧠Knowledge tab
      SemanticTab.tsx         ← 🔬Semantic tab
      HomeTab.tsx             ← Home dashboard tab
    
    shared/
      Toast.tsx               ← toast notifications
      Settings.tsx            ← settings panel
      DemoModeBanner.tsx      ← yellow demo mode banner
      HealthBadge.tsx         ← postgres/ollama/atlassian status dots

━━━ CONVERSATIONAL ASK MODE ━━━

Replace single-answer box with a chat thread (like this interface).
State lives in useConversation.ts hook.

interface ConversationMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  docHits: DocHit[];
  knowledgeHits: KnowledgeHit[];
  jiraHits: JiraHit[];
  dbHits: DbHit[];
  timestamp: Date;
}

Ollama call includes last N turns of history:
  messages: [
    { role: 'system', content: systemPrompt },
    ...conversationHistory.slice(-8),   // last 8 turns
    { role: 'user', content: currentQuestion }
  ]

New Conversation button: clears history, starts fresh.
History persists for browser session only (no localStorage).

━━━ CONTEXT LENGTH PROGRESS BAR (ContextBar.tsx) ━━━

Shown as thin bar below the chat input, always visible.

Token estimate: (systemPrompt.length + historyChars) / 4
Model context limits (fetch from GET /health → model field, then lookup):
  qwen2.5:14b  → 32,768 tokens
  gemma4:26b   → 131,072 tokens
  llama3       → 8,192 tokens
  default      → 8,192 tokens

Color thresholds:
  0–70%   → green  (#3fb950)
  70–90%  → amber  (#d29922)
  90%+    → red    (#f85149)

Display: "Context: 73% used  [███████████░░░░] 5,832 / 8,192 tokens"

At 90% — non-blocking prompt appears above input:
  ┌─────────────────────────────────────────────┐
  │ ⚠️  You're near the context limit (91%)      │
  │ [📋 Summarize & start new chat]              │
  │ [↩️  Keep going until limit]                 │
  └─────────────────────────────────────────────┘

If user chooses "Summarize & start new chat":
  1. Send Ollama request: "Summarize this conversation into a compact
     session brief covering: key WMS topics discussed, schemas/DCs mentioned,
     conclusions reached, any open questions. Max 200 words."
  2. Show brief to user + copy to clipboard
  3. Pre-fill new conversation with: "Continuing from previous session: {brief}"
  4. Clear history, start fresh conversation

If user chooses "Keep going":
  Dismiss prompt.
  At 95%: oldest turns start dropping (sliding window).
  Show note: "Oldest messages trimmed to fit context"

━━━ TYPESCRIPT INTERFACES (types/config.ts) ━━━

export interface Group {
  id: string;
  name: string;
  env: 'prod' | 'uat' | 'test';
  connectionName: string;
  schemas: string[];
  color: string;
  icon: string;
  readOnly: boolean;
  description: string;
}

export interface DistributionCenter {
  code: string;
  dcId: number;
  name: string;
  type: string;
  active: boolean;
  manhattanGroup: string | null;
  dmSchema: string | null;
  mdaSchema: string | null;
  cigwmsGroup: string | null;
  wmshubGroup: string | null;
}

export interface BridgeConfig {
  port: number;
  ollamaUrl: string;
  defaultModel: string;
  postgresEnabled: boolean;
  qaContextCharLimit: number;
  docsMaxResults: number;
  atlassianEnabled: boolean;
}

export interface HealthStatus {
  bridge: boolean;
  ollama: boolean;
  postgres: boolean;
  atlassian: boolean;
  graphReady: boolean;
  graphObjects: number;
  model: string;
  groups: number;
}

━━━ VITE CONFIG ━━━

vite.config.ts:
  build.outDir: '../public/dist'
  build.emptyOutDir: true
  server.proxy: { '/api': 'http://localhost:3333' }
  
  All fetch calls use relative paths (/api/...) in dev,
  and window.location.origin in production.

━━━ BRIDGE CHANGES (minimal) ━━━

bridge.js: add static file serving for public/dist/:
  if (fs.existsSync(path.join(__dirname, 'public/dist/index.html'))):
    serve public/dist/ for all non-API routes
  else:
    serve public/index.html (fallback to old app during migration)

This means old and new UIs coexist during migration.
Zero changes to any lib/*.js files.

━━━ MIGRATION STRATEGY ━━━

1. Scaffold Vite + React + TS in new src/ directory
2. Create all TypeScript interfaces
3. Create api/bridge.ts with typed wrappers for ALL endpoints
4. Build AskMode first (most complex — sendQA, context assembly, history)
5. Build ExploreMode (group picker, schema browser, ERD)
6. Build tabs one by one
7. Run both UIs side by side until React version is verified
8. Remove public/index.html when migration complete

⚠️ NEVER change lib/mcp-pool.js or queryDB() during this prompt.
⚠️ NEVER break the bridge API — all existing endpoints stay unchanged.
⚠️ Keep public/index.html until React version is fully verified.
⚠️ The hasAuthoritativeSourceContext guard must NOT suppress docs/knowledge/jira.
⚠️ Document context must always be included in the prompt when found.

━━━ START.SH ADDITION ━━━

If public/dist/index.html exists:
  echo "⚛️  React UI available at http://localhost:3333"
Else:
  echo "📄 Legacy UI at http://localhost:3333 (React build not found)"
```

---

### PROMPT 37 — LDAP Authentication (Multi-User) 🔜 FUTURE (post-demo)

```
After stakeholder approval and server deployment.
Use LDAP/Active Directory — already proven in another McLane POC.
Azure AD SSO requires IT approval (AADSTS65002 blocks direct access).

Config: ldapEnabled, ldapUrl, ldapBaseDn, sessionSecret.
Multi-user: capturedBy/confirmedBy = real LDAP username.
Do NOT implement until server deployment planned.
```

---

### PROMPT 38 — Docker Compose Multi-Container 🔜 FUTURE (post-demo)

```
Full production Docker Compose:
  frontend (Nginx), bridge (Node.js), worker (Python),
  postgres (pgvector + AGE), ollama (GPU preferred).
Secrets via environment variables, not config.json.
Do NOT implement until IT provisions server.
```

---

### PROMPT 23 — UT DC Activation (September 2026) 🔜 FUTURE

```
Config-only when UT goes live:
1. distributionCenters UT: set "active": true
2. manhattan-main schemas: add "UT_DM", "UT_MDA"
3. Verify schemas in DBA_OBJECTS
4. Restart bridge — zero code changes
5. Update this file: UT → "✅ Live"
```

---

## 🔧 Troubleshooting

| Symptom | Fix |
|---------|-----|
| "Bridge offline" | `node bridge.js`; check `/health` |
| "Ollama offline" | h.ollama === true check in verifyOllamaReady() |
| Ollama hangs | 60s AbortController — check qaContextCharLimit |
| AI hallucinated ticket numbers | jiraResults not injected into prompt |
| 🎫 pill not showing | Check jiraHits scope at pill render |
| atlassian:false | Check domain/email/token in config.json |
| JIRA returns empty | textfields~ JQL — check stop word filtering |
| Extension code stripped (EX01) | Never strip /^[A-Z]{1,4}[-_]?\d+$/i patterns |
| Ask mode "No prod groups" | groupsLoadPromise timing — await loadGroups() |
| Doc chunks show TOC garbage | preprocessDocText() — run POST /docs/reindex |
| Any /db/* returns [] | Using raw runMCP() — always use queryDB() |
| Schema shows (0) objects | Use DBA_OBJECTS not ALL_OBJECTS |
| E1 hangs forever | depth=1 parameter missing — never remove |
| Q&A hits wrong database | env field — all prod groups need "env":"prod" |
| AI answer ignores found documents | hasAuthoritativeSourceContext guard — must be removed from doc/knowledge/jira block conditions |
| EX01 returns wrong document | extractExtensionCode() not firing — check pattern match |
| Vector search all same similarity | mxbai-embed-large may be returning identical vectors — restart Ollama |
| nomic-embed-text identical vectors | Known broken — do NOT use, switch to mxbai-embed-large |
| upsertObject error: expected 768 not 1024 | Tables created with wrong dims — drop all tables, restart bridge |
| find-table returns empty | Query not enriched — check findTableByQuery enrichedQuery format |
| Colima won't start | colima start --runtime docker (not default k3s) |
| docker compose: unknown flag -d | Use docker-compose (hyphenated) not docker compose |
| Docker pull TLS error | Corporate SSL interception — pull via crane on home network |
| postgres won't start | colima start first, then docker-compose up -d postgres |
| pgvector index fails | Need at least 100 rows before ivfflat index works |
| start.sh permission denied | chmod +x start.sh stop.sh |
| ngrok blocked | Use phone hotspot or 10.98.215.99 directly |
| PDFs fail bulk upload | brew install poppler (provides pdftotext) |
| "076 rows selected." as object_type | SQLcl footer leaking — parseMCPResult() footer filter |

---

## 📁 Project File Structure

```
knowledgeBase/
├── .github/
│   └── copilot-instructions.md   ← this file (v20)
├── bridge.js                     ← thin entry point + HTTP server
├── lib/
│   ├── mcp-pool.js               ← MCP pool + queryDB() [NEVER CHANGE]
│   ├── db-routes.js              ← /db/* + scan/find graph endpoints
│   ├── docs-routes.js            ← /docs/* + hybrid search + migrate-to-graph
│   ├── knowledge-routes.js       ← /knowledge/*
│   ├── jira-routes.js            ← /jira/* + JQL builder + stop words
│   ├── ollama-routes.js          ← /ollama/*
│   ├── semantic-routes.js        ← /semantic/* proxy to Python worker
│   └── graph-store.js            ← PostgreSQL + pgvector + AGE (PROMPT 35)
├── src/                          ← React + TypeScript [PROMPT 36]
│   ├── main.tsx
│   ├── App.tsx
│   ├── types/
│   ├── api/
│   ├── hooks/
│   └── components/
├── public/
│   ├── index.html                ← Legacy frontend (kept until React verified)
│   └── dist/                    ← React build output [PROMPT 36]
├── semantic-worker/
│   ├── app.py                    ← Python Flask semantic engine
│   └── .venv/                    ← Python virtualenv
├── OP/                           ← Oracle PL/SQL source code
├── Dockerfile.postgres           ← pgvector + AGE image [PROMPT 35]
├── docker-compose.yml            ← PostgreSQL container
├── config.json                   ← ⚠️ in .gitignore — never commit
├── config.example.json           ← Safe template — commit this
├── .gitignore                    ← node_modules, config.json, pgdata/, docs-index/ etc
├── debug-mcp.js
├── test-bridge.js                ← 50/50 passing (A–L), M1–M3 added by prompt 35
├── bulk_upload_docs.py           ← Python bulk upload (needs poppler for PDFs)
├── start.sh / stop.sh            ← chmod +x required
├── SETUP.md
├── docs-index/                   ← 222 docs (in .gitignore)
├── knowledge-index/              ← institutional knowledge (in .gitignore)
├── semantic-index/               ← 76+ confirmed intents (in .gitignore)
│   └── intents.json
└── pgdata/                       ← PostgreSQL data (in .gitignore)
```

---

## 📊 POC vs Production Roadmap

### Current Phase — POC (MacBook, single user)
```
✅ PROMPTS 1–31   Complete and demo-ready
✅ PROMPT 32/33  Oracle Knowledge Graph + pgvector ✅
✅ PROMPT 35     Apache AGE graph ← COMPLETED
─────────────────────────────────────────────────────
🔜 PROMPT 36     React + TypeScript + Conversational UI ← DO NEXT
─────────────────────────────────────────────────────
SHOW TO McLane IT / Management
─────────────────────────────────────────────────────
```

### Post-Approval Phase
```
PROMPT 34   Oracle change detection (LAST_DDL_TIME)
PROMPT 37   LDAP authentication (multi-user)
PROMPT 38   Docker Compose (full production)
```

### Technology Stack
| Layer | POC (now) | Production |
|---|---|---|
| Oracle connection | SQLcl MCP (keep forever) | SQLcl MCP |
| Active WM | Not yet — replicas pending | PostgreSQL replica group |
| Knowledge graph | PostgreSQL + pgvector (Docker/Colima) | PostgreSQL (IT-managed) |
| Graph traversal | Apache AGE + Cypher (PROMPT 35) | Apache AGE |
| Frontend | Single HTML → React + TypeScript (PROMPT 36) | React + TypeScript |
| Auth | None | LDAP |
| LLM local | Ollama qwen2.5:14b | Ollama (GPU) |
| LLM cloud | Claude API (future) | Claude API |
| Embedding | mxbai-embed-large 1024d | mxbai-embed-large |
