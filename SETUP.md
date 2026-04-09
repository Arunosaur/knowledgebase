# Setup Instructions for Oracle Knowledge Base

This file documents the steps to get the knowledge base running locally.

## 1. Create Named Connections in SQLcl

This is the **crucial first step**. Credentials live in SQLcl, not in any file.

```bash
# Launch SQLcl
sql /nolog

# Create a named connection for each group
SQL> connmgr save -name "HR Production"    -user hr_readonly    -password -url jdbc:oracle:thin:@hrdb.company.com:1521/HRPROD
SQL> connmgr save -name "Finance DB"       -user fin_readonly   -password -url jdbc:oracle:thin:@findb.company.com:1521/FINPROD
SQL> connmgr save -name "DW Reporting"     -user dw_user        -password -url jdbc:oracle:thin:@dwdb.company.com:1521/DW

# Verify connections are saved
SQL> connmgr list

# Test a connection
SQL> conn -name "HR Production"
```

The connection name you use here must **exactly match** the `connectionName` field in `config.json`.

> **VS Code shortcut**: If you've already set up connections in the Oracle SQL Developer VS Code
> extension, those same named connections are available to `sql -mcp` automatically — no need to
> re-enter them via the CLI.

## 2. Configure Groups

Edit `config.json`. Set `connectionName` to match your saved connection names exactly:

```json
{
  "groups": [
    {
      "id": "hr-prod",
      "name": "HR System",
      "icon": "👥",
      "color": "#3fb950",
      "connectionName": "HR Production",
      "schemas": ["HR", "HR_AUDIT"],
      "readOnly": true
    }
  ]
}
```

## 3. Install & Start Ollama

```bash
brew install ollama
ollama pull llama3       # or: codellama (best for PL/SQL), mistral, deepseek-coder
ollama serve
```

## 4. Start the Bridge (recommended)

Instead of manually running `node bridge.js` every time, use the provided helper scripts.

```bash
chmod +x start.sh stop.sh   # first time only
./start.sh                  # starts Ollama (if needed) + bridge, opens browser
# ... work in the knowledge base ...
./stop.sh                   # stops bridge when done
```

The `start.sh` script checks for prerequisites, pulls the model if necessary, kills any
existing bridge on the configured port, and then launches `node bridge.js --debug`.
It also opens `knowledge-base.html` in your default browser.

## 5. Open the Knowledge Base

If you're not using the script, simply run:

```bash
node bridge.js
```

and then double-click `knowledge-base.html` (or use `open knowledge-base.html`).

## 6. Required Oracle Grants

```sql
GRANT SELECT ON ALL_OBJECTS      TO <user>;
GRANT SELECT ON ALL_SOURCE       TO <user>;
GRANT SELECT ON ALL_TAB_COLUMNS  TO <user>;
GRANT SELECT ON ALL_CONSTRAINTS  TO <user>;
GRANT SELECT ON ALL_CONS_COLUMNS TO <user>;
GRANT SELECT ON ALL_DEPENDENCIES TO <user>;
GRANT SELECT ON ALL_PROCEDURES   TO <user>;
```

## 7. Troubleshooting

| Symptom | Fix |
|---------|-----|
| "Bridge offline" in UI | Run `node bridge.js` or use `./start.sh` |
| "Ollama offline" warning | Run `ollama serve` |
| "Invalid connection" error from MCP | Connection name in config.json doesn't match saved SQLcl connection exactly (case-sensitive) |
| No connections listed | Run `sql /nolog` then `connmgr list` to verify |
| Timeout on DB calls | Increase `sleep()` delays in `runMCP()` for slow networks |
| No schemas appear | Check `schemas` array in config.json (must be UPPERCASE) or check Oracle grants |
| SQLcl not found | Ensure `sql` is on `$PATH`: `which sql` |

## 8. Power Automate Document Ingestion (Prompt 20)

Use this mode when Azure AD blocks MSAL sign-in from the browser.

### Bridge endpoints

- `GET /docs/upload-token` → returns `{ "token": "..." }`
- `POST /docs/upload` → ingests one document into `docs-index/`

`POST /docs/upload` body:

```json
{
  "filename": "Receiving SOP.docx",
  "content": "<base64-file-content>",
  "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  "webUrl": "https://mclaneco.sharepoint.com/...",
  "lastModified": "2026-03-12T18:00:00Z",
  "group": "wmshub-prod",
  "site": "WMSHUB"
}
```

### Build Power Automate flows (one per site)

Create one flow each for:
- WMSHUB SharePoint → `group: "wmshub-prod"`
- OP/CMS SharePoint → `group: "cigwms-prod"`
- Manhattan SharePoint → `group: "manhattan-main"`

Flow steps:
1. Trigger: **When a file is created or modified** (SharePoint)
2. Action: **Get file content**
3. Action: **HTTP** (POST) to `http://[mac-ip]:3333/docs/upload`
4. Body: map filename, file content (base64), mimeType, webUrl, lastModified, group, site

Get Mac IP:

```bash
ipconfig getifaddr en0
```

### Notes

- Set `bridge.uploadToken` in `config.json` and pass it as `x-upload-token` header (or Bearer token) from Power Automate for protected uploads.
- Uploaded docs are chunked at 800 chars with 100 overlap and stored in `docs-index/{group}-{sanitized-filename}.json`.

## Atlassian JIRA Integration

### Enabling JIRA Search
1. Open Settings drawer in WMS·IQ
2. Find the Atlassian section
3. Toggle "Enable JIRA search in Q&A" ON
4. Enter Domain (example: `mclaneco.atlassian.net`)
5. Enter Email (your Atlassian login email)
6. Enter API Token (from Atlassian)
7. Optionally select project keys in the multi-select filter
8. Click **Save Atlassian Settings**
9. Click **Test Connection** and confirm status shows ● Connected

### Atlassian Credentials
- Domain: your Atlassian domain, e.g. `mclaneco.atlassian.net`
- Email: your Atlassian login email
- Token: create at `id.atlassian.com/manage-profile/security/api-tokens`

### What JIRA Search Does
- Every Q&A question searches JIRA in parallel with docs and knowledge base
- Matching tickets are injected into the AI context
- 🎫 pill shows how many tickets were found
- Related tickets appear below the AI answer with direct ↗ Open in JIRA links
- If Atlassian is unreachable or credentials are missing, Q&A continues normally without error

### Power Automate Flow — SharePoint Sync (existing)
- Existing SharePoint create/modify ingestion flow content remains in this file above under Section 8, unchanged.

### Power Automate Flow — JIRA to Knowledge Base (Prompt 30)

JIRA resolved/closed tickets can now be pushed directly into `docs-index/` so they are searchable in Q&A context.

#### Endpoints
- `GET /jira/upload-token` → returns `{ "token": "..." }`
- `POST /jira/upload` → ingests one resolved/closed ticket into `docs-index/`

`POST /jira/upload` body:

```json
{
  "group": "manhattan-main",
  "ticket": {
    "key": "MANH-2284",
    "summary": "Wave release variance at MZ",
    "status": "Resolved",
    "resolution": "Fixed",
    "project": "Manhattan",
    "type": "Bug",
    "priority": "High",
    "assignee": "Jane Doe",
    "reporter": "John Doe",
    "updated": "2026-03-14T18:20:00Z",
    "url": "https://mclane.atlassian.net/browse/MANH-2284",
    "description": "Issue details...",
    "labels": ["wave", "variance"],
    "components": ["WMS"],
    "comments": [
      { "author": "Jane Doe", "created": "2026-03-14T17:00:00Z", "body": "Root cause and fix." }
    ]
  }
}
```

#### Behavior
- Only `status` = `Resolved` or `Closed` is ingested.
- Any other status is skipped with `reason: "status-not-resolved-or-closed"`.
- Stored file format is a docs-index JSON document with sentence-aware chunks (800 chars, 100 overlap), source=`jira-power-automate`, and embedded normalized ticket metadata.

#### Build Power Automate flow (JIRA)
1. Trigger: **When issue transitioned** (or equivalent webhook) in JIRA
2. Condition: status is `Resolved` OR `Closed`
3. Action: **HTTP** (POST) to `http://[mac-ip]:3333/jira/upload`
4. Headers:
   - `Content-Type: application/json`
   - `x-upload-token: <token from /jira/upload-token>` (if configured)
5. Body: map JIRA fields to the payload above

#### Security
- Reuses `bridge.uploadToken` token validation pattern from docs ingestion
- Supply token via `x-upload-token`, Bearer auth, or `?token=` query parameter

## Semantic Layer

The Semantic Layer builds and stores intent → database mapping entries in `semantic-index/intents.json` and makes them available to Ask mode and Explore mode.

### What it does
- Runs a Python Flask worker at `http://127.0.0.1:3334`
- Stores semantic intent entries as JSON (POC-safe, no PostgreSQL)
- Supports search, confirmation workflow, stats, and scan status
- Enables Q&A to use confirmed semantic mappings before normal fallback flow

### Pause/Resume scan
- Pause: `POST /semantic/pause`
- Resume: `POST /semantic/resume`
- Status: `GET /semantic/scan-status` (returns `running`, `paused`, `progress`, `lastScan`)

### Confirm mappings in UI
- Open Explore Systems mode
- Click `🔬 Semantic` tab
- Select an intent from the left list
- Use:
  - `✓ Confirm` to approve mapping
  - `✗ Reject` to remove mapping
  - `✏ Edit` to save field edits
  - `▶ Test SQL` to run the SQL template through `POST /db/query`

### Confidence scoring
- `confidence` is stored as `0.0` to `1.0`
- Confirmed mappings with confidence `> 0.8` are treated as high-confidence in Q&A
- Unconfirmed mappings with confidence `> 0.5` are used with caveats
- Usage updates can increase/decrease confidence over time

---

## Quick Start (Recommended)

```bash
chmod +x start.sh stop.sh   # first time only
./start.sh                  # starts Ollama + bridge, opens browser
./stop.sh                   # stops bridge when done
```

This script sequence streamlines development and testing by automating service management.

## PostgreSQL Knowledge Graph Setup

The knowledge graph stores Oracle schema metadata with pgvector embeddings,
enabling semantic search across tables, columns, and source code.

### Prerequisites
- Docker Desktop installed and running

### Steps

1. Start PostgreSQL:
  ```bash
  docker compose up -d postgres
  ```

2. Enable in config.json:
  ```json
  "postgresEnabled": true
  ```

3. Restart the bridge:
  ```bash
  ./stop.sh && ./start.sh
  ```

4. Migrate existing data (run once):
  ```bash
  curl -X POST http://localhost:3333/docs/migrate-to-graph
  curl -X POST http://localhost:3333/knowledge/migrate-to-graph
  curl -X POST http://localhost:3333/semantic/migrate-to-graph
  ```
  Note: Document migration (~226 files) takes ~17 minutes. Runs in background.

5. Scan Oracle schemas (takes 10-30 minutes per group):
  ```bash
  curl -X POST http://localhost:3333/db/scan-schema \
    -H "Content-Type: application/json" \
    -d '{"group":"manhattan-main","schemas":["MANH_CODE","SE_DM"]}'
  ```

6. Check status:
  ```bash
  curl http://localhost:3333/db/scan-status
  curl http://localhost:3333/health
  ```

### Fallback
All existing JSON-based search (docs, knowledge, semantic) remains fully active
if PostgreSQL is down or `postgresEnabled: false`. The graph enhances; it does not replace.

### Resetting the graph
```bash
docker compose down -v   # removes pgdata/ volume
docker compose up -d postgres
```
Then re-run migration and scan steps above.