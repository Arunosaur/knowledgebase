# Semantic Layer — Two-Phase Discovery System

## Problem Statement

**Before:** Hardcoded discovery worked only for questions implemented in PL/SQL packages.

**Now:** Two-phase system handles:
1. PL/SQL package-based logic (WAVE_PK, LOAD_PK, etc.)
2. Java/C++ application code that queries tables directly

---

## Architecture

```
User Question: "How many orders were waved?"
                        ↓
              GET /semantic/search
                        ↓
    ┌───────────────────┴─────────────────┐
    │                                     │
    ▼                                     ▼
Phase 1: Package Source            Phase 2: Table Patterns
(PL/SQL logic)                      (Java/C++ app queries)
╔══════════════════════╗            ╔══════════════════════╗
║ WAVE_PK package      │            ║ WAVE_HDR table       │
║ ├─ procedures       │            │ ├─ WAVE_NBR (ID)     │
║ ├─ functions        │            │ ├─ WAVE_STATUS (ST)  │
║ └─ business logic   │            │ ├─ CREATION_DATE     │
║                     │            │ ├─ LOAD_NBR (FK)     │
║ Ollama extracts:    │            │ └─ WAVE_COUNT (AMT)  │
║ intent, tables,     │            │                      │
║ columns, SQL        │            │ Pattern matcher:     │
║ confidence: 0.78    │            │ - STATUS column → status
║                     │            │ - DATE column → date
╚══════════════════════╝            │ - COUNT column → aggregate
                                    │ confidence: 0.68-0.72
                                    │
                                    │ Generates:
                                    │ SELECT WAVE_STATUS, COUNT(*)
                                    │ FROM WAVE_HDR
                                    │ GROUP BY WAVE_STATUS
                                    ╚══════════════════════╝
    │                                     │
    └───────────────────┬─────────────────┘
                        ↓
        Merged semantic index: 5 intents
        - Highest confidence first
        - Used to answer question
```

---

## Phase 1: Package Source Analysis

**When:** Bridge discovers WAVE_PK package in MANH_CODE schema

**How:**
1. Execute: `SELECT TEXT FROM DBA_SOURCE WHERE NAME='WAVE_PK' AND TYPE='PACKAGE BODY'`
2. Send to Ollama with prompt:
   ```
   Given this PL/SQL source, extract all business operations.
   For each: intent, tables, columns, input parameters, SQL template.
   Return JSON only.
   ```

**Example Output:**
```json
{
  "intent": "Check if orders were waved and reasons",
  "keywords": ["waved", "wave status", "cancellation"],
  "package": "WAVE_PK",
  "procedure": "check_wave_status",
  "tables": ["WAVE_HDR", "WAVE_DTL"],
  "columns": ["WAVE_STATUS", "LOAD_NBR", "WAVE_CREATION_DATE"],
  "sqlTemplate": "SELECT COUNT(*) FROM {schema}.WAVE_HDR WHERE WAVE_STATUS='W'",
  "confidence": 0.78
}
```

**Integrates Via:** Already exists in bridge.js (lines 1228-1346)

---

## Phase 2: Table Schema Analysis (NEW)

**When:** Bridge discovers key tables (WAVE_HDR, SHIPMENT, LOAD_HDR, etc.)

**How:**
1. Execute: `SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER='MANH_CODE' AND TABLE_NAME LIKE 'WAVE%'`
2. For each table, execute: `SELECT COLUMN_NAME, DATA_TYPE FROM DBA_TAB_COLUMNS WHERE TABLE_NAME='WAVE_HDR'`
3. Send columns to semantic worker `/analyze-table` endpoint
4. Pattern matcher classifies columns:
   ```
   WAVE_NBR        → ID column (matches pattern: _NBR)
   WAVE_STATUS     → STATUS column (matches: _STATUS)
   CREATION_DATE   → DATE column (matches: _DATE)
   WAVE_COUNT      → AMOUNT column (matches: _COUNT)
   ```
5. Generate SQL templates from patterns:
   - **Count by status**: `SELECT WAVE_STATUS, COUNT(*) FROM WAVE_HDR GROUP BY WAVE_STATUS`
   - **Recent records**: `SELECT WAVE_NBR FROM WAVE_HDR ORDER BY CREATION_DATE DESC FETCH FIRST 10`
   - **Time-series**: `SELECT TRUNC(CREATION_DATE) as day, WAVE_STATUS, COUNT(*) FROM WAVE_HDR WHERE CREATION_DATE >= TRUNC(SYSDATE)-30 GROUP BY TRUNC(CREATION_DATE), WAVE_STATUS`

**Example Output:**
```json
[
  {
    "intent": "Count WAVE_HDR records by WAVE_STATUS",
    "keywords": ["wave_status", "count", "status", "wave"],
    "tables": ["WAVE_HDR"],
    "columns": ["WAVE_STATUS"],
    "sqlTemplate": "SELECT WAVE_STATUS, COUNT(*) as cnt FROM {schema}.WAVE_HDR GROUP BY WAVE_STATUS ORDER BY cnt DESC",
    "confidence": 0.72,
    "source": "table-analysis"
  },
  {
    "intent": "Show recent WAVE_HDR records by CREATION_DATE",
    "keywords": ["recent", "latest", "creation_date", "last", "wave"],
    "tables": ["WAVE_HDR"],
    "columns": ["CREATION_DATE", "WAVE_NBR"],
    "sqlTemplate": "SELECT WAVE_NBR FROM {schema}.WAVE_HDR ORDER BY CREATION_DATE DESC FETCH FIRST 10 ROWS ONLY",
    "confidence": 0.68,
    "source": "table-analysis"
  }
]
```

**Integrates Via:** New in bridge.js (lines 1228-1290) + semantic-worker/app.py (POST /analyze-table)

---

## Key Components

### 1. Pattern Matching (semantic-worker/analyze_tables.py)

Classifies columns by regex patterns:
- **ID columns**: `_ID`, `_NBR`, `_NUM` → identifies entities
- **DATE columns**: `_DATE`, `_DTTM`, `_TIME`, `CREATED`, `UPDATED` → time dimension
- **STATUS columns**: `_STATUS`, `_STATE`, `_FLAG`, `_CODE`, `_TYPE` → categorical data
- **AMOUNT columns**: `_AMT`, `_VALUE`, `_QTY`, `_COUNT`, `_TOTAL` → numeric aggregates

### 2. SQL Template Generation (semantic-worker/analyze_tables.py)

From classified columns, generates intent-specific SQL:
- **Count-by-status** (if STATUS column): Aggregation query
- **Recent records** (if DATE column): Ordering by date
- **Time-series** (if DATE + STATUS): Time-bucketed aggregation

### 3. Semantic Index Integration (bridge.js + app.py)

Merges Phase 1 + Phase 2 intents:
- Deduplicates by (intent, schema, table)
- Preserves user confirmations
- Tracks usage count for learning
- Auto-promotes to confirmed after 5 thumbs-up

---

## Examples: From Question to Answer

### Example 1: "How many orders were waved?"

**Semantic Search:**
- Phase 1: ✅ WAVE_PK package analysis → intent confidence 0.78
- Phase 2: ✅ WAVE_HDR table analysis → intent confidence 0.72
- **Best match**: Phase 1 (higher confidence) or Phase 2 (no package available)

**SQL Execution:**
```sql
SELECT COUNT(*) FROM MANH_CODE.WAVE_HDR WHERE WAVE_STATUS='W'
-- Result: 1247
```

**Business Answer:**
> We have **1,247 orders that were waved**. This represents orders that were cancelled before wave execution, typically due to customer requests or load cancellations.

---

### Example 2: "Show me cycle count variance this week"

**Semantic Search:**
- Phase 1: ❌ No CYCLE_COUNT_PK package (or it's in Java)
- Phase 2: ✅ CYCLE_COUNT_HDR table → finds variance pattern
  - VARIANCE_AMT (amount column)
  - CYCLE_COUNT_DATE (date column)
  - VARIANCE_TYPE_CODE (status column)

**SQL Execution:**
```sql
SELECT VARIANCE_TYPE_CODE, SUM(VARIANCE_AMT) as total_variance, COUNT(*) as count
FROM CIGWMS.CYCLE_COUNT_HDR
WHERE CYCLE_COUNT_DATE >= TRUNC(SYSDATE)-7
GROUP BY VARIANCE_TYPE_CODE
ORDER BY total_variance DESC
```

**Business Answer:**
> This week's cycle count variance totals **$47,300** across 234 records.
> - **Over-stock** ($28,500): 142 items
> - **Shortage** ($18,800): 92 items
> Largest single variance: **SKU-445829** at $3,200 shortage in SE DC

---

## Score Interpretation

| Confidence | Source | Usage |
|---|---|---|
| 0.80+ | Phase 1 (Package) | Use immediately, show with caveat if unconfirmed |
| 0.70-0.79 | Phase 1 (Package) or Phase 2 (Table) | Use with "Based on system understanding..." |
| 0.60-0.69 | Phase 2 (Table) | Use with caveat, suggest confirming |
| <0.60 | Fallback | Only use if no better match |

---

## What This Enables

✅ **Questions about Java/C++ app logic** — no package needed
✅ **Legacy systems** — tables exist but code is compiled/external  
✅ **Third-party packages** — can't see source, but can see table patterns
✅ **New question types** — system learns automatically from tables
✅ **Multi-code language** — handles PL/SQL + Java + C++ uniformly

---

## Startup Flow

```
1. node bridge.js
   ↓
2. Bridge starts semantic worker (port 3334)
   ↓
3. Worker initializes semantic-index/intents.json
   ↓
4. Wait 7 seconds
   ↓
5. runSemanticScan() starts:
   - For each prod group (MANP, MAN002P, MAN001P, OMSP, OPCIGP)
   - For each priority schema (MANH_CODE, FRAMEWORK)
   ├─ Phase 1: Scan WAVE*, LOAD*, CYCLE* packages
   │            Send PACKAGE BODY to Ollama
   │            Extract: intent, tables, columns, sql_template
   │            Save to semantic-index/intents.json
   │
   └─ Phase 2: Scan WAVE*, LOAD*, SHIPMENT*, INVENTORY*, DOCK* tables
                Fetch DBA_TAB_COLUMNS
                Classify columns by pattern
                Generate SQL templates
                Save to semantic-index/intents.json
   ↓
6. Auto-pause (configurable, can resume manually)
   ↓
7. User asks: "How many orders were waved?"
   ↓
8. GET /semantic/search?q="how many orders were waved"
   ↓
9. Returns 3 matching intents (Phase 1 + Phase 2)
   ↓
10. sendQA() uses best match (0.78 confidence)
    ↓
11. Executes SQL template with resolved schema
    ↓
12. Injects result into LLM prompt
    ↓
13. Returns business answer
```

---

## Testing

```bash
# Verify both phases are running
curl http://localhost:3333/semantic/list | jq '.intents | length'
# Should show > 100 intents (mix of package + table analysis)

# Search for a table-derived intent (Phase 2)
curl "http://localhost:3333/semantic/search?q=wave%20count" | jq '.intents[0]'
# Should show: source: "table-analysis", tables: ["WAVE_HDR"]

# Search for a package-derived intent (Phase 1)
curl "http://localhost:3333/semantic/search?q=wave%20cancellation" | jq '.intents[0]'
# Should show: source: "package-analysis", package: "WAVE_PK"

# Ask a question in browser
# "How many orders were waved?"
# System automatically matches to best semantic intent
# Executes SQL, returns business answer
```
