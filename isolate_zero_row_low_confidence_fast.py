#!/usr/bin/env python3
import json
import urllib.request
from pathlib import Path

BASE = 'http://localhost:3333'
LOW_CONF_THRESHOLD = 0.60

root = Path(__file__).resolve().parent
intents_path = root / 'semantic-index' / 'intents.json'
config_path = root / 'config.json'
report_path = root / 'semantic-index' / 'low-confidence-probe-report.json'
isolation_path = root / 'semantic-index' / 'zero-row-low-confidence.json'

intents = json.loads(intents_path.read_text())
config = json.loads(config_path.read_text())

# Build Manhattan DM schema list from config
manhattan_dm_schemas = sorted({
    str(dc.get('dmSchema', '')).upper()
    for dc in (config.get('distributionCenters', []) or [])
    if dc and dc.get('active') and dc.get('manhattanGroup')
})

schema_csv = ','.join([f"'{s}'" for s in manhattan_dm_schemas if s])

# Low confidence intents
low_conf_intents = [item for item in intents if float(item.get('confidence', 0.0)) <= LOW_CONF_THRESHOLD]

# Collect target tables from low-confidence intents
target_tables = sorted({
    str(t).split('.')[-1].upper()
    for item in low_conf_intents
    for t in (item.get('tables') or [])
    if str(t).strip()
})

table_csv = ','.join([f"'{t}'" for t in target_tables])

# Single probe query using DBA_TABLES statistics (fast and good enough for isolation)
sql = f"""
SELECT OWNER, TABLE_NAME, NVL(NUM_ROWS, -1) AS NUM_ROWS,
       TO_CHAR(LAST_ANALYZED, 'YYYY-MM-DD HH24:MI:SS') AS LAST_ANALYZED
FROM DBA_TABLES
WHERE OWNER IN ({schema_csv})
  AND TABLE_NAME IN ({table_csv})
ORDER BY TABLE_NAME, OWNER
"""

body = json.dumps({'group': 'manhattan-main', 'sql': sql, 'maxRows': 2000}).encode('utf-8')
req = urllib.request.Request(
    BASE + '/db/query',
    data=body,
    headers={'Content-Type': 'application/json'},
    method='POST'
)

with urllib.request.urlopen(req, timeout=45) as response:
    probe_result = json.loads(response.read().decode('utf-8'))

rows = probe_result.get('rows', []) if isinstance(probe_result, dict) else []

# Index probe rows by table name
by_table = {}
for row in rows:
    table_name = str(row.get('TABLE_NAME', '')).upper()
    by_table.setdefault(table_name, []).append({
        'owner': row.get('OWNER'),
        'numRows': int(row.get('NUM_ROWS', -1)) if str(row.get('NUM_ROWS', '')).strip() else -1,
        'lastAnalyzed': row.get('LAST_ANALYZED')
    })

findings = []
updates = 0
isolated = []

for item in low_conf_intents:
    intent_id = item.get('id', '')
    intent_text = item.get('intent', '')
    source = item.get('source', '')
    old_conf = float(item.get('confidence', 0.0))
    tables = [str(t).split('.')[-1].upper() for t in (item.get('tables') or []) if str(t).strip()]

    probe_tables = []
    has_any_data = False
    has_any_zero = False

    for t in tables:
        entries = by_table.get(t, [])
        if not entries:
            probe_tables.append({'table': t, 'status': 'missing', 'owners': []})
            continue

        positive = any(e['numRows'] > 0 for e in entries)
        zeros = [e for e in entries if e['numRows'] == 0]
        unknown = [e for e in entries if e['numRows'] < 0]

        if positive:
            has_any_data = True
        if zeros:
            has_any_zero = True

        probe_tables.append({
            'table': t,
            'status': 'has-data' if positive else ('zero-rows' if zeros else 'unknown-stats'),
            'owners': entries
        })

    new_conf = old_conf
    evidence = item.get('dataEvidence')

    if has_any_data:
        new_conf = min(1.0, round(old_conf + 0.10, 2))
        evidence = 'data-active'
    elif has_any_zero:
        new_conf = min(old_conf, 0.30)
        evidence = 'data-dormant'
    else:
        evidence = evidence or 'data-unknown'

    if new_conf != old_conf or item.get('dataEvidence') != evidence:
        item['confidence'] = new_conf
        item['dataEvidence'] = evidence
        item['updatedAt'] = '2026-03-17T23:59:00Z'
        updates += 1

    findings.append({
        'id': intent_id,
        'intent': intent_text,
        'source': source,
        'oldConfidence': old_conf,
        'newConfidence': new_conf,
        'dataEvidence': evidence,
        'tables': probe_tables
    })

    # isolate all zero-row schema/table entries with low confidence
    if new_conf <= LOW_CONF_THRESHOLD:
        for tbl in probe_tables:
            owners = tbl.get('owners', []) or []
            zero_owners = [e for e in owners if int(e.get('numRows', -1)) == 0]
            if zero_owners:
                isolated.append({
                    'intentId': intent_id,
                    'intent': intent_text,
                    'source': source,
                    'confidence': new_conf,
                    'dataEvidence': evidence,
                    'table': tbl['table'],
                    'ownersWithZeroRows': [e['owner'] for e in zero_owners]
                })

report = {
    'lowConfidenceThreshold': LOW_CONF_THRESHOLD,
    'lowConfidenceIntentCount': len(low_conf_intents),
    'targetTables': target_tables,
    'probeRows': len(rows),
    'updatedIntentCount': updates,
    'findings': findings
}

isolation = {
    'count': len(isolated),
    'items': isolated
}

report_path.write_text(json.dumps(report, indent=2))
isolation_path.write_text(json.dumps(isolation, indent=2))
intents_path.write_text(json.dumps(intents, indent=2))

print(f'LOW_CONF_INTENTS={len(low_conf_intents)}')
print(f'PROBE_ROWS={len(rows)}')
print(f'UPDATED_INTENTS={updates}')
print(f'ZERO_ROW_LOW_CONF={len(isolated)}')
print(f'REPORT={report_path}')
print(f'ISOLATION={isolation_path}')
