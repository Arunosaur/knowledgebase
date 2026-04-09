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

schema_to_group = {}
for group in config.get('groups', []):
    if str(group.get('env', 'prod')).lower() != 'prod':
        continue
    group_id = group.get('id')
    for schema in group.get('schemas', []) or []:
        schema_to_group[str(schema).upper()] = group_id

# fallback probing scope for unresolved Manhattan warehouse intents
fallback_manhattan_dm_schemas = sorted({
    str(dc.get('dmSchema', '')).upper()
    for dc in (config.get('distributionCenters', []) or [])
    if dc and dc.get('active') and dc.get('manhattanGroup')
})


def db_query(group_id: str, sql: str, max_rows: int = 1):
    body = json.dumps({'group': group_id, 'sql': sql, 'maxRows': max_rows}).encode('utf-8')
    req = urllib.request.Request(
        BASE + '/db/query',
        data=body,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode('utf-8'))


def parse_table_reference(table_ref: str):
    value = str(table_ref or '').strip().upper()
    parts = [p for p in value.split('.') if p]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return None, value


low_conf_intents = [item for item in intents if float(item.get('confidence', 0.0)) <= LOW_CONF_THRESHOLD]
findings = []
updates = 0

for intent in low_conf_intents:
    intent_id = intent.get('id', '')
    schemas = [str(s).upper() for s in (intent.get('schemas') or []) if s]
    tables = [str(t).upper() for t in (intent.get('tables') or []) if t]

    table_results = []

    for table_ref in tables:
        owner_from_ref, table_name = parse_table_reference(table_ref)

        candidate_schemas = []
        if owner_from_ref and owner_from_ref in schema_to_group:
            candidate_schemas = [owner_from_ref]
        elif schemas:
            candidate_schemas = [s for s in schemas if s in schema_to_group]
        elif owner_from_ref:
            candidate_schemas = [owner_from_ref]

        if not candidate_schemas:
            candidate_schemas = [s for s in fallback_manhattan_dm_schemas if s in schema_to_group]
            if not candidate_schemas:
                table_results.append({
                    'tableRef': table_ref,
                    'status': 'unresolved-schema',
                    'hasRows': None
                })
                continue

        probed_ok = False
        seen_ok = 0
        seen_has_rows = False
        for schema in candidate_schemas:
            group_id = schema_to_group.get(schema)
            if not group_id:
                continue

            sql = (
                f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {schema}.{table_name} WHERE ROWNUM = 1) "
                f"THEN 1 ELSE 0 END AS HAS_ROWS FROM DUAL"
            )
            try:
                result = db_query(group_id, sql, 1)
                rows = result.get('rows', []) if isinstance(result, dict) else []
                has_rows = bool(rows and int(rows[0].get('HAS_ROWS', 0)) == 1)
                table_results.append({
                    'tableRef': table_ref,
                    'schema': schema,
                    'groupId': group_id,
                    'status': 'ok',
                    'hasRows': has_rows
                })
                probed_ok = True
                seen_ok += 1
                if has_rows:
                    seen_has_rows = True
                    break
            except Exception as exc:
                table_results.append({
                    'tableRef': table_ref,
                    'schema': schema,
                    'groupId': group_id,
                    'status': 'error',
                    'hasRows': None,
                    'error': str(exc)[:200]
                })

        if not probed_ok:
            pass

    ok_rows = [row for row in table_results if row.get('status') == 'ok']
    has_any_data = any(row.get('hasRows') is True for row in ok_rows)
    all_zero = bool(ok_rows) and all(row.get('hasRows') is False for row in ok_rows)

    old_conf = float(intent.get('confidence', 0.0))
    new_conf = old_conf
    data_evidence = intent.get('dataEvidence')

    if has_any_data:
        new_conf = min(1.0, round(old_conf + 0.10, 2))
        data_evidence = 'data-active'
    elif all_zero:
        new_conf = min(old_conf, 0.30)
        data_evidence = 'data-dormant'
    else:
        data_evidence = data_evidence or 'data-unknown'

    if new_conf != old_conf or intent.get('dataEvidence') != data_evidence:
        intent['confidence'] = new_conf
        intent['dataEvidence'] = data_evidence
        intent['updatedAt'] = '2026-03-17T23:55:00Z'
        updates += 1

    zero_tables = [row for row in ok_rows if row.get('hasRows') is False]

    findings.append({
        'id': intent_id,
        'intent': intent.get('intent', ''),
        'source': intent.get('source', ''),
        'oldConfidence': old_conf,
        'newConfidence': new_conf,
        'dataEvidence': data_evidence,
        'tablesProbed': table_results,
        'zeroRowTables': zero_tables
    })

# Build isolation list requested by user
isolated = []
for finding in findings:
    if float(finding.get('newConfidence', 0.0)) <= LOW_CONF_THRESHOLD and finding.get('zeroRowTables'):
        for table in finding['zeroRowTables']:
            isolated.append({
                'intentId': finding['id'],
                'intent': finding['intent'],
                'source': finding['source'],
                'confidence': finding['newConfidence'],
                'dataEvidence': finding['dataEvidence'],
                'schema': table.get('schema'),
                'table': table.get('tableRef')
            })

report = {
    'lowConfidenceThreshold': LOW_CONF_THRESHOLD,
    'lowConfidenceIntentCount': len(low_conf_intents),
    'updatedIntentCount': updates,
    'findings': findings
}

isolation = {
    'count': len(isolated),
    'items': isolated
}

# Persist outputs + confidence updates
report_path.write_text(json.dumps(report, indent=2))
isolation_path.write_text(json.dumps(isolation, indent=2))
intents_path.write_text(json.dumps(intents, indent=2))

print(f'LOW_CONF_INTENTS={len(low_conf_intents)}')
print(f'UPDATED_INTENTS={updates}')
print(f'ZERO_ROW_LOW_CONF={len(isolated)}')
print(f'REPORT={report_path}')
print(f'ISOLATION={isolation_path}')
