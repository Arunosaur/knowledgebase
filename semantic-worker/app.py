#!/usr/bin/env python3
import json
import os
import random
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
SEMANTIC_INDEX_DIR = ROOT_DIR / 'semantic-index'
SEMANTIC_INDEX_PATH = SEMANTIC_INDEX_DIR / 'intents.json'
CONFIG_PATH = ROOT_DIR / 'config.json'


def _load_bridge_config():
    try:
        raw = CONFIG_PATH.read_text(encoding='utf-8')
        cfg = json.loads(raw or '{}')
        bridge_cfg = cfg.get('bridge') if isinstance(cfg, dict) else {}
        return bridge_cfg if isinstance(bridge_cfg, dict) else {}
    except Exception:
        return {}


BRIDGE_CONFIG = _load_bridge_config()


def _env_or_config(name, config_key, default):
    env_val = os.environ.get(name)
    if env_val is not None and str(env_val).strip() != '':
        return env_val
    cfg_val = BRIDGE_CONFIG.get(config_key)
    if cfg_val is not None and str(cfg_val).strip() != '':
        return cfg_val
    return default


OLLAMA_URL = str(_env_or_config('OLLAMA_URL', 'ollamaUrl', 'http://localhost:11434')).strip()
OLLAMA_MODEL = os.environ.get('OLLAMA_MODEL', 'llama3')
SEMANTIC_PORT = int(float(_env_or_config('SEMANTIC_PORT', 'semanticWorkerPort', 3334)))
SEMANTIC_CONFIDENCE_HIGH = float(_env_or_config('SEMANTIC_CONFIDENCE_HIGH', 'semanticConfidenceHigh', 0.8))
SEMANTIC_CONFIDENCE_MEDIUM = float(_env_or_config('SEMANTIC_CONFIDENCE_MEDIUM', 'semanticConfidenceMedium', 0.5))
SEMANTIC_SCAN_PARALLEL_LIMIT = int(float(_env_or_config('SEMANTIC_SCAN_PARALLEL_LIMIT', 'semanticScanParallelLimit', 4)))

SEMANTIC_INDEX_DIR.mkdir(parents=True, exist_ok=True)
if not SEMANTIC_INDEX_PATH.exists():
    SEMANTIC_INDEX_PATH.write_text('[]\n', encoding='utf-8')

# Persist scan state across restarts
STATE_FILE = SEMANTIC_INDEX_DIR / 'scan-state.json'

state = {
    'running': False,
    'paused': False,
    'pauseReason': None,
    'progress': 0,
    'lastScan': None,
    'currentSchema': None,
    'scanned': {'groups': 0, 'docs': 0, 'tickets': 0},
    'pending': 0,
    'confirmed': 0
}
state_lock = threading.Lock()
index_lock = threading.Lock()


def _load_state():
    """Load persisted state fields on startup."""
    if STATE_FILE.exists():
        try:
            saved = json.loads(STATE_FILE.read_text(encoding='utf-8'))
            state['paused'] = bool(saved.get('paused', False))
            state['pauseReason'] = saved.get('pauseReason', None)
            state['lastScan'] = saved.get('lastScan', None)
            print(f"[STATE] Loaded: paused={state['paused']} lastScan={state['lastScan']}")
        except Exception as e:
            print(f"[STATE] Could not load scan-state.json: {e}")


def _save_state():
    """Persist paused/lastScan to disk."""
    try:
        STATE_FILE.write_text(json.dumps({
            'paused': state['paused'],
            'pauseReason': state['pauseReason'],
            'lastScan': state['lastScan'],
        }), encoding='utf-8')
    except Exception as e:
        print(f"[STATE] Could not save scan-state.json: {e}")


_load_state()

auto_probe = {
    'queue': [],
    'running': False,
    'lastQueuedAt': None,
    'lastProcessedAt': None,
    'lastQuestion': None,
    'processedCount': 0,
    'droppedDuplicates': 0,
    'recent': {}
}
auto_probe_lock = threading.Lock()


def _probe_signature(payload):
    question = str(payload.get('question') or '').strip().lower()
    group_id = str(payload.get('groupId') or '').strip().lower()
    schemas = payload.get('schemas') or []
    if not isinstance(schemas, list):
        schemas = []
    schema_sig = ','.join(sorted(str(s or '').strip().upper() for s in schemas if str(s or '').strip()))
    return f"{group_id}|{question}|{schema_sig}"


def _keywords_from_question(question):
    tokens = re.split(r'[^a-zA-Z0-9_]+', str(question or '').lower())
    stop = {
        'what', 'how', 'many', 'is', 'are', 'the', 'a', 'an', 'in', 'on', 'for',
        'to', 'of', 'and', 'or', 'with', 'from', 'this', 'that', 'these', 'those',
        'typically', 'please', 'can', 'you', 'we', 'i', 'do', 'does'
    }
    out = []
    seen = set()
    for t in tokens:
        if len(t) < 3 or t in stop:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= 12:
            break
    return out


def _upsert_auto_probe_seed(payload):
    question = str(payload.get('question') or '').strip()
    if not question:
        return None

    group_id = str(payload.get('groupId') or '').strip()
    schemas = payload.get('schemas') or []
    if not isinstance(schemas, list):
        schemas = []
    schemas = [str(s).strip().upper() for s in schemas if str(s).strip()][:6]

    keywords = _keywords_from_question(question)
    entity = str(payload.get('entity') or '').strip().lower()
    reason = str(payload.get('reason') or 'insufficient-context').strip().lower()

    probe_entry = _normalize_entry({
        'id': f"si-autoprobe-{int(time.time())}-{''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(4))}",
        'intent': f"Auto-probe seed: {question[:140]}",
        'keywords': ['auto-probe', reason, *(keywords or [])],
        'package': '',
        'procedure': 'auto_probe_seed',
        'tables': [],
        'columns': [],
        'schemas': schemas,
        'groupId': group_id,
        'dcCode': str(payload.get('dcCode') or '').strip().upper() or None,
        'entity': entity or 'unknown',
        'confidence': 0.35,
        'source': 'auto-probe',
        'confirmed': False,
        'confirmedBy': None,
        'usageCount': 0,
        'lastUsed': None,
        'sqlTemplate': '',
        'notes': f"Auto queued from Ask fallback ({reason}) at {_now_iso()}"
    })

    items = _load_index()
    sig = _probe_signature(payload)
    replaced = False
    for idx, item in enumerate(items):
        if str(item.get('source') or '') != 'auto-probe':
            continue
        existing_sig = _probe_signature({
            'question': str(item.get('intent') or '').replace('Auto-probe seed: ', ''),
            'groupId': item.get('groupId') or '',
            'schemas': item.get('schemas') or []
        })
        if existing_sig == sig:
            probe_entry['id'] = item.get('id', probe_entry['id'])
            probe_entry['createdAt'] = item.get('createdAt', probe_entry['createdAt'])
            probe_entry['usageCount'] = int(item.get('usageCount', 0) or 0)
            items[idx] = probe_entry
            replaced = True
            break
    if not replaced:
        items.append(probe_entry)
    _save_index(items)
    return probe_entry


def _auto_probe_worker_loop():
    while True:
        payload = None
        with auto_probe_lock:
            if auto_probe['queue']:
                payload = auto_probe['queue'].pop(0)
                auto_probe['running'] = True
                auto_probe['lastQuestion'] = str(payload.get('question') or '')
        if not payload:
            time.sleep(0.35)
            continue

        try:
            _upsert_auto_probe_seed(payload)
            with auto_probe_lock:
                auto_probe['processedCount'] += 1
                auto_probe['lastProcessedAt'] = _now_iso()
        except Exception:
            pass
        finally:
            with auto_probe_lock:
                auto_probe['running'] = bool(auto_probe['queue'])


def _start_auto_probe_worker_once():
    if getattr(_start_auto_probe_worker_once, '_started', False):
        return
    t = threading.Thread(target=_auto_probe_worker_loop, daemon=True)
    t.start()
    _start_auto_probe_worker_once._started = True


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _load_index():
    with index_lock:
        try:
            raw = SEMANTIC_INDEX_PATH.read_text(encoding='utf-8').strip() or '[]'
            data = json.loads(raw)
            return data if isinstance(data, list) else []
        except Exception:
            return []


def _save_index(items):
    with index_lock:
        SEMANTIC_INDEX_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def _normalize_entry(entry):
    now = _now_iso()
    item = dict(entry or {})
    if not item.get('id'):
        item['id'] = f"si-{int(time.time())}-{''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(4))}"
    item.setdefault('intent', '')
    item.setdefault('keywords', [])
    item.setdefault('package', '')
    item.setdefault('procedure', '')
    item.setdefault('tables', [])
    item.setdefault('columns', [])
    item.setdefault('schemas', [])
    item.setdefault('dcSpecific', False)
    item['confidence'] = max(0.0, min(1.0, _to_float(item.get('confidence', 0.5), 0.5)))
    item.setdefault('source', 'usage')
    item['confirmed'] = bool(item.get('confirmed', False))
    item.setdefault('confirmedBy', None)
    item['usageCount'] = int(item.get('usageCount', 0) or 0)
    item.setdefault('lastUsed', None)
    item.setdefault('sqlTemplate', '')
    item.setdefault('createdAt', now)
    item['updatedAt'] = now

    for key in ('keywords', 'tables', 'columns', 'schemas'):
        values = item.get(key, [])
        if not isinstance(values, list):
            values = [str(values)] if values else []
        dedup = []
        seen = set()
        for value in values:
            txt = str(value or '').strip()
            if not txt:
                continue
            mark = txt.lower()
            if mark in seen:
                continue
            seen.add(mark)
            dedup.append(txt)
        item[key] = dedup

    return item


def _score_for_query(entry, q):
    query = str(q or '').strip().lower()
    if not query:
        return 0.0
    hay_parts = [
        str(entry.get('intent', '')).lower(),
        str(entry.get('package', '')).lower(),
        str(entry.get('procedure', '')).lower(),
        ' '.join(str(x).lower() for x in entry.get('keywords', [])),
        ' '.join(str(x).lower() for x in entry.get('tables', [])),
        ' '.join(str(x).lower() for x in entry.get('columns', []))
    ]
    hay = ' '.join(hay_parts)
    terms = [t for t in re.split(r'\s+', query) if t]
    if not terms:
        return 0.0
    score = 0.0
    for term in terms:
        if term in hay:
            score += 1.0
        for kw in entry.get('keywords', []):
            kw_l = str(kw).lower()
            if term == kw_l:
                score += 1.5
            elif term in kw_l:
                score += 0.5
    score += _to_float(entry.get('confidence', 0), 0)
    if entry.get('confirmed'):
        score += 0.75
    return score


def _stats(items):
    by_source = {}
    by_conf = {'high': 0, 'medium': 0, 'low': 0}
    confirmed = 0
    for item in items:
        src = str(item.get('source', 'unknown'))
        by_source[src] = by_source.get(src, 0) + 1
        conf = _to_float(item.get('confidence', 0), 0)
        if conf >= SEMANTIC_CONFIDENCE_HIGH:
            by_conf['high'] += 1
        elif conf >= SEMANTIC_CONFIDENCE_MEDIUM:
            by_conf['medium'] += 1
        else:
            by_conf['low'] += 1
        if item.get('confirmed'):
            confirmed += 1
    return {
        'total': len(items),
        'bySource': by_source,
        'byConfidence': by_conf,
        'confirmed': confirmed,
        'unconfirmed': max(0, len(items) - confirmed)
    }


def _extract_json_array(text):
    if not text:
        return []
    raw = str(text).strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    m = re.search(r'\[.*\]', raw, flags=re.S)
    if not m:
        return []
    try:
        parsed = json.loads(m.group(0))
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _discover_from_source(schema, package_name, source_text):
    prompt = (
        'Given this PL/SQL source, list all business operations this code performs. '
        'For each: intent in plain English, tables read/written, key columns, input parameters. '
        'Return JSON only as an array. Each object should include intent, keywords, package, procedure, '
        'tables, columns, confidence, sqlTemplate.'
    )
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                'model': OLLAMA_MODEL,
                'stream': False,
                'messages': [
                    {'role': 'system', 'content': prompt},
                    {
                        'role': 'user',
                        'content': f"Schema: {schema}\nPackage: {package_name}\n\n{source_text[:18000]}"
                    }
                ]
            },
            timeout=45
        )
        if not resp.ok:
            return []
        body = resp.json()
        text = (
            body.get('message', {}).get('content')
            or body.get('response')
            or ''
        )
        raw_items = _extract_json_array(text)
        out = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            entry = _normalize_entry({
                'intent': raw.get('intent') or f"Analyze {package_name}",
                'keywords': raw.get('keywords') or [],
                'package': raw.get('package') or package_name,
                'procedure': raw.get('procedure') or '',
                'tables': raw.get('tables') or [],
                'columns': raw.get('columns') or [],
                'schemas': [schema],
                'dcSpecific': False,
                'confidence': raw.get('confidence', 0.62),
                'source': 'package-analysis',
                'confirmed': False,
                'confirmedBy': None,
                'usageCount': 0,
                'lastUsed': None,
                'sqlTemplate': raw.get('sqlTemplate') or ''
            })
            out.append(entry)
        return out
    except Exception:
        return []


@app.get('/status')
def get_status():
    items = _load_index()
    with state_lock:
        snapshot = dict(state)
    snapshot['confirmed'] = sum(1 for x in items if x.get('confirmed'))
    snapshot['scanParallelLimit'] = SEMANTIC_SCAN_PARALLEL_LIMIT
    return jsonify(snapshot)


@app.get('/list')
def list_intents():
    return jsonify({'intents': _load_index()})


@app.get('/search')
def search_intents():
    q = request.args.get('q', '')
    items = _load_index()
    scored = []
    for item in items:
        score = _score_for_query(item, q)
        if score > 0:
            scored.append((score, item))
    scored.sort(key=lambda x: x[0], reverse=True)
    intents = [i for _, i in scored[:25]]
    return jsonify({'intents': intents, 'q': q})


@app.get('/stats')
def get_stats():
    return jsonify(_stats(_load_index()))


@app.post('/entry')
def upsert_entry():
    payload = request.get_json(silent=True) or {}
    entry = payload.get('entry', payload)
    item = _normalize_entry(entry)
    items = _load_index()
    replaced = False
    for idx, cur in enumerate(items):
        if cur.get('id') == item.get('id'):
            item['createdAt'] = cur.get('createdAt', item.get('createdAt'))
            items[idx] = item
            replaced = True
            break
    if not replaced:
        items.append(item)
    _save_index(items)
    return jsonify({'ok': True, 'entry': item})


@app.delete('/entry/<path:item_id>')
def delete_entry(item_id):
    item_id = str(item_id or '').strip()
    items = _load_index()
    kept = [x for x in items if x.get('id') != item_id]
    _save_index(kept)
    return jsonify({'ok': True, 'deleted': len(items) - len(kept)})


@app.post('/confirm/<path:item_id>')
def confirm_entry(item_id):
    payload = request.get_json(silent=True) or {}
    who = payload.get('confirmedBy') or payload.get('user') or 'technical-user'
    items = _load_index()
    found = None
    for item in items:
        if item.get('id') == item_id:
            item['confirmed'] = True
            item['source'] = 'confirmed'
            item['confirmedBy'] = who
            item['updatedAt'] = _now_iso()
            if _to_float(item.get('confidence', 0), 0) < SEMANTIC_CONFIDENCE_HIGH:
                item['confidence'] = SEMANTIC_CONFIDENCE_HIGH
            found = item
            break
    if not found:
        return jsonify({'ok': False, 'error': 'not found'}), 404
    _save_index(items)
    return jsonify({'ok': True, 'confirmed': True, 'entry': found})


@app.post('/usage/<path:item_id>')
def record_usage(item_id):
    payload = request.get_json(silent=True) or {}
    direction = str(payload.get('direction') or '').strip().lower()
    items = _load_index()
    found = None
    for item in items:
        if item.get('id') == item_id:
            item['usageCount'] = int(item.get('usageCount', 0) or 0) + 1
            item['lastUsed'] = _now_iso()
            conf = _to_float(item.get('confidence', 0), 0)
            if direction == 'up':
                conf = min(1.0, conf + 0.05)
            elif direction == 'down':
                conf = max(0.0, conf - 0.1)
            item['confidence'] = conf
            if not item.get('confirmed') and direction == 'up' and item['usageCount'] >= 5:
                item['confirmed'] = True
                item['source'] = 'confirmed'
                item['confirmedBy'] = 'auto'
            item['updatedAt'] = _now_iso()
            found = item
            break
    if not found:
        return jsonify({'ok': False, 'error': 'not found'}), 404
    _save_index(items)
    return jsonify({'ok': True, 'entry': found})


@app.post('/pause')
def pause_scan():
    with state_lock:
        state['paused'] = True
        state['running'] = False
        state['pauseReason'] = 'manual-pause'
        _save_state()
    return jsonify({'ok': True, 'paused': True})


@app.post('/resume')
def resume_scan():
    with state_lock:
        state['paused'] = False
        state['pauseReason'] = None
        _save_state()
    return jsonify({'ok': True, 'paused': False})


@app.post('/discover')
def discover():
    payload = request.get_json(silent=True) or {}
    schema = str(payload.get('schema') or '').strip().upper()
    package_name = str(payload.get('package') or payload.get('name') or '').strip().upper()
    source = str(payload.get('source') or '').strip()

    if not schema or not source:
        return jsonify({'ok': False, 'error': 'schema and source are required'}), 400

    with state_lock:
        if state.get('paused'):
            return jsonify({'ok': False, 'error': 'paused'}), 409
        state['running'] = True
        state['pauseReason'] = None
        state['currentSchema'] = schema

    discovered = _discover_from_source(schema, package_name or 'UNKNOWN', source)
    if discovered:
        items = _load_index()
        by_key = {
            (str(x.get('intent', '')).lower(), str(x.get('package', '')).lower(), str(x.get('procedure', '')).lower()): x
            for x in items
        }
        for item in discovered:
            key = (str(item.get('intent', '')).lower(), str(item.get('package', '')).lower(), str(item.get('procedure', '')).lower())
            prev = by_key.get(key)
            if prev:
                item['id'] = prev.get('id')
                item['createdAt'] = prev.get('createdAt')
                item['usageCount'] = prev.get('usageCount', 0)
                item['lastUsed'] = prev.get('lastUsed')
                item['confirmed'] = prev.get('confirmed', False)
                item['confirmedBy'] = prev.get('confirmedBy')
            by_key[key] = item
        merged = list(by_key.values())
        _save_index(merged)
    else:
        merged = _load_index()

    with state_lock:
        state['running'] = False
        state['lastScan'] = _now_iso()
        state['progress'] = 100
        state['currentSchema'] = None
        state['confirmed'] = sum(1 for x in merged if x.get('confirmed'))

    return jsonify({'ok': True, 'discovered': len(discovered)})


def _classify_columns(columns):
    """Classify columns by pattern matching to infer meaning."""
    patterns = {
        'id': r'(_ID|_NBR|_NUM|^ID$|^NBR$)',
        'date': r'(_DATE|_DTTM|_TIME|CREATED|UPDATED|INSERTED)',
        'status': r'(_STATUS|_STATE|_FLAG|_CODE|_TYPE)',
        'amount': r'(_AMT|_VALUE|_QTY|_COUNT|_TOTAL|AMOUNT|QUANTITY)',
    }
    
    result = {
        'id_columns': [],
        'date_columns': [],
        'status_columns': [],
        'amount_columns': [],
        'all_columns': []
    }
    
    for col in (columns or []):
        name = str(col.get('COLUMN_NAME', '')).upper()
        result['all_columns'].append(name)
        
        if re.search(patterns['id'], name):
            result['id_columns'].append(name)
        if re.search(patterns['date'], name):
            result['date_columns'].append(name)
        if re.search(patterns['status'], name):
            result['status_columns'].append(name)
        if re.search(patterns['amount'], name):
            result['amount_columns'].append(name)
    
    return result


def _infer_table_intent(schema, table, classified):
    """Infer business operation from table/schema name + column pattern."""
    table_lower = table.lower()
    
    keywords = {
        'wave': 'Wave processing and order cancellation',
        'load': 'Load sequencing and shipment management',
        'shipment': 'Shipment tracking and delivery status',
        'inventory': 'Inventory management and location tracking',
        'receipt': 'Goods receiving and inbound processing',
        'dock': 'Dock assignment and loading dock scheduling',
        'order': 'Order fulfillment and processing',
        'container': 'Container and package handling',
        'variance': 'Inventory variance and reconciliation',
        'cycle': 'Cycle counting and inventory audit',
        'pick': 'Pick line processing and wave execution',
        'pack': 'Packing and consolidation',
        'sku': 'SKU product information and mastdata',
    }
    
    for keyword, meaning in keywords.items():
        if keyword in table_lower:
            return meaning
    
    # Fallback based on column patterns
    if classified['status_columns'] and classified['amount_columns']:
        return f'Track {table_lower} status and quantities'
    elif classified['status_columns']:
        return f'Manage {table_lower} status and state transitions'
    else:
        return f'Core business data for {table_lower}'


def _generate_table_intents(schema, table, classified):
    """Generate semantic entries from table schema patterns."""
    intents = []
    table_lower = str(table or '').lower()
    intent_base = _infer_table_intent(schema, table, classified)
    
    # Intent 1: Count/aggregate by status
    if classified['status_columns']:
        status_col = classified['status_columns'][0]
        sql_template = (
            f"SELECT {status_col}, COUNT(*) as cnt FROM {{schema}}.{table} "
            f"GROUP BY {status_col} ORDER BY cnt DESC"
        )
        intents.append({
            'intent': f'Count {table_lower} records by {status_col.lower()}',
            'keywords': [status_col.lower(), 'count', 'status', table_lower],
            'package': '',
            'procedure': f'analyze_{table_lower}_status',
            'tables': [table],
            'columns': [status_col],
            'schemas': [schema],
            'sqlTemplate': sql_template,
            'confidence': 0.72,
            'source': 'table-analysis'
        })
    
    # Intent 2: Recent records
    if classified['date_columns']:
        date_col = classified['date_columns'][0]
        id_col = classified['id_columns'][0] if classified['id_columns'] else '*'
        sql_template = (
            f"SELECT {id_col} FROM {{schema}}.{table} "
            f"ORDER BY {date_col} DESC FETCH FIRST 10 ROWS ONLY"
        )
        intents.append({
            'intent': f'Show recent {table_lower} records by {date_col.lower()}',
            'keywords': ['recent', 'latest', date_col.lower(), 'last', table_lower],
            'package': '',
            'procedure': f'query_{table_lower}_recent',
            'tables': [table],
            'columns': [date_col, id_col],
            'schemas': [schema],
            'sqlTemplate': sql_template,
            'confidence': 0.68,
            'source': 'table-analysis'
        })
    
    # Intent 3: Time-series summary
    if classified['date_columns'] and classified['status_columns']:
        date_col = classified['date_columns'][0]
        status_col = classified['status_columns'][0]
        sql_template = (
            f"SELECT TRUNC({{date_col}}) as day, {status_col}, COUNT(*) as cnt "
            f"FROM {{schema}}.{table} WHERE {date_col} >= TRUNC(SYSDATE)-30 "
            f"GROUP BY TRUNC({date_col}), {status_col} ORDER BY day DESC"
        )
        intents.append({
            'intent': f'Analyze {table_lower} trends by date and {status_col.lower()}',
            'keywords': ['trend', 'analysis', 'daily', date_col.lower(), status_col.lower(), table_lower],
            'package': '',
            'procedure': f'trend_{table_lower}',
            'tables': [table],
            'columns': [date_col, status_col],
            'schemas': [schema],
            'sqlTemplate': sql_template,
            'confidence': 0.65,
            'source': 'table-analysis'
        })
    
    # Normalize all and add metadata
    for item in intents:
        item.update({
            'dcSpecific': False,
            'confirmed': False,
            'confirmedBy': None,
            'usageCount': 0,
            'lastUsed': None,
            'createdAt': _now_iso(),
            'updatedAt': _now_iso()
        })
    
    return intents


def _ensure_seed_translation_intents():
    seeds = [
        {
            'id': 'si-domain-code-task-status',
            'intent': 'Translate task status codes to business descriptions',
            'keywords': ['task status meaning', 'status code translation', 'task stat_code description', 'decode status', 'status lookup'],
            'package': '',
            'procedure': '',
            'tables': ['CODE_DTLS', 'TASK'],
            'columns': ['CODE_TYP', 'CODE_ID', 'CODE_DESC', 'STAT_CODE'],
            'schemas': [],
            'dcSpecific': False,
            'confidence': 0.82,
            'source': 'domain-knowledge',
            'confirmed': False,
            'confirmedBy': None,
            'usageCount': 0,
            'lastUsed': None,
            'sqlTemplate': "SELECT c.CODE_ID AS STATUS_CODE, c.CODE_DESC AS STATUS_DESCRIPTION FROM {schema}.CODE_DTLS c WHERE UPPER(c.CODE_TYP) IN ('TASK_STATUS','STAT_CODE','TASK_STAT') ORDER BY c.CODE_ID FETCH FIRST 50 ROWS ONLY"
        },
        {
            'id': 'si-domain-code-task-type',
            'intent': 'Translate task type codes to business descriptions',
            'keywords': ['task type meaning', 'task type code', 'decode task_type', 'task code translation', 'task category'],
            'package': '',
            'procedure': '',
            'tables': ['CODE_DTLS', 'TASK'],
            'columns': ['CODE_TYP', 'CODE_ID', 'CODE_DESC', 'TASK_TYPE'],
            'schemas': [],
            'dcSpecific': False,
            'confidence': 0.82,
            'source': 'domain-knowledge',
            'confirmed': False,
            'confirmedBy': None,
            'usageCount': 0,
            'lastUsed': None,
            'sqlTemplate': "SELECT c.CODE_ID AS TASK_TYPE_CODE, c.CODE_DESC AS TASK_TYPE_DESCRIPTION FROM {schema}.CODE_DTLS c WHERE UPPER(c.CODE_TYP) IN ('TASK_TYPE','TASK_TYP') ORDER BY c.CODE_ID FETCH FIRST 50 ROWS ONLY"
        },
        {
            'id': 'si-domain-code-location-class',
            'intent': 'Translate location class codes to descriptions',
            'keywords': ['location class meaning', 'locn_class decode', 'location type code', 'decode locn class', 'location code description'],
            'package': '',
            'procedure': '',
            'tables': ['CODE_DTLS', 'LOCN_HDR'],
            'columns': ['CODE_TYP', 'CODE_ID', 'CODE_DESC', 'LOCN_CLASS'],
            'schemas': [],
            'dcSpecific': False,
            'confidence': 0.80,
            'source': 'domain-knowledge',
            'confirmed': False,
            'confirmedBy': None,
            'usageCount': 0,
            'lastUsed': None,
            'sqlTemplate': "SELECT c.CODE_ID AS LOCN_CLASS_CODE, c.CODE_DESC AS LOCN_CLASS_DESCRIPTION FROM {schema}.CODE_DTLS c WHERE UPPER(c.CODE_TYP) IN ('LOCN_CLASS','LOCATION_CLASS') ORDER BY c.CODE_ID FETCH FIRST 50 ROWS ONLY"
        }
    ]

    items = _load_index()
    by_id = {str(item.get('id')): idx for idx, item in enumerate(items)}
    changed = False
    for seed in seeds:
        normalized = _normalize_entry(seed)
        existing_idx = by_id.get(normalized.get('id'))
        if existing_idx is None:
            items.append(normalized)
            changed = True
        else:
            existing = items[existing_idx]
            normalized['createdAt'] = existing.get('createdAt', normalized.get('createdAt'))
            normalized['usageCount'] = existing.get('usageCount', 0)
            normalized['lastUsed'] = existing.get('lastUsed')
            items[existing_idx] = normalized
            changed = True
    if changed:
        _save_index(items)


@app.post('/analyze-table')
def analyze_table():
    """Analyze table schema and generate semantic intents from column patterns."""
    payload = request.get_json(silent=True) or {}
    schema = str(payload.get('schema') or '').strip().upper()
    table = str(payload.get('table') or '').strip().upper()
    columns = payload.get('columns') or []
    
    if not schema or not table or not columns:
        return jsonify({'ok': False, 'error': 'schema, table, and columns array required'}), 400
    
    # Classify columns
    classified = _classify_columns(columns)
    
    # Generate intents from table patterns
    intents = _generate_table_intents(schema, table, classified)
    
    if intents:
        items = _load_index()
        by_key = {}
        for x in items:
            key = (str(x.get('intent', '')).lower(), str(x.get('table', '')).lower())
            by_key[key] = x
        
        for item in intents:
            key = (str(item.get('intent', '')).lower(), table.lower())
            prev = by_key.get(key)
            if prev:
                item['id'] = prev.get('id')
                item['createdAt'] = prev.get('createdAt')
                item['usageCount'] = prev.get('usageCount', 0)
                item['confirmed'] = prev.get('confirmed', False)
            else:
                item['id'] = f"si-{int(time.time())}-{''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(4))}"
            by_key[key] = item
        
        merged = list(by_key.values())
        _save_index(merged)
    
    return jsonify({'ok': True, 'discovered': len(intents), 'intents': intents})


@app.post('/scan')
def scan():
    payload = request.get_json(silent=True) or {}
    with state_lock:
        if state.get('paused'):
            return jsonify({'started': False, 'reason': 'paused'})
        if state.get('running'):
            return jsonify({
                'started': False,
                'reason': 'scan-already-running',
                'progress': int(state.get('progress', 0) or 0)
            })
        state['running'] = True
        state['pauseReason'] = None
        state['progress'] = 5
        state['lastScan'] = _now_iso()

    schemas = payload.get('schemas') or []
    if not isinstance(schemas, list):
        schemas = []

    with state_lock:
        state['scanned'] = {
            'groups': int(payload.get('groupsScanned', 0) or 0),
            'docs': int(payload.get('docsScanned', 0) or 0),
            'tickets': int(payload.get('ticketsScanned', 0) or 0)
        }
        state['pending'] = int(payload.get('pending', 0) or 0)
        state['progress'] = 100
        state['running'] = False
        state['paused'] = True
        state['pauseReason'] = 'auto-paused after full scan'
        state['currentSchema'] = schemas[0] if schemas else None
        _save_state()

    items = _load_index()
    with state_lock:
        state['confirmed'] = sum(1 for x in items if x.get('confirmed'))
        state['currentSchema'] = None

    print('Scan complete — auto-paused. Resume manually or trigger via POST /scan')

    return jsonify({'started': True, 'reason': 'scan-started'})


@app.post('/auto-probe')
def auto_probe_enqueue():
    payload = request.get_json(silent=True) or {}
    question = str(payload.get('question') or '').strip()
    if not question:
        return jsonify({'ok': False, 'error': 'question is required'}), 400

    sig = _probe_signature(payload)
    now = time.time()
    cooldown = 15 * 60

    with auto_probe_lock:
        recent = auto_probe.get('recent', {})
        last = float(recent.get(sig, 0) or 0)
        if last and (now - last) < cooldown:
            auto_probe['droppedDuplicates'] += 1
            return jsonify({
                'ok': True,
                'queued': False,
                'reason': 'cooldown',
                'cooldownSeconds': int(cooldown - (now - last))
            })

        auto_probe['queue'].append(payload)
        auto_probe['lastQueuedAt'] = _now_iso()
        auto_probe['recent'][sig] = now
        # prune stale signatures
        auto_probe['recent'] = {k: v for k, v in auto_probe['recent'].items() if (now - float(v or 0)) < 6 * 3600}

    _start_auto_probe_worker_once()
    return jsonify({'ok': True, 'queued': True, 'queueDepth': len(auto_probe['queue'])})


@app.get('/auto-probe/status')
def auto_probe_status():
    with auto_probe_lock:
        snapshot = {
            'running': bool(auto_probe.get('running')),
            'queueDepth': len(auto_probe.get('queue', [])),
            'lastQueuedAt': auto_probe.get('lastQueuedAt'),
            'lastProcessedAt': auto_probe.get('lastProcessedAt'),
            'lastQuestion': auto_probe.get('lastQuestion'),
            'processedCount': int(auto_probe.get('processedCount', 0) or 0),
            'droppedDuplicates': int(auto_probe.get('droppedDuplicates', 0) or 0)
        }
    return jsonify(snapshot)


if __name__ == '__main__':
    _ensure_seed_translation_intents()
    app.run(host='127.0.0.1', port=SEMANTIC_PORT)
