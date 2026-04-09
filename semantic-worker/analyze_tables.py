#!/usr/bin/env python3
"""
Phase 2: Semantic Layer — Data Pattern Discovery
Probes actual table data to infer business intent + column meanings
"""

import json
import re
from typing import Dict, List, Any

def analyze_table_schema(schema_name: str, table_name: str, columns: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Infer business meaning from column names + data types
    
    Returns: {
      id_columns: ["WAVE_NBR", "ORDER_NBR"],
      date_columns: ["CREATION_DATE", "UPDATED_DATE"],
      status_columns: [("WAVE_STATUS", possible_values)],
      amount_columns: ["AMOUNT", "TOTAL_VALUE"],
      foreign_keys: ["LOAD_NBR", "ORDER_NBR"],
      summary: "Wave management table: tracks order waves with status"
    }
    """
    patterns = {
        'id': r'(_ID|_NBR|_NUM|^ID$|^NBR$)',
        'date': r'(_DATE|_DTTM|_TIME|CREATED|UPDATED|INSERTED)',
        'status': r'(_STATUS|_STATE|_FLAG|_CODE|_TYPE)',
        'amount': r'(_AMT|_VALUE|_QTY|_COUNT|_TOTAL|AMOUNT|QUANTITY)',
        'fk': r'(_ID|_NBR|_KEY)$'
    }
    
    classified = {
        'id_columns': [],
        'date_columns': [],
        'status_columns': [],
        'amount_columns': [],
        'foreign_keys': [],
        'text_columns': [],
        'all_columns': []
    }
    
    for col in columns:
        col_name = col.get('COLUMN_NAME', '')
        col_type = col.get('DATA_TYPE', '').upper()
        classified['all_columns'].append(col_name)
        
        if re.search(patterns['id'], col_name, re.I):
            classified['id_columns'].append(col_name)
        if re.search(patterns['date'], col_name, re.I):
            classified['date_columns'].append(col_name)
        if re.search(patterns['status'], col_name, re.I):
            classified['status_columns'].append(col_name)
        if re.search(patterns['amount'], col_name, re.I):
            classified['amount_columns'].append(col_name)
        if re.search(patterns['fk'], col_name, re.I) and col_name not in classified['id_columns']:
            classified['foreign_keys'].append(col_name)
        if 'CHAR' in col_type or 'CLOB' in col_type:
            classified['text_columns'].append(col_name)
    
    # Infer business meaning from table name + column pattern
    intent = infer_intent_from_schema(schema_name, table_name, classified)
    
    return {
        **classified,
        'intent': intent,
        'schema': schema_name,
        'table': table_name
    }


def infer_intent_from_schema(schema: str, table: str, classified: Dict) -> str:
    """
    Infer business operation from table/schema name + columns
    
    Examples:
      WAVE_HDR + [WAVE_STATUS, WAVE_NBR, LOAD_NBR] 
        → "Manage wave status for orders"
      INVENTORY + [LOCATION_ID, QUANTITY, RESERVE_QTY]
        → "Track inventory levels by location"
    """
    table_lower = table.lower()
    
    keywords = {
        'wave': 'Wave processing and cancellation',
        'load': 'Load sequencing and shipment',
        'shipment': 'Shipment tracking and status',
        'inventory': 'Inventory management and location',
        'receipt': 'Goods receiving and inbound',
        'dock': 'Dock assignment and scheduling',
        'order': 'Order fulfillment',
        'container': 'Container and handling',
        'variance': 'Inventory variance and reconciliation',
        'cycle': 'Cycle counting',
        'pick': 'Pick line processing',
        'pack': 'Packing and consolidation',
        'label': 'Labeling and tracking',
        'sku': 'SKU/product information',
        'location': 'Storage location management'
    }
    
    for keyword, meaning in keywords.items():
        if keyword in table_lower:
            return meaning
    
    # Fallback: describe by column patterns
    if classified['status_columns'] and classified['amount_columns']:
        return f"Manage {table.lower()} with status tracking and quantities"
    elif classified['status_columns']:
        return f"Track {table.lower()} status and state transitions"
    elif classified['amount_columns']:
        return f"Record {table.lower()} quantities and amounts"
    else:
        return f"Core data for {table.lower()}"


def suggest_sql_for_intent(schema: str, table: str, classified: Dict, intent: str) -> str:
    """
    Suggest SQL query template based on classified columns + intent
    
    Examples:
      intent="check if waves were cancelled" + status_columns=[WAVE_STATUS]
        → "SELECT COUNT(*) FROM {schema}.{table} WHERE WAVE_STATUS='C'"
      intent="show received items" + date_columns=[REC_DATE], amount_columns=[QTY]
        → "SELECT {id}, {amount}, {date} FROM {schema}.{table} ORDER BY {date} DESC"
    """
    templates = []
    
    # Template 1: Count by status
    if classified['status_columns']:
        status_col = classified['status_columns'][0]
        templates.append(
            f"SELECT {status_col}, COUNT(*) as cnt FROM {schema}.{table} "
            f"GROUP BY {status_col} ORDER BY cnt DESC"
        )
    
    # Template 2: Recent records
    if classified['date_columns']:
        date_col = classified['date_columns'][0]
        id_col = classified['id_columns'][0] if classified['id_columns'] else '*'
        templates.append(
            f"SELECT {id_col} FROM {schema}.{table} "
            f"ORDER BY {date_col} DESC FETCH FIRST 10 ROWS ONLY"
        )
    
    # Template 3: Summary by amount
    if classified['amount_columns']:
        amount_col = classified['amount_columns'][0]
        templates.append(
            f"SELECT SUM({amount_col}) as total, COUNT(*) as records "
            f"FROM {schema}.{table}"
        )
    
    # Template 4: Time-series breakdown
    if classified['date_columns'] and classified['status_columns']:
        date_col = classified['date_columns'][0]
        status_col = classified['status_columns'][0]
        templates.append(
            f"SELECT TRUNC({date_col}) as day, {status_col}, COUNT(*) as cnt "
            f"FROM {schema}.{table} WHERE {date_col} >= TRUNC(SYSDATE)-30 "
            f"GROUP BY TRUNC({date_col}), {status_col} ORDER BY day DESC"
        )
    
    # Return best template (prefer count + status)
    return templates[0] if templates else f"SELECT * FROM {schema}.{table} WHERE ROWNUM <= 100"


# Example: analyze a table
if __name__ == '__main__':
    # Mock column data for WAVE_HDR
    wave_hdr_columns = [
        {'COLUMN_NAME': 'WAVE_NBR', 'DATA_TYPE': 'NUMBER'},
        {'COLUMN_NAME': 'WAVE_STATUS', 'DATA_TYPE': 'VARCHAR2'},
        {'COLUMN_NAME': 'LOAD_NBR', 'DATA_TYPE': 'NUMBER'},
        {'COLUMN_NAME': 'WAVE_CREATION_DATE', 'DATA_TYPE': 'DATE'},
        {'COLUMN_NAME': 'WAVE_UPDATED_DATE', 'DATA_TYPE': 'DATE'},
        {'COLUMN_NAME': 'WAVE_COUNT', 'DATA_TYPE': 'NUMBER'},
        {'COLUMN_NAME': 'NOTES', 'DATA_TYPE': 'CLOB'}
    ]
    
    result = analyze_table_schema('MANH_CODE', 'WAVE_HDR', wave_hdr_columns)
    print("=== WAVE_HDR Analysis ===")
    print(json.dumps(result, indent=2))
    
    print("\n=== Suggested SQL ===")
    sql = suggest_sql_for_intent('MANH_CODE', 'WAVE_HDR', result, result['intent'])
    print(sql)
