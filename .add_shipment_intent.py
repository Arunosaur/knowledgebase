#!/usr/bin/env python3
import json
import sys

intent_file = '/Users/asrajag/Workspace/oracle/knowledgeBase/semantic-index/intents.json'

with open(intent_file, 'r') as f:
    intents = json.load(f)

new_entry = {
    "id": "si-shipment-max-units-dc",
    "intent": "Find the shipment or load with the most units for a specific DC",
    "keywords": [
        "shipment shipping most units",
        "load shipping most units",
        "highest unit count",
        "max units shipment",
        "which load units",
        "which shipment units"
    ],
    "package": "",
    "procedure": "query_shipment_max_units",
    "tables": ["SHIPMENT", "SHIPMENT_LINE", "ORDER_LINE_ITEM"],
    "columns": ["SHIPMENT_ID", "LOAD_NBR", "UNIT_QTY", "CREATE_DATE_TIME"],
    "schemas": [],
    "dcSpecific": True,
    "confidence": 0.90,
    "source": "domain-knowledge",
    "confirmed": False,
    "confirmedBy": None,
    "usageCount": 0,
    "lastUsed": None,
    "sqlTemplate": "SELECT sh.shipment_id, COUNT(DISTINCT shl.shipment_line_id) AS line_count, SUM(COALESCE(oli.unit_qty, shl.qty)) AS total_units, sh.create_date_time FROM {schema}.shipment sh LEFT JOIN {schema}.shipment_line shl ON sh.shipment_id = shl.shipment_id LEFT JOIN {schema}.order_line_item oli ON shl.order_line_item_id = oli.order_line_item_id WHERE sh.create_date_time >= TRUNC(SYSDATE) - 7 GROUP BY sh.shipment_id, sh.create_date_time ORDER BY total_units DESC FETCH FIRST 10 ROWS ONLY"
}

found = False
for i, entry in enumerate(intents):
    if entry.get('id') == new_entry['id']:
        intents[i] = new_entry
        found = True
        print(f"✓ Updated: {new_entry['id']}")
        break

if not found:
    intents.append(new_entry)
    print(f"✓ Added: {new_entry['id']}")

with open(intent_file, 'w') as f:
    json.dump(intents, f, indent=2)
    f.write('\n')

print(f"✓ Total intents: {len(intents)}")
