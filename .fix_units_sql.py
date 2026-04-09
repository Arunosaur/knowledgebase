#!/usr/bin/env python3
import json

intent_file = '/Users/asrajag/Workspace/oracle/knowledgeBase/semantic-index/intents.json'

with open(intent_file, 'r') as f:
    intents = json.load(f)

for i, entry in enumerate(intents):
    if entry.get('id') == 'si-shipment-max-units-dc':
        entry['tables'] = ['SHIPMENT', 'ORDERS', 'ORDER_LINE_ITEM']
        entry['columns'] = ['SHIPMENT_ID', 'TC_SHIPMENT_ID', 'SHIPMENT_STATUS', 'ORDER_QTY']
        entry['sqlTemplate'] = (
            "SELECT sh.shipment_id, sh.shipment_status, SUM(oli.order_qty) AS total_units "
            "FROM {schema}.shipment sh "
            "JOIN {schema}.orders ord ON sh.tc_shipment_id = ord.tc_shipment_id "
            "JOIN {schema}.order_line_item oli ON ord.order_id = oli.order_id "
            "WHERE sh.shipment_status < 80 "
            "GROUP BY sh.shipment_id, sh.shipment_status "
            "ORDER BY SUM(oli.order_qty) DESC"
        )
        intents[i] = entry
        print(f"✓ Updated: SUM(oli.order_qty) AS total_units")
        print(f"  {entry['sqlTemplate'][:100]}...")
        break

with open(intent_file, 'w') as f:
    json.dump(intents, f, indent=2)
    f.write('\n')

print("✓ Saved")
