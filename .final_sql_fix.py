#!/usr/bin/env python3
import json

intent_file = '/Users/asrajag/Workspace/oracle/knowledgeBase/semantic-index/intents.json'

with open(intent_file, 'r') as f:
    intents = json.load(f)

for i, entry in enumerate(intents):
    if entry.get('id') == 'si-shipment-max-units-dc':
        # Validated SQL: no CREATE_DATE_TIME (doesn't exist), ORDER BY expression not alias
        entry['sqlTemplate'] = (
            "SELECT sh.shipment_id, sh.shipment_status, COUNT(*) AS total_orders "
            "FROM {schema}.shipment sh "
            "JOIN {schema}.orders ord ON sh.tc_shipment_id = ord.tc_shipment_id "
            "WHERE sh.shipment_status < 80 "
            "GROUP BY sh.shipment_id, sh.shipment_status "
            "ORDER BY COUNT(*) DESC"
        )
        entry['tables'] = ['SHIPMENT', 'ORDERS']
        entry['columns'] = ['SHIPMENT_ID', 'TC_SHIPMENT_ID', 'SHIPMENT_STATUS']
        intents[i] = entry
        print(f"✓ Final validated SQL saved")
        print(f"  {entry['sqlTemplate']}")
        break

with open(intent_file, 'w') as f:
    json.dump(intents, f, indent=2)
    f.write('\n')

print("✓ Done")
