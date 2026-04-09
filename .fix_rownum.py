#!/usr/bin/env python3
import json

intent_file = '/Users/asrajag/Workspace/oracle/knowledgeBase/semantic-index/intents.json'

with open(intent_file, 'r') as f:
    intents = json.load(f)

for i, entry in enumerate(intents):
    if entry.get('id') == 'si-shipment-max-units-dc':
        # Use ROWNUM subquery pattern (works with SQLcl MCP; FETCH FIRST does not)
        entry['sqlTemplate'] = (
            "SELECT * FROM ("
            "SELECT sh.shipment_id, sh.shipment_status, "
            "COUNT(*) AS total_orders, sh.create_date_time "
            "FROM {schema}.shipment sh "
            "JOIN {schema}.orders ord ON sh.tc_shipment_id = ord.tc_shipment_id "
            "WHERE sh.shipment_status < 80 "
            "GROUP BY sh.shipment_id, sh.shipment_status, sh.create_date_time "
            "ORDER BY total_orders DESC"
            ") WHERE ROWNUM <= 10"
        )
        intents[i] = entry
        print(f"✓ Updated SQL to ROWNUM pattern")
        print(f"  {entry['sqlTemplate'][:100]}...")
        break

with open(intent_file, 'w') as f:
    json.dump(intents, f, indent=2)
    f.write('\n')

print("✓ Saved")
