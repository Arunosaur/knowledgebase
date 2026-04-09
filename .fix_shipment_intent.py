#!/usr/bin/env python3
import json

intent_file = '/Users/asrajag/Workspace/oracle/knowledgeBase/semantic-index/intents.json'

with open(intent_file, 'r') as f:
    intents = json.load(f)

for i, entry in enumerate(intents):
    if entry.get('id') == 'si-shipment-max-units-dc':
        # Use proven table joins from existing working intents
        # orders -> order_line_item via order_id (same as wave queries)
        # shipment -> orders via tc_shipment_id (same as wave queries)
        # No shipment_line (doesn't exist in Manhattan DM schemas)
        entry['tables'] = ['SHIPMENT', 'ORDERS', 'ORDER_LINE_ITEM']
        entry['columns'] = ['SHIPMENT_ID', 'TC_SHIPMENT_ID', 'SHIPMENT_STATUS', 'ORDER_LINE_ITEM_ID', 'CREATE_DATE_TIME']
        entry['keywords'] = [
            "which load is shipping the most units",
            "which shipment is shipping the most units",
            "open load shipping the most units",
            "open loads shipping most units",
            "open shipment most units",
            "load most units",
            "shipment most units",
            "most units for",
            "most units among open",
            "maximum units open",
            "highest unit count",
            "max units",
            "maximum units",
            "most items"
        ]
        # SQL using proven join pattern, open filter for "open loads"
        entry['sqlTemplate'] = (
            "SELECT sh.shipment_id, sh.shipment_status, "
            "COUNT(DISTINCT oli.order_line_item_id) AS total_lines, "
            "sh.create_date_time "
            "FROM {schema}.shipment sh "
            "JOIN {schema}.orders ord ON sh.tc_shipment_id = ord.tc_shipment_id "
            "JOIN {schema}.order_line_item oli ON ord.order_id = oli.order_id "
            "WHERE sh.shipment_status < 80 "
            "GROUP BY sh.shipment_id, sh.shipment_status, sh.create_date_time "
            "ORDER BY total_lines DESC NULLS LAST "
            "FETCH FIRST 10 ROWS ONLY"
        )
        intents[i] = entry
        print(f"✓ Updated si-shipment-max-units-dc")
        print(f"  Tables: {entry['tables']}")
        print(f"  SQL: {entry['sqlTemplate'][:80]}...")
        break

with open(intent_file, 'w') as f:
    json.dump(intents, f, indent=2)
    f.write('\n')

print("✓ Saved")
