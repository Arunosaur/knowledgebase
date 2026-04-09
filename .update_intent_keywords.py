#!/usr/bin/env python3
import json

intent_file = '/Users/asrajag/Workspace/oracle/knowledgeBase/semantic-index/intents.json'

with open(intent_file, 'r') as f:
    intents = json.load(f)

# Find and update the shipment max units intent
for i, entry in enumerate(intents):
    if entry.get('id') == 'si-shipment-max-units-dc':
        # Expand keywords to catch more variations and exact phrases
        entry['keywords'] = [
            "which load is shipping the most units",
            "which shipment is shipping the most units",
            "load shipping the most units",
            "shipment shipping the most units",
            "which load shipping most units",
            "which shipment shipping most units",
            "shipment most units",
            "load most units",
            "highest unit count",
            "max units",
            "most units for",
            "most units in",
            "most units by",
            "most items",
            "maximum units"
        ]
        intents[i] = entry
        print(f"✓ Updated si-shipment-max-units-dc with {len(entry['keywords'])} keywords")
        break

with open(intent_file, 'w') as f:
    json.dump(intents, f, indent=2)
    f.write('\n')

print("✓ Saved")
