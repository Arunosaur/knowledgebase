#!/usr/bin/env python3
import json
from datetime import datetime

# Read existing intents
with open('semantic-index/intents.json', 'r') as f:
    intents = json.load(f)

print(f"[BEFORE] Total intents: {len(intents)}")

# Get current max ID number for new intents
id_parts = [int(e.get('id', '').split('-')[-1]) if e.get('id', '').startswith('si-') else 0 for e in intents]
last_id_num = max(id_parts) if id_parts else 0
print(f"[ID] Last ID number: {last_id_num}")

# New workflow-specific intents based on SE_DM/DM source code analysis
workflow_intents = [
    {
        "id": f"si-workflow-pkg-{last_id_num + 1}",
        "intent": "Route shipment through optimal lane based on origin/destination location and carrier mode",
        "keywords": ["route shipment", "select lane", "carrier selection", "shipping mode", "optimal routing"],
        "package": "ROUTING_GUIDE_PKG",
        "procedure": "get_matching_lane / get_matching_lane_mcw / get_resource_options",
        "tables": ["RG_LANE", "RG_LANE_ZONE", "SHIPMENT", "FACILITY"],
        "columns": ["O_FACILITY_ID", "D_FACILITY_ID", "LANE_ID", "CARRIER_ID", "SERVICE_MODE"],
        "source": "package-source-analysis",
        "businessLogic": "Queries origin/destination zones, matches against RG_LANE to find valid lanes, filters by carrier and mode, returns sorted routes by cost/performance",
        "confidence": 0.82,
        "confirmed": False,
        "createdAt": datetime.now().isoformat(),
        "updatedAt": datetime.now().isoformat()
    },
    {
        "id": f"si-workflow-pkg-{last_id_num + 2}",
        "intent": "Update order status when shipment status changes to maintain consistency",
        "keywords": ["order status", "shipment status", "cascade update", "order sync"],
        "package": "ORDER_STATE_PKG",
        "procedure": "PRSETORDERSTATUS",
        "tables": ["ORDERS", "ORDER_MOVEMENT", "ORDER_SPLIT", "SHIPMENT"],
        "columns": ["ORDER_STATUS", "SHIPMENT_ID", "ORDER_ID", "LAST_UPDATED_DTTM"],
        "source": "package-source-analysis",
        "businessLogic": "When shipment status updates, iterates through ORDER_MOVEMENT and ORDER_SPLIT records. Computes new status for each order/split. Updates ORDERS table atomically with new status + source tracking.",
        "confidence": 0.85,
        "confirmed": False,
        "createdAt": datetime.now().isoformat(),
        "updatedAt": datetime.now().isoformat()
    },
    {
        "id": f"si-workflow-pkg-{last_id_num + 3}",
        "intent": "Consolidate multiple shipments by destination, weight, and facility rules",
        "keywords": ["consolidate shipments", "combine loads", "container packing", "load optimization"],
        "package": "PACK_CONSOL_PERF",
        "procedure": "get_comb_resource_options",
        "tables": ["SHIPMENT", "FACILITY_CONSOL_RULE", "CONTAINER", "LOAD"],
        "columns": ["SHIPMENT_ID", "D_FACILITY_ID", "TOTAL_WEIGHT", "CONSOL_LEVEL"],
        "source": "package-source-analysis",
        "businessLogic": "Groups shipments with same destination. Checks FACILITY_CONSOL_RULE thresholds by weight/count/mode. Evaluates available consolidation options weighted by cost savings.",
        "confidence": 0.76,
        "confirmed": False,
        "createdAt": datetime.now().isoformat(),
        "updatedAt": datetime.now().isoformat()
    },
    {
        "id": f"si-workflow-pkg-{last_id_num + 4}",
        "intent": "Validate carrier and lane eligibility before selecting routing option",
        "keywords": ["carrier validation", "lane eligibility", "routing rules", "carrier constraints"],
        "package": "ROUTING_GUIDE_PKG + RG_LANE_VALIDATION_PKG",
        "procedure": "check_if_rg_carrier / check_if_rg_lane_carrier",
        "tables": ["RG_LANE", "RG_LANE_CARRIER", "CARRIER_RESTRICTION"],
        "columns": ["LANE_ID", "CARRIER_ID", "RESTRICTION_TYPE", "EFFECTIVE_FROM"],
        "source": "package-source-analysis",
        "businessLogic": "Validates carrier is active on lane, checks lane constraints (hazmat, temp, equipment), validates sub-restriction rules, verifies effective date ranges. Rejects routes with expired rules.",
        "confidence": 0.81,
        "confirmed": False,
        "createdAt": datetime.now().isoformat(),
        "updatedAt": datetime.now().isoformat()
    },
    {
        "id": f"si-workflow-pkg-{last_id_num + 5}",
        "intent": "Calculate shipping rate with weight-distance pricing and surcharges",
        "keywords": ["calculate rate", "shipping cost", "pricing rules", "surcharge", "weight-distance"],
        "package": "RATING_PKG + RATING_LANE_VALIDATION_PKG",
        "procedure": "calculate_rate / apply_surcharges",
        "tables": ["RG_LANE_RATE", "RATING_SURCHARGE", "FACILITY_SURCHARGE", "SHIPPER_DISCOUNT"],
        "columns": ["LANE_ID", "MIN_WEIGHT", "MAX_WEIGHT", "BASE_RATE", "SURCHARGE_AMOUNT"],
        "source": "package-source-analysis",
        "businessLogic": "Looks up base rate by weight break from RG_LANE_RATE. Applies facility surcharges (origin/destination). Applies shipper/consignee discounts. Applies mode surcharges (hazmat, temp).",
        "confidence": 0.80,
        "confirmed": False,
        "createdAt": datetime.now().isoformat(),
        "updatedAt": datetime.now().isoformat()
    }
]

# Append and save
intents.extend(workflow_intents)

with open('semantic-index/intents.json', 'w') as f:
    json.dump(intents, f, indent=2)

print(f"[AFTER] Total intents: {len(intents)}")
print(f"[✅] Created {len(workflow_intents)} workflow intents")
print("\nWorkflow intents:")
for i in workflow_intents:
    print(f"  • {i['id']}: {i['intent'][:65]}")
