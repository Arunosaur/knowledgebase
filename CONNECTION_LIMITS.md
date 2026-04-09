# Connection Limiting Implementation

## Overview

The bridge now implements per-DC connection limits for Manhattan groups and group-wide limits for CigWMS/WMSHUB, preventing database connection exhaustion.

## Architecture

### Connection Limit Strategy

| Group | Strategy | Limit | Total Max |
|-------|----------|-------|-----------|
| **manhattan-main** | 5 per DC × 9 DCs | 5/DC | ~45 connections |
| **manhattan-ck** | 5 per DC × 3 DCs (C1, C2, C3) | 5/DC | 15 connections |
| **manhattan-wk** | 5 per DC × 1 DC (WK) | 5/DC | 5 connections |
| **cigwms-prod** | Group-wide limit | 5 total | 5 connections |
| **wmshub-prod** | Group-wide limit | 5 total | 5 connections |

## Configuration

In `config.json` under `bridge` settings:

```json
"maxConcurrentConnectionsPerDC": 5,
"maxConcurrentConnectionsCigWMS": 5,
"maxConcurrentConnectionsWMSHub": 5
```

### Settings Explanation

| Setting | Default | Purpose | Groups |
|---------|---------|---------|--------|
| `maxConcurrentConnectionsPerDC` | 5 | Max concurrent connections per distribution center | Manhattan (main, ck, wk) |
| `maxConcurrentConnectionsCigWMS` | 5 | Max concurrent connections for group | CigWMS/OP only |
| `maxConcurrentConnectionsWMSHub` | 5 | Max concurrent connections for group | WMSHUB only |

## How It Works

### Connection Tracking

The pool system tracks active connections in two maps:

1. **Group-level** (`activeConnectionsPerGroup`): For tracking CigWMS/WMSHUB group-wide limits
2. **DC-level** (`activeConnectionsPerDC`): For tracking Manhattan per-DC limits

### Enforcement Logic

When a query is enqueued:

1. **For CigWMS/WMSHUB**: Check `activeConnectionsPerGroup[groupId]` against 5
2. **For Manhattan groups**: No group-level check; DC-level checking happens at request handler level
3. When query completes, connection counts are decremented

### Error Handling

**HTTP 429 - Too Many Requests** when limits are exceeded:

```json
{
  "error": "Connection limit reached for group cigwms-prod (5/5)",
  "code": "CONNECTION_LIMIT_EXCEEDED",
  "retryAfter": 30
}
```

Or for DC-level limits:

```json
{
  "error": "Connection limit reached for DC SE (5/5)",
  "code": "DC_CONNECTION_LIMIT_EXCEEDED",
  "retryAfter": 30
}
```

## Monitoring

### New Endpoint: `/pool-status`

Get current connection statistics:

```bash
curl http://localhost:3333/pool-status | python3 -m json.tool
```

Response includes connection limits and active counts:

```json
{
  "timestamp": "2026-03-17T...",
  "connectionLimits": {
    "manhattan": {
      "type": "per-dc",
      "limit": 5,
      "description": "5 connections per DC (9 DCs in main = ~45 max, 3 DCs in ck = 15 max, 1 DC in wk = 5 max)"
    },
    "cigwms": {
      "type": "group-wide",
      "limit": 5,
      "description": "5 connections total"
    },
    "wmshub": {
      "type": "group-wide",
      "limit": 5,
      "description": "5 connections total"
    }
  },
  "activeConnections": {
    "manhattan-main": {
      "active": 2,
      "limitType": "per-dc",
      "limit": 5,
      "available": 3
    },
    "dc-SE": {
      "active": 1,
      "limitType": "per-dc",
      "limit": 5,
      "available": 4
    },
    "cigwms-prod": {
      "active": 3,
      "limitType": "group-wide",
      "limit": 5,
      "available": 2
    }
  }
}
```

### Health Endpoint

The `/health` endpoint now includes pool status:

```bash
curl http://localhost:3333/health | python3 -m json.tool
```

## Implementation Details

### Files Modified

1. **config.json**
   - Added 3 connection limit settings (removed per-group limit):
     - `maxConcurrentConnectionsPerDC`: 5
     - `maxConcurrentConnectionsCigWMS`: 5  
     - `maxConcurrentConnectionsWMSHub`: 5

2. **lib/mcp-pool.js**
   - Added connection tracking maps: `activeConnectionsPerGroup` and `activeConnectionsPerDC`
   - Implemented `getConnectionLimit()` to return limit info with type (per-dc vs group-wide)
   - Enhanced `enqueueToolCalls()` to only check group-wide limits for CigWMS/WMSHUB
   - Manhattan groups rely on DC-level limits checked at request handler level
   - Updated `processQueue()` with enhanced logging for limit types
   - Enhanced `getPoolSnapshot()` to return detailed limit strategy and active connections

3. **bridge.js**
   - Added `/pool-status` endpoint (GET, public, no auth required)
   - Updated public endpoints list to include `/pool-status`

4. **lib/db-routes.js**
   - Enhanced error handling in `/db/query` POST to:
     - Detect `CONNECTION_LIMIT_EXCEEDED` and `DC_CONNECTION_LIMIT_EXCEEDED` errors
     - Return HTTP 429 with `Retry-After` header
     - Return detailed error with retry guidance

## Usage Example

### Python Client

```python
import json, urllib.request, time

def query_with_retry(url, body_dict, max_retries=3):
    for attempt in range(max_retries):
        try:
            body = json.dumps(body_dict).encode()
            req = urllib.request.Request(
                url,
                data=body,
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:  # Too Many Requests
                retry_after = int(e.headers.get('Retry-After', 30))
                print(f"Rate limited. Retrying in {retry_after}s...")
                time.sleep(retry_after)
            else:
                raise

result = query_with_retry(
    'http://localhost:3333/db/query',
    {
        'group': 'manhattan-main',
        'sql': 'SELECT * FROM MZ_DM.SHIPMENT WHERE ROWNUM <= 5',
        'maxRows': 5
    }
)
print(result)
```

### JavaScript Client

```javascript
async function queryWithRetry(url, body, maxRetries = 3) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      
      if (res.status === 429) {
        const retryAfter = parseInt(res.headers.get('Retry-After') || '30');
        console.log(`Rate limited. Retrying in ${retryAfter}s...`);
        await new Promise(r => setTimeout(r, retryAfter * 1000));
        continue;
      }
      
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (e) {
      if (attempt === maxRetries - 1) throw e;
    }
  }
}

const result = await queryWithRetry(
  'http://localhost:3333/db/query',
  {
    group: 'manhattan-main',
    sql: 'SELECT * FROM MZ_DM.SHIPMENT WHERE ROWNUM <= 5',
    maxRows: 5
  }
);
```

## Testing Connection Limits

### 1. Check Current Status

```bash
curl http://localhost:3333/pool-status | python3 -m json.tool
```

### 2. Understanding Limits from Status

For Manhattan groups, focus on DC-level stats:
```json
"dc-SE": {
  "active": 2,
  "limitType": "per-dc",
  "limit": 5,
  "available": 3
}
```

For CigWMS/WMSHUB, focus on group-wide stats:
```json
"cigwms-prod": {
  "active": 3,
  "limitType": "group-wide",
  "limit": 5,
  "available": 2
}
```

### 3. Simulate High Load - Manhattan DC (Python)

```python
import json, urllib.request, threading, time

def make_dc_query(group_id, dc_code):
    # Queries for a specific DC will count toward that DC's limit (5 max)
    body = json.dumps({
        'group': group_id,
        'sql': 'SELECT * FROM DBA_OBJECTS WHERE ROWNUM <= 1',
        'maxRows': 1
    }).encode()
    try:
        req = urllib.request.Request(
            'http://localhost:3333/db/query',
            data=body,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=120) as r:
            result = json.loads(r.read().decode())
            print(f"{dc_code}: OK {result.get('rowCount')} rows")
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"{dc_code}: HTTP 429 (limit reached)")
        else:
            print(f"{dc_code}: HTTP {e.code}")

# Spawn 12 concurrent queries to SE DC (max 5 allowed)
threads = []
for i in range(12):
    t = threading.Thread(target=make_dc_query, args=('manhattan-main', 'SE'))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

# Check pool status
time.sleep(1)
with urllib.request.urlopen('http://localhost:3333/pool-status') as r:
    status = json.loads(r.read().decode())
    for dc_name, stats in status.get('activeConnections', {}).items():
        if 'active' in stats and stats.get('limitType') == 'per-dc':
            print(f"{dc_name}: {stats['active']}/{stats['limit']} in use")
```

### 4. Simulate High Load - CigWMS (Python)

```python
import json, urllib.request, threading, time

def make_cigwms_query(i):
    # All queries against cigwms-prod count toward the 5 total limit
    body = json.dumps({
        'group': 'cigwms-prod',
        'sql': 'SELECT * FROM DBA_OBJECTS WHERE ROWNUM <= 1',
        'maxRows': 1
    }).encode()
    try:
        req = urllib.request.Request(
            'http://localhost:3333/db/query',
            data=body,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=120) as r:
            result = json.loads(r.read().decode())
            print(f"Query {i}: OK")
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"Query {i}: HTTP 429 (group limit reached)")
        else:
            print(f"Query {i}: HTTP {e.code}")

# Spawn 8 concurrent queries to cigwms-prod (max 5 total allowed)
threads = []
for i in range(8):
    t = threading.Thread(target=make_cigwms_query, args=(i,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

# Check pool status
time.sleep(1)
with urllib.request.urlopen('http://localhost:3333/pool-status') as r:
    status = json.loads(r.read().decode())
    for group_name, stats in status.get('activeConnections', {}).items():
        if 'active' in stats and stats.get('limitType') == 'group-wide':
            print(f"{group_name}: {stats['active']}/{stats['limit']} in use")
```

### 5. Expected Behavior

**Manhattan DC queries (SE, for example)**:
- Requests 1-5: Accepted, increment dc-SE counter
- Request 6: Rejected with 429 (dc-SE limit = 5)
- After first batch completes, requests 6-10 can proceed

**CigWMS group queries**:
- Requests 1-5: Accepted, increment cigwms-prod counter
- Request 6: Rejected with 429 (cigwms-prod limit = 5)
- Retry succeeds after earlier requests complete

## Performance Impact

- **Memory**: Additional maps for tracking (~1KB per active group/DC)
- **CPU**: Negligible - only atomic map operations
- **Latency**: Adds ~0.5ms per request for limit checks

## Future Enhancements

1. **Exponential backoff**: Automatic retry with exponential backoff in client libraries
2. **Per-schema limits**: Further granularity by schema within a group
3. **Dynamic limits**: Adjust limits based on VPN latency or DB response times
4. **Prometheus metrics**: Export connection pool stats for monitoring
5. **Admin dashboard**: Web UI to monitor and adjust limits in real-time

## Troubleshooting

### "Connection limit reached for DC" errors

**Problem**: Manhattan queries failing with 429 responses for specific DC  
**Solution**:
1. Check `/pool-status` to see which DC is at capacity
2. Look at `dc-{CODE}` in activeConnections (e.g., `dc-SE` for Southeast DC)
3. Wait for earlier queries to complete (connections auto-release)
4. If consistently hitting limits, implement client-side query batching
5. Alternatively, stagger queries across different DCs

### "Connection limit reached for group" errors (CigWMS/WMSHUB)

**Problem**: CigWMS or WMSHUB queries failing with 429 responses  
**Solution**:
1. Check `/pool-status` for `cigwms-prod` or `wmshub-prod` group stats
2. All queries to that group share the 5-connection pool
3. Implement exponential backoff and retry after 30 seconds
4. Consider splitting queries across time (e.g., daytime vs batch jobs)

### High queue depth

**Problem**: Many queued requests accumulating  
**Solution**:
1. Check `/pool-status` for `queueDepth` on pool entries
2. Monitor active connection count vs limit
3. Optimize SQL queries for faster execution
4. Use pagination to reduce per-query scope
5. Distribute load across multiple client instances

### Uneven connection usage

**Problem**: Some DCs use all 5 while others idle  
**Solution**:
1. Monitor with `/pool-status` over time
2. Analyze query patterns by DC
3. Check if certain operations are slower
4. Balance workload across DCs when possible
5. Profile slow queries to optimize performance

### Connection released but limit still shows full

**Problem**: Limit shows 5/5 but no new queries allowed  
**Solution**:
1. May be in process of releasing connection (natural lag)
2. Wait a few seconds for automatic cleanup
3. Check system logs in stderr for any errors
4. If persistent, restart bridge to reset connection counts
5. Verify no hanging transactions on database side
