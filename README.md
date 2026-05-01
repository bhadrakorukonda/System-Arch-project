# Distributed Key-Value Store

A production-inspired distributed KV store built in Python, implementing consistent hashing, replication, quorum-based reads/writes, and automatic failover.

---

## Architecture

```
Client
  │  HTTP (PUT/GET/DELETE /kv/{key})
  ▼
┌─────────────────────────────────┐
│         Coordinator             │  Port 8000
│  - Owns the consistent hash ring│
│  - Routes to correct nodes      │
│  - Quorum logic                 │
│  - Health checker (every 5s)    │
└───────┬───────┬───────┬─────────┘
        │       │       │  replicated writes (RF=3)
  ┌─────▼─┐ ┌───▼───┐ ┌─▼─────┐ ┌───────┐ ┌───────┐
  │ node1 │ │ node2 │ │ node3 │ │ node4 │ │ node5 │
  │ :8001 │ │ :8002 │ │ :8003 │ │ :8004 │ │ :8005 │
  └───────┘ └───────┘ └───────┘ └───────┘ └───────┘
```

## Key Concepts Implemented

### Consistent Hashing
Keys are mapped onto a ring using MD5. Each physical node gets 150 virtual nodes for even distribution. When a node is added or removed, only `keys / N` keys need to move — not a full reshuffle.

### Replication
Every write goes to `REPLICATION_FACTOR=3` successor nodes on the ring. If a key hashes to node2, copies also go to node3 and node4.

### Quorum
- **Write quorum (W=2):** A PUT succeeds only when ≥2 nodes acknowledge it.  
- **Read quorum (R=1):** A GET returns on the first successful replica response.  
- This is a W+R > N style consistency model (CAP: AP with tunable consistency).

### Fault Tolerance
The coordinator runs an async health checker every 5 seconds. Unhealthy nodes are excluded from routing. Reads automatically fall to healthy replicas.

---

## Running It

```bash
# Start the cluster
docker compose up --build

# In another terminal
python client/demo.py           # basic demo
python client/demo.py benchmark # 1000 writes + reads with latency stats
python client/demo.py chaos     # instructions for fault tolerance demo
```

## Manual Chaos Test

```bash
# Write some data
curl -X PUT http://localhost:8000/kv/mykey -H "Content-Type: application/json" -d '{"value": "hello"}'

# Kill a node
docker compose stop node1

# Wait 5 seconds, then read — still works
curl http://localhost:8000/kv/mykey

# See cluster state
curl http://localhost:8000/ring

# Bring it back
docker compose start node1
```

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/kv/{key}` | Write a key (body: `{"value": "..."}`) |
| GET | `/kv/{key}` | Read a key |
| DELETE | `/kv/{key}` | Delete a key from all replicas |
| GET | `/kv/{key}/replicas` | Debug: which nodes own this key |
| GET | `/ring` | Cluster health + ring info |

## What's Next (Week 2-3)

- [ ] Anti-entropy / gossip: sync keys written during a node outage when it rejoins  
- [ ] Vector clocks: detect write conflicts during concurrent updates  
- [ ] Node join rebalancing: redistribute keys when a new node joins  
- [ ] Persistence: swap in-memory dict for RocksDB or SQLite  
- [ ] Metrics endpoint: Prometheus-compatible `/metrics`
