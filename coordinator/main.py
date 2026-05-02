"""
Coordinator — the entry point for all client requests.
It owns the hash ring, decides which storage nodes own a key,
and handles replication + failover transparently.
"""

import os
import httpx
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from hash_ring import HashRing

logging.basicConfig(level=logging.INFO, format="%(asctime)s [COORDINATOR] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", 3))
WRITE_QUORUM       = int(os.getenv("WRITE_QUORUM", 2))   # how many acks needed
READ_QUORUM        = int(os.getenv("READ_QUORUM", 1))    # how many reads needed
HEALTH_INTERVAL    = int(os.getenv("HEALTH_INTERVAL", 5)) # seconds

# Storage nodes registered at startup (add more in docker-compose)
STORAGE_NODES: dict[str, str] = {
    "node1": os.getenv("NODE1_URL", "http://localhost:8001"),
    "node2": os.getenv("NODE2_URL", "http://localhost:8002"),
    "node3": os.getenv("NODE3_URL", "http://localhost:8003"),
    "node4": os.getenv("NODE4_URL", "http://localhost:8004"),
    "node5": os.getenv("NODE5_URL", "http://localhost:8005"),
}

ring = HashRing(replicas=150)
healthy_nodes: set[str] = set()

# ── Lifespan: seed ring + start health checker ─────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    for node_id in STORAGE_NODES:
        ring.add_node(node_id)
    asyncio.create_task(health_check_loop())
    log.info(f"Ring initialised with nodes: {sorted(ring.nodes)}")
    yield

app = FastAPI(title="KV Store — Coordinator", lifespan=lifespan)

# ── Health checker ─────────────────────────────────────────────────────────
async def health_check_loop():
    async with httpx.AsyncClient(timeout=2.0) as client:
        while True:
            for node_id, url in STORAGE_NODES.items():
                try:
                    r = await client.get(f"{url}/health")
                    if r.status_code == 200:
                        if node_id not in healthy_nodes:
                            log.info(f"Node {node_id} came UP")
                            healthy_nodes.add(node_id)
                    else:
                        raise Exception("non-200")
                except Exception:
                    if node_id in healthy_nodes:
                        log.warning(f"Node {node_id} went DOWN")
                        healthy_nodes.discard(node_id)
            await asyncio.sleep(HEALTH_INTERVAL)

# ── Helpers ────────────────────────────────────────────────────────────────
def get_target_nodes(key: str) -> list[str]:
    """Return healthy replica nodes for this key (up to REPLICATION_FACTOR)."""
    all_replicas = ring.get_nodes(key, REPLICATION_FACTOR)
    alive = [n for n in all_replicas if n in healthy_nodes]
    return alive

async def node_put(client: httpx.AsyncClient, node_id: str, key: str, value: str) -> bool:
    try:
        url = f"{STORAGE_NODES[node_id]}/store/{key}"
        r = await client.put(url, json={"value": value}, timeout=3.0)
        return r.status_code == 200
    except Exception as e:
        log.warning(f"PUT to {node_id} failed: {e}")
        return False

async def node_get(client: httpx.AsyncClient, node_id: str, key: str) -> str | None:
    try:
        url = f"{STORAGE_NODES[node_id]}/store/{key}"
        r = await client.get(url, timeout=3.0)
        if r.status_code == 200:
            return r.json()["value"]
    except Exception as e:
        log.warning(f"GET from {node_id} failed: {e}")
    return None

async def node_delete(client: httpx.AsyncClient, node_id: str, key: str) -> bool:
    try:
        url = f"{STORAGE_NODES[node_id]}/store/{key}"
        r = await client.delete(url, timeout=3.0)
        return r.status_code == 200
    except Exception as e:
        log.warning(f"DELETE on {node_id} failed: {e}")
        return False

# ── Models ─────────────────────────────────────────────────────────────────
class PutRequest(BaseModel):
    value: str

# ── Routes ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "healthy_nodes": sorted(healthy_nodes)}

@app.get("/ring")
def ring_info():
    return {
        "all_nodes": sorted(ring.nodes),
        "healthy_nodes": sorted(healthy_nodes),
        "replication_factor": REPLICATION_FACTOR,
    }

@app.put("/kv/{key}")
async def put(key: str, body: PutRequest):
    targets = get_target_nodes(key)
    if not targets:
        raise HTTPException(503, "No healthy nodes available")

    async with httpx.AsyncClient() as client:
        tasks = [node_put(client, n, key, body.value) for n in targets]
        results = await asyncio.gather(*tasks)

    acks = sum(results)
    if acks < WRITE_QUORUM:
        raise HTTPException(503, f"Write quorum not met: {acks}/{WRITE_QUORUM} acks")

    log.info(f"PUT '{key}' → {targets} ({acks} acks)")
    return {"key": key, "replicas": targets, "acks": acks}

@app.get("/kv/{key}")
async def get(key: str):
    targets = get_target_nodes(key)
    if not targets:
        raise HTTPException(503, "No healthy nodes available")

    async with httpx.AsyncClient() as client:
        for node_id in targets:
            value = await node_get(client, node_id, key)
            if value is not None:
                log.info(f"GET '{key}' ← {node_id}")
                return {"key": key, "value": value, "served_by": node_id}

    raise HTTPException(404, f"Key '{key}' not found")

@app.delete("/kv/{key}")
async def delete(key: str):
    targets = get_target_nodes(key)
    if not targets:
        raise HTTPException(503, "No healthy nodes available")

    async with httpx.AsyncClient() as client:
        tasks = [node_delete(client, n, key) for n in targets]
        results = await asyncio.gather(*tasks)

    acks = sum(results)
    log.info(f"DELETE '{key}' → {targets} ({acks} acks)")
    return {"key": key, "deleted_from": targets, "acks": acks}

@app.get("/kv/{key}/replicas")
async def where_is(key: str):
    """Debug: show which nodes own this key."""
    all_replicas  = ring.get_nodes(key, REPLICATION_FACTOR)
    alive_replicas = [n for n in all_replicas if n in healthy_nodes]
    return {
        "key": key,
        "all_replicas": all_replicas,
        "healthy_replicas": alive_replicas,
    }
