"""
Storage Node — a single node in the cluster.
Stores key-value pairs in memory (swap dict for RocksDB/SQLite for persistence).
Multiple instances of this run on different ports.
"""

import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

NODE_ID = os.getenv("NODE_ID", "node?")

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{NODE_ID}] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title=f"KV Store — {NODE_ID}")

# In-memory store. Replace with: import shelve / sqlite3 / rocksdb for persistence
_store: dict[str, str] = {}


class PutRequest(BaseModel):
    value: str


@app.get("/health")
def health():
    return {"status": "ok", "node_id": NODE_ID, "keys_stored": len(_store)}


@app.put("/store/{key}")
def put(key: str, body: PutRequest):
    _store[key] = body.value
    log.info(f"PUT {key!r}")
    return {"ok": True}


@app.get("/store/{key}")
def get(key: str):
    if key not in _store:
        raise HTTPException(404, f"Key {key!r} not found on {NODE_ID}")
    return {"key": key, "value": _store[key], "node_id": NODE_ID}


@app.delete("/store/{key}")
def delete(key: str):
    _store.pop(key, None)
    log.info(f"DELETE {key!r}")
    return {"ok": True}


@app.get("/store")
def list_keys():
    """Debug: see all keys on this node."""
    return {"node_id": NODE_ID, "keys": sorted(_store.keys()), "count": len(_store)}
