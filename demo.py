"""
Demo client — run this to see the cluster in action.

Usage:
  python client/demo.py              # full demo
  python client/demo.py benchmark    # write 1000 keys, measure latency
  python client/demo.py chaos        # kill-node fault tolerance demo (manual)
"""

import sys
import time
import httpx
import statistics

BASE = "http://localhost:8000"


def put(key: str, value: str):
    r = httpx.put(f"{BASE}/kv/{key}", json={"value": value})
    r.raise_for_status()
    return r.json()

def get(key: str):
    r = httpx.get(f"{BASE}/kv/{key}")
    r.raise_for_status()
    return r.json()

def delete(key: str):
    r = httpx.delete(f"{BASE}/kv/{key}")
    r.raise_for_status()
    return r.json()

def where(key: str):
    r = httpx.get(f"{BASE}/kv/{key}/replicas")
    r.raise_for_status()
    return r.json()

def ring_info():
    r = httpx.get(f"{BASE}/ring")
    r.raise_for_status()
    return r.json()


def demo():
    print("\n=== Cluster Info ===")
    print(ring_info())

    print("\n=== Basic PUT / GET / DELETE ===")
    print(put("name", "Bhadra"))
    print(put("role", "SDE"))
    print(put("company", "Amazon"))

    print(get("name"))
    print(get("role"))

    print(delete("role"))
    try:
        print(get("role"))
    except httpx.HTTPStatusError as e:
        print(f"Correctly got 404 after delete: {e.response.status_code}")

    print("\n=== Replication: where does 'name' live? ===")
    print(where("name"))


def benchmark(n: int = 1000):
    print(f"\n=== Benchmark: {n} writes then {n} reads ===")

    write_times = []
    for i in range(n):
        key = f"bench_key_{i}"
        t0 = time.perf_counter()
        put(key, f"value_{i}")
        write_times.append((time.perf_counter() - t0) * 1000)

    read_times = []
    for i in range(n):
        key = f"bench_key_{i}"
        t0 = time.perf_counter()
        get(key)
        read_times.append((time.perf_counter() - t0) * 1000)

    print(f"Writes — mean: {statistics.mean(write_times):.1f}ms  "
          f"p95: {statistics.quantiles(write_times, n=20)[18]:.1f}ms  "
          f"max: {max(write_times):.1f}ms")
    print(f"Reads  — mean: {statistics.mean(read_times):.1f}ms  "
          f"p95: {statistics.quantiles(read_times, n=20)[18]:.1f}ms  "
          f"max: {max(read_times):.1f}ms")


def chaos_instructions():
    print("""
=== Chaos / Fault Tolerance Demo ===

1. Start the cluster:     docker compose up
2. Write some keys:       python client/demo.py
3. Kill a node:           docker compose stop node1
4. Wait 5s for health checker to detect it
5. Reads should still work (replicas on node2/3 take over)
6. Writes should still work if WRITE_QUORUM is met
7. Bring it back:         docker compose start node1
8. Keys written while node1 was down won't be on it (no anti-entropy yet)
   — that's Week 3's gossip protocol exercise
""")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "demo"
    if mode == "benchmark":
        benchmark()
    elif mode == "chaos":
        chaos_instructions()
    else:
        demo()
