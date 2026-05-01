import hashlib
import bisect


class HashRing:
    """
    Consistent hash ring with virtual nodes.
    Each physical node gets `replicas` virtual nodes spread across the ring
    to ensure even key distribution even with few nodes.
    """

    def __init__(self, replicas: int = 150):
        self.replicas = replicas
        self._ring: dict[int, str] = {}   # hash position -> node_id
        self._sorted_keys: list[int] = [] # sorted list of positions
        self._nodes: set[str] = set()

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node_id: str):
        """Add a physical node (and its virtual nodes) to the ring."""
        self._nodes.add(node_id)
        for i in range(self.replicas):
            virtual_key = f"{node_id}#vnode{i}"
            h = self._hash(virtual_key)
            self._ring[h] = node_id
            bisect.insort(self._sorted_keys, h)

    def remove_node(self, node_id: str):
        """Remove a node and all its virtual nodes from the ring."""
        self._nodes.discard(node_id)
        for i in range(self.replicas):
            virtual_key = f"{node_id}#vnode{i}"
            h = self._hash(virtual_key)
            self._ring.pop(h, None)
            idx = bisect.bisect_left(self._sorted_keys, h)
            if idx < len(self._sorted_keys) and self._sorted_keys[idx] == h:
                self._sorted_keys.pop(idx)

    def get_node(self, key: str) -> str | None:
        """Return the node responsible for this key."""
        if not self._ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect(self._sorted_keys, h) % len(self._sorted_keys)
        return self._ring[self._sorted_keys[idx]]

    def get_nodes(self, key: str, n: int) -> list[str]:
        """
        Return up to n *distinct* physical nodes starting from the key's
        position — used for replication.
        """
        if not self._ring:
            return []
        h = self._hash(key)
        idx = bisect.bisect(self._sorted_keys, h) % len(self._sorted_keys)

        seen: set[str] = set()
        result: list[str] = []
        for _ in range(len(self._sorted_keys)):
            node = self._ring[self._sorted_keys[idx]]
            if node not in seen:
                seen.add(node)
                result.append(node)
                if len(result) == n:
                    break
            idx = (idx + 1) % len(self._sorted_keys)
        return result

    @property
    def nodes(self) -> set[str]:
        return set(self._nodes)

    def __repr__(self):
        return f"HashRing(nodes={sorted(self._nodes)}, replicas={self.replicas})"
