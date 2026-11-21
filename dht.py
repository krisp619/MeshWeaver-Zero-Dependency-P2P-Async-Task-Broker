import hashlib
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

def generate_node_id(host: str, port: int) -> int:
    raw = f"{host}:{port}".encode()
    return int(hashlib.sha1(raw).hexdigest(), 16)

@dataclass
class PeerInfo:
    node_id: int
    host: str
    udp_port: int
    tcp_port: int
    cpu_load: float = 0.0
    ram_load: float = 0.0
    last_seen: float = field(default_factory=time.time)

    @property
    def short_id(self) -> str:
        return hex(self.node_id)[2:8]

class KademliaRoutingTable:
    def __init__(self, self_id: int, timeout: float = 10.0):
        self.self_id = self_id
        self.timeout = timeout
        self.peers: Dict[int, PeerInfo] = {}

    def xor_distance(self, other_id: int) -> int:
        return self.self_id ^ other_id

    def update_peer(self, peer: PeerInfo):
        if peer.node_id == self.self_id:
            return
        peer.last_seen = time.time()
        self.peers[peer.node_id] = peer

    def prune_dead_peers(self) -> List[PeerInfo]:
        now = time.time()
        dead = [p for p in self.peers.values() if (now - p.last_seen) > self.timeout]
        for p in dead:
            del self.peers[p.node_id]
        return dead

    def closest_peers(self, target_id: int, k: int = 5) -> List[PeerInfo]:
        alive = list(self.peers.values())
        alive.sort(key=lambda p: p.node_id ^ target_id)
        return alive[:k]

    def get_lowest_cpu_peer(self, exclude_ids: Optional[set] = None) -> Optional[PeerInfo]:
        exclude_ids = exclude_ids or set()
        candidates = [
            p for p in self.peers.values()
            if p.node_id not in exclude_ids and (time.time() - p.last_seen) <= self.timeout
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda p: (p.cpu_load, p.ram_load))