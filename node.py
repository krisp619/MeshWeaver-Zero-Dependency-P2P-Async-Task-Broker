import asyncio
import json
import time
import uuid
import base64
import cloudpickle
import psutil
from typing import Callable, Any, Dict, List, Optional

from dht import KademliaRoutingTable, PeerInfo, generate_node_id
from security import (
    get_server_ssl_context,
    get_client_ssl_context,
    sign_payload,
    verify_signature,
)

class GossipUDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, node: "MeshNode"):
        self.node = node

    def datagram_received(self, data: bytes, addr):
        try:
            msg = json.loads(data.decode())
            self.node.handle_gossip(msg, addr)
        except Exception:
            pass

class MeshNode:
    def __init__(self, host: str, udp_port: int, tcp_port: int, bootstrap: Optional[tuple] = None):
        self.host = host
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self.node_id = generate_node_id(host, tcp_port)
        self.short_id = hex(self.node_id)[2:8]
        self.bootstrap = bootstrap

        self.dht = KademliaRoutingTable(self.node_id, timeout=9.0)
        self.cpu_load = psutil.cpu_percent(interval=None)
        self.ram_load = psutil.virtual_memory().percent

        self.task_history: List[Dict[str, Any]] = []
        self.logs: List[str] = []
        self.udp_transport = None
        self.tcp_server = None
        self._running = False

    def log(self, text: str):
        ts = time.strftime("%H:%M:%S")
        self.logs.append(f"[{ts}] {text}")
        if len(self.logs) > 30:
            self.logs.pop(0)

    async def start(self):
        self._running = True
        loop = asyncio.get_running_loop()

        # 1. Start UDP Gossip & DHT Socket
        self.udp_transport, _ = await loop.create_datagram_endpoint(
            lambda: GossipUDPProtocol(self),
            local_addr=(self.host, self.udp_port),
        )

        # 2. Start TLS-Encrypted TCP Task Worker
        ssl_ctx = get_server_ssl_context()
        self.tcp_server = await asyncio.start_server(
            self._handle_tcp_task, self.host, self.tcp_port, ssl=ssl_ctx
        )

        self.log(f"Node {self.short_id} online | UDP:{self.udp_port} TLS-TCP:{self.tcp_port}")

        # 3. Bootstrap into Kademlia Mesh
        if self.bootstrap:
            self._send_gossip_packet(self.bootstrap)

        # 4. Background Gossip & Heartbeat Monitor Loops
        asyncio.create_task(self._gossip_loop())
        asyncio.create_task(self._heartbeat_monitor_loop())

    async def stop(self):
        self._running = False
        if self.udp_transport:
            self.udp_transport.close()
        if self.tcp_server:
            self.tcp_server.close()
            await self.tcp_server.wait_closed()
        self.log(f"Node {self.short_id} stopped.")

    def _build_gossip_payload(self) -> bytes:
        self.cpu_load = psutil.cpu_percent(interval=None)
        self.ram_load = psutil.virtual_memory().percent
        known_peers = [
            {
                "node_id": p.node_id,
                "host": p.host,
                "udp_port": p.udp_port,
                "tcp_port": p.tcp_port,
                "cpu_load": p.cpu_load,
                "ram_load": p.ram_load,
            }
            for p in self.dht.peers.values()
        ]
        packet = {
            "type": "GOSSIP",
            "sender": {
                "node_id": self.node_id,
                "host": self.host,
                "udp_port": self.udp_port,
                "tcp_port": self.tcp_port,
                "cpu_load": self.cpu_load,
                "ram_load": self.ram_load,
            },
            "peers": known_peers,
        }
        return json.dumps(packet).encode()

    def _send_gossip_packet(self, addr: tuple):
        if self.udp_transport:
            self.udp_transport.sendto(self._build_gossip_payload(), addr)

    def handle_gossip(self, msg: dict, addr: tuple):
        if msg.get("type") != "GOSSIP":
            return
        s = msg["sender"]
        sender_peer = PeerInfo(
            node_id=s["node_id"],
            host=s["host"],
            udp_port=s["udp_port"],
            tcp_port=s["tcp_port"],
            cpu_load=s["cpu_load"],
            ram_load=s["ram_load"],
        )
        is_new = sender_peer.node_id not in self.dht.peers
        self.dht.update_peer(sender_peer)
        if is_new:
            self.log(f"DHT Discovered peer {sender_peer.short_id} (CPU: {sender_peer.cpu_load:.1f}%)")
            # Reply back immediately for fast Kademlia convergence
            self._send_gossip_packet((sender_peer.host, sender_peer.udp_port))

        # Merge gossiped neighbor table
        for p in msg.get("peers", []):
            if p["node_id"] != self.node_id and p["node_id"] not in self.dht.peers:
                discovered = PeerInfo(
                    node_id=p["node_id"],
                    host=p["host"],
                    udp_port=p["udp_port"],
                    tcp_port=p["tcp_port"],
                    cpu_load=p["cpu_load"],
                    ram_load=p["ram_load"],
                )
                self.dht.update_peer(discovered)
                self._send_gossip_packet((discovered.host, discovered.udp_port))

    async def _gossip_loop(self):
        while self._running:
            neighbors = self.dht.closest_peers(self.node_id, k=6)
            for peer in neighbors:
                self._send_gossip_packet((peer.host, peer.udp_port))
            if not neighbors and self.bootstrap:
                self._send_gossip_packet(self.bootstrap)
            await asyncio.sleep(3.0)  # Fast gossip interval for live demo

    async def _heartbeat_monitor_loop(self):
        while self._running:
            dead_peers = self.dht.prune_dead_peers()
            for dp in dead_peers:
                self.log(f"[ALERT] Lost heartbeat from {dp.short_id}. Removed from DHT.")
            await asyncio.sleep(2.0)

    async def _handle_tcp_task(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            raw_len = await reader.readexactly(4)
            msg_len = int.from_bytes(raw_len, "big")
            data = await reader.readexactly(msg_len)
            envelope = json.loads(data.decode())

            task_id = envelope["task_id"]
            payload_b64 = envelope["payload"]
            signature = envelope["signature"]
            raw_payload = base64.b64decode(payload_b64)

            if not verify_signature(raw_payload, signature):
                resp = {"status": "REJECTED", "error": "Invalid HMAC Signature"}
            else:
                fn, args, kwargs = cloudpickle.loads(raw_payload)
                self.log(f"Executing task {task_id} locally...")
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, lambda: fn(*args, **kwargs))
                res_b64 = base64.b64encode(cloudpickle.dumps(result)).decode()
                resp = {"status": "SUCCESS", "result": res_b64}
                self.log(f"Completed task {task_id}")

            resp_bytes = json.dumps(resp).encode()
            writer.write(len(resp_bytes).to_bytes(4, "big") + resp_bytes)
            await writer.drain()
        except Exception as e:
            self.log(f"Worker error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def submit_task(self, fn: Callable, *args, **kwargs) -> Any:
        task_id = str(uuid.uuid4())[:6]
        raw_payload = cloudpickle.dumps((fn, args, kwargs))
        signature = sign_payload(raw_payload)
        envelope = json.dumps({
            "task_id": task_id,
            "payload": base64.b64encode(raw_payload).decode(),
            "signature": signature,
        }).encode()

        failed_nodes = set()
        record = {
            "task_id": task_id,
            "func": getattr(fn, "__name__", "lambda"),
            "target": "pending",
            "status": "ROUTING",
            "result": "-",
        }
        self.task_history.append(record)

        # Fault-tolerant routing loop with automatic failover
        for attempt in range(1, 5):
            target = self.dht.get_lowest_cpu_peer(exclude_ids=failed_nodes)
            if not target:
                record["target"] = f"self({self.short_id})"
                record["status"] = "LOCAL_EXEC"
                res = fn(*args, **kwargs)
                record["result"] = str(res)
                return res

            record["target"] = f"{target.short_id} ({target.cpu_load:.1f}%)"
            record["status"] = f"RUNNING (Try {attempt})"
            self.log(f"Routing task {task_id} -> {target.short_id} (CPU: {target.cpu_load:.1f}%)")

            try:
                ssl_ctx = get_client_ssl_context()
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(target.host, target.tcp_port, ssl=ssl_ctx),
                    timeout=3.0,
                )
                writer.write(len(envelope).to_bytes(4, "big") + envelope)
                await writer.drain()

                raw_len = await asyncio.wait_for(reader.readexactly(4), timeout=10.0)
                resp_len = int.from_bytes(raw_len, "big")
                resp_data = await reader.readexactly(resp_len)
                writer.close()
                await writer.wait_closed()

                resp = json.loads(resp_data.decode())
                if resp["status"] == "SUCCESS":
                    result = cloudpickle.loads(base64.b64decode(resp["result"]))
                    record["status"] = "COMPLETED"
                    record["result"] = str(result)
                    self.log(f"Task {task_id} succeeded on {target.short_id} -> {result}")
                    return result
                else:
                    raise RuntimeError(resp.get("error", "Execution failed"))

            except Exception as e:
                self.log(f"[FAILOVER] Node {target.short_id} failed ({type(e).__name__})! Re-routing...")
                record["status"] = "FAILED -> RETRY"
                failed_nodes.add(target.node_id)
                if target.node_id in self.dht.peers:
                    del self.dht.peers[target.node_id]

        record["status"] = "FAILED"
        raise RuntimeError(f"Task {task_id} failed after all failover attempts.")