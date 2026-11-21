import asyncio
import math
import random
from node import MeshNode
from dashboard import run_live_ui

# Complex Compute Function (Serialized via cloudpickle)
def monte_carlo_pi_and_matrix(iterations: int):
    inside = 0
    for _ in range(iterations):
        x, y = random.random(), random.random()
        if x * x + y * y <= 1.0:
            inside += 1
    pi_est = (inside / iterations) * 4
    # Extra CPU math load
    eigen_approx = sum(math.sin(i) * math.cos(i) for i in range(50000))
    return f"Pi≈{pi_est:.4f} | Check={eigen_approx:.2f}"

async def main():
    host = "127.0.0.1"
    nodes = []

    # 1. Start 10 P2P Nodes (Node 0 acts as initial bootstrap seed)
    seed_node = MeshNode(host, udp_port=9000, tcp_port=10000, bootstrap=None)
    await seed_node.start()
    nodes.append(seed_node)

    for i in range(1, 10):
        # Notice: Only pointing to seed (9000); Kademlia gossip discovers all 10 peers dynamically!
        n = MeshNode(host, udp_port=9000 + i, tcp_port=10000 + i, bootstrap=(host, 9000))
        await n.start()
        nodes.append(n)

    # Start Live Rich CLI Dashboard on Node 0
    ui_task = asyncio.create_task(run_live_ui(seed_node, duration=35.0))

    # Wait 5 seconds for Kademlia DHT convergence across all 10 nodes
    await asyncio.sleep(5.0)
    seed_node.log(f"Audit Complete: {len(seed_node.dht.peers)}/9 peers discovered dynamically!")

    # 2. Submit 4 Complex Serialized Tasks (Routed to Lowest-CPU Peers)
    for idx in range(4):
        asyncio.create_task(seed_node.submit_task(monte_carlo_pi_and_matrix, 200000))
        await asyncio.sleep(1.5)

    await asyncio.sleep(4.0)

    # 3. Simulate Sudden Node Failure (Kill the lowest-CPU peer right before submitting a task!)
    victim = seed_node.dht.get_lowest_cpu_peer()
    if victim:
        seed_node.log(f"[CHAOS TEST] Killing lowest-CPU node {victim.short_id} mid-flight!")
        for n in nodes:
            if n.node_id == victim.node_id:
                await n.stop()
                break

    # Submit task immediately while victim is still in DHT -> triggers instant failover & re-routing!
    await asyncio.sleep(1.0)
    await seed_node.submit_task(monte_carlo_pi_and_matrix, 300000)

    await ui_task

    # Cleanup
    for n in nodes:
        await n.stop()

if __name__ == "__main__":
    asyncio.run(main())