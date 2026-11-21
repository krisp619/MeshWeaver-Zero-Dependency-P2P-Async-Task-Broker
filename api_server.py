import asyncio
import math
import random
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from node import MeshNode

# Global cluster state
cluster_nodes: list[MeshNode] = []
seed_node: MeshNode | None = None

def monte_carlo_pi_task(iterations: int):
    inside = 0
    for _ in range(iterations):
        x, y = random.random(), random.random()
        if x * x + y * y <= 1.0:
            inside += 1
    pi_est = (inside / iterations) * 4
    eigen_approx = sum(math.sin(i) * math.cos(i) for i in range(50000))
    return f"Pi≈{pi_est:.4f} | Eigen={eigen_approx:.2f}"

def matrix_stress_task(size: int):
    total = 0.0
    for i in range(size):
        total += math.sqrt(i * 3.14159)
    return f"StressSum={total:.2f}"

@asynccontextmanager
async def lifespan(app: FastAPI):
    global seed_node, cluster_nodes
    host = "127.0.0.1"

    # Spin up 10 decentralized nodes
    seed_node = MeshNode(host, udp_port=9000, tcp_port=10000, bootstrap=None)
    await seed_node.start()
    cluster_nodes.append(seed_node)

    for i in range(1, 10):
        n = MeshNode(host, udp_port=9000 + i, tcp_port=10000 + i, bootstrap=(host, 9000))
        await n.start()
        cluster_nodes.append(n)

    yield

    # Cleanup on shutdown
    for n in cluster_nodes:
        await n.stop()

app = FastAPI(title="MeshWeaver P2P API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class TaskRequest(BaseModel):
    task_type: str = "monte_carlo"
    iterations: int = 250000

@app.get("/")
async def serve_dashboard():
    return FileResponse("index.html")

@app.get("/api/mesh/status")
async def get_mesh_status():
    if not seed_node:
        return {"error": "Cluster initializing"}

    peers_data = []
    for p in sorted(seed_node.dht.peers.values(), key=lambda x: x.cpu_load):
        peers_data.append({
            "node_id": p.short_id,
            "full_id": str(p.node_id),
            "endpoint": f"{p.host}:{p.tcp_port}",
            "xor_distance": hex(seed_node.dht.xor_distance(p.node_id))[:10],
            "cpu_load": round(p.cpu_load, 1),
            "ram_load": round(p.ram_load, 1),
        })

    return {
        "seed_node": {
            "node_id": seed_node.short_id,
            "udp_port": seed_node.udp_port,
            "tcp_port": seed_node.tcp_port,
            "cpu_load": round(seed_node.cpu_load, 1),
            "ram_load": round(seed_node.ram_load, 1),
            "active_peers_count": len(peers_data),
        },
        "peers": peers_data,
        "tasks": list(reversed(seed_node.task_history[-15:])),
        "logs": list(reversed(seed_node.logs[-20:])),
    }

@app.post("/api/tasks/submit")
async def submit_task(req: TaskRequest):
    if not seed_node:
        return {"status": "error", "message": "Seed node not ready"}

    if req.task_type == "matrix_stress":
        asyncio.create_task(seed_node.submit_task(matrix_stress_task, req.iterations))
    else:
        asyncio.create_task(seed_node.submit_task(monte_carlo_pi_task, req.iterations))

    return {"status": "dispatched", "task_type": req.task_type}

@app.post("/api/chaos/kill-lowest")
async def kill_lowest_cpu_node():
    if not seed_node:
        return {"status": "error"}

    victim = seed_node.dht.get_lowest_cpu_peer()
    if not victim:
        return {"status": "no_peers_available"}

    seed_node.log(f"[CHAOS API] Killing lowest-CPU peer {victim.short_id}!")
    for n in cluster_nodes:
        if n.node_id == victim.node_id:
            await n.stop()
            break

    # Immediately dispatch a task to showcase instant failover & re-routing
    asyncio.create_task(seed_node.submit_task(monte_carlo_pi_task, 300000))
    return {"status": "killed_and_rerouted", "killed_node": victim.short_id}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="127.0.0.1", port=8000, reload=False)