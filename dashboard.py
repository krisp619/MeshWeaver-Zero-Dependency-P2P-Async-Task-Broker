import asyncio
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from node import MeshNode

def render_dashboard(node: MeshNode) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(name="logs", size=10),
    )
    layout["body"].split_row(
        Layout(name="peers", ratio=1),
        Layout(name="tasks", ratio=1),
    )

    # Header
    header_text = (
        f"[bold cyan]MeshWeaver P2P Async Task Broker[/bold cyan] | "
        f"Local Node: [bold green]{node.short_id}[/bold green] "
        f"(UDP:{node.udp_port} / TLS-TCP:{node.tcp_port}) | "
        f"CPU: [yellow]{node.cpu_load:.1f}%[/yellow] | RAM: [yellow]{node.ram_load:.1f}%[/yellow]"
    )
    layout["header"].update(Panel(header_text, border_style="cyan"))

    # Peers Table (Kademlia DHT)
    peer_table = Table(title="Kademlia DHT Mesh Peers (Gossip Telemetry)", expand=True)
    peer_table.add_column("Node ID", style="green")
    peer_table.add_column("Endpoint", style="white")
    peer_table.add_column("XOR Dist", style="dim")
    peer_table.add_column("CPU %", justify="right", style="yellow")
    peer_table.add_column("RAM %", justify="right", style="magenta")

    for p in sorted(node.dht.peers.values(), key=lambda x: x.cpu_load):
        dist = hex(node.dht.xor_distance(p.node_id))[:8]
        peer_table.add_row(
            p.short_id,
            f"{p.host}:{p.tcp_port}",
            dist,
            f"{p.cpu_load:.1f}%",
            f"{p.ram_load:.1f}%",
        )
    layout["peers"].update(Panel(peer_table, border_style="green"))

    # Task Execution Table
    task_table = Table(title="Distributed Task Queue & Failover State", expand=True)
    task_table.add_column("Task ID", style="cyan")
    task_table.add_column("Function", style="white")
    task_table.add_column("Assigned Peer", style="yellow")
    task_table.add_column("Status", style="bold")
    task_table.add_column("Result", style="green")

    for t in node.task_history[-10:]:
        task_table.add_row(t["task_id"], t["func"], t["target"], t["status"], t["result"])
    layout["tasks"].update(Panel(task_table, border_style="blue"))

    # Event Logs
    log_lines = "\n".join(node.logs[-7:]) or "Initializing P2P mesh..."
    layout["logs"].update(Panel(log_lines, title="Live Mesh & Failover Logs", border_style="magenta"))

    return layout

async def run_live_ui(node: MeshNode, duration: float = 40.0):
    console = Console()
    with Live(render_dashboard(node), console=console, refresh_per_second=4) as live:
        end_time = asyncio.get_event_loop().time() + duration
        while asyncio.get_event_loop().time() < end_time:
            live.update(render_dashboard(node))
            await asyncio.sleep(0.25)