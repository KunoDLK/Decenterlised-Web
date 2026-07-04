"""
tui.py — Terminal UI

Rich-based interactive terminal UI with live peer list, file search, tabs, keyboard shortcuts.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Optional, TYPE_CHECKING

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

if TYPE_CHECKING:
    from app import App

# ---------------------------------------------------------------------------
# TUI
# ---------------------------------------------------------------------------


class TUI:
    """Rich-based interactive terminal UI."""

    def __init__(self, app: "App") -> None:
        self.node = app
        self.console = Console()
        self.running = False
        self.search_query = ""
        self.active_tab = 1  # 1=Network Files, 2=My Files, 3=Storage, 4=Peers
        self.selected_index = 0
        self.expanded_file_id: Optional[str] = None

    def run(self) -> None:
        """Main TUI loop."""
        self.running = True

        layout = Layout()
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )
        layout["body"].split_row(
            Layout(name="main", ratio=3),
            Layout(name="sidebar", ratio=1),
        )

        with Live(layout, console=self.console, refresh_per_second=4, screen=True) as live:
            while self.running:
                layout["header"].update(self._render_header())
                layout["main"].update(self._render_main())
                layout["sidebar"].update(self._render_sidebar())
                layout["footer"].update(self._render_footer())
                live.update(layout)
                time.sleep(0.25)

    def stop(self) -> None:
        """Stop the TUI."""
        self.running = False

    # ------------------------------------------------------------------
    # Render helpers
    # ------------------------------------------------------------------

    def _render_header(self) -> Panel:
        text = Text()
        text.append("Decentralised File Storage Network", style="bold cyan")
        if self.node.author_identity:
            text.append(f"  |  @{self.node.author_identity.username}", style="green")
        text.append(f"  |  Node: {self.node.node_identity.node_id[:8]}...", style="dim")
        text.append(f"  |  Peers: {len(self.node.udp_engine.get_connected_peers())}", style="yellow")
        text.append(f"  |  Files: {self.node.file_registry.count()}", style="blue")
        return Panel(text, box=box.ROUNDED)

    def _render_main(self) -> Panel:
        if self.active_tab == 1:
            return self._render_file_table(all_files=True)
        elif self.active_tab == 2:
            return self._render_file_table(all_files=False)
        elif self.active_tab == 3:
            return self._render_storage_tab()
        else:
            return self._render_peer_book_tab()

    def _render_sidebar(self) -> Panel:
        table = Table(title="Connected Peers", box=box.SIMPLE, expand=True)
        table.add_column("Node", style="cyan", width=10)
        table.add_column("Status", width=8)

        peers = self.node.udp_engine.get_connected_peers()
        for nid in peers[:20]:
            conn = self.node.udp_engine.connections.get(nid)
            status = "🟢" if conn and conn.is_connected else "🟡"
            table.add_row(nid[:8] + "...", status)

        return Panel(table, title="Peers", border_style="blue")

    def _render_footer(self) -> Panel:
        text = Text()
        text.append("[1]Browse ", style="bold")
        text.append("[2]My Files ", style="bold")
        text.append("[3]Storage ", style="bold")
        text.append("[4]Peers ", style="bold")
        text.append("| [p]Publish [c]Connect [l]Login [d]Download [q]Quit", style="dim")

        storage = self.node.storage.get_storage_breakdown()
        used_mb = (storage["own"] + storage["replicas"]) / (1024 * 1024)
        total_mb = storage["total"] / (1024 * 1024)
        text.append(f" | Storage: {used_mb:.0f}/{total_mb:.0f}MB", style="magenta")

        return Panel(text, box=box.MINIMAL)

    def _render_file_table(self, all_files: bool = True) -> Panel:
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("#", style="dim", width=4)
        table.add_column("Name", style="cyan")
        table.add_column("Size", style="green", width=10)
        table.add_column("Reps", style="yellow", width=6)
        table.add_column("Author", style="dim", width=10)

        entries = self.node.file_registry.get_all()
        if not all_files and self.node.author_identity:
            entries = [
                e
                for e in entries
                if e.author_id == self.node.author_identity.author_id
            ]

        # Filter by search
        if self.search_query:
            q = self.search_query.lower()
            entries = [
                e
                for e in entries
                if q in e.file_name.lower() or q in e.file_id[:16]
            ]

        for i, entry in enumerate(entries[:50]):
            style = "reverse" if i == self.selected_index else ""
            size_str = f"{entry.file_size / 1024:.0f}KB"
            table.add_row(
                str(i + 1),
                entry.file_name[:30],
                size_str,
                str(entry.replica_count),
                entry.author_id[:8],
                style=style,
            )

        if self.search_query:
            title = f"Files (search: '{self.search_query}')"
        else:
            title = "Network Files" if all_files else "My Files"
        return Panel(table, title=title, border_style="green")

    def _render_storage_tab(self) -> Panel:
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("File ID", style="cyan", width=14)
        table.add_column("Type", style="yellow", width=10)
        table.add_column("Size", style="green", width=10)
        table.add_column("Status", width=10)

        meta = self.node.storage._load_metadata()
        for fid in meta.get("own_files", []):
            size = 0
            try:
                size = self.node.storage.read_file(fid)
                size = len(size)
            except Exception:
                pass
            table.add_row(fid[:12], "Own", f"{size/1024:.0f}KB", "✓")

        for fid in meta.get("replica_files", []):
            size = 0
            try:
                size = self.node.storage.read_file(fid)
                size = len(size)
            except Exception:
                pass
            table.add_row(fid[:12], "Replica", f"{size/1024:.0f}KB", "✓")

        for fid, info in meta.get("temporary_files", {}).items():
            size = 0
            try:
                size = self.node.storage.read_file(fid)
                size = len(size)
            except Exception:
                pass
            table.add_row(fid[:12], "Temp", f"{size/1024:.0f}KB", "⏳")

        return Panel(table, title="Local Storage", border_style="magenta")

    def _render_peer_book_tab(self) -> Panel:
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Node ID", style="cyan", width=14)
        table.add_column("Tier", style="yellow", width=6)
        table.add_column("Address", style="green")
        table.add_column("Last Seen", style="dim")

        peers = self.node.peer_book.get_all_ordered()
        for p in peers[:50]:
            table.add_row(
                p["node_id"][:12],
                str(p["tier"]),
                f"{p['public_ip']}:{p['public_port']}",
                time.strftime("%H:%M", time.localtime(p["last_seen"])),
            )

        return Panel(table, title="Peer Book", border_style="blue")

    # ------------------------------------------------------------------
    # Input
    # ------------------------------------------------------------------

    def handle_key(self, key: str) -> None:
        """Dispatch keyboard input."""
        if key == "q":
            self.stop()
            self.node.stop()
        elif key == "1":
            self.active_tab = 1
        elif key == "2":
            self.active_tab = 2
        elif key == "3":
            self.active_tab = 3
        elif key == "4":
            self.active_tab = 4
        elif key == "p":
            self._do_publish()
        elif key == "c":
            self._do_connect()
        elif key == "l":
            self._do_login()
        elif key == "d":
            self._do_download_selected()
        elif key == "escape":
            self.search_query = ""
        elif len(key) == 1 and key.isprintable():
            self.search_query += key
        elif key == "backspace" and self.search_query:
            self.search_query = self.search_query[:-1]
        elif key == "up":
            self.selected_index = max(0, self.selected_index - 1)
        elif key == "down":
            self.selected_index += 1

    def _do_publish(self) -> None:
        self.console.print("[yellow]Publish: Enter file path:[/] ", end="")
        # In practice, would need async input handling
        pass

    def _do_connect(self) -> None:
        self.console.print("[yellow]Connect: Enter peer URL:[/] ", end="")
        pass

    def _do_login(self) -> None:
        self.console.print("[yellow]Login: Enter username:[/] ", end="")
        pass

    def _do_download_selected(self) -> None:
        entries = self.node.file_registry.get_all()
        if self.selected_index < len(entries):
            entry = entries[self.selected_index]
            self.console.print(
                f"[green]Downloading {entry.file_name}...[/]"
            )
            try:
                self.node.download_file(entry.file_id)
            except Exception as e:
                self.console.print(f"[red]Error: {e}[/]")
