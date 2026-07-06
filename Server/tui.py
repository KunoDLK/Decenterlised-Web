"""
tui.py — Terminal UI

Event-driven Rich-based interactive terminal UI with live peer list,
file search, tabs, and keyboard shortcuts.

Uses cbreak mode (not raw) to preserve terminal output processing
while still getting instant key-by-key input.
"""

from __future__ import annotations

import os
import select
import subprocess
import sys
import termios
import threading
import time
import tty
from pathlib import Path
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


class TUI:
    """Event-driven Rich-based interactive terminal UI.

    Keyboard input is handled via a background thread using cbreak mode
    (character-at-a-time without disabling output processing, so no
    line-wrapping artefacts).  The render loop is driven by an
    event-style poll — it redraws on every keypress and also on a
    background timer, whichever comes first.
    """

    def __init__(self, app: "App") -> None:
        self.node = app
        self.console = Console()
        self.running = False
        self.search_query = ""
        self.active_tab = 1  # 1=Network Files, 2=My Files, 3=Storage, 4=Peers
        self.selected_index = 0
        self.expanded_file_id: Optional[str] = None

        # ---- Input mode ----
        self.input_mode: Optional[str] = None
        self.input_buffer: str = ""
        self.input_prompt: str = ""
        self._login_username: str = ""

        # ---- Status message (footer, auto-clears) ----
        self._status_message: str = ""
        self._status_time: float = 0.0

        # ---- Download progress ----
        self.download_progress: dict[str, dict[str, Any]] = {}

        # ---- Keyboard ----
        self._key_thread: Optional[threading.Thread] = None
        self._stdin_fd = sys.stdin.fileno()
        self._old_tc: Optional[list] = None
        self._key_event = threading.Event()  # set on each keypress for instant redraw

    # ==================================================================
    # Lifecycle
    # ==================================================================

    def run(self) -> None:
        self.running = True
        self._start_keyboard()

        layout = Layout()
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=5),
        )
        layout["body"].split_row(
            Layout(name="main", ratio=3),
            Layout(name="sidebar", ratio=1),
        )

        with Live(
            layout,
            console=self.console,
            refresh_per_second=10,
            screen=False,
            transient=True,
        ) as live:
            while self.running:
                layout["header"].update(self._render_header())
                layout["main"].update(self._render_main())
                layout["sidebar"].update(self._render_sidebar())
                layout["footer"].update(self._render_footer())

                # Wait for either a keypress or a 0.2-second timeout,
                # whichever comes first — this makes the UI event-driven.
                self._key_event.wait(timeout=0.2)
                self._key_event.clear()

    def stop(self) -> None:
        self.running = False
        self._key_event.set()  # unblock the wait
        self._stop_keyboard()
        # Give Rich Live display time to release stdout before interpreter exit
        time.sleep(0.05)

    # ==================================================================
    # Keyboard — cbreak mode (char-by-char, output processing ON)
    # ==================================================================

    def _start_keyboard(self) -> None:
        try:
            self._old_tc = termios.tcgetattr(self._stdin_fd)
            tty.setcbreak(self._stdin_fd)
        except (termios.error, OSError):
            self._old_tc = None

        self._key_thread = threading.Thread(target=self._input_loop, daemon=True)
        self._key_thread.start()

    def _stop_keyboard(self) -> None:
        if self._old_tc is not None:
            try:
                termios.tcsetattr(self._stdin_fd, termios.TCSADRAIN, self._old_tc)
            except Exception:
                pass
            self._old_tc = None

    def _input_loop(self) -> None:
        """Blocking-select loop — fires _key_event on every keypress."""
        while self.running:
            try:
                ready, _, _ = select.select([sys.stdin], [], [], 0.1)
                if not ready:
                    continue
                ch = os.read(self._stdin_fd, 1).decode("utf-8", errors="replace")
                if not ch:
                    continue

                # Arrow keys / escape sequences
                if ch == "\x1b":
                    ready2, _, _ = select.select([sys.stdin], [], [], 0.01)
                    if ready2:
                        seq = os.read(self._stdin_fd, 2).decode("utf-8", errors="replace")
                        if seq == "[A":
                            self._dispatch("up")
                        elif seq == "[B":
                            self._dispatch("down")
                        else:
                            self._dispatch("escape")
                    else:
                        self._dispatch("escape")
                elif ch in ("\x7f", "\x08"):
                    self._dispatch("backspace")
                elif ch in ("\r", "\n"):
                    self._dispatch("enter")
                elif ch == "\t":
                    continue
                else:
                    self._dispatch(ch)
            except Exception:
                pass

    # ==================================================================
    # Dispatch
    # ==================================================================

    def _dispatch(self, key: str) -> None:
        self._key_event.set()  # trigger instant redraw
        if self.input_mode:
            self._handle_input_key(key)
        else:
            self._handle_key(key)

    # ==================================================================
    # Input-mode keys
    # ==================================================================

    def _handle_input_key(self, key: str) -> None:
        if key == "escape":
            self.input_mode = None
            self.input_buffer = ""
            self.input_prompt = ""
            self._login_username = ""
        elif key == "enter":
            self._submit_input()
        elif key == "backspace":
            self.input_buffer = self.input_buffer[:-1]
        elif len(key) == 1 and key.isprintable():
            self.input_buffer += key

    def _submit_input(self) -> None:
        value = self.input_buffer.strip()
        self.input_buffer = ""

        if self.input_mode == "publish":
            self.input_mode = None
            self.input_prompt = ""
            if value and os.path.isfile(value):
                try:
                    with open(value, "rb") as f:
                        data = f.read()
                    self.node.publish_file(data, os.path.basename(value), "application/octet-stream")
                    self._set_status(f"Published: {os.path.basename(value)}")
                except Exception as e:
                    self._set_status(f"Publish error: {e}")
            elif value:
                self._set_status(f"File not found: {value}")

        elif self.input_mode == "connect":
            self.input_mode = None
            self.input_prompt = ""
            if value:
                try:
                    if any(value.startswith(p) for p in ("http://", "https://", "decentralised://")):
                        result = self.node.connect_via_url(value)
                    elif ":" in value:
                        result = self.node.connect_via_url(f"decentralised://?addr={value}")
                    else:
                        result = False
                    self._set_status("Connected!" if result else "Connection in progress...")
                except Exception as e:
                    self._set_status(f"Connect error: {e}")

        elif self.input_mode == "login_user":
            self._login_username = value
            self.input_mode = "login_pass"
            self.input_prompt = "Password: "

        elif self.input_mode == "login_pass":
            self.input_mode = None
            self.input_prompt = ""
            if self._login_username and value:
                try:
                    self.node.login(self._login_username, value)
                    self._set_status(f"Logged in as {self._login_username}")
                except Exception as e:
                    self._set_status(f"Login error: {e}")
            self._login_username = ""

    # ==================================================================
    # Normal-mode keys
    # ==================================================================

    def _handle_key(self, key: str) -> None:
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
            self.input_mode = "publish"
            self.input_prompt = "Publish file path: "
            self.input_buffer = ""
        elif key == "c":
            self.input_mode = "connect"
            self.input_prompt = "Connect (URL or ip:port): "
            self.input_buffer = ""
        elif key == "l":
            self.input_mode = "login_user"
            self.input_prompt = "Username: "
            self.input_buffer = ""
            self._login_username = ""
        elif key == "d":
            self._download_selected()
        elif key == "u":
            self._show_my_url()
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

    def _download_selected(self) -> None:
        entries = self.node.file_registry.get_all()
        if self.selected_index < len(entries):
            entry = entries[self.selected_index]
            file_id = entry.file_id
            file_name = entry.file_name
            self._set_status(f"Download queued: {file_name}")
            threading.Thread(
                target=self._do_download,
                args=(file_id, file_name),
                daemon=True,
            ).start()

    def _do_download(self, file_id: str, file_name: str) -> None:
        """Background download, safe to block."""
        try:
            self.node.download_file(file_id)
            self._set_status(f"Downloaded: {file_name}")
        except Exception as e:
            self._set_status(f"Download error: {e}")

    def _show_my_url(self) -> None:
        """Copy this node's connection URL to the clipboard."""
        from identity import public_key_to_base64
        nid = self.node.node_identity.node_id
        pk = public_key_to_base64(self.node.node_identity.public_key_bytes)
        ip = self.node.udp_engine.public_ip
        port = self.node.udp_engine.public_port
        url = f"decentralised://?join={nid}&pk={pk}&addr={ip}:{port}"

        # Try clipboard tools
        for cmd in (["pbcopy"], ["xclip", "-selection", "clipboard"], ["wl-copy"]):
            try:
                subprocess.run(cmd, input=url.encode(), check=True, timeout=2)
                self._set_status("URL copied to clipboard!")
                return
            except Exception:
                continue

        self._set_status(f"Your URL: {url}")

    # ==================================================================
    # Status & progress
    # ==================================================================

    def _set_status(self, msg: str) -> None:
        self._status_message = msg
        self._status_time = time.time()
        self._key_event.set()  # redraw immediately

    def set_download_progress(self, file_id: str, current: int, total: int, name: str = "") -> None:
        self.download_progress[file_id] = {"current": current, "total": total, "name": name}
        self._key_event.set()
        if current >= total:
            def _clear():
                time.sleep(1)
                self.download_progress.pop(file_id, None)
                self._key_event.set()
            threading.Thread(target=_clear, daemon=True).start()

    # ==================================================================
    # Rendering
    # ==================================================================

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
        table = Table(title="Connected Peers", box=box.SIMPLE, padding=(0, 2))
        table.add_column("Node", style="cyan", width=10)
        table.add_column("Status", width=8)
        peers = self.node.udp_engine.get_connected_peers()
        if not peers:
            table.add_row("(none)", "")
        for nid in peers[:20]:
            cs = self.node.peer_book.get_connection_state(nid)
            table.add_row(nid[:8] + "...", "🟢" if (cs and cs["state"] == "CONNECTED") else "🟡")
        return Panel(table, title="Peers", border_style="blue")

    def _render_footer(self) -> Panel:
        text = Text(no_wrap=True)

        # Status
        if self._status_message:
            if time.time() - self._status_time > 3.0:
                self._status_message = ""
            else:
                text.append(self._status_message + "  ", style="bold green")

        # Input mode
        if self.input_mode:
            buf = self.input_buffer
            if self.input_mode == "login_pass":
                buf = "*" * len(buf)
            text.append(f"{self.input_prompt}{buf}▌", style="bold yellow")
            text.append("  [Esc] cancel  [Enter] confirm", style="dim")
            return Panel(text, box=box.MINIMAL)

        # Download progress
        if self.download_progress:
            for fid, prog in list(self.download_progress.items())[:2]:
                name = prog.get("name", fid[:8])
                pct = int(prog["current"] / max(1, prog["total"]) * 100)
                done = int(20 * pct / 100)
                bar = "█" * done + "░" * (20 - done)
                text.append(f"⬇ {name} [{bar}] {pct}%  ", style="cyan")

        text.append("[1]Browse ", style="bold")
        text.append("[2]My Files ", style="bold")
        text.append("[3]Storage ", style="bold")
        text.append("[4]Peers ", style="bold")
        text.append("| [p]Publish [c]Connect [l]Login [d]Download [u]URL [q]Quit", style="dim")

        s = self.node.storage.get_storage_breakdown()
        text.append(f" | Storage: {(s['own']+s['replicas'])/1048576:.0f}/{s['total']/1048576:.0f}MB", style="magenta")

        return Panel(text, box=box.MINIMAL)

    def _render_file_table(self, all_files: bool = True) -> Panel:
        table = Table(box=box.SIMPLE, padding=(0, 1))
        table.add_column("#", style="dim", width=4)
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Size", style="green", width=10)
        table.add_column("Reps", style="yellow", width=6)
        table.add_column("Author", style="dim", width=10)

        entries = self.node.file_registry.get_all()
        if not all_files and self.node.author_identity:
            entries = [e for e in entries if e.author_id == self.node.author_identity.author_id]

        if self.search_query:
            q = self.search_query.lower()
            entries = [e for e in entries if q in e.file_name.lower() or q in e.file_id[:16]]

        if not entries:
            msg = "No matching files" if self.search_query else "No files yet — publish one with [p]"
            table.add_row("", msg, "", "", "")

        for i, entry in enumerate(entries[:50]):
            style = "reverse" if i == self.selected_index else ""
            table.add_row(
                str(i + 1),
                entry.file_name[:40],
                f"{entry.file_size / 1024:.0f}KB",
                str(entry.replica_count),
                entry.author_id[:8],
                style=style,
            )

        title = f"Files (search: '{self.search_query}')" if self.search_query else ("Network Files" if all_files else "My Files")
        return Panel(table, title=title, border_style="green")

    def _render_storage_tab(self) -> Panel:
        table = Table(box=box.SIMPLE, padding=(0, 1))
        table.add_column("File ID", style="cyan", width=14)
        table.add_column("Type", style="yellow", width=10)
        table.add_column("Size", style="green", width=10)
        table.add_column("Status", width=10)

        meta = self.node.storage._load_metadata()
        all_ids = meta.get("own_files", []) + meta.get("replica_files", []) + list(meta.get("temporary_files", {}).keys())
        if not all_ids:
            table.add_row("", "No local files", "", "")

        for fid in meta.get("own_files", []):
            sz = 0
            try: sz = (Path(self.node.storage.files_dir) / fid).stat().st_size
            except Exception: pass
            table.add_row(fid[:12], "Own", f"{sz/1024:.0f}KB", "✓")

        for fid in meta.get("replica_files", []):
            sz = 0
            try: sz = (Path(self.node.storage.files_dir) / fid).stat().st_size
            except Exception: pass
            table.add_row(fid[:12], "Replica", f"{sz/1024:.0f}KB", "✓")

        for fid in meta.get("temporary_files", {}):
            sz = 0
            try: sz = (Path(self.node.storage.files_dir) / fid).stat().st_size
            except Exception: pass
            table.add_row(fid[:12], "Temp", f"{sz/1024:.0f}KB", "⏳")

        return Panel(table, title="Local Storage", border_style="magenta")

    def _render_peer_book_tab(self) -> Panel:
        table = Table(box=box.SIMPLE, padding=(0, 1))
        table.add_column("Node ID", style="cyan", width=14)
        table.add_column("Tier", style="yellow", width=6)
        table.add_column("Address", style="green")
        table.add_column("Last Seen", style="dim")

        peers = self.node.peer_book.get_all_ordered()
        if not peers:
            table.add_row("", "", "(no peers)", "")
        for p in peers[:50]:
            table.add_row(
                p["node_id"][:12],
                str(p["tier"]),
                f"{p['public_ip']}:{p['public_port']}",
                time.strftime("%H:%M", time.localtime(p["last_seen"])),
            )

        return Panel(table, title="Peer Book", border_style="blue")

    # ---- Backwards-compatible aliases used by tests ----
    handle_key = _handle_key          # type: ignore[assignment]
    _dispatch_key = _dispatch         # type: ignore[assignment]
    _do_download_selected = _download_selected  # type: ignore[assignment]
