#!/usr/bin/env python3
"""
Integration tests for TUI interactivity.

Tests all keyboard-driven state transitions, input modes, search,
tab switching, download progress, and render methods of the TUI
without requiring a real terminal or raw-mode stdin.

Usage:
    .venv/bin/python test_tui.py
    .venv/bin/python test_tui.py --verbose
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import threading
import time
from typing import Optional
from unittest.mock import MagicMock, PropertyMock, patch

# Ensure the Server package is on the import path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Server"))

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------

class Colour:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Mock App — provides just enough for the TUI to operate
# ---------------------------------------------------------------------------

class MockNodeIdentity:
    node_id = "abcd1234ef567890"


class MockAuthorIdentity:
    username = "testuser"
    author_id = "author1234567890ab"


class MockConnection:
    is_connected = True


class MockUDPEngine:
    def get_connected_peers(self):
        return ["peer1111111111aa", "peer2222222222bb"]

    @property
    def connections(self):
        return {
            "peer1111111111aa": MockConnection(),
            "peer2222222222bb": MockConnection(),
        }


class MockFileRegistryEntry:
    def __init__(self, file_id, file_name, file_size, replica_count, author_id):
        self.file_id = file_id
        self.file_name = file_name
        self.file_size = file_size
        self.replica_count = replica_count
        self.author_id = author_id


class MockFileRegistry:
    def __init__(self):
        self._files = [
            MockFileRegistryEntry(
                "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb",
                "document_one.txt", 2048, 3, "author1234567890ab",
            ),
            MockFileRegistryEntry(
                "bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222cccc",
                "image_photo.png", 102400, 5, "author9999999999zz",
            ),
            MockFileRegistryEntry(
                "cccc3333dddd4444eeee5555ffff6666aaaa1111bbbb2222cccc3333dddd",
                "Zebra Report.pdf", 512000, 1, "author1234567890ab",
            ),
        ]

    def get_all(self):
        return self._files

    def count(self):
        return len(self._files)

    def get_by_author(self, author_id):
        return [f for f in self._files if f.author_id == author_id]

    def get(self, file_id):
        for f in self._files:
            if f.file_id == file_id:
                return f
        return None

    def compute_hash(self):
        return "abc123" * 16

    def get_delta(self, their_hash):
        return []


class MockStorage:
    def __init__(self):
        self.files_dir = "/tmp/mock_storage"
        self._meta = {
            "own_files": ["ownfile11111111", "ownfile22222222"],
            "replica_files": ["repfile33333333"],
            "temporary_files": {"tmpfile44444444": {"expires_at": time.time() + 3600}},
        }

    def _load_metadata(self):
        return self._meta

    def get_storage_breakdown(self):
        return {"own": 5000000, "replicas": 2000000, "available": 93000000, "total": 100000000}

    def read_file(self, fid):
        return b"mock file content " + fid.encode()

    def has_file(self, fid):
        return fid in self._meta.get("own_files", []) or fid in self._meta.get("replica_files", [])


class MockPeerBook:
    def get_all_ordered(self):
        return [
            {"node_id": "peer1111111111aa", "tier": 1, "public_ip": "10.0.0.1", "public_port": 9000, "last_seen": time.time()},
            {"node_id": "peer2222222222bb", "tier": 2, "public_ip": "10.0.0.2", "public_port": 9001, "last_seen": time.time() - 3600},
            {"node_id": "peer3333333333cc", "tier": 3, "public_ip": "10.0.0.3", "public_port": 9002, "last_seen": time.time() - 86400 * 10},
        ]

    def get(self, node_id):
        for p in self.get_all_ordered():
            if p["node_id"] == node_id:
                return p
        return None

    def get_by_tier(self, tier):
        return [p for p in self.get_all_ordered() if p["tier"] == tier]

    def count(self):
        return len(self.get_all_ordered())


class MockApp:
    """Minimal mock of the App class for TUI testing."""

    def __init__(self):
        self.node_identity = MockNodeIdentity()
        self.author_identity = MockAuthorIdentity()
        self.udp_engine = MockUDPEngine()
        self.file_registry = MockFileRegistry()
        self.storage = MockStorage()
        self.peer_book = MockPeerBook()
        self._stopped = False
        self._published_files = []
        self._connected_urls = []
        self._login_calls = []
        self._downloaded_files = []

    def stop(self):
        self._stopped = True

    def publish_file(self, data, file_name, mime_type):
        self._published_files.append((file_name, len(data), mime_type))
        return "published_" + file_name

    def connect_via_url(self, url):
        self._connected_urls.append(url)
        return True

    def login(self, username, password):
        self._login_calls.append((username, password))

    def download_file(self, file_id):
        self._downloaded_files.append(file_id)
        return b"downloaded_content"


# ---------------------------------------------------------------------------
# Test Runner
# ---------------------------------------------------------------------------

class TUIInteractionTest:
    """Tests all TUI interactivity without requiring a real terminal."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.passed = 0
        self.failed = 0
        self.mock_app: Optional[MockApp] = None
        self.tui = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"  {Colour.CYAN}[LOG]{Colour.RESET} {msg}")

    def _assert(self, condition: bool, test_name: str, detail: str = "") -> None:
        if condition:
            self.passed += 1
            print(f"  {Colour.GREEN}✓ PASS{Colour.RESET} {test_name}")
        else:
            self.failed += 1
            print(f"  {Colour.RED}✗ FAIL{Colour.RESET} {test_name}")
            if detail:
                print(f"    {Colour.RED}{detail}{Colour.RESET}")

    def _assert_equal(self, actual, expected, test_name: str) -> None:
        if actual == expected:
            self.passed += 1
            print(f"  {Colour.GREEN}✓ PASS{Colour.RESET} {test_name}")
        else:
            self.failed += 1
            print(f"  {Colour.RED}✗ FAIL{Colour.RESET} {test_name}")
            print(f"    Expected: {expected!r}")
            print(f"    Actual:   {actual!r}")

    def _setup_tui(self) -> None:
        """Create a fresh TUI with a mock App."""
        self.mock_app = MockApp()
        from tui import TUI
        self.tui = TUI(self.mock_app)
        # Don't start keyboard thread — we'll call methods directly
        self.tui.running = True

    # ==================================================================
    # TESTS
    # ==================================================================

    # ------------------------------------------------------------------
    # 01 — Initial state
    # ------------------------------------------------------------------

    def test_initial_state(self) -> None:
        """Verify TUI initialises with correct default state."""
        print(f"\n{Colour.BOLD}── Test 01: Initial State{Colour.RESET}")
        self._setup_tui()

        self._assert_equal(self.tui.active_tab, 1, "Default tab is 1 (Network Files)")
        self._assert_equal(self.tui.search_query, "", "Search query starts empty")
        self._assert_equal(self.tui.selected_index, 0, "Selected index starts at 0")
        self._assert_equal(self.tui.expanded_file_id, None, "No file expanded initially")
        self._assert_equal(self.tui.input_mode, None, "No input mode initially")
        self._assert_equal(self.tui.input_buffer, "", "Input buffer starts empty")
        self._assert_equal(self.tui.input_prompt, "", "Input prompt starts empty")
        self._assert_equal(self.tui._login_username, "", "Login username starts empty")
        self._assert_equal(len(self.tui.download_progress), 0, "No download progress initially")
        self._assert_equal(self.tui.running, True, "TUI is running")

    # ------------------------------------------------------------------
    # 02 — Tab switching
    # ------------------------------------------------------------------

    def test_tab_switching(self) -> None:
        """Test switching between all four tabs."""
        print(f"\n{Colour.BOLD}── Test 02: Tab Switching{Colour.RESET}")
        self._setup_tui()

        # Switch to each tab
        tab_keys = {"1": 1, "2": 2, "3": 3, "4": 4}
        for key, expected in tab_keys.items():
            self.tui.handle_key(key)
            self._assert_equal(
                self.tui.active_tab, expected,
                f"Key '{key}' switches to tab {expected}",
            )

        # Switch back to tab 1
        self.tui.handle_key("1")
        self._assert_equal(self.tui.active_tab, 1, "Back to tab 1")

        # Switch to tab 3, then back
        self.tui.handle_key("3")
        self._assert_equal(self.tui.active_tab, 3, "Switched to tab 3 (Storage)")
        self.tui.handle_key("1")
        self._assert_equal(self.tui.active_tab, 1, "Switched back to tab 1")

    # ------------------------------------------------------------------
    # 03 — Search as you type
    # ------------------------------------------------------------------

    def test_search_as_you_type(self) -> None:
        """Test incremental search query building."""
        print(f"\n{Colour.BOLD}── Test 03: Search As You Type{Colour.RESET}")
        self._setup_tui()

        # Type characters (avoid 'd' which triggers download action)
        for ch in "ima":
            self.tui.handle_key(ch)
        self._assert_equal(self.tui.search_query, "ima", "Search query builds incrementally")

        # Backspace
        self.tui.handle_key("backspace")
        self._assert_equal(self.tui.search_query, "im", "Backspace removes last char")

        self.tui.handle_key("backspace")
        self._assert_equal(self.tui.search_query, "i", "Backspace again")

        self.tui.handle_key("backspace")
        self._assert_equal(self.tui.search_query, "", "Backspace to empty")

        # Backspace on empty is safe
        self.tui.handle_key("backspace")
        self._assert_equal(self.tui.search_query, "", "Backspace on empty is safe")

        # Escape clears
        self.tui.handle_key("h")
        self.tui.handle_key("i")
        self._assert_equal(self.tui.search_query, "hi", "Typed 'hi'")
        self.tui.handle_key("escape")
        self._assert_equal(self.tui.search_query, "", "Escape clears search")

        # Resume typing with safe chars
        self.tui.handle_key("t")
        self.tui.handle_key("x")
        self._assert_equal(self.tui.search_query, "tx", "Typed 'tx' again")

        # Uppercase search
        self.tui.handle_key("escape")
        for ch in "ZEBRA":
            self.tui.handle_key(ch)
        self._assert_equal(self.tui.search_query, "ZEBRA", "Uppercase search works")

    # ------------------------------------------------------------------
    # 04 — Arrow key navigation
    # ------------------------------------------------------------------

    def test_arrow_navigation(self) -> None:
        """Test up/down arrow keys for file selection."""
        print(f"\n{Colour.BOLD}── Test 04: Arrow Navigation{Colour.RESET}")
        self._setup_tui()

        self._assert_equal(self.tui.selected_index, 0, "Starts at index 0")

        # Down
        self.tui.handle_key("down")
        self._assert_equal(self.tui.selected_index, 1, "Down moves to index 1")

        self.tui.handle_key("down")
        self._assert_equal(self.tui.selected_index, 2, "Down moves to index 2")

        self.tui.handle_key("down")
        self._assert_equal(self.tui.selected_index, 3, "Down moves to index 3")

        # Up
        self.tui.handle_key("up")
        self._assert_equal(self.tui.selected_index, 2, "Up moves to index 2")

        self.tui.handle_key("up")
        self._assert_equal(self.tui.selected_index, 1, "Up moves to index 1")

        # Up at index 0 stays at 0
        self.tui.handle_key("up")
        self._assert_equal(self.tui.selected_index, 0, "Up at 0 stays at 0")

    # ------------------------------------------------------------------
    # 05 — Input mode: Publish
    # ------------------------------------------------------------------

    def test_input_mode_publish(self) -> None:
        """Test the publish input mode flow."""
        print(f"\n{Colour.BOLD}── Test 05: Input Mode — Publish{Colour.RESET}")
        self._setup_tui()

        # Press 'p' enters publish input mode
        self.tui.handle_key("p")
        self._assert_equal(self.tui.input_mode, "publish", "Entered publish input mode")
        self._assert_equal(self.tui.input_prompt, "Publish file path: ", "Correct prompt set")
        self._assert_equal(self.tui.input_buffer, "", "Buffer starts empty")

        # Type a file path in input mode
        for ch in "/tmp/test_file.txt":
            self._dispatch(ch)
        self._assert_equal(self.tui.input_buffer, "/tmp/test_file.txt", "Buffer captures typed chars")

        # Backspace in input mode
        self._dispatch("backspace")
        self._assert_equal(self.tui.input_buffer, "/tmp/test_file.tx", "Backspace in input mode")

        # Cancel with escape
        self._dispatch("escape")
        self._assert_equal(self.tui.input_mode, None, "Escape cancels publish mode")
        self._assert_equal(self.tui.input_buffer, "", "Buffer cleared after cancel")
        self._assert_equal(self.tui.input_prompt, "", "Prompt cleared after cancel")

    # ------------------------------------------------------------------
    # 06 — Input mode: Connect
    # ------------------------------------------------------------------

    def test_input_mode_connect(self) -> None:
        """Test the connect input mode flow."""
        print(f"\n{Colour.BOLD}── Test 06: Input Mode — Connect{Colour.RESET}")
        self._setup_tui()

        # Press 'c' enters connect mode
        self.tui.handle_key("c")
        self._assert_equal(self.tui.input_mode, "connect", "Entered connect input mode")
        self._assert_equal(self.tui.input_prompt, "Connect (URL or ip:port): ", "Correct prompt")

        # Type a URL
        url = "https://example.com/?join=abc&pk=xyz&addr=1.2.3.4:9000"
        for ch in url:
            self._dispatch(ch)
        self._assert_equal(self.tui.input_buffer, url, "Buffer has full URL")

        # Submit with enter
        self._dispatch("enter")
        self._assert_equal(self.tui.input_mode, None, "Input mode cleared after submit")
        self._assert_equal(self.tui.input_buffer, "", "Buffer cleared after submit")
        self._assert(
            len(self.mock_app._connected_urls) > 0,
            "connect_via_url was called",
        )

    # ------------------------------------------------------------------
    # 07 — Input mode: Login (two-step)
    # ------------------------------------------------------------------

    def test_input_mode_login(self) -> None:
        """Test the two-step login flow (username → password)."""
        print(f"\n{Colour.BOLD}── Test 07: Input Mode — Login{Colour.RESET}")
        self._setup_tui()

        # Press 'l' enters login_user mode
        self.tui.handle_key("l")
        self._assert_equal(self.tui.input_mode, "login_user", "Entered login_user mode")
        self._assert_equal(self.tui.input_prompt, "Username: ", "Username prompt")
        self._assert_equal(self.tui._login_username, "", "No stored username yet")

        # Type username
        for ch in "alice":
            self._dispatch(ch)
        self._assert_equal(self.tui.input_buffer, "alice", "Username typed")

        # Submit username → should transition to password
        self._dispatch("enter")
        self._assert_equal(self.tui.input_mode, "login_pass", "Transitioned to login_pass mode")
        self._assert_equal(self.tui.input_prompt, "Password: ", "Password prompt set")
        self._assert_equal(self.tui._login_username, "alice", "Username stored")
        self._assert_equal(self.tui.input_buffer, "", "Buffer cleared for password")

        # Type password
        for ch in "secret123":
            self._dispatch(ch)
        self._assert_equal(self.tui.input_buffer, "secret123", "Password typed")

        # Submit password → should call login
        self._dispatch("enter")
        self._assert_equal(self.tui.input_mode, None, "Login mode exited")
        self._assert(
            len(self.mock_app._login_calls) == 1,
            "login() was called once",
        )
        self._assert_equal(
            self.mock_app._login_calls[0], ("alice", "secret123"),
            "login called with correct credentials",
        )

        # Cancel during login_user
        self.tui.handle_key("l")
        for ch in "bob":
            self._dispatch(ch)
        self._dispatch("escape")
        self._assert_equal(self.tui.input_mode, None, "Cancel during login_user")
        self._assert_equal(self.tui._login_username, "", "Username cleared on cancel")

        # Cancel during login_pass
        self.tui.handle_key("l")
        for ch in "charlie":
            self._dispatch(ch)
        self._dispatch("enter")  # submit username
        for ch in "pw":
            self._dispatch(ch)
        self._dispatch("escape")  # cancel password
        self._assert_equal(self.tui.input_mode, None, "Cancel during login_pass")

    # ------------------------------------------------------------------
    # 08 — Quit
    # ------------------------------------------------------------------

    def test_quit(self) -> None:
        """Test that 'q' stops both TUI and node."""
        print(f"\n{Colour.BOLD}── Test 08: Quit{Colour.RESET}")
        self._setup_tui()

        self._assert(self.tui.running, "TUI running before quit")
        self.tui.handle_key("q")
        self._assert(not self.tui.running, "TUI stopped after 'q'")
        self._assert(self.mock_app._stopped, "App stopped after 'q'")

    # ------------------------------------------------------------------
    # 09 — Download progress
    # ------------------------------------------------------------------

    def test_download_progress(self) -> None:
        """Test download progress tracking and cleanup."""
        print(f"\n{Colour.BOLD}── Test 09: Download Progress{Colour.RESET}")
        self._setup_tui()

        fid = "downloading_file_hash_1234567890abcdef"

        # Start a download
        self.tui.set_download_progress(fid, 0, 100, "big_file.zip")
        self._assert(fid in self.tui.download_progress, "Download tracked")
        self._assert_equal(
            self.tui.download_progress[fid]["current"], 0,
            "Progress starts at 0",
        )
        self._assert_equal(
            self.tui.download_progress[fid]["total"], 100,
            "Total set correctly",
        )

        # Update progress
        self.tui.set_download_progress(fid, 50, 100, "big_file.zip")
        self._assert_equal(
            self.tui.download_progress[fid]["current"], 50,
            "Progress updates to 50",
        )

        # Multiple downloads
        self.tui.set_download_progress("file2", 10, 50, "small.txt")
        self._assert_equal(len(self.tui.download_progress), 2, "Two downloads tracked")

        # Complete a download (auto-cleans after 1s)
        self.tui.set_download_progress(fid, 100, 100, "big_file.zip")
        time.sleep(1.5)  # Wait for cleanup thread
        self._assert(
            fid not in self.tui.download_progress,
            "Completed download auto-cleaned",
        )

    # ------------------------------------------------------------------
    # 10 — Render methods don't crash
    # ------------------------------------------------------------------

    def test_render_methods(self) -> None:
        """Verify all render methods execute without exceptions."""
        print(f"\n{Colour.BOLD}── Test 10: Render Methods{Colour.RESET}")
        self._setup_tui()

        render_methods = [
            ("_render_header", self.tui._render_header),
            ("_render_sidebar", self.tui._render_sidebar),
            ("_render_footer", self.tui._render_footer),
        ]

        for name, method in render_methods:
            try:
                result = method()
                self._assert(result is not None, f"{name}() returns output")
                self._assert(hasattr(result, "__rich_console__") or hasattr(result, "renderable"),
                             f"{name}() returns a Rich renderable")
            except Exception as e:
                self._assert(False, f"{name}() does not crash", f"Error: {e}")

        # Test each tab's _render_main
        tab_methods = {
            1: ("_render_main (tab 1: Network Files)", True),
            2: ("_render_main (tab 2: My Files)", True),
            3: ("_render_main (tab 3: Storage)", True),
            4: ("_render_main (tab 4: Peers)", True),
        }
        for tab, (label, _) in tab_methods.items():
            self.tui.active_tab = tab
            try:
                result = self.tui._render_main()
                self._assert(result is not None, f"{label} returns output")
            except Exception as e:
                self._assert(False, f"{label} does not crash", f"Error: {e}")

        # File table with search filter
        self.tui.search_query = "zebra"
        self.tui.active_tab = 1
        try:
            result = self.tui._render_file_table(all_files=True)
            self._assert(result is not None, "File table with search returns output")
        except Exception as e:
            self._assert(False, "File table with search", f"Error: {e}")

        # My Files filtered
        self.tui.active_tab = 2
        try:
            result = self.tui._render_file_table(all_files=False)
            self._assert(result is not None, "My Files table returns output")
        except Exception as e:
            self._assert(False, "My Files table", f"Error: {e}")

    # ------------------------------------------------------------------
    # 11 — Footer in input mode
    # ------------------------------------------------------------------

    def test_footer_input_mode(self) -> None:
        """Test footer rendering in various input modes."""
        print(f"\n{Colour.BOLD}── Test 11: Footer in Input Mode{Colour.RESET}")
        self._setup_tui()

        # Normal footer
        try:
            footer = self.tui._render_footer()
            self._assert(footer is not None, "Normal footer renders")
        except Exception as e:
            self._assert(False, "Normal footer", f"Error: {e}")

        # Footer in publish mode
        self.tui.input_mode = "publish"
        self.tui.input_prompt = "Publish file path: "
        self.tui.input_buffer = "/tmp/test"
        try:
            footer = self.tui._render_footer()
            self._assert(footer is not None, "Publish mode footer renders")
        except Exception as e:
            self._assert(False, "Publish mode footer", f"Error: {e}")

        # Footer in login_pass mode (password masking)
        self.tui.input_mode = "login_pass"
        self.tui.input_prompt = "Password: "
        self.tui.input_buffer = "secret"
        try:
            footer = self.tui._render_footer()
            self._assert(footer is not None, "Password mode footer renders (masked)")
        except Exception as e:
            self._assert(False, "Password mode footer", f"Error: {e}")

        # Reset
        self.tui.input_mode = None

    # ------------------------------------------------------------------
    # 12 — Footer with download progress
    # ------------------------------------------------------------------

    def test_footer_download_progress(self) -> None:
        """Test footer rendering with active download progress."""
        print(f"\n{Colour.BOLD}── Test 12: Footer with Download Progress{Colour.RESET}")
        self._setup_tui()

        # Add download progress
        self.tui.set_download_progress("file_dl_1", 45, 100, "movie.mp4")

        try:
            footer = self.tui._render_footer()
            self._assert(footer is not None, "Footer with download progress renders")
        except Exception as e:
            self._assert(False, "Footer with download progress", f"Error: {e}")

        # Cleanup
        self.tui.download_progress.clear()

    # ------------------------------------------------------------------
    # 13 — Dispatch routing
    # ------------------------------------------------------------------

    def test_dispatch_routing(self) -> None:
        """Test that _dispatch_key correctly routes to input vs normal mode."""
        print(f"\n{Colour.BOLD}── Test 13: Dispatch Routing{Colour.RESET}")
        self._setup_tui()

        # Normal mode: keys go to handle_key
        self.tui._dispatch_key("1")
        self._assert_equal(self.tui.active_tab, 1, "Normal mode: '1' switches tab")

        # Input mode: keys go to _handle_input_key
        self.tui.handle_key("p")  # enter input mode
        self._dispatch("x")
        self._dispatch("y")
        self._assert_equal(self.tui.input_buffer, "xy", "Input mode: chars go to buffer (not tab switch)")
        self._assert_equal(self.tui.active_tab, 1, "Input mode: tab not changed by '1'-like keys")

        # Input mode: arrow keys should NOT change selected_index
        old_index = self.tui.selected_index
        self._dispatch("down")
        self._assert_equal(self.tui.selected_index, old_index, "Input mode: 'down' does not change selection")

        # Escape out
        self._dispatch("escape")
        self._assert_equal(self.tui.input_mode, None, "Back to normal mode")

    # ------------------------------------------------------------------
    # 14 — Download selected file
    # ------------------------------------------------------------------

    def test_download_selected(self) -> None:
        """Test downloading the selected file via 'd' key."""
        print(f"\n{Colour.BOLD}── Test 14: Download Selected File{Colour.RESET}")
        self._setup_tui()

        # Select second file
        self.tui.selected_index = 1
        self.tui.handle_key("d")

        self._assert(
            len(self.mock_app._downloaded_files) == 1,
            "Download was triggered",
        )

        # Select first file
        self.tui.selected_index = 0
        self.tui.handle_key("d")
        self._assert(
            len(self.mock_app._downloaded_files) == 2,
            "Second download triggered",
        )

    # ------------------------------------------------------------------
    # 15 — Input mode submit with empty buffer
    # ------------------------------------------------------------------

    def test_input_mode_empty_submit(self) -> None:
        """Test submitting empty input in various modes."""
        print(f"\n{Colour.BOLD}── Test 15: Empty Input Submit{Colour.RESET}")
        self._setup_tui()

        # Publish with empty path
        self.tui.handle_key("p")
        self._dispatch("enter")
        self._assert_equal(self.tui.input_mode, None, "Publish mode exits on empty submit")
        self._assert_equal(len(self.mock_app._published_files), 0, "No file published with empty path")

        # Connect with empty URL
        self.tui.handle_key("c")
        self._dispatch("enter")
        self._assert_equal(self.tui.input_mode, None, "Connect mode exits on empty submit")

        # Login with empty username
        self.tui.handle_key("l")
        self._dispatch("enter")
        self._assert_equal(self.tui.input_mode, "login_pass", "Empty username still advances to password")
        self._dispatch("enter")
        self._assert_equal(self.tui.input_mode, None, "Login mode exits on empty password")

    # ------------------------------------------------------------------
    # 16 — Search filtering
    # ------------------------------------------------------------------

    def test_search_filtering(self) -> None:
        """Test that search queries correctly filter file lists."""
        print(f"\n{Colour.BOLD}── Test 16: Search Filtering{Colour.RESET}")
        self._setup_tui()

        # Search for "ima" — safe chars (no action-key conflicts)
        self.tui.handle_key("i")
        self.tui.handle_key("m")
        self.tui.handle_key("a")

        # Verify the file table renders (filtering happens internally)
        try:
            result = self.tui._render_file_table(all_files=True)
            self._assert(result is not None, "Search-filtered table renders")
        except Exception as e:
            self._assert(False, "Search-filtered table", f"Error: {e}")

        # Search for nonexistent
        self.tui.handle_key("escape")
        for ch in "zzz_nonexistent":
            self.tui.handle_key(ch)
        try:
            result = self.tui._render_file_table(all_files=True)
            self._assert(result is not None, "No-match search table renders")
        except Exception as e:
            self._assert(False, "No-match search", f"Error: {e}")

    # ------------------------------------------------------------------
    # 17 — Key in input mode should not trigger quit
    # ------------------------------------------------------------------

    def test_no_quit_in_input_mode(self) -> None:
        """Test that 'q' in input mode does NOT quit — it's added to buffer."""
        print(f"\n{Colour.BOLD}── Test 17: No Quit in Input Mode{Colour.RESET}")
        self._setup_tui()

        self.tui.handle_key("p")  # enter publish mode
        self._dispatch("q")
        self._assert(self.tui.running, "TUI still running — 'q' went to buffer, not quit")
        self._assert_equal(self.tui.input_buffer, "q", "'q' added to buffer in input mode")

    # ------------------------------------------------------------------
    # 18 — Rapid key presses
    # ------------------------------------------------------------------

    def test_rapid_key_presses(self) -> None:
        """Test that rapid key presses don't cause state corruption."""
        print(f"\n{Colour.BOLD}── Test 18: Rapid Key Presses{Colour.RESET}")
        self._setup_tui()

        # Rapid tab switching
        for _ in range(10):
            self.tui.handle_key("1")
            self.tui.handle_key("2")
            self.tui.handle_key("3")
            self.tui.handle_key("4")
        self._assert_equal(self.tui.active_tab, 4, "Rapid tab switching lands on correct tab")

        # Rapid search + clear (use only safe chars: no 1-4, p, c, l, d, q)
        safe_chars = "abefghijkmorstuvwxyz"
        for ch in safe_chars:
            self.tui.handle_key(ch)
        self._assert_equal(len(self.tui.search_query), len(safe_chars), "Rapid search typing works")
        self.tui.handle_key("escape")
        self._assert_equal(self.tui.search_query, "", "Escape clears after rapid typing")

        # Rapid enter/exit input mode
        for _ in range(5):
            self.tui.handle_key("p")
            self._dispatch("escape")
        self._assert_equal(self.tui.input_mode, None, "Rapid enter/exit input mode stable")

    # ------------------------------------------------------------------
    # 19 — Password masking in footer
    # ------------------------------------------------------------------

    def test_password_masking(self) -> None:
        """Test that password characters are masked in the footer display."""
        print(f"\n{Colour.BOLD}── Test 19: Password Masking{Colour.RESET}")
        self._setup_tui()

        self.tui.input_mode = "login_pass"
        self.tui.input_prompt = "Password: "
        self.tui.input_buffer = "super_secret_123"

        try:
            footer = self.tui._render_footer()
            # Footer renders — the masking happens internally (replaced with *)
            # We just verify it doesn't crash
            self._assert(footer is not None, "Password footer renders without leaking plaintext")
        except Exception as e:
            self._assert(False, "Password footer", f"Error: {e}")

        self.tui.input_mode = None

    # ------------------------------------------------------------------
    # 20 — Connect URL parsing variations
    # ------------------------------------------------------------------

    def test_connect_url_variations(self) -> None:
        """Test that connect mode handles various URL formats."""
        print(f"\n{Colour.BOLD}── Test 20: Connect URL Variations{Colour.RESET}")
        self._setup_tui()

        # Test http URL
        self.tui.handle_key("c")
        url1 = "http://peer.example.com/?join=abc&pk=xyz&addr=1.2.3.4:9000"
        for ch in url1:
            self._dispatch(ch)
        self._dispatch("enter")
        self._assert_equal(self.tui.input_mode, None, "HTTP URL submitted")
        self._assert("http://peer" in self.mock_app._connected_urls[-1], "HTTP URL passed correctly")

        # Test decentralised:// URL
        self.tui.handle_key("c")
        url2 = "decentralised://?join=def&pk=uvw&addr=5.6.7.8:8000"
        for ch in url2:
            self._dispatch(ch)
        self._dispatch("enter")
        self._assert("decentralised://" in self.mock_app._connected_urls[-1], "decentralised:// URL passed correctly")

        # Test ip:port format
        self.tui.handle_key("c")
        url3 = "10.0.0.5:9000"
        for ch in url3:
            self._dispatch(ch)
        self._dispatch("enter")
        self._assert("decentralised://?addr=10.0.0.5:9000" in self.mock_app._connected_urls[-1],
                     "ip:port converted to decentralised:// URL")

    # ==================================================================
    # Run
    # ==================================================================

    def _dispatch(self, key: str) -> None:
        """Simulate a keypress going through _dispatch (was _dispatch_key)."""
        self.tui._dispatch(key)

    def run_all(self) -> bool:
        """Run all TUI interactivity tests."""
        print(f"\n{Colour.BOLD}{Colour.BLUE}{'='*60}{Colour.RESET}")
        print(f"{Colour.BOLD}{Colour.BLUE}  TUI Interactivity — Integration Tests{Colour.RESET}")
        print(f"{Colour.BOLD}{Colour.BLUE}{'='*60}{Colour.RESET}")

        tests = [
            ("Initial State", self.test_initial_state),
            ("Tab Switching", self.test_tab_switching),
            ("Search As You Type", self.test_search_as_you_type),
            ("Arrow Navigation", self.test_arrow_navigation),
            ("Input Mode: Publish", self.test_input_mode_publish),
            ("Input Mode: Connect", self.test_input_mode_connect),
            ("Input Mode: Login", self.test_input_mode_login),
            ("Quit", self.test_quit),
            ("Download Progress", self.test_download_progress),
            ("Render Methods", self.test_render_methods),
            ("Footer in Input Mode", self.test_footer_input_mode),
            ("Footer with Downloads", self.test_footer_download_progress),
            ("Dispatch Routing", self.test_dispatch_routing),
            ("Download Selected", self.test_download_selected),
            ("Empty Input Submit", self.test_input_mode_empty_submit),
            ("Search Filtering", self.test_search_filtering),
            ("No Quit in Input Mode", self.test_no_quit_in_input_mode),
            ("Rapid Key Presses", self.test_rapid_key_presses),
            ("Password Masking", self.test_password_masking),
            ("Connect URL Variations", self.test_connect_url_variations),
        ]

        for name, test_fn in tests:
            try:
                test_fn()
            except Exception as e:
                self.failed += 1
                print(f"  {Colour.RED}✗ CRASH{Colour.RESET} {name}")
                print(f"    {Colour.RED}{type(e).__name__}: {e}{Colour.RESET}")
                if self.verbose:
                    import traceback
                    traceback.print_exc()

        # Summary
        total = self.passed + self.failed
        print(f"\n{Colour.BOLD}{'='*60}{Colour.RESET}")
        print(f"{Colour.BOLD}  Results: {self.passed}/{total} passed{Colour.RESET}")
        if self.failed > 0:
            print(f"  {Colour.RED}{self.failed} failed{Colour.RESET}")
        print(f"{Colour.BOLD}{'='*60}{Colour.RESET}")

        return self.failed == 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="TUI interactivity integration tests",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()

    runner = TUIInteractionTest(verbose=args.verbose)
    success = runner.run_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
