"""
log_utils.py — Centralised Logging

Provides a `get_logger(name)` factory.  Loggers write to `app.log` inside
`data_dir` when `--log app.log` is configured, and also to stderr when the
TUI is disabled (so the operator can see live logs in the terminal).

Also provides `udp_trace()` for raw packet hex-dump logging when
``--udp-trace`` is enabled.
"""

from __future__ import annotations

import binascii
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Module-level state — set once by App.__init__
# ---------------------------------------------------------------------------

_log_file_path: Optional[str] = None
_log_to_stderr: bool = False

_udp_trace_path: Optional[str] = None
_udp_trace_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def configure(data_dir: str, log_filename: str, no_tui: bool) -> None:
    """Configure the root logger.

    Args:
        data_dir: Node data directory (e.g. ``~/.decweb/data``).
        log_filename: The filename portion of ``--log`` (e.g. ``app.log``).
        no_tui: ``True`` when ``--no-tui`` is set → also log to stderr.
    """
    global _log_file_path, _log_to_stderr

    _log_file_path = str(Path(data_dir) / log_filename)
    _log_to_stderr = no_tui

    # Ensure directory exists
    Path(_log_file_path).parent.mkdir(parents=True, exist_ok=True)

    # Root logger — capture everything at DEBUG
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Remove any previously attached handlers (idempotent)
    root.handlers.clear()

    # File handler — always
    fh = logging.FileHandler(_log_file_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d  %(levelname)-7s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(fh)

    # Stderr handler — only when TUI is off
    if _log_to_stderr:
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(logging.DEBUG)
        sh.setFormatter(
            logging.Formatter(
                "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        root.addHandler(sh)


def configure_udp_trace(data_dir: str, filename: str) -> None:
    """Enable raw UDP packet hex-dump tracing to *filename* inside *data_dir*."""
    global _udp_trace_path
    _udp_trace_path = str(Path(data_dir) / filename)
    Path(_udp_trace_path).parent.mkdir(parents=True, exist_ok=True)


def udp_trace(direction: str, data: bytes, addr: tuple[str, int]) -> None:
    """Write a raw hex dump of *data* to the UDP trace file.

    Args:
        direction: ``"SENT"`` or ``"RECV"``.
        data: Raw UDP payload bytes.
        addr: Remote ``(ip, port)`` tuple.
    """
    if _udp_trace_path is None:
        return

    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    ms = int((time.time() % 1) * 1000)
    hex_str = binascii.hexlify(data).decode("ascii")

    line = f"{ts}.{ms:03d}  {direction:4s}  {len(data):5d} B  {addr[0]}:{addr[1]}  {hex_str}\n"

    with _udp_trace_lock:
        try:
            with open(_udp_trace_path, "a", encoding="ascii") as f:
                f.write(line)
        except OSError:
            pass


def get_logger(name: str) -> logging.Logger:
    """Return a logger for *name*.

    The logger inherits the handlers & level configured on the root logger,
    so callers only need to do ``logger.debug(...)`` / ``logger.info(...)``
    etc.
    """
    return logging.getLogger(name)
