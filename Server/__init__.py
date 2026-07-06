"""
Server/__init__.py — Decentralised Web Server Package

All source modules live here.  Run with:

    python Server/app.py [args]

Or import programmatically:

    import sys
    sys.path.insert(0, "Server")
    from app import App
"""

import os
import sys

# Ensure sibling modules are importable regardless of CWD
_server_dir = os.path.dirname(os.path.abspath(__file__))
if _server_dir not in sys.path:
    sys.path.insert(0, _server_dir)
