"""
web/ws.py — WebSocket Handler

Real-time push updates to the web UI via WebSocket (flask-sock).
"""

import json
import threading
import time

from flask import current_app
from flask_sock import Sock

sock = Sock()


def init_ws(app):
    """Initialize WebSocket support on the Flask app."""
    sock.init_app(app)


@sock.route("/ws")
def ws_handler(ws):
    """WebSocket endpoint for real-time updates."""
    node = current_app.config["node"]

    # Track connected clients
    if not hasattr(current_app, "ws_clients"):
        current_app.ws_clients = set()
    current_app.ws_clients.add(ws)

    # Debounced health state
    health_state = {"status": "reconnecting", "timer": None}

    def broadcast(data: dict):
        """Send JSON to all connected WebSocket clients."""
        dead = set()
        for client in current_app.ws_clients:
            try:
                client.send(json.dumps(data))
            except Exception:
                dead.add(client)
        current_app.ws_clients -= dead

    def on_event(event_data: dict):
        """Handle events from the node's EventBus."""
        event_type = event_data.get("type", "")
        if event_type == "peer_connected":
            broadcast(
                {
                    "type": "peer_update",
                    "node_id": event_data.get("node_id", ""),
                    "status": "connected",
                }
            )
        elif event_type == "peer_disconnected":
            broadcast(
                {
                    "type": "peer_update",
                    "node_id": event_data.get("node_id", ""),
                    "status": "disconnected",
                }
            )
        elif event_type in ("file_added", "file_updated", "file_deleted"):
            broadcast(
                {
                    "type": "file_update",
                    "action": event_type.split("_")[1],
                    "file_id": event_data.get("file_id", ""),
                }
            )
        elif event_type == "storage_changed":
            broadcast(
                {
                    "type": "storage_update",
                    "storage": event_data.get("storage", {}),
                }
            )
        elif event_type == "health_changed":
            nonlocal health_state
            status = event_data.get("status", "reconnecting")

            # Debounce: wait 5 seconds before broadcasting health change
            if health_state["timer"]:
                health_state["timer"].cancel()

            def _delayed_broadcast(s=status):
                broadcast(
                    {
                        "type": "health_update",
                        "status": s,
                        "peers_connected": len(
                            node.udp_engine.get_connected_peers()
                        ),
                    }
                )
                health_state["timer"] = None

            health_state["timer"] = threading.Timer(5.0, _delayed_broadcast)
            health_state["timer"].start()

        elif event_type == "download_progress":
            broadcast(
                {
                    "type": "download_progress",
                    "file_id": event_data.get("file_id", ""),
                    "progress": event_data.get("progress", 0.0),
                    "status": event_data.get("status", "downloading"),
                }
            )

    # Subscribe to events
    node.event_bus.subscribe("*", on_event)

    try:
        while True:
            msg = ws.receive()
            if msg is None:
                break

            try:
                data = json.loads(msg)
                msg_type = data.get("type", "")

                if msg_type == "download":
                    file_id = data.get("file_id")
                    if file_id:
                        node.download_file(file_id)

                elif msg_type == "open":
                    file_id = data.get("file_id")
                    if file_id:
                        node.open_file(file_id)

                elif msg_type == "share":
                    file_id = data.get("file_id")
                    if file_id:
                        share_url = node.create_share_link(file_id)
                        ws.send(
                            json.dumps(
                                {
                                    "type": "share_response",
                                    "file_id": file_id,
                                    "url": share_url,
                                }
                            )
                        )

                elif msg_type == "connect_peer":
                    url = data.get("url")
                    if url:
                        node.connect_via_url(url)

                elif msg_type == "tab_closed":
                    file_id = data.get("file_id")
                    if file_id:
                        node.storage.promote_temporary(file_id)

            except json.JSONDecodeError:
                pass
            except Exception:
                pass

    finally:
        current_app.ws_clients.discard(ws)
        node.event_bus.unsubscribe("*", on_event)
