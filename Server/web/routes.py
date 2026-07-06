"""
web/routes.py — HTTP Routes

Flask blueprint for all HTTP endpoints.
"""

from functools import wraps

from flask import (
    Blueprint,
    jsonify,
    render_template,
    request,
    redirect,
    session,
    url_for,
)

main = Blueprint("main", __name__)


# ---------------------------------------------------------------------------
# Auth decorator
# ---------------------------------------------------------------------------


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return jsonify({"error": "Authentication required"}), 401
        return f(*args, **kwargs)

    return decorated


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------


@main.route("/")
def index():
    """Serve index.html if authenticated, else redirect to login (preserving join params)."""
    if not session.get("authenticated"):
        qs = request.query_string.decode("utf-8")
        target = url_for("main.login_page")
        if qs:
            target += "?" + qs
        return redirect(target)
    return render_template("index.html")


@main.route("/login")
def login_page():
    """Serve login page."""
    # Preserve join/pk/addr params from referrer so they survive the login flow
    join_id = request.args.get("join", "")
    join_pk = request.args.get("pk", "")
    join_addr = request.args.get("addr", "")
    return render_template("login.html",
                          join_id=join_id, join_pk=join_pk, join_addr=join_addr)


# ---------------------------------------------------------------------------
# Auth API
# ---------------------------------------------------------------------------


@main.route("/api/login", methods=["POST"])
def api_login():
    """Authenticate user: derive AuthorIdentity from username+password."""
    from flask import current_app

    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    username = data.get("username", "").strip()
    password = data.get("password", "")

    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400

    node = current_app.config["node"]
    try:
        author = node.login(username, password)
        session["username"] = username
        session["author_id"] = author.author_id
        session["authenticated"] = True
        return jsonify({"success": True, "author_id": author.author_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 401


@main.route("/api/logout", methods=["POST"])
def api_logout():
    """Clear session."""
    session.clear()
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


@main.route("/api/status")
@login_required
def api_status():
    """Return node status."""
    node = __import__("flask").current_app.config["node"]
    storage = node.storage.get_storage_breakdown()
    connected = len(node.udp_engine.get_connected_peers())
    return jsonify(
        {
            "node_id": node.node_identity.node_id,
            "author_id": node.author_identity.author_id
            if node.author_identity
            else None,
            "peers_connected": connected,
            "files_count": node.file_registry.count(),
            "storage": storage,
            "health": "healthy" if connected > 0 else "reconnecting",
        }
    )


# ---------------------------------------------------------------------------
# Files API
# ---------------------------------------------------------------------------


@main.route("/api/files")
@login_required
def api_files():
    """Return all registry entries."""
    node = __import__("flask").current_app.config["node"]
    entries = node.file_registry.get_all()
    return jsonify(
        [
            {
                "file_id": e.file_id,
                "file_name": e.file_name,
                "file_size": e.file_size,
                "mime_type": e.mime_type,
                "author_id": e.author_id,
                "replica_count": e.replica_count,
                "timestamp": e.timestamp,
                "previous_file_id": e.previous_file_id,
                "replicas": [
                    {"node_id": r.node_id, "added_at": r.added_at}
                    for r in e.replicas
                ],
            }
            for e in entries
        ]
    )


@main.route("/api/files/my")
@login_required
def api_files_my():
    """Return files authored by logged-in user."""
    node = __import__("flask").current_app.config["node"]
    author_id = session.get("author_id")
    if not author_id:
        return jsonify([])
    entries = node.file_registry.get_by_author(author_id)
    return jsonify(
        [
            {
                "file_id": e.file_id,
                "file_name": e.file_name,
                "file_size": e.file_size,
                "mime_type": e.mime_type,
                "replica_count": e.replica_count,
                "timestamp": e.timestamp,
                "previous_file_id": e.previous_file_id,
            }
            for e in entries
        ]
    )


@main.route("/api/files/<file_id>")
@login_required
def api_file_detail(file_id):
    """Get single file entry."""
    node = __import__("flask").current_app.config["node"]
    entry = node.file_registry.get(file_id)
    if entry is None:
        return jsonify({"error": "File not found"}), 404
    return jsonify(
        {
            "file_id": entry.file_id,
            "file_name": entry.file_name,
            "file_size": entry.file_size,
            "mime_type": entry.mime_type,
            "author_id": entry.author_id,
            "replica_count": entry.replica_count,
            "timestamp": entry.timestamp,
            "previous_file_id": entry.previous_file_id,
            "replicas": [
                {"node_id": r.node_id, "added_at": r.added_at}
                for r in entry.replicas
            ],
        }
    )


@main.route("/api/files/<file_id>/download")
@login_required
def api_file_download(file_id):
    """Download a file."""
    from flask import Response, current_app

    node = current_app.config["node"]
    entry = node.file_registry.get(file_id)
    if entry is None:
        return jsonify({"error": "File not found"}), 404

    try:
        data = node.open_file(file_id)
        return Response(
            data,
            mimetype=entry.mime_type,
            headers={
                "Content-Disposition": f'attachment; filename="{entry.file_name}"'
            },
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@main.route("/api/files/<file_id>/open")
@login_required
def api_file_open(file_id):
    """Open a file inline."""
    from flask import Response, current_app

    node = current_app.config["node"]
    entry = node.file_registry.get(file_id)
    if entry is None:
        return jsonify({"error": "File not found"}), 404

    try:
        data = node.open_file(file_id)
        return Response(
            data,
            mimetype=entry.mime_type,
            headers={
                "Content-Disposition": f'inline; filename="{entry.file_name}"'
            },
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@main.route("/api/files/upload", methods=["POST"])
@login_required
def api_file_upload():
    """Upload a file."""
    from flask import current_app

    node = current_app.config["node"]

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if file.filename is None or file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    data = file.read()
    file_name = file.filename
    mime_type = file.content_type or "application/octet-stream"

    try:
        file_id = node.publish_file(data, file_name, mime_type)
        return jsonify({"file_id": file_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@main.route("/api/files/<file_id>/update", methods=["POST"])
@login_required
def api_file_update(file_id):
    """Update a file."""
    from flask import current_app

    node = current_app.config["node"]

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    data = file.read()

    try:
        new_id = node.update_file(file_id, data)
        return jsonify({"file_id": new_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@main.route("/api/files/<file_id>/delete", methods=["DELETE"])
@login_required
def api_file_delete(file_id):
    """Delete a file."""
    from flask import current_app

    node = current_app.config["node"]
    try:
        node.delete_file(file_id)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@main.route("/api/files/<file_id>/close", methods=["POST"])
@login_required
def api_file_close(file_id):
    """Close an opened file (promote temporary replica)."""
    from flask import current_app

    node = current_app.config["node"]
    node.storage.promote_temporary(file_id)
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Peers API
# ---------------------------------------------------------------------------


@main.route("/api/peers")
@login_required
def api_peers():
    """Return known peers."""
    from flask import current_app

    node = current_app.config["node"]
    peers = node.peer_book.get_all_ordered()
    connected = set(node.udp_engine.get_connected_peers())
    return jsonify(
        [
            {
                "node_id": p["node_id"],
                "ip": p["public_ip"],
                "port": p["public_port"],
                "tier": p["tier"],
                "last_seen": p["last_seen"],
                "connected": p["node_id"] in connected,
            }
            for p in peers
        ]
    )


@main.route("/api/peers/connect", methods=["POST"])
@login_required
def api_peer_connect():
    """Connect to a peer via URL."""
    from flask import current_app

    node = current_app.config["node"]
    data = request.get_json()
    if not data or "url" not in data:
        return jsonify({"error": "URL required"}), 400

    try:
        result = node.connect_via_url(data["url"])
        return jsonify({"success": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Network name
# ---------------------------------------------------------------------------


@main.route("/api/network-name", methods=["GET", "POST"])
@login_required
def api_network_name():
    """Get or set network name."""
    from flask import current_app
    import json

    node = current_app.config["node"]
    config_path = __import__("os").path.join(node.data_dir, "config.json")

    if request.method == "GET":
        name = "KNet"
        if __import__("os").path.exists(config_path):
            with open(config_path, "r") as f:
                config = json.load(f)
                name = config.get("network_name", name)
        return jsonify({"name": name})

    # POST
    data = request.get_json()
    if not data or "name" not in data:
        return jsonify({"error": "Name required"}), 400

    config = {}
    if __import__("os").path.exists(config_path):
        with open(config_path, "r") as f:
            config = json.load(f)
    config["network_name"] = data["name"]
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# QR code
# ---------------------------------------------------------------------------


@main.route("/api/qr")
@login_required
def api_qr():
    """Return connection URL for this node with a real QR code (PNG data URL)."""
    import base64
    import io
    import socket
    import qrcode
    from flask import current_app

    node = current_app.config["node"]
    web_port = current_app.config.get("web_port", 9001)
    web_host = current_app.config.get("web_host", "127.0.0.1")

    # Auto-detect LAN IP if web_host is localhost (required for QR to work on other devices)
    if web_host in ("127.0.0.1", "localhost", "0.0.0.0", "::1"):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            web_host = s.getsockname()[0]
            s.close()
        except OSError:
            pass  # fall back to 127.0.0.1

    pk_b64 = __import__("identity").public_key_to_base64(
        node.node_identity.public_key_bytes
    )
    url = (
        f"http://{web_host}:{web_port}/?"
        f"join={node.node_identity.node_id}&"
        f"pk={pk_b64}&"
        f"addr={node.udp_engine.public_ip}:{node.udp_engine.public_port}"
    )

    # Generate real QR code as PNG data URL
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=4,
    )
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    qr_data_url = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")

    return jsonify({"url": url, "qr_data_url": qr_data_url})


# ---------------------------------------------------------------------------
# Share
# ---------------------------------------------------------------------------


@main.route("/api/share/<file_id>")
@login_required
def api_share(file_id):
    """Generate share link for a file."""
    from flask import current_app

    node = current_app.config["node"]
    try:
        share_url = node.create_share_link(file_id)
        return jsonify({"url": share_url, "qr_url": share_url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Storage config
# ---------------------------------------------------------------------------


@main.route("/api/storage/config", methods=["GET", "POST"])
@login_required
def api_storage_config():
    """Get or set storage config."""
    from flask import current_app

    node = current_app.config["node"]

    if request.method == "GET":
        breakdown = node.storage.get_storage_breakdown()
        return jsonify(
            {
                "total_mb": node.storage.total_configured_mb,
                "used_own": breakdown["own"],
                "used_replicas": breakdown["replicas"],
                "available": breakdown["available"],
            }
        )

    # POST
    data = request.get_json()
    if not data or "total_mb" not in data:
        return jsonify({"error": "total_mb required"}), 400

    total_mb = int(data["total_mb"])
    min_required = (
        node.storage.used_for_own_files() + 1024 * 1024 - 1
    ) // (1024 * 1024)
    if total_mb < min_required:
        return (
            jsonify(
                {
                    "error": "Quota too low",
                    "min_required_mb": min_required,
                }
            ),
            400,
        )

    node.storage.set_quota(total_mb)
    return jsonify({"success": True})
