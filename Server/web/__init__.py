"""
web/__init__.py — Flask App Factory
"""

from flask import Flask


def create_app(node, web_port: int, web_host: str) -> Flask:
    """Create and configure the Flask application.

    Args:
        node: Reference to the main App instance.
        web_port: Port to bind to.
        web_host: Host to bind to.

    Returns:
        Configured Flask app.
    """
    import os

    _web_dir = os.path.dirname(os.path.abspath(__file__))

    app = Flask(
        __name__,
        template_folder=os.path.join(_web_dir, "templates"),
        static_folder=os.path.join(_web_dir, "static"),
    )
    app.config["SECRET_KEY"] = os.urandom(32).hex()
    app.config["node"] = node
    app.config["web_port"] = web_port
    app.config["web_host"] = web_host

    # Register blueprints
    from web.routes import main as main_bp

    app.register_blueprint(main_bp)

    return app
