#!/usr/bin/env python3
"""
One-command launcher: sets up the venv if needed, then starts the app.
Silent on subsequent runs — just launches straight away.
"""

import os
import sys
import subprocess
import platform

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
VENV_DIR = os.path.join(PROJECT_ROOT, ".venv")
REQUIREMENTS_FILE = os.path.join(PROJECT_ROOT, "Server", "requirements.txt")
MAIN_APP = os.path.join(PROJECT_ROOT, "Server", "app.py")
REQUIRED_PACKAGES = ["flask", "flask_sock", "cryptography", "rich"]


def get_python_exe(venv_dir: str) -> str:
    """Path to the venv's Python interpreter."""
    if platform.system() == "Windows":
        return os.path.join(venv_dir, "Scripts", "python.exe")
    return os.path.join(venv_dir, "bin", "python")


def deps_installed(python_exe: str) -> bool:
    """Check whether all required packages are already installed."""
    for pkg in REQUIRED_PACKAGES:
        ret = subprocess.run(
            [python_exe, "-c", f"import {pkg}"],
            capture_output=True, text=True,
        )
        if ret.returncode != 0:
            return False
    return True


def run(cmd: list[str], cwd: str | None = None) -> None:
    """Run a command, print it, and exit on failure."""
    print(f"  -> {' '.join(cmd)}")
    sys.stdout.flush()
    result = subprocess.run(cmd, cwd=cwd or PROJECT_ROOT)
    if result.returncode != 0:
        print(f"ERROR: Command failed with exit code {result.returncode}", file=sys.stderr)
        sys.exit(result.returncode)


def setup(python_exe: str) -> None:
    """Run the full setup with visible output."""
    print("=" * 60)
    print("  Decentralised Web — Automated Setup & Launch")
    print("=" * 60)
    print()

    # ── Step 1: Create the virtual environment ──────────────────────────
    if os.path.isdir(VENV_DIR):
        print(f"[1/5] Virtual environment already exists at: {VENV_DIR}")
    else:
        print(f"[1/5] Creating virtual environment at: {VENV_DIR}")
        run([sys.executable, "-m", "venv", VENV_DIR])

    # ── Step 2: Upgrade pip ─────────────────────────────────────────────
    print("[2/5] Upgrading pip...")
    run([python_exe, "-m", "pip", "install", "--upgrade", "pip"])

    # ── Step 3: Install dependencies from requirements.txt ──────────────
    if os.path.isfile(REQUIREMENTS_FILE):
        print(f"[3/5] Installing dependencies from {REQUIREMENTS_FILE}...")
        run([python_exe, "-m", "pip", "install", "-r", REQUIREMENTS_FILE])
    else:
        print(f"[3/5] Skipped — requirements file not found at {REQUIREMENTS_FILE}")

    # ── Step 4: Verify installation ────────────────────────────────────
    print("[4/5] Verifying installation...")
    run([python_exe, "-m", "pip", "list", "--format=columns"])

    print()
    print("=" * 60)
    print("  Setup complete! Launching the application...")
    print("=" * 60)
    print()


def main():
    needs_setup = not os.path.isdir(VENV_DIR)

    if not needs_setup:
        python_exe = get_python_exe(VENV_DIR)
        needs_setup = not os.path.isfile(python_exe) or not deps_installed(python_exe)

    if needs_setup:
        if not os.path.isdir(VENV_DIR):
            run([sys.executable, "-m", "venv", VENV_DIR])
        python_exe = get_python_exe(VENV_DIR)
        if not os.path.isfile(python_exe):
            print(f"ERROR: Could not find python in the venv at {python_exe}", file=sys.stderr)
            sys.exit(1)
        setup(python_exe)
    else:
        python_exe = get_python_exe(VENV_DIR)

    # ── Launch the main application ────────────────────────────────────
    os.chdir(PROJECT_ROOT)
    sys.stdout.flush()
    subprocess.run([python_exe, MAIN_APP])


if __name__ == "__main__":
    main()
