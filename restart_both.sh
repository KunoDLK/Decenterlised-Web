#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Cleaning data directories ==="
rm -rf Server/data1 Server/data2
echo "Done."

echo ""
echo "=== Starting Instance 1 (UDP :32128, Web :9001) ==="
python3 knet.py --data-dir Server/data1 --no-tui --debug --log app1.log --udp-trace packets1.hex &
PID1=$!

echo "=== Starting Instance 2 (UDP :32129, Web :9002) ==="
python3 knet.py --data-dir Server/data2 --tui-port-offset 1 --no-tui --debug --log app2.log --udp-trace packets2.hex &
PID2=$!

echo ""
echo "Both instances launched:"
echo "  Instance 1  PID=$PID1  Web: http://127.0.0.1:9001"
echo "  Instance 2  PID=$PID2  Web: http://127.0.0.1:9002"
echo ""
echo "Press Ctrl+C to stop both."

cleanup() {
    echo ""
    echo "=== Stopping both instances ==="
    kill $PID1 $PID2 2>/dev/null
    wait $PID1 $PID2 2>/dev/null
    echo "Stopped."
}
trap cleanup INT TERM

wait
