#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv-targeted"
TEST_DB="$SCRIPT_DIR/targeted_compat.db"
PORT="${PORT:-15531}"
PGSQLITE_PID=""

cleanup() {
    if [[ -n "$PGSQLITE_PID" ]]; then
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    rm -f "$TEST_DB" "$TEST_DB-journal" "$TEST_DB-wal" "$TEST_DB-shm"
    rm -f "$SCRIPT_DIR/targeted_compat.log"
    rm -f "/tmp/.s.PGSQL.$PORT"
}

trap cleanup EXIT INT TERM

cd "$PROJECT_ROOT"

if [[ ! -x "$PROJECT_ROOT/target/release/pgsqlite" ]]; then
    cargo build --release --quiet
fi

python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --quiet --upgrade pip
"$VENV_DIR/bin/python" -m pip install --quiet "sqlalchemy>=2,<3" "psycopg[binary]>=3.2,<4"

./target/release/pgsqlite --database "$TEST_DB" --port "$PORT" > "$SCRIPT_DIR/targeted_compat.log" 2>&1 &
PGSQLITE_PID=$!

for _ in {1..30}; do
    if python3 - <<PY
import socket
sock = socket.socket()
sock.settimeout(0.2)
try:
    sock.connect(("127.0.0.1", $PORT))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
PY
    then
        break
    fi
    sleep 1
done

if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
    echo "pgsqlite failed to start"
    cat "$SCRIPT_DIR/targeted_compat.log"
    exit 1
fi

"$VENV_DIR/bin/python" "$SCRIPT_DIR/compat_targeted_smoke.py" --port "$PORT"
