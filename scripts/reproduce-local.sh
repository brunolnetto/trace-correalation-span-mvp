#!/usr/bin/env bash
#
# Reproduce the complete local observability flow:
# uv tests/lint -> build/start services -> HTTP smoke test ->
# validate JSON logs and the distributed trace in Jaeger.
#
# Usage:
#   ./scripts/reproduce-local.sh
#   ./scripts/reproduce-local.sh --cleanup
#
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CORRELATION_ID="550e8400-e29b-41d4-a716-446655440000"
ORDER_URL="${ORDER_URL:-http://localhost:8000}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:8001}"
JAEGER_URL="${JAEGER_URL:-http://localhost:16686}"
WAIT_SECONDS="${WAIT_SECONDS:-60}"
CLEANUP=0

for arg in "$@"; do
  case "$arg" in
    --cleanup) CLEANUP=1 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_command docker
require_command curl
require_command python3
require_command uv

docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required"
docker info >/dev/null 2>&1 || die "Docker daemon is not reachable"

TMP_DIR="$(mktemp -d)"
cleanup_tmp() {
  rm -rf "$TMP_DIR"
}
trap cleanup_tmp EXIT

if (( CLEANUP )); then
  trap 'docker compose down --remove-orphans || true; cleanup_tmp' EXIT
fi

echo "==> 1/6: Resolve and install Python dependencies"
if [[ -f uv.lock ]]; then
  uv sync --all-groups --locked
else
  echo "uv.lock is absent; resolving dependencies and generating it."
  uv sync --all-groups
  [[ -f uv.lock ]] || die "uv did not create uv.lock"
  echo "NOTE: uv.lock was generated locally. Review and commit it for repeatable installs."
fi

echo "==> 2/6: Run unit and concurrency tests"
uv run pytest

echo "==> 3/6: Run Ruff"
uv run ruff check .

echo "==> 4/6: Build and start Jaeger plus both APIs"
docker compose up --build --detach

wait_for_url() {
  local url="$1"
  local description="$2"
  local deadline=$((SECONDS + WAIT_SECONDS))

  until curl --silent --show-error --fail "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      docker compose ps >&2 || true
      docker compose logs --tail=100 >&2 || true
      die "Timed out waiting for $description at $url"
    fi
    sleep 2
  done
  echo "    ready: $description"
}

echo "    waiting for service health endpoints (up to ${WAIT_SECONDS}s)"
wait_for_url "$INVENTORY_URL/healthz" "inventory-api"
wait_for_url "$ORDER_URL/healthz" "order-api"
wait_for_url "$JAEGER_URL/" "Jaeger UI"

echo "==> 5/6: Exercise order -> inventory with a known correlation ID"
curl --silent --show-error --fail \
  --dump-header "$TMP_DIR/response.headers" \
  --output "$TMP_DIR/response.json" \
  --header "X-Correlation-ID: $CORRELATION_ID" \
  "$ORDER_URL/orders/123"

python3 - "$TMP_DIR/response.headers" "$TMP_DIR/response.json" "$CORRELATION_ID" <<'PY'
import json
import sys
from pathlib import Path

headers_path, body_path, expected_id = sys.argv[1:]
headers = Path(headers_path).read_text().lower().splitlines()
body = json.loads(Path(body_path).read_text())

actual_id = next(
    (line.split(":", 1)[1].strip() for line in headers
     if line.startswith("x-correlation-id:")),
    None,
)
assert actual_id == expected_id, (
    f"response correlation ID mismatch: expected {expected_id}, got {actual_id}"
)
assert body["order_id"] == 123, body
assert body["inventory"]["product_id"] == 123, body
assert body["inventory"]["available"] is True, body
print(f"    response correlation ID: {actual_id}")
print(f"    response body: {json.dumps(body, sort_keys=True)}")
PY

echo "==> 6/6: Verify JSON logs and trace export in Jaeger"
# Allow BatchSpanProcessor time to export spans to the OTLP endpoint.
TRACE_URL="$JAEGER_URL/api/traces?service=order-api&limit=20&lookback=1h"
TRACE_FOUND=0
deadline=$((SECONDS + WAIT_SECONDS))
while (( SECONDS < deadline )); do
  if curl --silent --show-error --fail "$TRACE_URL" -o "$TMP_DIR/traces.json"; then
    if python3 - "$TMP_DIR/traces.json" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text())
for trace in payload.get("data", []):
    processes = trace.get("processes", {})
    services = {p.get("serviceName") for p in processes.values()}
    operations = {span.get("operationName") for span in trace.get("spans", [])}
    if (
        {"order-api", "inventory-api"}.issubset(services)
        and "order.load_inventory" in operations
        and "inventory.lookup" in operations
    ):
        raise SystemExit(0)
raise SystemExit(1)
PY
    then
      TRACE_FOUND=1
      break
    fi
  fi
  sleep 2
done

if (( ! TRACE_FOUND )); then
  echo "Jaeger did not return the expected cross-service trace." >&2
  echo "Recent container logs:" >&2
  docker compose logs --tail=150 >&2 || true
  die "Trace validation failed; inspect $JAEGER_URL"
fi
echo "    Jaeger contains one trace with order-api + inventory-api spans"

docker compose logs --no-color order-api inventory-api > "$TMP_DIR/service.log"
python3 - "$TMP_DIR/service.log" "$CORRELATION_ID" <<'PY'
import json
import sys
from pathlib import Path

path, expected_id = sys.argv[1:]
records = []
for line in Path(path).read_text().splitlines():
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    if record.get("correlation_id") == expected_id:
        records.append(record)

required_services = {"order-api", "inventory-api"}
found_services = {record.get("service") for record in records}
assert required_services.issubset(found_services), (
    f"expected correlated logs from {required_services}, got {found_services}"
)
for record in records:
    if record.get("message") in {
        "order.lookup.started",
        "order.lookup.completed",
        "inventory.lookup.started",
        "inventory.lookup.completed",
    }:
        assert record.get("trace_id"), f"missing trace_id: {record}"
        assert record.get("span_id"), f"missing span_id: {record}"

print(f"    correlated JSON log records: {len(records)}")
print(f"    services represented: {', '.join(sorted(found_services))}")
PY

echo
echo "SUCCESS: local observability reproduction passed."
echo "  Order API:     $ORDER_URL"
echo "  Inventory API: $INVENTORY_URL"
echo "  Jaeger UI:     $JAEGER_URL"
echo
if (( CLEANUP )); then
  echo "The Compose stack will be stopped automatically."
else
  echo "The Compose stack is still running so you can inspect Jaeger and logs."
  echo "Stop it with: docker compose down"
fi
