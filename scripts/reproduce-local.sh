#!/usr/bin/env bash
#
# Reproduce the Saga + inventory + observability flow:
# uv tests/lint -> build/start services -> catalog/stock setup ->
# success + compensation + insufficient-stock scenarios -> trace/log checks.
#
# Usage:
#   ./scripts/reproduce-local.sh
#   ./scripts/reproduce-local.sh --cleanup
#
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

READ_CORRELATION_ID="550e8400-e29b-41d4-a716-446655440000"
PROCESS_CORRELATION_ID="550e8400-e29b-41d4-a716-446655440001"
DECLINE_CORRELATION_ID="550e8400-e29b-41d4-a716-446655440002"
INSUFFICIENT_CORRELATION_ID="550e8400-e29b-41d4-a716-446655440003"
ORDER_URL="${ORDER_URL:-http://127.0.0.1:18000}"
INVENTORY_URL="${INVENTORY_URL:-http://127.0.0.1:18001}"
JAEGER_URL="${JAEGER_URL:-http://127.0.0.1:16686}"
WAIT_SECONDS="${WAIT_SECONDS:-60}"
CLEANUP=0

for arg in "$@"; do
  case "$arg" in
    --cleanup) CLEANUP=1 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

die() { echo "ERROR: $*" >&2; exit 1; }
require_command() { command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"; }

require_command docker
require_command curl
require_command python3
require_command uv
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required"
docker info >/dev/null 2>&1 || die "Docker daemon is not reachable"

TMP_DIR="$(mktemp -d)"
cleanup_tmp() { rm -rf "$TMP_DIR"; }
trap cleanup_tmp EXIT
if (( CLEANUP )); then
  trap 'docker compose down --remove-orphans || true; cleanup_tmp' EXIT
fi

echo "==> 1/6: Resolve and install Python dependencies"
uv lock --python 3.12
uv sync --python 3.12 --all-groups --locked
[[ -f uv.lock ]] || die "uv did not create uv.lock"
if ! git ls-files --error-unmatch uv.lock >/dev/null 2>&1; then
  echo "NOTE: uv.lock is generated locally but is not committed. Review and commit it for repeatable installs."
fi

echo "==> 2/6: Run unit and concurrency tests"
uv run --python 3.12 pytest

echo "==> 3/6: Run Ruff"
uv run --python 3.12 ruff check .

echo "==> 4/6: Build and start Jaeger plus both APIs"
docker compose up --build --detach

wait_for_url() {
  local url="$1"
  local description="$2"
  local deadline=$((SECONDS + WAIT_SECONDS))
  until curl --ipv4 --connect-timeout 2 --max-time 5 --silent --show-error --fail "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Docker port mappings:" >&2
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

echo "==> 5/6: Create product, receive stock, and exercise order scenarios"
curl --silent --show-error --fail \
  --header "Content-Type: application/json" \
  --data '{"sku":"LOW-STOCK-001","name":"Low stock demo product","category":"demo","unit_price":"10.00"}' \
  "$INVENTORY_URL/products" > "$TMP_DIR/product.json"

LOW_STOCK_PRODUCT_ID="$(python3 - "$TMP_DIR/product.json" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text())["product_id"])
PY
)"

curl --silent --show-error --fail \
  --header "Content-Type: application/json" \
  --data '{"quantity":4,"idempotency_key":"demo-opening-receipt-001","reference":"DEMO-PO-001"}' \
  "$INVENTORY_URL/inventory/$LOW_STOCK_PRODUCT_ID/receipts" \
  > "$TMP_DIR/receipt.json"

curl --silent --show-error --fail \
  --dump-header "$TMP_DIR/read.headers" \
  --output "$TMP_DIR/read.json" \
  --header "X-Correlation-ID: $READ_CORRELATION_ID" \
  "$ORDER_URL/orders/7"

PROCESS_STATUS="$(curl --silent --show-error \
  --dump-header "$TMP_DIR/process.headers" \
  --output "$TMP_DIR/process.json" \
  --write-out '%{http_code}' \
  --header "Content-Type: application/json" \
  --header "X-Correlation-ID: $PROCESS_CORRELATION_ID" \
  --data '{"product_id":7,"quantity":2,"amount":99.90}' \
  "$ORDER_URL/orders/123/process")"
[[ "$PROCESS_STATUS" == "201" ]] || {
  cat "$TMP_DIR/process.json" >&2
  die "Expected successful order workflow to return 201, got $PROCESS_STATUS"
}

DECLINE_STATUS="$(curl --silent --show-error \
  --dump-header "$TMP_DIR/decline.headers" \
  --output "$TMP_DIR/decline.json" \
  --write-out '%{http_code}' \
  --header "Content-Type: application/json" \
  --header "X-Correlation-ID: $DECLINE_CORRELATION_ID" \
  --data '{"product_id":7,"quantity":3,"amount":1001.00}' \
  "$ORDER_URL/orders/124/process")"
[[ "$DECLINE_STATUS" == "402" ]] || {
  cat "$TMP_DIR/decline.json" >&2
  die "Expected payment decline with compensation to return 402, got $DECLINE_STATUS"
}

INSUFFICIENT_STATUS="$(curl --silent --show-error \
  --dump-header "$TMP_DIR/insufficient.headers" \
  --output "$TMP_DIR/insufficient.json" \
  --write-out '%{http_code}' \
  --header "Content-Type: application/json" \
  --header "X-Correlation-ID: $INSUFFICIENT_CORRELATION_ID" \
  --data "{\"product_id\":$LOW_STOCK_PRODUCT_ID,\"quantity\":5,\"amount\":10.00}" \
  "$ORDER_URL/orders/125/process")"
[[ "$INSUFFICIENT_STATUS" == "409" ]] || {
  cat "$TMP_DIR/insufficient.json" >&2
  die "Expected insufficient inventory to return 409, got $INSUFFICIENT_STATUS"
}

curl --silent --show-error --fail --output "$TMP_DIR/inventory.json" "$INVENTORY_URL/inventory/7"
curl --silent --show-error --fail --output "$TMP_DIR/low-stock.json" \
  "$INVENTORY_URL/inventory/$LOW_STOCK_PRODUCT_ID"

python3 - "$TMP_DIR/process.headers" "$TMP_DIR/process.json" \
  "$TMP_DIR/decline.json" "$TMP_DIR/inventory.json" \
  "$TMP_DIR/low-stock.json" "$TMP_DIR/insufficient.json" \
  "$PROCESS_CORRELATION_ID" "$DECLINE_CORRELATION_ID" \
  "$LOW_STOCK_PRODUCT_ID" <<'PY'
import json
import sys
from pathlib import Path

(
    process_headers,
    process_body,
    decline_body,
    inventory_body,
    low_stock_body,
    insufficient_body,
    process_id,
    decline_id,
    low_stock_product_id,
) = sys.argv[1:]

headers = Path(process_headers).read_text().lower().splitlines()
process = json.loads(Path(process_body).read_text())
decline = json.loads(Path(decline_body).read_text())
inventory = json.loads(Path(inventory_body).read_text())
low_stock = json.loads(Path(low_stock_body).read_text())
insufficient = json.loads(Path(insufficient_body).read_text())

returned_id = next(
    (line.split(":", 1)[1].strip() for line in headers
     if line.startswith("x-correlation-id:")),
    None,
)
assert returned_id == process_id, (returned_id, process_id)
assert process["order_id"] == 123, process
assert process["status"] == "processed", process
assert process["correlation_id"] == process_id, process
assert process["saga_id"], process
assert process["reservation_id"], process
assert process["payment_id"], process
assert decline["detail"] == (
    "Payment declined; completed actions were compensated."
), decline
assert inventory["on_hand"] == 42, inventory
assert inventory["reserved"] == 2, inventory
assert inventory["available"] == 40, inventory
assert inventory["quantity"] == 40, inventory
assert insufficient["detail"] == "Insufficient inventory", insufficient
assert low_stock["product_id"] == int(low_stock_product_id), low_stock
assert low_stock["on_hand"] == 4, low_stock
assert low_stock["reserved"] == 0, low_stock
assert low_stock["available"] == 4, low_stock
print(f"    successful saga ID: {process['saga_id']}")
print(f"    successful order correlation ID: {process_id}")
print(f"    declined order correlation ID: {decline_id}")
print(f"    low-stock product ID: {low_stock_product_id}")
print("    inventory after compensation: on_hand=42 reserved=2 available=40")
print("    insufficient-stock order rejected without changing inventory")
PY

echo "==> 6/6: Verify JSON logs and Saga traces in Jaeger"
TRACE_URL="$JAEGER_URL/api/traces?service=order-api&limit=50&lookback=1h"
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
    services = {process.get("serviceName") for process in processes.values()}
    operations = {span.get("operationName") for span in trace.get("spans", [])}
    if (
        {"order-api", "inventory-api"}.issubset(services)
        and "inventory.reserve" in operations
        and "inventory.release" in operations
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
  echo "Jaeger did not return the expected compensation trace." >&2
  docker compose logs --tail=150 >&2 || true
  die "Trace validation failed; inspect $JAEGER_URL"
fi
echo "    Jaeger contains a cross-service reservation + compensation trace"

docker compose logs --no-log-prefix --no-color order-api inventory-api > "$TMP_DIR/service.log"
python3 - "$TMP_DIR/service.log" "$PROCESS_CORRELATION_ID" \
  "$DECLINE_CORRELATION_ID" "$INSUFFICIENT_CORRELATION_ID" <<'PY'
import json
import sys
from pathlib import Path

path, process_id, decline_id, insufficient_id = sys.argv[1:]
records = []
for line in Path(path).read_text().splitlines():
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    if record.get("correlation_id") in {
        process_id,
        decline_id,
        insufficient_id,
    }:
        records.append(record)

scenarios = (
    (
        process_id,
        {
            "order.process.started",
            "order.process.completed",
            "inventory.reservation.created",
        },
    ),
    (
        decline_id,
        {
            "order.process.failed",
            "inventory.reservation.created",
            "inventory.reservation.released",
        },
    ),
    (
        insufficient_id,
        {
            "order.process.failed",
            "inventory.reservation.rejected",
        },
    ),
)
for correlation_id, required_messages in scenarios:
    correlated = [
        record for record in records
        if record.get("correlation_id") == correlation_id
    ]
    services = {record.get("service") for record in correlated}
    messages = {record.get("message") for record in correlated}
    assert {"order-api", "inventory-api"}.issubset(services), (
        f"missing service logs for {correlation_id}: {services}"
    )
    assert required_messages.issubset(messages), (
        f"missing log events for {correlation_id}: {messages}"
    )
    for record in correlated:
        if record.get("message") in required_messages:
            assert record.get("trace_id"), f"missing trace_id: {record}"
            assert record.get("span_id"), f"missing span_id: {record}"

print(f"    correlated JSON log records: {len(records)}")
print("    success, compensation, and stock-rejection events correlated across both services")
PY

echo
echo "SUCCESS: Catalog, inventory, Saga, and observability reproduction passed."
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
