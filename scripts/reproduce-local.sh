#!/usr/bin/env bash
#
# Reproduce the catalog, order, supplier, raw-event and observability flow:
# uv tests/lint -> start services -> order scenarios -> supplier replenishment ->
# generator coverage -> Jaeger/log/raw-event verification -> dbt build.
#
# Usage:
#   ./scripts/reproduce-local.sh
#   ./scripts/reproduce-local.sh --cleanup
#
set -Eeuo pipefail

# Isolate each reproduction run so named volumes cannot leak application state
# across runs. --cleanup therefore removes only this run's disposable volumes.
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-trace-correalation-span-mvp}-repro-${BASHPID}"
export COMPOSE_PROJECT_NAME

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REPRO_CORRELATION_IDS=()
ORDER_API_HOST_PORT="${ORDER_API_HOST_PORT:-18000}"
INVENTORY_API_HOST_PORT="${INVENTORY_API_HOST_PORT:-18001}"
SUPPLIER_HOST_PORT="${SUPPLIER_HOST_PORT:-18002}"
EVENT_COLLECTOR_HOST_PORT="${EVENT_COLLECTOR_HOST_PORT:-18003}"
JAEGER_UI_HOST_PORT="${JAEGER_UI_HOST_PORT:-16686}"
GRAFANA_HOST_PORT="${GRAFANA_HOST_PORT:-13000}"
LOKI_HOST_PORT="${LOKI_HOST_PORT:-13100}"
OTLP_GRPC_HOST_PORT="${OTLP_GRPC_HOST_PORT:-4317}"
OTLP_HTTP_HOST_PORT="${OTLP_HTTP_HOST_PORT:-4318}"
OTEL_HEALTH_HOST_PORT="${OTEL_HEALTH_HOST_PORT:-13133}"
export ORDER_API_HOST_PORT INVENTORY_API_HOST_PORT SUPPLIER_HOST_PORT
export EVENT_COLLECTOR_HOST_PORT JAEGER_UI_HOST_PORT GRAFANA_HOST_PORT LOKI_HOST_PORT
export OTLP_GRPC_HOST_PORT OTLP_HTTP_HOST_PORT OTEL_HEALTH_HOST_PORT
ORDER_URL="${ORDER_URL:-http://127.0.0.1:${ORDER_API_HOST_PORT}}"
INVENTORY_URL="${INVENTORY_URL:-http://127.0.0.1:${INVENTORY_API_HOST_PORT}}"
JAEGER_URL="${JAEGER_URL:-http://127.0.0.1:${JAEGER_UI_HOST_PORT}}"
GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:${GRAFANA_HOST_PORT}}"
LOKI_URL="${LOKI_URL:-http://127.0.0.1:${LOKI_HOST_PORT}}"
OTEL_HEALTH_URL="${OTEL_HEALTH_URL:-http://127.0.0.1:${OTEL_HEALTH_HOST_PORT}}"
SUPPLIER_URL="${SUPPLIER_URL:-http://127.0.0.1:${SUPPLIER_HOST_PORT}}"
EVENT_COLLECTOR_URL="${EVENT_COLLECTOR_URL:-http://127.0.0.1:${EVENT_COLLECTOR_HOST_PORT}}"
export POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-15432}"
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
mapfile -t REPRO_CORRELATION_IDS < <(
  python3 - <<'PY'
import uuid

for _ in range(5):
    print(uuid.uuid4())
PY
)
[[ ${#REPRO_CORRELATION_IDS[@]} -eq 5 ]] || die "Could not generate unique reproduction correlation IDs"
READ_CORRELATION_ID="${REPRO_CORRELATION_IDS[0]}"
PROCESS_CORRELATION_ID="${REPRO_CORRELATION_IDS[1]}"
DECLINE_CORRELATION_ID="${REPRO_CORRELATION_IDS[2]}"
INSUFFICIENT_CORRELATION_ID="${REPRO_CORRELATION_IDS[3]}"
SUPPLIER_CORRELATION_ID="${REPRO_CORRELATION_IDS[4]}"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required"
docker info >/dev/null 2>&1 || die "Docker daemon is not reachable"

TMP_DIR="$(mktemp -d)"
cleanup_tmp() { rm -rf "$TMP_DIR"; }
trap cleanup_tmp EXIT
if (( CLEANUP )); then
  trap 'docker compose down --volumes --remove-orphans || true; cleanup_tmp' EXIT
fi

echo "    isolated Compose project: $COMPOSE_PROJECT_NAME"
echo "==> 1/7: Resolve and install Python dependencies"
uv lock --python 3.12
uv sync --python 3.12 --all-groups --locked
[[ -f uv.lock ]] || die "uv did not create uv.lock"
if ! git ls-files --error-unmatch uv.lock >/dev/null 2>&1; then
  echo "NOTE: uv.lock is generated locally but is not committed. Review and commit it for repeatable installs."
fi

echo "==> 2/7: Start PostgreSQL and run unit/integration tests"
docker compose up --detach --wait postgres
POSTGRES_TEST_URL="${POSTGRES_TEST_URL:-postgresql://app:app@127.0.0.1:${POSTGRES_HOST_PORT}/app}" \
  uv run --python 3.12 pytest

echo "==> 3/7: Run Ruff"
uv run --python 3.12 ruff check .

echo "==> 4/7: Migrate PostgreSQL and start Jaeger, APIs, supplier, and event collector"
docker compose down --remove-orphans
docker compose up --detach --wait postgres
bash scripts/migrate.sh
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

wait_for_internal_service() {
  local service="$1"
  local health_url="$2"
  local deadline=$((SECONDS + WAIT_SECONDS))
  until docker compose exec --no-TTY "$service" python -c \
    "import urllib.request; urllib.request.urlopen('$health_url', timeout=2)" \
    >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      docker compose ps >&2 || true
      docker compose logs --tail=100 "$service" >&2 || true
      die "Timed out waiting for $service at $health_url"
    fi
    sleep 2
  done
  echo "    ready: $service"
}

echo "    waiting for service health endpoints (up to ${WAIT_SECONDS}s)"
wait_for_url "$INVENTORY_URL/healthz" "inventory-api"
wait_for_url "$ORDER_URL/healthz" "order-api"
wait_for_url "$JAEGER_URL/" "Jaeger UI"
wait_for_url "$LOKI_URL/ready" "Loki"
wait_for_url "$GRAFANA_URL/api/health" "Grafana"
wait_for_url "$OTEL_HEALTH_URL/" "OpenTelemetry Collector"
wait_for_url "$EVENT_COLLECTOR_URL/healthz" "event-collector"
wait_for_url "$SUPPLIER_URL/healthz" "supplier-simulator"
wait_for_internal_service "outbox-dispatcher" "http://127.0.0.1:8011/healthz"

echo "==> 5/7: Exercise catalog, orders, and supplier replenishment"
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
  --data '{"product_id":7,"quantity":2}' \
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
  --data '{"product_id":7,"quantity":11}' \
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
  --data "{\"product_id\":$LOW_STOCK_PRODUCT_ID,\"quantity\":5}" \
  "$ORDER_URL/orders/125/process")"
[[ "$INSUFFICIENT_STATUS" == "409" ]] || {
  cat "$TMP_DIR/insufficient.json" >&2
  die "Expected insufficient inventory to return 409, got $INSUFFICIENT_STATUS"
}

echo "    exercising supplier onboarding, replenishment, and receipt"
curl --silent --show-error --fail \
  --header "X-Correlation-ID: $SUPPLIER_CORRELATION_ID" \
  --output "$TMP_DIR/supplier-onboard.json" \
  --request POST "$SUPPLIER_URL/supplier-catalog/BEANS-250/onboard"

SUPPLIER_PRODUCT_ID="$(python3 - "$TMP_DIR/supplier-onboard.json" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text())["product"]["product_id"])
PY
)"

curl --silent --show-error --fail \
  --header "X-Correlation-ID: $SUPPLIER_CORRELATION_ID" \
  --request POST \
  "$SUPPLIER_URL/replenishments/evaluate?product_id=$SUPPLIER_PRODUCT_ID" \
  > "$TMP_DIR/supplier-decision.json"

PURCHASE_ORDER_ID="$(python3 - "$TMP_DIR/supplier-decision.json" <<'PY'
import json
import sys
from pathlib import Path

decision = json.loads(Path(sys.argv[1]).read_text())["decisions"][0]
assert decision["decision"] == "purchase_order_created", decision
print(decision["purchase_order"]["purchase_order_id"])
PY
)"

curl --silent --show-error --fail \
  --header "X-Correlation-ID: $SUPPLIER_CORRELATION_ID" \
  --request POST \
  "$SUPPLIER_URL/purchase-orders/$PURCHASE_ORDER_ID/order" \
  > "$TMP_DIR/supplier-ordered.json"

curl --silent --show-error --fail \
  --header "Content-Type: application/json" \
  --header "X-Correlation-ID: $SUPPLIER_CORRELATION_ID" \
  --data '{"seconds":3600}' \
  "$SUPPLIER_URL/clock/advance" > "$TMP_DIR/supplier-clock.json"

curl --silent --show-error --fail \
  --header "X-Correlation-ID: $SUPPLIER_CORRELATION_ID" \
  --request POST \
  "$SUPPLIER_URL/purchase-orders/$PURCHASE_ORDER_ID/receive" \
  > "$TMP_DIR/supplier-received.json"

curl --silent --show-error --fail \
  --header "X-Correlation-ID: $SUPPLIER_CORRELATION_ID" \
  --request POST \
  "$SUPPLIER_URL/replenishments/evaluate?product_id=$SUPPLIER_PRODUCT_ID" \
  > "$TMP_DIR/supplier-recheck.json"

curl --silent --show-error --fail \
  --output "$TMP_DIR/supplier-inventory.json" \
  "$INVENTORY_URL/inventory/$SUPPLIER_PRODUCT_ID"

python3 - "$TMP_DIR/supplier-onboard.json" \
  "$TMP_DIR/supplier-decision.json" "$TMP_DIR/supplier-ordered.json" \
  "$TMP_DIR/supplier-received.json" "$TMP_DIR/supplier-recheck.json" \
  "$TMP_DIR/supplier-inventory.json" "$SUPPLIER_PRODUCT_ID" <<'PY'
import json
import sys
from pathlib import Path

onboard, decision, ordered, received, recheck, inventory = (
    json.loads(Path(path).read_text()) for path in sys.argv[1:7]
)
product_id = sys.argv[7]
assert onboard["product"]["product_id"] == int(product_id)
assert decision["decisions"][0]["decision"] == "purchase_order_created"
purchase_order = decision["decisions"][0]["purchase_order"]
assert purchase_order["quantity"] == 48
assert ordered["status"] == "ordered"
assert received["status"] == "received"
assert received["receipt_id"]
assert recheck["decisions"][0]["decision"] == "no_reorder"
assert inventory["on_hand"] == 48, inventory
assert inventory["reserved"] == 0, inventory
assert inventory["available_quantity"] == 48, inventory
print("    supplier purchase order received 48 units exactly once")
print("    inventory position above reorder point; no second PO created")
PY

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
assert process["unit_price"] == "99.90", process
assert process["total_amount"] == "199.80", process
assert decline["detail"] == (
    "Payment declined; completed actions were compensated."
), decline
assert inventory["on_hand"] == 42, inventory
assert inventory["reserved"] == 2, inventory
assert inventory["available_quantity"] == 40, inventory
assert inventory["quantity"] == 40, inventory
assert insufficient["detail"] == (
    "Inventory service rejected the reservation."
), insufficient
assert low_stock["product_id"] == int(low_stock_product_id), low_stock
assert low_stock["on_hand"] == 4, low_stock
assert low_stock["reserved"] == 0, low_stock
assert low_stock["available_quantity"] == 4, low_stock
print(f"    successful saga ID: {process['saga_id']}")
print(f"    successful order correlation ID: {process_id}")
print(f"    declined order correlation ID: {decline_id}")
print(f"    low-stock product ID: {low_stock_product_id}")
print("    inventory after compensation: on_hand=42 reserved=2 available=40")
print("    insufficient-stock order rejected without changing inventory")
PY

echo "    running generator coverage and publishing attempts to raw storage"
docker compose --profile simulation run --build --no-deps --rm order-generator \
  python -m app.order_generator --mode coverage \
  > "$TMP_DIR/generator.log"

echo "==> 6/7: Verify JSON logs, Saga traces, and raw event collection"
docker compose logs --no-log-prefix --no-color order-api inventory-api > "$TMP_DIR/service.log"

# Resolve the exact trace from the declined request's correlation ID. Querying
# Jaeger's first N traces is lossy because generator/collector traffic can push
# the manual saga trace out of the capped result set.
DECLINE_TRACE_ID="$(python3 - "$TMP_DIR/service.log" "$DECLINE_CORRELATION_ID" <<'PY'
import json
import sys
from pathlib import Path

path, correlation_id = sys.argv[1:]
for line in Path(path).read_text().splitlines():
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    if (
        record.get("correlation_id") == correlation_id
        and record.get("message") == "order.process.failed"
        and record.get("trace_id")
    ):
        print(record["trace_id"])
        break
PY
)"
[[ -n "$DECLINE_TRACE_ID" ]] || die "Could not resolve trace ID for declined order $DECLINE_CORRELATION_ID"

TRACE_URL="$JAEGER_URL/api/traces/$DECLINE_TRACE_ID"
TRACE_FOUND=0
deadline=$((SECONDS + WAIT_SECONDS))
while (( SECONDS < deadline )); do
  if curl --silent --show-error --fail "$TRACE_URL" -o "$TMP_DIR/trace.json"; then
    if python3 - "$TMP_DIR/trace.json" <<'PY'
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
  echo "Jaeger did not return the expected compensation trace $DECLINE_TRACE_ID." >&2
  docker compose logs --tail=150 >&2 || true
  die "Trace validation failed; inspect $TRACE_URL"
fi
echo "    Jaeger contains declined-order trace $DECLINE_TRACE_ID with cross-service reservation + compensation"

# Search Loki by the same business correlation ID used to find the trace.
# Correlation ID is structured metadata, not an indexed label.
LOKI_QUERY="{service_name=\\"order-api\\"} | correlation_id=\\"$DECLINE_CORRELATION_ID\\""
LOKI_FOUND=0
deadline=$((SECONDS + WAIT_SECONDS))
while (( SECONDS < deadline )); do
  if curl --silent --show-error --fail --get \\
    --data-urlencode "query=$LOKI_QUERY" \\
    --data-urlencode "limit=1000" \\
    "$LOKI_URL/loki/api/v1/query" -o "$TMP_DIR/loki-query.json"; then
    if python3 - "$TMP_DIR/loki-query.json" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text())
streams = payload.get("data", {}).get("result", [])
messages = [
    line
    for stream in streams
    for _, line in stream.get("values", [])
]
if any("order.process.failed" in line for line in messages):
    print(f"    Loki returned {len(messages)} records for the declined-order correlation ID")
    raise SystemExit(0)
raise SystemExit(1)
PY
    then
      LOKI_FOUND=1
      break
    fi
  fi
  sleep 2
done
(( LOKI_FOUND )) || {
  echo "Loki query failed: $LOKI_QUERY" >&2
  cat "$TMP_DIR/loki-query.json" >&2 || true
  die "Could not find the declined-order diagnostic log in Loki"
}
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
deadline=$((SECONDS + WAIT_SECONDS))
COLLECTED=0
while (( SECONDS < deadline )); do
  if curl --silent --show-error --fail \
    "$EVENT_COLLECTOR_URL/v1/events?limit=10000" \
    -o "$TMP_DIR/collected-events.json"; then
    if python3 - "$TMP_DIR/collected-events.json" \
      "$PROCESS_CORRELATION_ID" "$DECLINE_CORRELATION_ID" \
      "$INSUFFICIENT_CORRELATION_ID" "$SUPPLIER_CORRELATION_ID" \
      "$TMP_DIR/generator.log" <<'PY'
import json
import sys
from pathlib import Path

path, process_id, decline_id, insufficient_id, supplier_id, generator_log = sys.argv[1:]
payload = json.loads(Path(path).read_text())
events = [record["event"] for record in payload.get("events", [])]
attempt_records = []
summary_records = []
for line in Path(generator_log).read_text().splitlines():
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    if record.get("event") == "order_generator.attempt":
        attempt_records.append(record)
    elif record.get("event") == "order_generator.summary":
        summary_records.append(record)
assert len(attempt_records) == 3, attempt_records
assert len(summary_records) == 1, summary_records
generator_run_id = summary_records[0]["run_id"]
assert {record["run_id"] for record in attempt_records} == {generator_run_id}
assert {record["outcome"] for record in attempt_records} == {
    "processed",
    "payment_declined",
    "inventory_rejected",
}

required = {
    process_id: {
        "order.process.started",
        "order.process.completed",
        "inventory.reservation.created",
    },
    decline_id: {
        "order.process.failed",
        "inventory.reservation.created",
        "inventory.reservation.released",
    },
    insufficient_id: {
        "order.process.failed",
        "inventory.reservation.rejected",
    },
    supplier_id: {
        "supplier.product.onboarded",
        "supplier.replenishment.evaluated",
        "supplier.purchase_order.ordered",
        "supplier.purchase_order.received",
        "inventory.stock.received",
    },
}
for attempt in attempt_records:
    required[attempt["correlation_id"]] = {
        "order_generator.attempt",
        "order.process.started",
        "order.process.completed"
        if attempt["outcome"] == "processed"
        else "order.process.failed",
    }
    if attempt["outcome"] == "payment_declined":
        required[attempt["correlation_id"]].update(
            {
                "inventory.reservation.created",
                "inventory.reservation.released",
            }
        )
    elif attempt["outcome"] == "inventory_rejected":
        required[attempt["correlation_id"]].add(
            "inventory.reservation.rejected"
        )
    else:
        required[attempt["correlation_id"]].add(
            "inventory.reservation.created"
        )
for correlation_id, messages in required.items():
    correlated = [
        event for event in events
        if event.get("correlation_id") == correlation_id
    ]
    found = {event.get("event_name") for event in correlated}
    services = {event.get("service") for event in correlated}
    if not messages.issubset(found):
        raise SystemExit(1)
    if correlation_id == supplier_id:
        required_services = {"supplier-simulator", "inventory-api"}
    elif correlation_id in {
        attempt["correlation_id"] for attempt in attempt_records
    }:
        required_services = {"order-generator", "order-api", "inventory-api"}
    else:
        required_services = {"order-api", "inventory-api"}
    if not required_services.issubset(services):
        raise SystemExit(1)
    for event in correlated:
        if (
            event.get("event_name") in messages
            and event.get("service") != "order-generator"
        ):
            if not event.get("trace_id") or not event.get("span_id"):
                raise SystemExit(1)
print(f"    raw events persisted: {len(events)}")
print("    order, supplier, and generator events persisted and correlated")
PY
    then
      COLLECTED=1
      break
    fi
  fi
  sleep 1
done

if (( ! COLLECTED )); then
  curl --silent --show-error "$EVENT_COLLECTOR_URL/v1/stats" >&2 || true
  die "Raw event collector did not persist all expected scenario events"
fi

echo "==> 7/7: Build DuckDB analytics from the collected event stream"
docker compose --profile analytics run --build --rm analytics

echo "SUCCESS: Catalog, inventory, Saga, supplier, generator, observability, and dbt reproduction passed."
echo "  Order API:     $ORDER_URL"
echo "  Inventory API: $INVENTORY_URL"
echo "  Jaeger UI:     $JAEGER_URL"
echo "  Supplier API:  $SUPPLIER_URL"
echo "  Event API:     $EVENT_COLLECTOR_URL"
echo
if (( CLEANUP )); then
  echo "The Compose stack will be stopped automatically."
else
  echo "The Compose stack is still running so you can inspect Jaeger and logs."
  echo "Stop it with: docker compose down"
fi
