#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
TMP_DIR="$(mktemp -d)"

COMPOSE=(docker compose --profile standalone)
BASE_COMPOSE_FILE="${ROOT_DIR}/docker-compose.yml"

cleanup() {
  local status=$?
  trap - EXIT
  "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  "${COMPOSE[@]}" ps --all >&2 || true
  "${COMPOSE[@]}" logs --no-color --tail 120 \
    simulation-runner analytics-runner >&2 || true
  exit 1
}

run_expected_failure() {
  local label="$1"
  local expected_status="$2"
  local override_file="$3"
  local actual_status=0

  echo
  echo "==> ${label}"
  COMPOSE_FILE="${BASE_COMPOSE_FILE}:${override_file}" \
    STANDALONE_KEEP_STACK=1 \
    "${SCRIPT_DIR}/run-standalone.sh" || actual_status=$?

  if [[ "$actual_status" != "$expected_status" ]]; then
    fail "${label}: expected exit ${expected_status}, got ${actual_status}"
  fi
  echo "    wrapper returned expected non-zero status ${actual_status}"
}

# Case 1: a failed Simulation Runner must prevent Analytics Runner startup.
SIMULATION_OVERRIDE="${TMP_DIR}/simulation-failure.yml"
cat > "$SIMULATION_OVERRIDE" <<'YAML'
services:
  simulation-runner:
    command:
      - python
      - -c
      - "import sys; print('intentional simulation failure', file=sys.stderr); sys.exit(42)"
YAML

run_expected_failure \
  "Simulation failure blocks analytics" \
  42 \
  "$SIMULATION_OVERRIDE"

analytics_state_json="$(
  COMPOSE_FILE="${BASE_COMPOSE_FILE}:${SIMULATION_OVERRIDE}" \
    "${COMPOSE[@]}" ps --all --format json analytics-runner
)"

printf '%s\n' "$analytics_state_json" | python3 -c '
import json
import sys

rows = [json.loads(line) for line in sys.stdin if line.strip()]
started = [
    row for row in rows
    if row.get("Service") == "analytics-runner"
    and row.get("State") in {"running", "exited"}
]
if started:
    raise SystemExit(
        "Analytics Runner started despite the failed Simulation Runner: "
        + repr(started)
    )
print("    Analytics Runner was not started.")
'

COMPOSE_FILE="${BASE_COMPOSE_FILE}:${SIMULATION_OVERRIDE}" \
  "${COMPOSE[@]}" down --remove-orphans

# Case 2: replace only the dbt executable. The real Analytics Runner still
# validates the simulation report, invokes dbt, and writes its failure report.
DBT_BIN_DIR="${TMP_DIR}/dbt-bin"
mkdir -p "$DBT_BIN_DIR"
cat > "${DBT_BIN_DIR}/dbt" <<'SH'
#!/bin/sh
echo "intentional dbt failure from standalone integration test" >&2
exit 43
SH
chmod +x "${DBT_BIN_DIR}/dbt"

ANALYTICS_OVERRIDE="${TMP_DIR}/analytics-failure.yml"
cat > "$ANALYTICS_OVERRIDE" <<YAML
services:
  analytics-runner:
    environment:
      PATH: "/test-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/bin"
    volumes:
      - "${DBT_BIN_DIR}:/test-bin:ro"
YAML

run_expected_failure \
  "dbt failure propagates through Analytics Runner" \
  1 \
  "$ANALYTICS_OVERRIDE"

simulation_report="$(
  COMPOSE_FILE="${BASE_COMPOSE_FILE}:${ANALYTICS_OVERRIDE}" \
    "${COMPOSE[@]}" run --rm --no-deps \
      --entrypoint cat simulation-runner \
      /run-state/simulation-report.json
)"
analytics_report="$(
  COMPOSE_FILE="${BASE_COMPOSE_FILE}:${ANALYTICS_OVERRIDE}" \
    "${COMPOSE[@]}" run --rm --no-deps \
      --entrypoint cat analytics-runner \
      /analytics-data/run-report.json
)"

python3 - "$simulation_report" "$analytics_report" <<'PY'
import json
import sys

simulation = json.loads(sys.argv[1])
analytics = json.loads(sys.argv[2])

assert simulation["status"] == "succeeded", simulation
assert analytics["status"] == "failed", analytics
assert analytics["simulation_run_id"] == simulation["run_id"], analytics
assert analytics["exit_code"] == 43, analytics
assert analytics["runner_exit_code"] == 1, analytics
assert analytics["dbt_summary"] is None, analytics
assert "was not created" in analytics["dbt_artifact_error"], analytics
assert analytics["failure"], analytics

print("    Simulation report and Analytics failure report refer to the same run.")
print("    dbt exit code 43 is recorded; Analytics Runner exits 1.")
PY

echo
echo "PASS: both standalone orchestration failure paths behaved as expected."
echo "Named volumes were preserved; this test does not run 'docker compose down -v'."
