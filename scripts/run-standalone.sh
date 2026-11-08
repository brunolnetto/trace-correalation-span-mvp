#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(docker compose --profile standalone)
BUILD="${STANDALONE_BUILD:-1}"
KEEP_STACK="${STANDALONE_KEEP_STACK:-0}"
LOG_TAIL="${STANDALONE_LOG_TAIL:-120}"

case "$BUILD" in
  0|1) ;;
  *) echo "STANDALONE_BUILD must be 0 or 1" >&2; exit 2 ;;
esac

case "$KEEP_STACK" in
  0|1) ;;
  *) echo "STANDALONE_KEEP_STACK must be 0 or 1" >&2; exit 2 ;;
esac

cleanup() {
  local status=$?
  trap - EXIT

  if [[ "$KEEP_STACK" == "1" ]]; then
    echo "Keeping the Compose stack for inspection (STANDALONE_KEEP_STACK=1)."
  else
    echo "Stopping the standalone Compose stack; named volumes are preserved."
    if "${COMPOSE[@]}" down --remove-orphans; then
      :
    else
      local cleanup_status=$?
      echo "Warning: docker compose down failed with status ${cleanup_status}." >&2
      if (( status == 0 )); then
        status="$cleanup_status"
      fi
    fi
  fi

  exit "$status"
}
trap cleanup EXIT

show_diagnostics() {
  echo >&2
  echo "Standalone Compose state:" >&2
  "${COMPOSE[@]}" ps --all >&2 || true
  echo >&2
  echo "Finite runner logs (last ${LOG_TAIL} lines):" >&2
  "${COMPOSE[@]}" logs --no-color --tail "$LOG_TAIL" \
    simulation-runner analytics-runner >&2 || true
}

read_exit_code() {
  local service="$1"

  "${COMPOSE[@]}" ps --all --format json "$service" |
    python3 -c '
import json
import sys

service = sys.argv[1]
rows = [json.loads(line) for line in sys.stdin if line.strip()]
matches = [row for row in rows if row.get("Service") == service]

if len(matches) != 1:
    raise SystemExit(
        f"Expected exactly one container for {service}; found {len(matches)}"
    )

row = matches[0]
if row.get("State") != "exited":
    raise SystemExit(
        "{} has not exited (state={!r})".format(service, row.get("State"))
    )

exit_code = row.get("ExitCode")
if isinstance(exit_code, bool) or not isinstance(exit_code, int):
    raise SystemExit(f"{service} has an invalid exit code: {exit_code!r}")

print(exit_code)
' "$service"
}

wait_for_runner() {
  local service="$1"
  local wait_status=0
  local exit_code

  echo "Waiting for ${service} to finish..."
  # Compose wait only attaches to containers that are still running. If a
  # one-shot job already exited between up and this command, Compose reports
  # "no containers for project"; ps --all below is the source of truth.
  if "${COMPOSE[@]}" wait "$service" >/dev/null 2>&1; then
    :
  else
    wait_status=$?
  fi

  if ! exit_code="$(read_exit_code "$service")"; then
    echo "Could not determine ${service}'s container exit code (compose wait status: ${wait_status})." >&2
    return 1
  fi

  echo "${service} exit code: ${exit_code}"
  return "$exit_code"
}

up_args=(up --detach)
if [[ "$BUILD" == "1" ]]; then
  up_args+=(--build)
fi

echo "Starting the standalone Compose stack..."
if "${COMPOSE[@]}" "${up_args[@]}"; then
  :
else
  up_status=$?
  echo "docker compose up failed with status ${up_status}." >&2

  # A finite dependency may already have exited unsuccessfully while Compose
  # was resolving service_completed_successfully for the Analytics Runner.
  "${COMPOSE[@]}" wait simulation-runner >/dev/null 2>&1 || true
  simulation_status="$(read_exit_code simulation-runner 2>/dev/null || true)"

  if [[ "$simulation_status" =~ ^[0-9]+$ ]] && (( simulation_status != 0 )); then
    show_diagnostics
    exit "$simulation_status"
  fi

  if [[ "$simulation_status" == "0" ]]; then
    "${COMPOSE[@]}" wait analytics-runner >/dev/null 2>&1 || true
    analytics_status="$(read_exit_code analytics-runner 2>/dev/null || true)"
    if [[ "$analytics_status" =~ ^[0-9]+$ ]] && (( analytics_status != 0 )); then
      show_diagnostics
      exit "$analytics_status"
    fi
  fi

  show_diagnostics
  exit "$up_status"
fi

if wait_for_runner simulation-runner; then
  simulation_status=0
else
  simulation_status=$?
fi

if (( simulation_status != 0 )); then
  echo "Simulation Runner failed; Analytics Runner must not run." >&2
  show_diagnostics
  exit "$simulation_status"
fi

if wait_for_runner analytics-runner; then
  analytics_status=0
else
  analytics_status=$?
fi

if (( analytics_status != 0 )); then
  echo "Analytics Runner failed." >&2
  show_diagnostics
  exit "$analytics_status"
fi

echo
echo "Standalone execution completed successfully."
echo "  Simulation report: run-state volume /simulation-report.json"
echo "  Analytics report:  analytics-data volume /run-report.json"
echo "  dbt artifacts:     analytics-data volume /dbt-target"
echo "Named volumes are preserved when the containers are removed."
