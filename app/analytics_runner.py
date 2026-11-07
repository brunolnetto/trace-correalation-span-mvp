from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class AnalyticsRunnerError(RuntimeError):
    """Raised when analytics inputs or dbt outputs violate the runner contract."""


@dataclass(frozen=True)
class RunnerConfig:
    simulation_report_path: str = "/run-state/simulation-report.json"
    raw_events_path: str = "/raw/events.jsonl"
    duckdb_path: str = "/analytics-data/trace_events.duckdb"
    target_path: str = "/analytics-data/dbt-target"
    report_path: str = "/analytics-data/run-report.json"
    project_dir: str = "/workspace"
    profiles_dir: str = "/workspace"
    dbt_executable: str = "dbt"
    timeout_seconds: float = 3600.0

    def __post_init__(self) -> None:
        for name in (
            "simulation_report_path",
            "raw_events_path",
            "duckdb_path",
            "target_path",
            "report_path",
            "project_dir",
            "profiles_dir",
            "dbt_executable",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _load_simulation_report(path: str) -> dict[str, Any]:
    report_path = Path(path)
    if not report_path.is_file():
        raise AnalyticsRunnerError(
            f"simulation report does not exist: {report_path}"
        )
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AnalyticsRunnerError(
            f"simulation report is unreadable or invalid JSON: {exc}"
        ) from exc
    if not isinstance(report, dict):
        raise AnalyticsRunnerError("simulation report must be a JSON object")

    run_id = report.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise AnalyticsRunnerError("simulation report has no valid run_id")
    if report.get("schema_version") != 1:
        raise AnalyticsRunnerError("unsupported simulation report schema_version")
    if report.get("status") != "succeeded":
        raise AnalyticsRunnerError(
            f"simulation did not succeed (status={report.get('status')!r})"
        )

    assertions = report.get("assertions")
    if (
        not isinstance(assertions, list)
        or not assertions
        or any(
            not isinstance(assertion, dict) or assertion.get("passed") is not True
            for assertion in assertions
        )
    ):
        raise AnalyticsRunnerError(
            "simulation report does not contain a fully successful assertion set"
        )

    barrier = report.get("collector_barrier")
    if not isinstance(barrier, dict):
        raise AnalyticsRunnerError("simulation report has no Collector barrier")
    expected = barrier.get("expected_count")
    observed = barrier.get("observed_count")
    if (
        barrier.get("status") != "complete"
        or barrier.get("complete") is not True
        or not isinstance(expected, int)
        or isinstance(expected, bool)
        or expected < 1
        or observed != expected
        or barrier.get("missing") != []
    ):
        raise AnalyticsRunnerError(
            "Collector barrier is incomplete or inconsistent "
            f"(status={barrier.get('status')!r}, expected={expected!r}, "
            f"observed={observed!r})"
        )
    return report


def _parse_dbt_summary(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise AnalyticsRunnerError(f"dbt run_results.json was not created: {path}")
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AnalyticsRunnerError(f"dbt run_results.json is invalid: {exc}") from exc
    if not isinstance(artifact, dict) or not isinstance(artifact.get("results"), list):
        raise AnalyticsRunnerError(
            "dbt run_results.json must contain a results array"
        )

    counts: dict[str, int] = {}
    for result in artifact["results"]:
        if not isinstance(result, dict) or not isinstance(result.get("status"), str):
            raise AnalyticsRunnerError(
                "dbt run_results.json contains a result without a status"
            )
        status = result["status"]
        counts[status] = counts.get(status, 0) + 1

    return {
        "total": len(artifact["results"]),
        "status_counts": counts,
        "elapsed_time": artifact.get("elapsed_time"),
        "dbt_schema_version": artifact.get("metadata", {}).get("dbt_schema_version")
        if isinstance(artifact.get("metadata"), dict)
        else None,
    }


def _write_report(report: dict[str, Any], path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)


def run_analytics(config: RunnerConfig) -> dict[str, Any]:
    started_at = _now()
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
        "simulation_run_id": None,
        "simulation_report_path": config.simulation_report_path,
        "raw_events_path": config.raw_events_path,
        "raw_events_size_bytes": None,
        "command": None,
        "exit_code": None,
        "runner_exit_code": 1,
        "dbt_summary": None,
        "dbt_run_results_path": str(Path(config.target_path) / "run_results.json"),
        "failure": None,
    }

    try:
        simulation = _load_simulation_report(config.simulation_report_path)
        report["simulation_run_id"] = simulation["run_id"]

        raw_events = Path(config.raw_events_path)
        if not raw_events.is_file():
            raise AnalyticsRunnerError(
                f"Collector raw-event file does not exist: {raw_events}"
            )
        raw_size = raw_events.stat().st_size
        if raw_size <= 0:
            raise AnalyticsRunnerError(
                f"Collector raw-event file is empty: {raw_events}"
            )
        report["raw_events_size_bytes"] = raw_size

        target_path = Path(config.target_path)
        target_path.mkdir(parents=True, exist_ok=True)
        Path(config.duckdb_path).parent.mkdir(parents=True, exist_ok=True)

        command = [
            config.dbt_executable,
            "build",
            "--project-dir",
            config.project_dir,
            "--profiles-dir",
            config.profiles_dir,
            "--target-path",
            config.target_path,
        ]
        report["command"] = command
        environment = os.environ.copy()
        environment["RAW_EVENTS_PATH"] = str(raw_events)
        environment["DUCKDB_PATH"] = config.duckdb_path

        try:
            completed = subprocess.run(
                command,
                cwd=config.project_dir,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
                timeout=config.timeout_seconds,
            )
            report["exit_code"] = completed.returncode
            stdout = completed.stdout or ""
            stderr = completed.stderr or ""
        except subprocess.TimeoutExpired as exc:
            report["exit_code"] = 124
            stdout = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            report["failure"] = {
                "type": "TimeoutExpired",
                "message": f"dbt build exceeded {config.timeout_seconds} seconds",
                "stdout_tail": stdout[-4000:],
                "stderr_tail": stderr[-4000:],
            }
        except FileNotFoundError as exc:
            report["exit_code"] = 127
            report["failure"] = {
                "type": type(exc).__name__,
                "message": f"dbt executable was not found: {config.dbt_executable}",
            }
        except OSError as exc:
            report["exit_code"] = 1
            report["failure"] = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
        else:
            report["stdout_tail"] = stdout[-4000:]
            report["stderr_tail"] = stderr[-4000:]

        try:
            report["dbt_summary"] = _parse_dbt_summary(
                Path(config.target_path) / "run_results.json"
            )
        except AnalyticsRunnerError as exc:
            if report["failure"] is None:
                report["failure"] = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                }

        if report["exit_code"] != 0 and report["failure"] is None:
            report["failure"] = {
                "type": "DbtBuildError",
                "message": f"dbt build exited with status {report['exit_code']}",
            }

        if report["exit_code"] == 0 and report["failure"] is None:
            report["status"] = "succeeded"
            report["runner_exit_code"] = 0
        else:
            report["status"] = "failed"
    except (AnalyticsRunnerError, OSError, ValueError) as exc:
        report["status"] = "failed"
        report["failure"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
    finally:
        report["finished_at"] = _now()

    return report


def _config_from_environment() -> RunnerConfig:
    return RunnerConfig(
        simulation_report_path=os.getenv(
            "SIMULATION_REPORT_PATH", "/run-state/simulation-report.json"
        ),
        raw_events_path=os.getenv("RAW_EVENTS_PATH", "/raw/events.jsonl"),
        duckdb_path=os.getenv("DUCKDB_PATH", "/analytics-data/trace_events.duckdb"),
        target_path=os.getenv("DBT_TARGET_PATH", "/analytics-data/dbt-target"),
        report_path=os.getenv(
            "ANALYTICS_REPORT_PATH", "/analytics-data/run-report.json"
        ),
        project_dir=os.getenv("DBT_PROJECT_DIR", "/workspace"),
        profiles_dir=os.getenv("DBT_PROFILES_DIR", "/workspace"),
        dbt_executable=os.getenv("DBT_EXECUTABLE", "dbt"),
        timeout_seconds=float(os.getenv("ANALYTICS_TIMEOUT_SECONDS", "3600")),
    )


def main() -> int:
    try:
        config = _config_from_environment()
        report = run_analytics(config)
        _write_report(report, config.report_path)
    except (OSError, ValueError) as exc:
        print(f"analytics-runner could not write its report: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(report, indent=2, sort_keys=True))
    return int(report["runner_exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
