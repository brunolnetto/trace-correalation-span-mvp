from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from app.analytics_runner import RunnerConfig, _write_report, main, run_analytics


def _simulation_report() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": "simulation-run-123",
        "status": "succeeded",
        "assertions": [{"name": "scenario invariant", "passed": True}],
        "collector_barrier": {
            "status": "complete",
            "complete": True,
            "expected_count": 55,
            "observed_count": 55,
            "missing": [],
        },
    }


def _config(tmp_path: Path) -> RunnerConfig:
    run_state = tmp_path / "run-state"
    raw = tmp_path / "raw"
    analytics = tmp_path / "analytics"
    run_state.mkdir()
    raw.mkdir()
    analytics.mkdir()
    (run_state / "simulation-report.json").write_text(
        json.dumps(_simulation_report()), encoding="utf-8"
    )
    (raw / "events.jsonl").write_text('{"event_name":"example"}\n', encoding="utf-8")
    return RunnerConfig(
        simulation_report_path=str(run_state / "simulation-report.json"),
        raw_events_path=str(raw / "events.jsonl"),
        duckdb_path=str(analytics / "trace_events.duckdb"),
        target_path=str(analytics / "dbt-target"),
        report_path=str(analytics / "run-report.json"),
        project_dir=str(tmp_path),
        profiles_dir=str(tmp_path),
    )


def _run_results(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "run_results.json").write_text(
        json.dumps(
            {
                "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"},
                "elapsed_time": 1.25,
                "results": [
                    {"status": "pass"},
                    {"status": "pass"},
                    {"status": "warn"},
                ],
            }
        ),
        encoding="utf-8",
    )


def test_analytics_runner_builds_and_writes_machine_readable_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    calls: list[dict[str, Any]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append({"command": command, **kwargs})
        _run_results(Path(config.target_path))
        return subprocess.CompletedProcess(command, 0, "dbt succeeded", "")

    monkeypatch.setattr("app.analytics_runner.subprocess.run", fake_run)

    report = run_analytics(config)
    _write_report(report, config.report_path)

    assert report["status"] == "succeeded"
    assert report["simulation_run_id"] == "simulation-run-123"
    assert report["exit_code"] == 0
    assert report["runner_exit_code"] == 0
    assert report["dbt_summary"] == {
        "total": 3,
        "status_counts": {"pass": 2, "warn": 1},
        "elapsed_time": 1.25,
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
    }
    assert calls[0]["command"][:2] == ["dbt", "build"]
    assert calls[0]["env"]["RAW_EVENTS_PATH"] == config.raw_events_path
    assert calls[0]["env"]["DUCKDB_PATH"] == config.duckdb_path
    assert json.loads(Path(config.report_path).read_text())["status"] == "succeeded"


def test_runner_refuses_failed_simulation_without_invoking_dbt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    report_path = Path(config.simulation_report_path)
    simulation = _simulation_report()
    simulation["status"] = "failed"
    report_path.write_text(json.dumps(simulation), encoding="utf-8")

    def unexpected_run(*args: Any, **kwargs: Any) -> None:
        pytest.fail("dbt must not run for a failed simulation")

    monkeypatch.setattr("app.analytics_runner.subprocess.run", unexpected_run)
    report = run_analytics(config)

    assert report["status"] == "failed"
    assert report["exit_code"] is None
    assert "simulation did not succeed" in report["failure"]["message"]


def test_runner_refuses_incomplete_collector_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    simulation = _simulation_report()
    simulation["collector_barrier"]["missing"] = [
        {"correlation_id": "order-1", "event_name": "order.process.completed"}
    ]
    Path(config.simulation_report_path).write_text(
        json.dumps(simulation), encoding="utf-8"
    )

    monkeypatch.setattr(
        "app.analytics_runner.subprocess.run",
        lambda *args, **kwargs: pytest.fail("dbt must not run before barrier completion"),
    )
    report = run_analytics(config)

    assert report["status"] == "failed"
    assert "Collector barrier is incomplete" in report["failure"]["message"]


def test_dbt_failure_preserves_exit_code_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        _run_results(Path(config.target_path))
        return subprocess.CompletedProcess(command, 2, "partial output", "dbt error")

    monkeypatch.setattr("app.analytics_runner.subprocess.run", fake_run)
    report = run_analytics(config)
    _write_report(report, config.report_path)

    assert report["status"] == "failed"
    assert report["exit_code"] == 2
    assert report["runner_exit_code"] == 1
    assert report["dbt_summary"]["total"] == 3
    assert Path(config.report_path).is_file()


def test_dbt_failure_does_not_reuse_previous_run_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    target = Path(config.target_path)
    _run_results(target)
    old_results = target / "run_results.json"
    assert old_results.is_file()

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command, 43, "", "intentional dbt failure"
        )

    monkeypatch.setattr("app.analytics_runner.subprocess.run", fake_run)
    report = run_analytics(config)

    assert report["status"] == "failed"
    assert report["exit_code"] == 43
    assert report["runner_exit_code"] == 1
    assert report["dbt_summary"] is None
    assert "was not created" in report["dbt_artifact_error"]
    assert report["failure"]["type"] == "DbtBuildError"
    assert "status 43" in report["failure"]["message"]
    assert not old_results.exists()


@pytest.mark.parametrize(
    ("artifact_content", "expected_message"),
    [
        (None, "was not created"),
        ("{not-json", "is invalid"),
        ('{"results": "not-a-list"}', "must contain a results array"),
        ('{"results": [{"unique_id": "model.x"}]}', "without a status"),
    ],
)
def test_successful_dbt_process_with_invalid_artifact_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_content: str | None,
    expected_message: str,
) -> None:
    config = _config(tmp_path)

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        target = Path(config.target_path)
        target.mkdir(parents=True, exist_ok=True)
        if artifact_content is not None:
            (target / "run_results.json").write_text(
                artifact_content, encoding="utf-8"
            )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr("app.analytics_runner.subprocess.run", fake_run)
    report = run_analytics(config)

    assert report["status"] == "failed"
    assert report["exit_code"] == 0
    assert expected_message in report["failure"]["message"]


def test_missing_simulation_report_fails_without_running_dbt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    missing = tmp_path / "missing-simulation-report.json"
    config = RunnerConfig(**{**config.__dict__, "simulation_report_path": str(missing)})

    monkeypatch.setattr(
        "app.analytics_runner.subprocess.run",
        lambda *args, **kwargs: pytest.fail("dbt must not run without simulation report"),
    )
    report = run_analytics(config)

    assert report["status"] == "failed"
    assert "does not exist" in report["failure"]["message"]


def test_missing_raw_events_fails_before_running_dbt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    config = RunnerConfig(**{**config.__dict__, "raw_events_path": str(tmp_path / "missing.jsonl")})

    monkeypatch.setattr(
        "app.analytics_runner.subprocess.run",
        lambda *args, **kwargs: pytest.fail("dbt must not run without persisted raw events"),
    )
    report = run_analytics(config)

    assert report["status"] == "failed"
    assert "does not exist" in report["failure"]["message"]


def test_main_writes_failure_report_when_dbt_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    report_path = Path(config.report_path)

    monkeypatch.setenv("SIMULATION_REPORT_PATH", config.simulation_report_path)
    monkeypatch.setenv("RAW_EVENTS_PATH", config.raw_events_path)
    monkeypatch.setenv("DUCKDB_PATH", config.duckdb_path)
    monkeypatch.setenv("DBT_TARGET_PATH", config.target_path)
    monkeypatch.setenv("ANALYTICS_REPORT_PATH", str(report_path))
    monkeypatch.setenv("DBT_PROJECT_DIR", config.project_dir)
    monkeypatch.setenv("DBT_PROFILES_DIR", config.profiles_dir)
    monkeypatch.setattr(
        "app.analytics_runner.subprocess.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError("dbt")),
    )

    assert main() == 1
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["status"] == "failed"
    assert saved["exit_code"] == 127
    assert saved["failure"]["type"] == "FileNotFoundError"
