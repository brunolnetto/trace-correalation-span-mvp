from __future__ import annotations

import json
from pathlib import Path

import pytest

MATRIX_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "flow-coverage-matrix.json"
)

COVERAGE_ASSESSMENTS = {"covered", "partial", "gap"}
AUTOMATED_STATES = {"present", "partial", "gap", "documented", "none"}
LIVE_STATES = {
    "reported_pass_open_pr",
    "reproducible_script_only",
    "documented_only",
    "not_required",
    "none",
}


@pytest.fixture(scope="module")
def matrix() -> dict:
    return json.loads(MATRIX_PATH.read_text(encoding="utf-8"))


def _unique_ids(items: list[dict], collection: str) -> set[str]:
    ids = [item["id"] for item in items]
    assert len(ids) == len(set(ids)), f"duplicate IDs in {collection}"
    return set(ids)


def test_manifest_declares_schema_and_snapshot(matrix: dict) -> None:
    assert matrix["schema_version"]
    assert matrix["snapshot"]["repository"] == (
        "brunolnetto/trace-correalation-span-mvp"
    )
    assert matrix["snapshot"]["base_commit"]


def test_node_and_edge_references_are_valid(matrix: dict) -> None:
    node_ids = _unique_ids(matrix["nodes"], "nodes")
    edge_ids = _unique_ids(matrix["edges"], "edges")
    evidence_ids = _unique_ids(matrix["evidence_catalog"], "evidence_catalog")

    for edge in matrix["edges"]:
        assert edge["source"] in node_ids, edge["id"]
        assert edge["destination"] in node_ids, edge["id"]
        assert edge["coverage"]["assessment"] in COVERAGE_ASSESSMENTS
        assert edge["coverage"]["automated"] in AUTOMATED_STATES
        assert edge["coverage"]["live"] in LIVE_STATES
        assert set(edge["evidence"]) <= evidence_ids


def test_flows_reference_known_edges_and_evidence(matrix: dict) -> None:
    edge_ids = {edge["id"] for edge in matrix["edges"]}
    evidence_ids = {item["id"] for item in matrix["evidence_catalog"]}
    flow_ids = _unique_ids(matrix["flows"], "flows")

    assert flow_ids
    for flow in matrix["flows"]:
        assert flow["edge_path"], flow["id"]
        assert set(flow["edge_path"]) <= edge_ids
        assert set(flow["evidence"]) <= evidence_ids
        assert flow["assessment"] in COVERAGE_ASSESSMENTS


def test_prioritized_gaps_reference_known_edges(matrix: dict) -> None:
    edge_ids = {edge["id"] for edge in matrix["edges"]}
    gap_ids = _unique_ids(matrix["prioritized_gaps"], "prioritized_gaps")

    assert gap_ids
    for gap in matrix["prioritized_gaps"]:
        assert gap["priority"] in {"P0", "P1", "P2"}
        assert gap["affected_edges"]
        assert set(gap["affected_edges"]) <= edge_ids
        assert gap["required_scenarios"]


def test_pull_request_evidence_is_recorded_as_merged(
    matrix: dict,
) -> None:
    for item in matrix["review_pr_evidence"]:
        assert item["state"] == "merged"
    for item in matrix["evidence_catalog"]:
        if item["kind"] == "open_pull_request":
            assert item["state_at_snapshot"] == "merged"


def test_flow_edge_sequence_semantics_are_explicit(matrix: dict) -> None:
    statements = matrix["interpretation"]["exhaustive_means"]
    assert any(
        "not necessarily a strict graph walk" in statement
        for statement in statements
    )
