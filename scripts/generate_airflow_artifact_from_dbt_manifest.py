#!/usr/bin/env python3
"""Generate a lightweight Airflow dependency artifact from dbt manifest v12."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    workspace_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description=(
            "Extract a minimal artifact from dbt manifest.json for Airflow DAG and "
            "dependency orchestration"
        )
    )
    parser.add_argument(
        "--manifest",
        default=str(workspace_root / "main" / "target" / "manifest.json"),
        help="Path to dbt manifest.json (v12 expected)",
    )
    parser.add_argument(
        "--ge-index",
        default=str(workspace_root / "ge" / "suites_all" / "index.json"),
        help="Path to GE suites index.json (optional)",
    )
    parser.add_argument(
        "--output",
        default=str(workspace_root / "main" / "target" / "airflow_artifact.json"),
        help="Output path for the minimal Airflow artifact",
    )
    parser.add_argument(
        "--allow-missing-ge",
        action="store_true",
        help=(
            "Allow model nodes without mapped GE suite. "
            "By default, missing GE suites fail artifact generation."
        ),
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def safe_identifier(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()


def infer_node_group(node: dict[str, Any]) -> tuple[str, str, str | None] | None:
    resource_type = node.get("resource_type")
    original_file_path = (node.get("original_file_path") or "").replace("\\", "/")

    if resource_type == "seed":
        return "dbt_seeds", "seeds", None

    if resource_type != "model":
        return None

    if original_file_path.startswith("models/staging/"):
        return "dbt_staging", "staging", None

    match = re.match(r"^models/marts/([^/]+)/", original_file_path)
    if match:
        subject = safe_identifier(match.group(1))
        return f"dbt_marts_{subject}", "marts", subject

    return None


def load_ge_suite_map(index_path: Path) -> dict[str, str]:
    if not index_path.exists():
        return {}

    payload = load_json(index_path)
    mapping: dict[str, str] = {}

    for item in payload.get("suite_files") or []:
        attached_node = item.get("attached_node")
        suite_path = item.get("path")
        if isinstance(attached_node, str) and isinstance(suite_path, str):
            mapping[attached_node] = suite_path

    return mapping


def ensure_manifest_v12(manifest: dict[str, Any]) -> tuple[str, str]:
    metadata = manifest.get("metadata") or {}
    schema_version = metadata.get("dbt_schema_version")
    dbt_version = metadata.get("dbt_version")

    if not isinstance(schema_version, str) or "manifest/v12" not in schema_version:
        raise ValueError(
            "Unsupported manifest schema. Expected dbt manifest v12 in "
            "metadata.dbt_schema_version"
        )

    return schema_version, str(dbt_version)


def detect_cycles(
    nodes_by_id: dict[str, dict[str, Any]], edges: list[dict[str, Any]]
) -> None:
    dag_nodes: dict[str, set[str]] = defaultdict(set)
    graph: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = defaultdict(int)

    for unique_id, payload in nodes_by_id.items():
        dag_nodes[payload["dag_id"]].add(unique_id)

    for edge in edges:
        if edge["edge_type"] != "intra_dag":
            continue
        upstream = edge["upstream_unique_id"]
        downstream = edge["downstream_unique_id"]
        if downstream not in graph[upstream]:
            graph[upstream].add(downstream)
            indegree[downstream] += 1

    for dag_id, node_ids in dag_nodes.items():
        queue = deque([node for node in node_ids if indegree[node] == 0])
        visited = 0

        while queue:
            current = queue.popleft()
            visited += 1
            for nxt in graph[current]:
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)

        if visited != len(node_ids):
            raise ValueError(
                f"Cycle detected inside DAG group '{dag_id}'. "
                "Please review model refs in dbt project."
            )


def build_artifact(
    manifest: dict[str, Any],
    ge_suite_map: dict[str, str],
    strict_ge: bool,
) -> dict[str, Any]:
    raw_nodes = manifest.get("nodes") or {}
    selected_nodes: dict[str, dict[str, Any]] = {}
    dag_to_nodes: dict[str, list[str]] = defaultdict(list)

    for unique_id, node in sorted(raw_nodes.items()):
        resource_type = node.get("resource_type")
        if resource_type not in {"model", "seed"}:
            continue

        config = node.get("config") or {}
        if config.get("enabled", True) is False:
            continue

        group = infer_node_group(node)
        if not group:
            continue

        dag_id, dag_group, subject = group
        ge_suite_path = ge_suite_map.get(unique_id)

        selected_nodes[unique_id] = {
            "unique_id": unique_id,
            "name": node.get("name"),
            "resource_type": resource_type,
            "dag_id": dag_id,
            "dag_group": dag_group,
            "subject": subject,
            "task_group_id": f"model_{safe_identifier(unique_id)}",
            "dbt_select": node.get("name") or unique_id,
            "original_file_path": node.get("original_file_path"),
            "ge_suite_path": ge_suite_path,
            "dq_enabled": bool(ge_suite_path),
        }
        dag_to_nodes[dag_id].append(unique_id)

    if strict_ge:
        missing_ge = [
            node_id
            for node_id, payload in selected_nodes.items()
            if payload["resource_type"] == "model" and not payload["dq_enabled"]
        ]
        if missing_ge:
            raise ValueError(
                "Missing GE suite mappings for model nodes: "
                + ", ".join(sorted(missing_ge))
            )

    edges: list[dict[str, Any]] = []
    external_dependencies: list[dict[str, str]] = []

    for downstream_id, downstream_payload in selected_nodes.items():
        upstream_candidates = (
            (raw_nodes.get(downstream_id) or {}).get("depends_on", {}).get("nodes", [])
        )

        for upstream_id in upstream_candidates:
            if upstream_id in selected_nodes:
                upstream_payload = selected_nodes[upstream_id]
                is_cross_dag = (
                    upstream_payload["dag_id"] != downstream_payload["dag_id"]
                )
                edges.append(
                    {
                        "upstream_unique_id": upstream_id,
                        "downstream_unique_id": downstream_id,
                        "edge_type": "cross_dag" if is_cross_dag else "intra_dag",
                        "cross_dag": is_cross_dag,
                    }
                )
            elif isinstance(upstream_id, str) and upstream_id.startswith("source."):
                external_dependencies.append(
                    {
                        "source_unique_id": upstream_id,
                        "downstream_unique_id": downstream_id,
                        "edge_type": "external_source",
                    }
                )

    deduped_edges = {
        (
            edge["upstream_unique_id"],
            edge["downstream_unique_id"],
            edge["edge_type"],
        ): edge
        for edge in edges
    }
    edges = list(deduped_edges.values())

    detect_cycles(selected_nodes, edges)

    dag_payloads: list[dict[str, Any]] = []
    for dag_id, node_ids in sorted(dag_to_nodes.items()):
        dag_group = (
            "marts" if dag_id.startswith("dbt_marts_") else dag_id.replace("dbt_", "")
        )
        subject = dag_id.replace("dbt_marts_", "") if dag_group == "marts" else None
        dag_payloads.append(
            {
                "dag_id": dag_id,
                "dag_group": dag_group,
                "subject": subject,
                "node_ids": sorted(node_ids),
            }
        )

    metadata = manifest.get("metadata") or {}
    artifact = {
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_manifest_generated_at": metadata.get("generated_at"),
            "dbt_schema_version": metadata.get("dbt_schema_version"),
            "dbt_version": metadata.get("dbt_version"),
            "adapter_type": metadata.get("adapter_type"),
        },
        "summary": {
            "dag_count": len(dag_payloads),
            "node_count": len(selected_nodes),
            "edge_count": len(edges),
            "external_dependency_count": len(external_dependencies),
        },
        "dags": dag_payloads,
        "nodes": selected_nodes,
        "edges": sorted(
            edges,
            key=lambda edge: (
                edge["downstream_unique_id"],
                edge["upstream_unique_id"],
                edge["edge_type"],
            ),
        ),
        "external_dependencies": sorted(
            external_dependencies,
            key=lambda dep: (dep["downstream_unique_id"], dep["source_unique_id"]),
        ),
    }

    return artifact


def main() -> int:
    args = parse_args()

    manifest_path = Path(args.manifest).expanduser().resolve()
    ge_index_path = Path(args.ge_index).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    manifest = load_json(manifest_path)
    ensure_manifest_v12(manifest)
    ge_suite_map = load_ge_suite_map(ge_index_path)

    artifact = build_artifact(
        manifest=manifest,
        ge_suite_map=ge_suite_map,
        strict_ge=not args.allow_missing_ge,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(artifact, file, ensure_ascii=True, indent=2)

    summary = artifact["summary"]
    print(f"Artifact written to: {output_path}")
    print(
        "Created "
        f"{summary['dag_count']} DAG payload(s), "
        f"{summary['node_count']} node(s), "
        f"{summary['edge_count']} edge(s), "
        f"{summary['external_dependency_count']} external dependency record(s)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
