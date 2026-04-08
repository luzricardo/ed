#!/usr/bin/env python3
"""Generate Great Expectations suites from dbt manifest test nodes.

The parser relies on structured fields (`test_metadata`, `attached_node`,
`column_name`) and does not parse flattened test names.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
    expectationConfigurationSchema,
)


CORE_DBT_TO_GE_EXPECTATION = {
    "not_null": "expect_column_values_to_not_be_null",
    "unique": "expect_column_values_to_be_unique",
}


def parse_args() -> argparse.Namespace:
    workspace_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Generate Great Expectations suites from dbt manifest.json"
    )
    parser.add_argument(
        "--manifest",
        default=str(workspace_root / "main" / "target" / "manifest.json"),
        help="Path to dbt manifest.json",
    )
    parser.add_argument(
        "--output-dir",
        default=str(workspace_root / "main" / "ge" / "suites"),
        help="Directory where suite JSON files will be written",
    )
    parser.add_argument(
        "--include-core-tests",
        action="store_true",
        help="Also map supported core dbt tests (not_null, unique)",
    )
    return parser.parse_args()


def safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def load_manifest(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def normalize_kwargs(
    ge_expectation_type: str,
    raw_kwargs: dict[str, Any],
    column_name: str | None,
) -> dict[str, Any]:
    kwargs = dict(raw_kwargs)

    # dbt injects a model macro string in kwargs; GE suites should not keep it.
    kwargs.pop("model", None)

    if "column_name" in kwargs and "column" not in kwargs:
        kwargs["column"] = kwargs.pop("column_name")

    if (
        ge_expectation_type.startswith("expect_column_")
        and "column" not in kwargs
        and column_name
    ):
        kwargs["column"] = column_name

    cleaned_kwargs: dict[str, Any] = {}
    for key, value in kwargs.items():
        if value is not None:
            cleaned_kwargs[key] = value
    return cleaned_kwargs


def map_test_to_ge(
    test_node: dict[str, Any],
    include_core_tests: bool,
) -> tuple[ExpectationConfiguration | None, str | None]:
    metadata = test_node.get("test_metadata") or {}
    test_name = metadata.get("name")
    namespace = metadata.get("namespace")
    raw_kwargs = metadata.get("kwargs") or {}
    column_name = test_node.get("column_name")

    if namespace == "dbt_expectations":
        ge_expectation_type = test_name
    elif include_core_tests and namespace in (None, ""):
        ge_expectation_type = CORE_DBT_TO_GE_EXPECTATION.get(test_name)
        if not ge_expectation_type:
            return None, f"unsupported core test '{test_name}'"
    else:
        return None, f"unsupported namespace '{namespace}'"

    if not ge_expectation_type:
        return None, "missing expectation type"

    ge_kwargs = normalize_kwargs(ge_expectation_type, raw_kwargs, column_name)
    expectation_config = ExpectationConfiguration(
        type=ge_expectation_type,
        kwargs=ge_kwargs,
        meta={
            "source": "dbt_manifest",
            "dbt_unique_id": test_node.get("unique_id"),
            "dbt_test_name": test_node.get("name"),
            "dbt_namespace": namespace,
            "dbt_attached_node": test_node.get("attached_node"),
        },
    )

    validation_errors = expectationConfigurationSchema.validate(
        expectation_config.to_json_dict()
    )
    if validation_errors:
        return None, f"invalid expectation schema: {validation_errors}"

    return expectation_config, None


def make_suite_payload(
    attached_node: str,
    target_node: dict[str, Any],
    expectations: list[ExpectationConfiguration],
    manifest_path: Path,
) -> dict[str, Any]:
    relation_name = target_node.get("relation_name")
    suite_name = relation_name or attached_node
    suite = ExpectationSuite(
        name=suite_name,
        expectations=expectations,
        meta={
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "generator": "generate_ge_suites_from_dbt_manifest.py",
            "source_manifest": str(manifest_path),
            "attached_node": attached_node,
            "target_relation_name": relation_name,
            "target_name": target_node.get("name"),
            "target_schema": target_node.get("schema"),
            "target_database": target_node.get("database"),
        },
    )

    suite_payload = suite.to_json_dict()
    validation_errors = expectationSuiteSchema.validate(suite_payload)
    if validation_errors:
        raise ValueError(
            f"invalid suite schema for '{attached_node}': {validation_errors}"
        )
    return suite_payload


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(manifest_path)
    nodes = manifest.get("nodes") or {}

    grouped_expectations: dict[str, list[ExpectationConfiguration]] = defaultdict(list)
    skipped: list[dict[str, Any]] = []

    for node in nodes.values():
        if node.get("resource_type") != "test":
            continue

        attached_node = node.get("attached_node")
        if not attached_node:
            skipped.append(
                {
                    "dbt_unique_id": node.get("unique_id"),
                    "reason": "missing attached_node",
                }
            )
            continue

        expectation, reason = map_test_to_ge(
            test_node=node,
            include_core_tests=args.include_core_tests,
        )
        if expectation is None:
            skipped.append(
                {
                    "dbt_unique_id": node.get("unique_id"),
                    "reason": reason,
                }
            )
            continue

        grouped_expectations[attached_node].append(expectation)

    index_payload: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_manifest": str(manifest_path),
        "suite_files": [],
        "skipped_tests": skipped,
    }

    for attached_node, expectations in sorted(grouped_expectations.items()):
        target_node = nodes.get(attached_node, {})
        suite_payload = make_suite_payload(
            attached_node=attached_node,
            target_node=target_node,
            expectations=expectations,
            manifest_path=manifest_path,
        )

        file_name = f"{safe_filename(attached_node)}.suite.json"
        suite_path = output_dir / file_name
        with suite_path.open("w", encoding="utf-8") as file:
            json.dump(suite_payload, file, ensure_ascii=True, indent=2)

        index_payload["suite_files"].append(
            {
                "attached_node": attached_node,
                "target_relation_name": target_node.get("relation_name"),
                "expectation_count": len(expectations),
                "path": str(suite_path),
            }
        )

    index_path = output_dir / "index.json"
    with index_path.open("w", encoding="utf-8") as file:
        json.dump(index_payload, file, ensure_ascii=True, indent=2)

    print(
        f"Generated {len(index_payload['suite_files'])} suite file(s) in {output_dir}"
    )
    print(f"Skipped {len(skipped)} test node(s). See: {index_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
