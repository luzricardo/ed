"""Great Expectations suite validation for dbt nodes."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]


def _resolve_suite_path(suite_path: str) -> Path:
    resolved_path = Path(suite_path).expanduser()
    if not resolved_path.is_absolute():
        resolved_path = WORKSPACE_ROOT / resolved_path
    return resolved_path.resolve()


def _resolve_relation_name(payload: dict[str, Any], node_unique_id: str) -> str:
    meta = payload.get("meta") or {}
    relation_name = meta.get("target_relation_name") or payload.get("name")
    if not relation_name:
        raise ValueError(
            "GE suite is missing target relation metadata for "
            f"{node_unique_id}. Expected meta.target_relation_name or suite name."
        )
    return str(relation_name)


def _result_success(result: Any) -> bool | None:
    if hasattr(result, "success"):
        return bool(result.success)
    if isinstance(result, dict) and "success" in result:
        return bool(result["success"])
    if hasattr(result, "to_json_dict"):
        payload = result.to_json_dict()
        if isinstance(payload, dict) and "success" in payload:
            return bool(payload["success"])
    return None


def _run_ge_validation(
    payload: dict[str, Any],
    relation_name: str,
    node_unique_id: str,
) -> None:
    from great_expectations.core.batch import Batch
    from great_expectations.core.expectation_suite import expectationSuiteSchema
    from great_expectations.execution_engine.sparkdf_execution_engine import (
        SparkDFExecutionEngine,
    )
    from great_expectations.validator.validator import Validator
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    dataframe = spark.table(relation_name)
    expectation_suite = expectationSuiteSchema.load(payload)

    validator = Validator(
        execution_engine=SparkDFExecutionEngine(),
        batches=[Batch(data=dataframe)],
        expectation_suite=expectation_suite,
    )
    result = validator.validate()
    success = _result_success(result)

    if success is not True:
        raise RuntimeError(
            "Great Expectations validation failed for "
            f"{node_unique_id} (relation: {relation_name})"
        )


def run_ge_validation(
    node_unique_id: str,
    suite_path: str | None,
    resource_type: str,
) -> None:
    logger = logging.getLogger("airflow.task")

    if not suite_path:
        if resource_type == "model":
            raise ValueError(
                f"Strict GE policy: missing GE suite mapping for model {node_unique_id}"
            )
        logger.warning(
            "No GE suite mapped for %s (%s). Skipping GE validation.",
            node_unique_id,
            resource_type,
        )
        return

    resolved_path = _resolve_suite_path(suite_path)

    if not resolved_path.exists():
        raise FileNotFoundError(
            f"GE suite file not found for node {node_unique_id}: {resolved_path}"
        )

    with resolved_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    expectations = payload.get("expectations") or []
    if not expectations:
        raise ValueError(f"GE suite for {node_unique_id} has no expectations")

    relation_name = _resolve_relation_name(payload, node_unique_id)
    _run_ge_validation(
        payload=payload,
        relation_name=relation_name,
        node_unique_id=node_unique_id,
    )

    logger.info(
        "GE validation executed for %s on %s (%d expectation(s)).",
        node_unique_id,
        relation_name,
        len(expectations),
    )


validate_ge_suite = run_ge_validation
