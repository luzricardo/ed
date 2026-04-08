"""Build dynamic Airflow DAGs from a lightweight dbt artifact."""

from __future__ import annotations

import logging
import re
import shlex
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, TaskGroup

from .artifact import DEFAULT_ARTIFACT_PATH, load_airflow_artifact
from .operators import DataQualityOperator
from ..dbt.commands import DBT_PROFILES_DIR, DBT_PROJECT_DIR

DEFAULT_DAG_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def safe_identifier(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()


def sensor_task_id(upstream_unique_id: str, downstream_unique_id: str) -> str:
    raw_value = f"wait_{safe_identifier(upstream_unique_id)}_for_{safe_identifier(downstream_unique_id)}"
    return raw_value[:220]


def normalize_dbt_select(dbt_select: str, resource_type: str) -> str:
    match = re.match(r"^(model|seed)\.[^.]+\.(.+)$", dbt_select)
    if match and match.group(1) == resource_type:
        return match.group(2)
    return dbt_select


def build_dbt_bash_command(dbt_select: str, resource_type: str) -> str:
    operation = "seed" if resource_type == "seed" else "run"
    project_dir = shlex.quote(str(DBT_PROJECT_DIR))
    profiles_dir = shlex.quote(str(DBT_PROFILES_DIR))
    select_expr = shlex.quote(normalize_dbt_select(dbt_select, resource_type))
    return f"set -euo pipefail; export DBT_PROFILES_DIR={profiles_dir}; cd {project_dir}; dbt {operation} --select {select_expr}"


def build_dynamic_dags(
    artifact_path: Path | str = DEFAULT_ARTIFACT_PATH,
) -> dict[str, DAG]:
    logger = logging.getLogger(__name__)

    path = Path(artifact_path).expanduser().resolve()
    if not path.exists():
        logger.warning(
            "Airflow artifact not found at %s. "
            "Run scripts/generate_airflow_artifact_from_dbt_manifest.py first.",
            path,
        )
        return {}

    artifact = load_airflow_artifact(path)
    nodes: dict[str, dict[str, Any]] = artifact["nodes"]
    edges: list[dict[str, Any]] = artifact["edges"]

    missing_model_suites = [
        node_id
        for node_id, node in sorted(nodes.items())
        if node.get("resource_type") == "model" and not node.get("ge_suite_path")
    ]
    if missing_model_suites:
        raise ValueError(
            "Strict GE policy: missing GE suites for model nodes: "
            + ", ".join(missing_model_suites)
        )

    dags: dict[str, DAG] = {}

    for dag_payload in sorted(artifact["dags"], key=lambda item: item["dag_id"]):
        dag_id = dag_payload["dag_id"]
        dag_group = dag_payload.get("dag_group", "dbt")
        tags = ["dbt", dag_group]
        if dag_payload.get("subject"):
            tags.append(str(dag_payload["subject"]))

        with DAG(
            dag_id=dag_id,
            start_date=datetime(2024, 1, 1),
            schedule="@daily",
            catchup=False,
            default_args=DEFAULT_DAG_ARGS,
            tags=tags,
        ) as dag:
            run_tasks: dict[str, Any] = {}
            dq_tasks: dict[str, Any] = {}

            for node_id in dag_payload["node_ids"]:
                node = nodes[node_id]
                node_resource_type = node.get("resource_type", "model")

                with TaskGroup(group_id=node["task_group_id"], tooltip=node_id):
                    dbt_build = BashOperator(
                        task_id="dbt_build",
                        bash_command=build_dbt_bash_command(
                            dbt_select=node["dbt_select"],
                            resource_type=node_resource_type,
                        ),
                    )
                    data_quality = DataQualityOperator(
                        task_id="data_quality",
                        node_unique_id=node_id,
                        suite_path=node.get("ge_suite_path"),
                        resource_type=node_resource_type,
                    )
                    dbt_build >> data_quality

                run_tasks[node_id] = dbt_build
                dq_tasks[node_id] = data_quality

            for edge in edges:
                upstream_id = edge["upstream_unique_id"]
                downstream_id = edge["downstream_unique_id"]
                edge_type = edge["edge_type"]

                if downstream_id not in run_tasks:
                    continue

                if edge_type == "intra_dag":
                    if upstream_id in dq_tasks:
                        dq_tasks[upstream_id] >> run_tasks[downstream_id]
                    continue

                if edge_type != "cross_dag":
                    continue

                upstream_node = nodes.get(upstream_id)
                if not upstream_node:
                    continue

                wait_task = ExternalTaskSensor(
                    task_id=sensor_task_id(upstream_id, downstream_id),
                    external_dag_id=upstream_node["dag_id"],
                    external_task_id=f"{upstream_node['task_group_id']}.data_quality",
                    allowed_states=["success"],
                    failed_states=["failed", "skipped"],
                    mode="reschedule",
                    poke_interval=60,
                    timeout=6 * 60 * 60,
                )
                wait_task >> run_tasks[downstream_id]

        dags[dag_id] = dag

    return dags
