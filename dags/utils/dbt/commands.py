"""dbt command helpers used by Airflow operators."""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = WORKSPACE_ROOT / "main"
DBT_PROFILES_DIR = Path(
    os.getenv("DBT_PROFILES_DIR", str(WORKSPACE_ROOT / ".dbt"))
).expanduser()


def run_dbt_command(*args: str) -> None:
    logger = logging.getLogger("airflow.task")
    command = ["dbt", *args]
    logger.info("Starting dbt command: %s", " ".join(command))
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)

    try:
        process = subprocess.Popen(
            command,
            cwd=str(DBT_PROJECT_DIR),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("dbt executable not found in PATH") from exc

    if process.stdout is not None:
        for line in process.stdout:
            logger.info(line.rstrip())

    process.wait()

    if process.returncode != 0:
        raise RuntimeError(
            f"dbt {' '.join(args)} failed with exit code {process.returncode}"
        )


def run_dbt_resource(dbt_select: str, resource_type: str) -> None:
    operation = "seed" if resource_type == "seed" else "run"
    run_dbt_command(operation, "--select", dbt_select)


def run_dbt_model(dbt_select: str) -> None:
    run_dbt_resource(dbt_select=dbt_select, resource_type="model")


def test_dbt_node(node_unique_id: str) -> None:
    run_dbt_command("test", "--select", node_unique_id)
