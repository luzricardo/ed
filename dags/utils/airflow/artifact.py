"""Airflow artifact parsing and validation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = WORKSPACE_ROOT / "main"
DEFAULT_ARTIFACT_PATH = DBT_PROJECT_DIR / "target" / "airflow_artifact.json"


def load_airflow_artifact(
    artifact_path: Path | str = DEFAULT_ARTIFACT_PATH,
) -> dict[str, Any]:
    path = Path(artifact_path).expanduser().resolve()
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    required_keys = {"metadata", "dags", "nodes", "edges"}
    missing = sorted(required_keys - set(payload.keys()))
    if missing:
        raise ValueError(f"Invalid artifact: missing key(s): {', '.join(missing)}")

    return payload
