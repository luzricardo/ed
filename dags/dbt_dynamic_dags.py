"""Airflow entrypoint that registers dbt dynamic DAGs."""

import logging
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = CURRENT_DIR.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from dags.utils import build_dynamic_dags

logger = logging.getLogger(__name__)

try:
    dynamic_dags = build_dynamic_dags()
except Exception:  # pragma: no cover
    logger.exception("Failed to build dynamic dbt DAGs")
    dynamic_dags = {}

for _dag_id, _dag in dynamic_dags.items():
    globals()[_dag_id] = _dag
