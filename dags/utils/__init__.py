"""Domain-organized helpers for dynamic dbt DAG creation."""

from .airflow.dynamic_dags import build_dynamic_dags

__all__ = ["build_dynamic_dags"]
