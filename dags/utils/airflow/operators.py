"""Custom Airflow operators for dbt and data quality execution."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseOperator

from ..dbt import run_dbt_resource
from ..ge import run_ge_validation


class DbtBuildOperator(BaseOperator):
    """Execute dbt run/seed using --select based on resource type."""

    template_fields = ("dbt_select", "resource_type")

    def __init__(
        self,
        *,
        dbt_select: str,
        resource_type: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_select = dbt_select
        self.resource_type = resource_type

    def execute(self, context: Any) -> None:  # noqa: ARG002
        run_dbt_resource(
            dbt_select=self.dbt_select,
            resource_type=self.resource_type,
        )


class DataQualityOperator(BaseOperator):
    """Execute Great Expectations validation for a node."""

    template_fields = ("node_unique_id", "suite_path", "resource_type")

    def __init__(
        self,
        *,
        node_unique_id: str,
        suite_path: str | None,
        resource_type: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.node_unique_id = node_unique_id
        self.suite_path = suite_path
        self.resource_type = resource_type

    def execute(self, context: Any) -> None:  # noqa: ARG002
        run_ge_validation(
            node_unique_id=self.node_unique_id,
            suite_path=self.suite_path,
            resource_type=self.resource_type,
        )


DbtRunOperator = DbtBuildOperator
