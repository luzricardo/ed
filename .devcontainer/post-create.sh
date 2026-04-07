#!/usr/bin/env bash
set -euo pipefail

mkdir -p "$HOME/.dbt"
mkdir -p "$HOME/.spark-events"
mkdir -p "$HOME/.spark-logs"

echo "Verifying installed tooling..."
echo "Python version:"
python --version
echo "Pip version:"
pip --version
echo "dbt version:"
dbt --version
echo "AWS version:"
aws --version
echo "Session Manager Plugin version:"
session-manager-plugin --version || true
echo "Airflow version:"
airflow version
python - <<'PY'
import great_expectations as gx
print(f"great_expectations {gx.__version__}")
PY
