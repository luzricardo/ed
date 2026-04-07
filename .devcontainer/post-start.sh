#!/usr/bin/env bash
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-$(python3 - <<'PY'
from pyspark.find_spark_home import _find_spark_home
print(_find_spark_home())
PY
)}"
SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR:-$HOME/.spark-events}"
SPARK_LOG_DIR="${SPARK_LOG_DIR:-$HOME/.spark-logs}"

mkdir -p "$SPARK_EVENTS_DIR" "$SPARK_LOG_DIR"

export SPARK_HOME
export SPARK_LOG_DIR
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:${SPARK_EVENTS_DIR} -Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=200"

if pgrep -f org.apache.spark.deploy.history.HistoryServer >/dev/null 2>&1; then
  echo "Spark History Server already running"
else
  "$SPARK_HOME/sbin/start-history-server.sh"

  sleep 2
  if pgrep -f org.apache.spark.deploy.history.HistoryServer >/dev/null 2>&1; then
    echo "Spark History Server started on port 18080"
  else
    echo "Spark History Server failed to start"
    exit 1
  fi
fi

AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/.airflow}"
AIRFLOW_LOG_DIR="${AIRFLOW_LOG_DIR:-$HOME/.airflow/logs}"
AIRFLOW_DAGS_DIR="${AIRFLOW_DAGS_DIR:-/workspaces/dbt/dags}"
export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_DAGS_DIR"

mkdir -p "$AIRFLOW_HOME" "$AIRFLOW_LOG_DIR"
mkdir -p "$AIRFLOW_DAGS_DIR"

get_airflow_admin_password() {
  local standalone_password_file="$AIRFLOW_HOME/standalone_admin_password.txt"
  local simple_auth_password_file="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
  local password=""

  if [[ -f "$standalone_password_file" ]]; then
    password="$(tr -d '\r\n' < "$standalone_password_file")"
  elif [[ -f "$simple_auth_password_file" ]]; then
    password="$(python3 - "$simple_auth_password_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)

print(data.get("admin", ""))
PY
)"
  fi

  if [[ -n "$password" ]]; then
    printf '%s\n' "$password"
    return 0
  fi

  return 1
}

if pgrep -f "airflow webserver|airflow standalone" >/dev/null 2>&1; then
  echo "Airflow webserver already running"
else
  nohup airflow standalone >"$AIRFLOW_LOG_DIR/standalone.log" 2>&1 &

  sleep 3
  if pgrep -f "airflow webserver|airflow standalone" >/dev/null 2>&1; then
    echo "Airflow started on port 8080"
  else
    echo "Airflow failed to start"
    exit 1
  fi
fi

airflow_admin_password=""
for _ in $(seq 1 30); do
  if airflow_admin_password="$(get_airflow_admin_password)"; then
    echo "Airflow admin password: $airflow_admin_password"
    break
  fi
  sleep 1
done

if [[ -z "$airflow_admin_password" ]]; then
  echo "Warning: could not capture Airflow admin password automatically."
fi
