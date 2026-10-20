from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="iceberg_table_maintenance",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "maintenance"],
) as dag:
    compact_files = BashOperator(task_id="compact_data_files", bash_command="echo 'Running Iceberg compaction (rewrite_data_files)'")
    expire_snapshots = BashOperator(task_id="expire_old_snapshots", bash_command="echo 'Expiring old Iceberg snapshots'")
    remove_orphans = BashOperator(task_id="remove_orphan_files", bash_command="echo 'Removing orphan files'")
    health_check = BashOperator(task_id="pipeline_health_check", bash_command="echo 'Pipeline health check complete'")

    compact_files >> expire_snapshots >> remove_orphans >> health_check