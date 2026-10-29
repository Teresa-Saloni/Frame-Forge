"""
frameforge_maintenance — weekly compaction + snapshot expiry (see MaintenanceJob.java).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="frameforge_maintenance",
    start_date=datetime(2026, 10, 1),
    schedule="0 3 * * 0",     # Sundays 03:00
    catchup=False,
    default_args={"owner": "frameforge", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["frameforge", "ops"],
) as dag:
    SparkSubmitOperator(
        task_id="compact_and_expire",
        conn_id="spark_default",
        application="/opt/jobs/spark-jobs.jar",
        java_class="com.frameforge.spark.MaintenanceJob",
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.hadoop:hadoop-aws:3.3.6",
        application_args=["--runId=maint_{{ ds_nodash }}", "--retainDays=30"],
        executor_memory="2g",
    )
