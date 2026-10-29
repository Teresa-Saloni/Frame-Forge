"""
frameforge_video — one run per raw video.

Triggered by the ingest service via the REST API with conf:
  {videoId, bucket, key, cameraId, sizeBytes}

extract -> dedup -> label -> quality_gate -> build_gold

Every Spark task is a MERGE-based job, so a retry (or a whole re-run with the same conf)
produces identical tables. The gate runs BEFORE gold: a failed gate stops the DAG and gold is
never rebuilt from bad data.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

sys.path.insert(0, "/opt/quality")

JAR = "/opt/jobs/spark-jobs.jar"
PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.6",
    "io.openlineage:openlineage-spark_2.12:1.23.0",
])

DEFAULT_ARGS = {
    "owner": "frameforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def spark_job(task_id: str, main_class: str, extra_args: list[str]) -> SparkSubmitOperator:
    """Factory so all four jobs share identical cluster/catalog configuration."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=JAR,
        java_class=f"com.frameforge.spark.{main_class}",
        packages=PACKAGES,
        application_args=extra_args,
        executor_memory="2g",
        driver_memory="1g",
        num_executors=1,
        executor_cores=2,
        name=f"ff-{task_id}-{{{{ dag_run.conf.get('videoId', 'x')[:8] }}}}",
        verbose=False,
    )


def run_quality_gate(**ctx):
    """Great Expectations gate on silver for this video. Raises -> task fails -> gold never runs."""
    from gate import run_gate  # /opt/quality/gate.py

    conf = ctx["dag_run"].conf
    result = run_gate(video_id=conf["videoId"], run_id=ctx["run_id"])
    ctx["ti"].xcom_push(key="gate_summary", value=result)
    if not result["success"]:
        raise RuntimeError(f"quality gate failed: {result['failed']}")


with DAG(
    dag_id="frameforge_video",
    description="Raw video -> bronze -> silver (dedup + labels) -> gate -> gold",
    start_date=datetime(2026, 1, 1),
    schedule=None,           # event-driven via REST trigger
    catchup=False,
    max_active_runs=4,       # four videos in flight at once on a laptop
    default_args=DEFAULT_ARGS,
    tags=["frameforge", "video", "lakehouse"],
) as dag:

    V = "{{ dag_run.conf['videoId'] }}"
    R = "{{ run_id }}"

    extract = spark_job("extract_frames", "ExtractFramesJob", [
        f"--videoId={V}",
        "--bucket={{ dag_run.conf['bucket'] }}",
        "--key={{ dag_run.conf['key'] }}",
        "--cameraId={{ dag_run.conf['cameraId'] }}",
        f"--runId={R}",
        "--fps={{ dag_run.conf.get('fps', 1) }}",
    ])

    dedup = spark_job("dedup_frames", "DedupFramesJob", [
        f"--videoId={V}", f"--runId={R}", "--threshold=8",
    ])

    label = spark_job("label_frames", "LabelFramesJob", [
        f"--videoId={V}", f"--runId={R}",
        "--model=/opt/models/yolov8n.onnx", "--score=0.25", "--iou=0.45", "--partitions=4",
    ])

    gate = PythonOperator(
        task_id="quality_gate",
        python_callable=run_quality_gate,
        retries=0,  # a data-quality failure is not transient; never auto-retry it
    )

    gold = spark_job("build_gold", "BuildGoldSplitsJob", [
        f"--runId={R}",
        "--datasetVersion=v{{ ds_nodash }}",
    ])

    extract >> dedup >> label >> gate >> gold
