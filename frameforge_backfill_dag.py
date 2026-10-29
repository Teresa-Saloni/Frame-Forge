"""
frameforge_backfill — daily scheduled DAG that (re)processes every video under
raw-video/<camera>/<ds>/ for the data interval. Also used for re-labelling with a new model.

Run a range:  airflow dags backfill -s 2026-10-01 -e 2026-10-20 frameforge_backfill

Safe to run over already-processed days because every job writes with MERGE; the only
observable effect of a re-run is a fresh run_id in ops.run_log and, if --relabel=true, new
label rows with the newer model_version.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JAR = "/opt/jobs/spark-jobs.jar"
PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.6",
    "io.openlineage:openlineage-spark_2.12:1.23.0",
])


@task
def list_videos_for_day(ds: str) -> list[dict]:
    """List raw-video/*/<ds>/*.mp4 in MinIO and return one conf dict per video."""
    import hashlib
    import os
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    out = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="raw-video"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if len(parts) < 3 or parts[1] != ds or not key.lower().endswith((".mp4", ".mkv", ".mov")):
                continue
            out.append({
                "videoId": hashlib.sha256(f"raw-video/{key}".encode()).hexdigest(),
                "bucket": "raw-video",
                "key": key,
                "cameraId": parts[0],
            })
    return out


def spark_args(main_class: str, args: list[str]) -> dict:
    return dict(
        conn_id="spark_default",
        application=JAR,
        java_class=f"com.frameforge.spark.{main_class}",
        packages=PACKAGES,
        application_args=args,
        executor_memory="2g",
        driver_memory="1g",
    )


with DAG(
    dag_id="frameforge_backfill",
    description="Daily batch (re)processing of all videos for a date; backfill-safe via MERGE",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,          # one day at a time keeps the laptop alive
    default_args={"owner": "frameforge", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["frameforge", "batch", "backfill"],
) as dag:

    videos = list_videos_for_day("{{ ds }}")

    # Dynamic task mapping: one extract/dedup/label chain per video found for the day.
    extract = SparkSubmitOperator.partial(
        task_id="extract_frames", **spark_args("ExtractFramesJob", [])
    ).expand(application_args=videos.map(lambda v: [
        f"--videoId={v['videoId']}", f"--bucket={v['bucket']}", f"--key={v['key']}",
        f"--cameraId={v['cameraId']}", "--runId=backfill_{{ ds_nodash }}", "--fps=1",
    ]))

    dedup = SparkSubmitOperator.partial(
        task_id="dedup_frames", **spark_args("DedupFramesJob", [])
    ).expand(application_args=videos.map(lambda v: [
        f"--videoId={v['videoId']}", "--runId=backfill_{{ ds_nodash }}", "--threshold=8",
    ]))

    label = SparkSubmitOperator.partial(
        task_id="label_frames", **spark_args("LabelFramesJob", [])
    ).expand(application_args=videos.map(lambda v: [
        f"--videoId={v['videoId']}", "--runId=backfill_{{ ds_nodash }}",
        "--model=/opt/models/yolov8n.onnx", "--score=0.25", "--iou=0.45",
    ]))

    gold = SparkSubmitOperator(
        task_id="build_gold",
        **spark_args("BuildGoldSplitsJob", [
            "--runId=backfill_{{ ds_nodash }}", "--datasetVersion=v{{ ds_nodash }}",
        ]),
    )

    extract >> dedup >> label >> gold
