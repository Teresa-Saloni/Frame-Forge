# FrameForge

**A data platform for computer vision.** Raw video in, a versioned, quality-gated, SQL-queryable training dataset out.

Most CV projects assume a labelled dataset already exists. FrameForge is the machinery that produces one — and the project is deliberately about the *data*, not the model: idempotent pipelines, a medallion lakehouse, data-quality gates, lineage, and time-travel. The detector is an off-the-shelf pretrained YOLOv8n.

```
raw-video (MinIO) ──Kafka──▶ Airflow DAG ──▶ Spark: extract → pHash dedup → YOLO label
                                              │
                                              ▼
                       Iceberg lakehouse:  bronze ──▶ silver ──[Great Expectations gate]──▶ gold
                                              │                                             │
                                        OpenLineage → Marquez                        Trino SQL / tags
```

| | |
|---|---|
| **Stack** | Java 17 · Apache Spark 3.5 · Apache Airflow 2.10 · Apache Iceberg 1.6 · Kafka (KRaft) · MinIO · Trino · Great Expectations 1.x · OpenLineage + Marquez · ONNX Runtime · Docker Compose · GitHub Actions |
| **Unit of work** | one video file = one DAG run = one atomic set of Iceberg commits |
| **Idempotency** | every write is `MERGE INTO … ON (video_id, frame_no)`; `video_id = sha256(bucket/key)` |
| **Quality gate** | 8 expectations + bbox geometry + class-distribution drift (KL vs last gold tag); gate fails → gold never rebuilt |
| **Versioning** | gold snapshot tagged per dataset version; `SELECT … FOR VERSION AS OF 'v20261020'` |

## Quickstart (≈15 min, needs Docker with ~10 GB RAM, Java 17, Maven, ffmpeg, Python 3.11)

```bash
git clone … && cd frameforge
mvn -q package                      # builds spark-jobs.jar + ingest-service.jar, runs unit tests
scripts/make-sample-video.sh        # 10 s synthetic clip (CI uses this; swap in real footage for demos)
scripts/get-model.sh                # exports yolov8n.onnx (pip install ultralytics onnx)
docker compose up -d --build        # 9 services
scripts/bootstrap.sh                # buckets, Kafka topic, Iceberg tables
scripts/trigger-sample.sh           # drop the clip into raw-video → pipeline runs
scripts/wait-for-dag.sh             # polls Airflow until success
docker compose exec trino trino --catalog lakehouse -f /dev/stdin < sql/demo.sql
```

| UI | URL |
|---|---|
| Airflow | http://localhost:8088 (admin / admin) |
| Marquez lineage | http://localhost:3000 |
| MinIO console | http://localhost:9001 (minioadmin / minioadmin) |
| Spark master | http://localhost:8080 |
| Trino | `docker compose exec trino trino --catalog lakehouse` |

Low on RAM? Drop `spark-master`/`spark-worker` from Compose and set `spark.master=local[*]` in `spark/spark-defaults.conf`; nothing else changes.

## What is where

```
ingest-service/   Spring Boot: bucket watcher → Kafka producer; Kafka consumer → Airflow REST trigger
spark-jobs/       ExtractFramesJob · DedupFramesJob · LabelFramesJob · BuildGoldSplitsJob · MaintenanceJob
                  PHash (pure-Java DCT hash) · YoloDetector (ONNX Runtime, letterbox + NMS)
airflow/dags/     frameforge_video (event-driven) · frameforge_backfill (@daily, catchup) · frameforge_maintenance
quality/          gate.py — Great Expectations suite + custom checks; inject_corrupt.sql — negative fixture
sql/              ddl.sql (bronze/silver/gold/ops) · demo.sql · ci_assert.sql
scripts/          bootstrap, sample video, model export, trigger, wait-for-dag, CI assertions
trino/catalog/    Iceberg REST connector
spark/            spark-defaults.conf (catalog, s3a, OpenLineage)
.github/          ci.yml — unit tests, then the full pipeline on the sample video + the negative gate test
```

## The medallion layers

| Layer | Table | What it is | Who writes it |
|---|---|---|---|
| Bronze | `bronze.frames` | one row per decoded frame; immutable | `ExtractFramesJob` (MERGE, insert-only) |
| Silver | `silver.frames` | deduped frames, one row per detection, validated | `DedupFramesJob`, `LabelFramesJob` |
| Gold | `gold.training_frames` | train/val/test by `hash(video_id)`, tagged per version | `BuildGoldSplitsJob` |
| Ops | `ops.run_log` | one row per job per run | every job + the gate |

## Measured numbers

Fill this table after running the experiments in the project doc; keep resume bullets architecture-only until then.

| Metric | Command | Result |
|---|---|---|
| Dedup ratio | `sql/demo.sql` #3 | |
| End-to-end frames/min | DAG duration ÷ bronze rows | |
| Label throughput, 1 vs 4 partitions | `--partitions=` on `label_frames` | |
| Idempotency | re-run same video, counts unchanged | |
| Files before/after compaction | `sql/demo.sql` #6 around `frameforge_maintenance` | |
| Gate catch rate | `scripts/ci-gate-negative.sh` | 5/5 injected defects blocked |

Measured on a single laptop with Compose. The design scales out by adding Spark workers and Kafka partitions; not benchmarked.

## Interview one-liners

- **Why lakehouse, not warehouse?** Frames are images in object storage; Iceberg gives ACID, schema evolution and time-travel on files already there.
- **Exactly-once?** At-least-once Kafka + offset committed only after Airflow accepts + deterministic run id (409 = success) + MERGE sink = effectively exactly-once.
- **Skew?** Repartition by `video_id` for the sequential dedup scan; by frame range for labelling, so one long video does not pin one executor.
- **Small files?** Weekly `rewrite_data_files` / `rewrite_manifests` / `expire_snapshots` / `remove_orphan_files`.
- **Why split by video?** Adjacent frames are near-duplicates; frame-level splits leak and inflate validation accuracy.

## Next steps (not built, on purpose)

Write-Audit-Publish with Iceberg branches · Iceberg v3 (deletion vectors, row lineage) · Airflow 3 asset-driven scheduling · embedding-based semantic dedup · open-vocabulary labelling · active-learning review queue. See the project doc for the ranked list.
