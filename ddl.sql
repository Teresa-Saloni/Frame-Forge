-- FrameForge lakehouse DDL. Idempotent: safe to re-run.
-- Medallion layers: bronze (raw, append-only) -> silver (deduped, labelled, validated) -> gold (splits, tagged)

CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;
CREATE NAMESPACE IF NOT EXISTS lakehouse.silver;
CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;
CREATE NAMESPACE IF NOT EXISTS lakehouse.ops;

-- ---------- BRONZE: one row per extracted frame, exactly as decoded ----------
CREATE TABLE IF NOT EXISTS lakehouse.bronze.frames (
  video_id    STRING    COMMENT 'sha256(bucket/key) of the source video',
  camera_id   STRING,
  frame_no    INT       COMMENT 'frame index in the source video',
  ts          TIMESTAMP COMMENT 'wall-clock time of the frame (video start + offset)',
  frame_path  STRING    COMMENT 's3a://warehouse/frames/<camera>/<video>/<frame>.png',
  width       INT,
  height      INT,
  phash       BIGINT    COMMENT '64-bit perceptual hash',
  source_key  STRING    COMMENT 'raw-video object key',
  run_id      STRING    COMMENT 'Airflow run id that produced this row'
)
USING iceberg
PARTITIONED BY (days(ts), camera_id)
TBLPROPERTIES (
  'format-version' = '2',
  'write.target-file-size-bytes' = '134217728',
  'write.merge.mode' = 'merge-on-read'
);

-- ---------- SILVER: deduplicated frames + labels (one row per detection) ----------
CREATE TABLE IF NOT EXISTS lakehouse.silver.frames (
  video_id      STRING,
  camera_id     STRING,
  frame_no      INT,
  ts            TIMESTAMP,
  frame_path    STRING,
  phash         BIGINT,
  label_class   STRING  COMMENT 'COCO class name; NULL until labelled',
  bbox_x        FLOAT   COMMENT 'normalised 0..1, top-left',
  bbox_y        FLOAT,
  bbox_w        FLOAT,
  bbox_h        FLOAT,
  confidence    FLOAT,
  model_version STRING,
  label_source  STRING  COMMENT 'yolo | human',
  run_id        STRING
)
USING iceberg
PARTITIONED BY (days(ts), camera_id)
TBLPROPERTIES (
  'format-version' = '2',
  'write.target-file-size-bytes' = '134217728',
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read'
);

-- ---------- GOLD: training-ready, split by video, tagged per dataset version ----------
CREATE TABLE IF NOT EXISTS lakehouse.gold.training_frames (
  video_id      STRING,
  camera_id     STRING,
  frame_no      INT,
  ts            TIMESTAMP,
  frame_path    STRING,
  label_class   STRING,
  bbox_x        FLOAT, bbox_y FLOAT, bbox_w FLOAT, bbox_h FLOAT,
  confidence    FLOAT,
  model_version STRING,
  split         STRING  COMMENT 'train | val | test',
  dataset_version STRING
)
USING iceberg
PARTITIONED BY (split, label_class)
TBLPROPERTIES ('format-version' = '2');

-- ---------- OPS: one row per pipeline run, written by the gate task ----------
CREATE TABLE IF NOT EXISTS lakehouse.ops.run_log (
  run_id        STRING,
  video_id      STRING,
  stage         STRING,
  status        STRING,
  rows_in       BIGINT,
  rows_out      BIGINT,
  detail        STRING,
  logged_at     TIMESTAMP
)
USING iceberg;
