-- Run against Trino:  docker compose exec trino trino --catalog lakehouse -f /dev/stdin < sql/demo.sql

-- 1. The question from the pitch: truck labels per camera, last 7 days
SELECT camera_id, label_class, COUNT(*) AS n
FROM lakehouse.gold.training_frames
WHERE ts >= current_date - INTERVAL '7' DAY AND label_class = 'truck'
GROUP BY 1, 2
ORDER BY n DESC;

-- 2. Time-travel: the dataset exactly as tagged v1
SELECT split, COUNT(*) AS rows
FROM lakehouse.gold.training_frames FOR VERSION AS OF 'v1'
GROUP BY 1;

-- 3. Dedup effectiveness per video
SELECT b.video_id,
       COUNT(DISTINCT b.frame_no) AS extracted,
       COUNT(DISTINCT s.frame_no) AS kept,
       ROUND(100.0 * (1 - COUNT(DISTINCT s.frame_no) * 1.0 / COUNT(DISTINCT b.frame_no)), 1) AS pct_removed
FROM lakehouse.bronze.frames b
LEFT JOIN lakehouse.silver.frames s USING (video_id, frame_no)
GROUP BY 1;

-- 4. Snapshot history: every commit Iceberg knows about
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM lakehouse.gold."training_frames$snapshots"
ORDER BY committed_at DESC;

-- 5. Class distribution (feeds the drift check)
SELECT label_class, COUNT(*) AS n, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM lakehouse.silver.frames
WHERE label_class IS NOT NULL
GROUP BY 1 ORDER BY n DESC;

-- 6. Small-file check: data files per partition in gold
SELECT partition, COUNT(*) AS files, ROUND(SUM(file_size_in_bytes) / 1048576.0, 1) AS mb
FROM lakehouse.gold."training_frames$files"
GROUP BY 1 ORDER BY files DESC;

-- 7. Pipeline run log
SELECT run_id, stage, status, rows_in, rows_out, logged_at
FROM lakehouse.ops.run_log
ORDER BY logged_at DESC LIMIT 20;
