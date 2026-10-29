-- CI assertions. Each query must return exactly one row with ok = true.
-- Executed by scripts/ci-assert.sh which fails the build on any false.

-- bronze has frames from the sample video (10 s at 1 fps -> ~10 rows)
SELECT COUNT(*) >= 8 AS ok, 'bronze_has_frames' AS check_name FROM lakehouse.bronze.frames;

-- silver has fewer or equal distinct frames than bronze (dedup happened, nothing invented)
SELECT (SELECT COUNT(DISTINCT frame_no) FROM lakehouse.silver.frames)
       <= (SELECT COUNT(DISTINCT frame_no) FROM lakehouse.bronze.frames) AS ok,
       'silver_not_larger_than_bronze' AS check_name;

-- every silver row that has a label has a bbox inside the image
SELECT COUNT(*) = 0 AS ok, 'bbox_in_range' AS check_name
FROM lakehouse.silver.frames
WHERE label_class IS NOT NULL
  AND (bbox_x < 0 OR bbox_y < 0 OR bbox_x + bbox_w > 1.0001 OR bbox_y + bbox_h > 1.0001);

-- gold contains all three splits
SELECT COUNT(DISTINCT split) = 3 AS ok, 'gold_has_three_splits' AS check_name
FROM lakehouse.gold.training_frames;

-- no video leaks across splits
SELECT COUNT(*) = 0 AS ok, 'no_split_leak' AS check_name
FROM (SELECT video_id FROM lakehouse.gold.training_frames GROUP BY 1 HAVING COUNT(DISTINCT split) > 1);

-- the run was logged as passed at the gate
SELECT COUNT(*) >= 1 AS ok, 'gate_logged_pass' AS check_name
FROM lakehouse.ops.run_log WHERE stage = 'quality_gate' AND status = 'PASS';
