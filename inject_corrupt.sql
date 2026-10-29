-- Negative test fixture: five deliberately bad label rows for a fake video.
-- The gate MUST fail on this video. Used by scripts/ci-gate-negative.sh.
INSERT INTO lakehouse.silver.frames VALUES
  -- 1. missing label
  ('corrupt-video', 'camera-99', 1, TIMESTAMP '2026-10-20 00:00:01', 's3a://warehouse/frames/x/1.png', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'ci-neg'),
  -- 2. bbox outside image (x+w > 1)
  ('corrupt-video', 'camera-99', 2, TIMESTAMP '2026-10-20 00:00:02', 's3a://warehouse/frames/x/2.png', 0, 'car',   0.9, 0.1, 0.5, 0.2, 0.8, 'yolov8n-coco-1.0', 'yolo', 'ci-neg'),
  -- 3. confidence below threshold
  ('corrupt-video', 'camera-99', 3, TIMESTAMP '2026-10-20 00:00:03', 's3a://warehouse/frames/x/3.png', 0, 'car',   0.1, 0.1, 0.2, 0.2, 0.05, 'yolov8n-coco-1.0', 'yolo', 'ci-neg'),
  -- 4. class not in COCO (wrong model loaded)
  ('corrupt-video', 'camera-99', 4, TIMESTAMP '2026-10-20 00:00:04', 's3a://warehouse/frames/x/4.png', 0, 'dragon', 0.1, 0.1, 0.2, 0.2, 0.9, 'yolov8n-coco-1.0', 'yolo', 'ci-neg'),
  -- 5. exact duplicate detection
  ('corrupt-video', 'camera-99', 5, TIMESTAMP '2026-10-20 00:00:05', 's3a://warehouse/frames/x/5.png', 0, 'truck', 0.2, 0.2, 0.3, 0.3, 0.9, 'yolov8n-coco-1.0', 'yolo', 'ci-neg'),
  ('corrupt-video', 'camera-99', 5, TIMESTAMP '2026-10-20 00:00:05', 's3a://warehouse/frames/x/5.png', 0, 'truck', 0.2, 0.2, 0.3, 0.3, 0.9, 'yolov8n-coco-1.0', 'yolo', 'ci-neg');
