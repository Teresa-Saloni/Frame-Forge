"""
FrameForge quality gate: Great Expectations 1.x over Trino, run on silver for ONE video.

Returns {"success": bool, "failed": [names], "stats": {...}}. The Airflow task raises on failure,
so gold is never rebuilt from a bad batch.

Six checks, each mapped to a real failure mode:
  1. label completeness      -> labelling job crashed mid-way
  2. bbox inside image        -> decoder returned the wrong size; boxes outside the frame
  3. confidence in [score,1]  -> NMS/threshold misconfigured
  4. valid class set          -> wrong model file loaded
  5. no duplicate detections  -> MERGE key bug
  6. class-distribution drift -> camera moved / scene changed vs last gold tag (custom, KL divergence)

Run standalone:  python gate.py <video_id> <run_id>
"""
from __future__ import annotations

import math
import os
import sys
from datetime import datetime

import great_expectations as gx
from great_expectations import expectations as gxe
from trino.dbapi import connect

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
SCORE_THRESHOLD = float(os.environ.get("FF_SCORE_THRESHOLD", "0.25"))
DRIFT_KL_MAX = float(os.environ.get("FF_DRIFT_KL_MAX", "0.10"))

COCO = [
    "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck", "boat",
    "traffic light", "fire hydrant", "stop sign", "parking meter", "bench", "bird", "cat", "dog",
    "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella",
    "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball", "kite",
    "baseball bat", "baseball glove", "skateboard", "surfboard", "tennis racket", "bottle",
    "wine glass", "cup", "fork", "knife", "spoon", "bowl", "banana", "apple", "sandwich", "orange",
    "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair", "couch", "potted plant",
    "bed", "dining table", "toilet", "tv", "laptop", "mouse", "remote", "keyboard", "cell phone",
    "microwave", "oven", "toaster", "sink", "refrigerator", "book", "clock", "vase", "scissors",
    "teddy bear", "hair drier", "toothbrush", "__none__",
]


def _trino():
    return connect(host=TRINO_HOST, port=TRINO_PORT, user="gate", catalog="lakehouse", schema="silver")


def _scalar(sql: str):
    with _trino() as c:
        cur = c.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None


def _rows(sql: str):
    with _trino() as c:
        cur = c.cursor()
        cur.execute(sql)
        return cur.fetchall()


# ---------------------------------------------------------------- GX checks

def run_gx_suite(video_id: str) -> tuple[bool, list[str]]:
    """Checks 1-5 as a Great Expectations suite on a Trino SQL datasource."""
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_sql(
        name="trino_silver",
        connection_string=f"trino://gate@{TRINO_HOST}:{TRINO_PORT}/lakehouse/silver",
    )
    asset = ds.add_query_asset(
        name="silver_video",
        query=f"SELECT * FROM lakehouse.silver.frames WHERE video_id = '{video_id}'",
    )
    batch_def = asset.add_batch_definition_whole_table("whole")

    suite = context.suites.add(gx.ExpectationSuite(name="silver_frames"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="label_class"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="bbox_x", min_value=0, max_value=1))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="bbox_y", min_value=0, max_value=1))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="bbox_w", min_value=0, max_value=1))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="bbox_h", min_value=0, max_value=1))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="confidence", min_value=0, max_value=1))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="label_class", value_set=COCO))
    suite.add_expectation(gxe.ExpectCompoundColumnsToBeUnique(
        column_list=["video_id", "frame_no", "label_class", "bbox_x", "bbox_y"]))
    suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1))

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name="silver_video_vd", data=batch_def, suite=suite))
    result = vd.run()

    failed = []
    for r in result.results:
        if not r.success:
            cfg = r.expectation_config
            failed.append(f"{cfg.type}({cfg.kwargs.get('column') or cfg.kwargs.get('column_list')})")
    return bool(result.success), failed


# ---------------------------------------------------------------- custom checks

def check_bbox_inside(video_id: str) -> tuple[bool, str]:
    """x+w and y+h must not exceed 1 (GX between-checks look at each column alone)."""
    n = _scalar(f"""
        SELECT COUNT(*) FROM lakehouse.silver.frames
        WHERE video_id = '{video_id}' AND label_class <> '__none__'
          AND (bbox_x + bbox_w > 1.0001 OR bbox_y + bbox_h > 1.0001)
    """)
    return n == 0, f"bbox_exceeds_image({n})"


def check_confidence_floor(video_id: str) -> tuple[bool, str]:
    n = _scalar(f"""
        SELECT COUNT(*) FROM lakehouse.silver.frames
        WHERE video_id = '{video_id}' AND label_class <> '__none__' AND confidence < {SCORE_THRESHOLD}
    """)
    return n == 0, f"confidence_below_threshold({n})"


def check_drift(video_id: str) -> tuple[bool, str]:
    """
    KL divergence between this video's class histogram and the class histogram of the latest
    gold snapshot (what the current dataset looks like). High KL = this camera is seeing
    something the dataset has never seen -> stop and let a human look.
    Skipped (passes) when gold is empty, i.e. the very first run.
    """
    gold_total = _scalar("SELECT COUNT(*) FROM lakehouse.gold.training_frames")
    if not gold_total:
        return True, "drift_skipped_no_gold"

    cur = dict(_rows(f"""
        SELECT label_class, COUNT(*) FROM lakehouse.silver.frames
        WHERE video_id = '{video_id}' AND label_class <> '__none__' GROUP BY 1
    """))
    ref = dict(_rows("SELECT label_class, COUNT(*) FROM lakehouse.gold.training_frames GROUP BY 1"))
    if not cur:
        return True, "drift_skipped_no_labels"

    classes = set(cur) | set(ref)
    eps = 1e-6
    cur_n, ref_n = sum(cur.values()), sum(ref.values())
    kl = 0.0
    for c in classes:
        p = (cur.get(c, 0) + eps) / (cur_n + eps * len(classes))
        q = (ref.get(c, 0) + eps) / (ref_n + eps * len(classes))
        kl += p * math.log(p / q)
    return kl <= DRIFT_KL_MAX, f"class_distribution_drift(kl={kl:.3f},max={DRIFT_KL_MAX})"


# ---------------------------------------------------------------- entry point

def _log(run_id: str, video_id: str, status: str, rows: int, detail: str):
    with _trino() as c:
        cur = c.cursor()
        cur.execute(
            "INSERT INTO lakehouse.ops.run_log VALUES (?, ?, 'quality_gate', ?, ?, ?, ?, ?)",
            (run_id, video_id, status, rows, rows, detail[:900], datetime.utcnow()),
        )


def run_gate(video_id: str, run_id: str) -> dict:
    rows = _scalar(f"SELECT COUNT(*) FROM lakehouse.silver.frames WHERE video_id = '{video_id}'") or 0
    failed: list[str] = []

    ok, gx_failed = run_gx_suite(video_id)
    failed += gx_failed

    for fn in (check_bbox_inside, check_confidence_floor, check_drift):
        ok_i, name = fn(video_id)
        if not ok_i:
            failed.append(name)

    success = not failed
    _log(run_id, video_id, "PASS" if success else "FAIL", rows, ",".join(failed) or "all checks passed")
    return {"success": success, "failed": failed, "stats": {"silver_rows": rows}}


if __name__ == "__main__":
    vid, rid = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "manual")
    res = run_gate(vid, rid)
    print(res)
    sys.exit(0 if res["success"] else 1)
