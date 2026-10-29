package com.frameforge.spark;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Job 2: perceptual-hash deduplication, bronze -> silver (unlabelled).
 *
 * For each video, walk frames in order and keep a frame only if its pHash is further than
 * {@code --threshold} (default 8) Hamming bits from every frame already kept in that video.
 * Near-identical consecutive frames (parked car, static scene) collapse to one.
 *
 * Spark layout: repartition by video_id, sort within partition by frame_no, then run the
 * sequential scan inside mapPartitions. One video never spans two partitions, so the scan is
 * correct, and different videos run in parallel on different executors.
 *
 * Cost: O(n * k) per video where k = frames kept so far. A 1-hour video at 1 fps is 3,600 frames
 * against at most a few hundred kept — milliseconds. If k ever grows large, swap the list for a
 * BK-tree or a multi-index LSH; say that in interviews, don't build it.
 *
 * Args: --videoId --runId [--threshold=8]
 */
public class DedupFramesJob {

    private static final Logger log = LoggerFactory.getLogger(DedupFramesJob.class);

    public static void main(String[] args) {
        JobArgs a = new JobArgs(args);
        String videoId = a.require("videoId");
        String runId = a.require("runId");
        int threshold = a.getInt("threshold", 8);

        SparkSession spark = SparkSession.builder().appName("frameforge-dedup-" + videoId.substring(0, 8)).getOrCreate();

        Dataset<Row> bronze = spark.table("lakehouse.bronze.frames")
                .filter(col("video_id").equalTo(videoId))
                .select("video_id", "camera_id", "frame_no", "ts", "frame_path", "phash");
        long in = bronze.count();

        final int VID = 0, FRAME = 2, HASH = 5;
        Dataset<Row> kept = bronze
                .repartition(col("video_id"))
                .sortWithinPartitions("video_id", "frame_no")
                .mapPartitions((MapPartitionsFunction<Row, Row>) it -> {
                    List<Row> out = new ArrayList<>();
                    List<Long> keptHashes = new ArrayList<>();
                    String current = null;
                    while (it.hasNext()) {
                        Row r = it.next();
                        String v = r.getString(VID);
                        if (!v.equals(current)) { current = v; keptHashes.clear(); }
                        long h = r.getLong(HASH);
                        boolean dup = false;
                        for (long k : keptHashes) {
                            if (PHash.distance(k, h) <= threshold) { dup = true; break; }
                        }
                        if (!dup) { keptHashes.add(h); out.add(r); }
                    }
                    return out.iterator();
                }, RowEncoder.apply(bronze.schema()));

        kept.createOrReplaceTempView("deduped");
        spark.sql("""
                MERGE INTO lakehouse.silver.frames t
                USING deduped s
                ON t.video_id = s.video_id AND t.frame_no = s.frame_no
                WHEN NOT MATCHED THEN INSERT
                  (video_id, camera_id, frame_no, ts, frame_path, phash,
                   label_class, bbox_x, bbox_y, bbox_w, bbox_h, confidence, model_version, label_source, run_id)
                VALUES
                  (s.video_id, s.camera_id, s.frame_no, s.ts, s.frame_path, s.phash,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '%s')
                """.formatted(runId));

        long out = spark.table("lakehouse.silver.frames")
                .filter(col("video_id").equalTo(videoId)).select("frame_no").distinct().count();
        double removedPct = in == 0 ? 0 : 100.0 * (in - out) / in;
        RunLog.write(spark, runId, videoId, "dedup", "PASS", in, out,
                String.format("threshold=%d removed=%.1f%%", threshold, removedPct));
        log.info("dedup video={} in={} kept={} removed={}%", videoId, in, out, String.format("%.1f", removedPct));
        spark.stop();
    }
}
