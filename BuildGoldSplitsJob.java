package com.frameforge.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * Job 4: silver -> gold. Deterministic, leak-free train/val/test split, then tag the snapshot.
 *
 * Split by VIDEO, not by frame: pmod(hash(video_id), 100) < 70 -> train, < 85 -> val, else test.
 * Adjacent frames of one video are near-duplicates; putting them on both sides of the split would
 * make validation accuracy a lie. The hash makes the assignment stable across reruns, so adding a
 * new video never moves an old one between splits.
 *
 * Gold is rebuilt for the touched partitions with overwritePartitions (dynamic overwrite), and the
 * resulting snapshot is tagged so a trainer can pin `FOR VERSION AS OF 'v<N>'` forever.
 *
 * Args: --runId --datasetVersion [--videoId=<only this video's rows, default all>]
 */
public class BuildGoldSplitsJob {

    private static final Logger log = LoggerFactory.getLogger(BuildGoldSplitsJob.class);

    public static void main(String[] args) {
        JobArgs a = new JobArgs(args);
        String runId = a.require("runId");
        String version = a.require("datasetVersion");

        SparkSession spark = SparkSession.builder().appName("frameforge-gold-" + version).getOrCreate();

        Dataset<Row> silver = spark.table("lakehouse.silver.frames")
                .filter(col("label_class").isNotNull().and(col("label_class").notEqual("__none__")));

        Dataset<Row> gold = silver
                .withColumn("bucket", pmod(hash(col("video_id")), lit(100)))
                .withColumn("split",
                        when(col("bucket").lt(70), "train")
                        .when(col("bucket").lt(85), "val")
                        .otherwise("test"))
                .withColumn("dataset_version", lit(version))
                .select("video_id", "camera_id", "frame_no", "ts", "frame_path", "label_class",
                        "bbox_x", "bbox_y", "bbox_w", "bbox_h", "confidence", "model_version",
                        "split", "dataset_version");

        long rows = gold.count();
        gold.writeTo("lakehouse.gold.training_frames").overwritePartitions();

        // Tag the snapshot we just created. Replace if the tag exists (re-run of the same version).
        spark.sql("ALTER TABLE lakehouse.gold.training_frames CREATE OR REPLACE TAG `" + version + "`");

        Dataset<Row> bySplit = spark.table("lakehouse.gold.training_frames").groupBy("split").count();
        String detail = bySplit.collectAsList().stream()
                .map(r -> r.getString(0) + "=" + r.getLong(1))
                .reduce((x, y) -> x + "," + y).orElse("");
        RunLog.write(spark, runId, "*", "gold", "PASS", rows, rows, "version=" + version + " " + detail);
        log.info("gold {} rows tagged {} ({})", rows, version, detail);
        spark.stop();
    }
}
