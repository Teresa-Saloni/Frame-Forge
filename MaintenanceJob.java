package com.frameforge.spark;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Weekly lakehouse hygiene. Every per-video run adds a few small files per partition; left alone,
 * a lakehouse drowns in them (the "small-file problem"). This job:
 *   1. rewrite_data_files   - compacts small Parquet files toward the 128 MB target
 *   2. rewrite_manifests    - compacts metadata
 *   3. expire_snapshots     - drops snapshots older than --retainDays (default 30); tags are kept
 *   4. remove_orphan_files  - deletes files no snapshot references
 *
 * Measure files before/after (sql/demo.sql query 6) and put the number in the README.
 *
 * Args: --runId [--retainDays=30]
 */
public class MaintenanceJob {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceJob.class);

    public static void main(String[] args) {
        JobArgs a = new JobArgs(args);
        String runId = a.require("runId");
        int retainDays = a.getInt("retainDays", 30);

        SparkSession spark = SparkSession.builder().appName("frameforge-maintenance").getOrCreate();
        List<String> tables = List.of("bronze.frames", "silver.frames", "gold.training_frames");

        for (String t : tables) {
            long before = spark.sql("SELECT count(*) FROM lakehouse." + t + ".files").first().getLong(0);

            spark.sql("CALL lakehouse.system.rewrite_data_files(table => '" + t + "', options => map('target-file-size-bytes','134217728'))");
            spark.sql("CALL lakehouse.system.rewrite_manifests('" + t + "')");
            spark.sql("CALL lakehouse.system.expire_snapshots(table => '" + t
                    + "', older_than => TIMESTAMP '" + java.time.LocalDate.now().minusDays(retainDays) + " 00:00:00', retain_last => 5)");
            spark.sql("CALL lakehouse.system.remove_orphan_files(table => '" + t + "')");

            long after = spark.sql("SELECT count(*) FROM lakehouse." + t + ".files").first().getLong(0);
            RunLog.write(spark, runId, "*", "maintenance", "PASS", before, after, t + " files " + before + " -> " + after);
            log.info("{}: data files {} -> {}", t, before, after);
        }
        spark.stop();
    }
}
