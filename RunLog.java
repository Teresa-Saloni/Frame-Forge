package com.frameforge.spark;

import org.apache.spark.sql.SparkSession;

/**
 * Appends one row to lakehouse.ops.run_log. Every job calls this at the end so the whole run
 * can be reconstructed from SQL (see sql/demo.sql, query 7).
 */
public final class RunLog {
    private RunLog() {}

    public static void write(SparkSession spark, String runId, String videoId, String stage,
                             String status, long rowsIn, long rowsOut, String detail) {
        String safeDetail = detail == null ? "" : detail.replace("'", "''");
        spark.sql(String.format(
                "INSERT INTO lakehouse.ops.run_log VALUES ('%s','%s','%s','%s',%d,%d,'%s',current_timestamp())",
                runId, videoId, stage, status, rowsIn, rowsOut, safeDetail));
    }
}
