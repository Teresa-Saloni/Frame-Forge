package com.frameforge.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Job 1: decode one video, sample frames at a fixed rate, write PNGs to the warehouse bucket,
 * and MERGE a manifest row per frame into lakehouse.bronze.frames.
 *
 * Decoding a single file is inherently sequential, so this job uses the driver for FFmpeg and
 * Spark for the parallel PNG upload + the Iceberg MERGE. The parallelism in FrameForge comes
 * from Airflow running several videos at once (max_active_runs), not from splitting one video.
 *
 * Idempotency: MERGE on (video_id, frame_no) with WHEN NOT MATCHED only. Bronze is append-only;
 * rerunning the same video inserts nothing and overwrites PNGs with identical bytes.
 *
 * Args: --videoId --bucket --key --cameraId --runId [--fps=1] [--maxFrames=0]
 */
public class ExtractFramesJob {

    private static final Logger log = LoggerFactory.getLogger(ExtractFramesJob.class);

    public static void main(String[] args) throws Exception {
        JobArgs a = new JobArgs(args);
        String videoId = a.require("videoId");
        String bucket = a.require("bucket");
        String key = a.require("key");
        String cameraId = a.require("cameraId");
        String runId = a.require("runId");
        double targetFps = a.getDouble("fps", 1.0);
        int maxFrames = a.getInt("maxFrames", 0);

        SparkSession spark = SparkSession.builder().appName("frameforge-extract-" + videoId.substring(0, 8)).getOrCreate();
        Configuration hconf = spark.sparkContext().hadoopConfiguration();

        // 1. Pull the video to local disk once. FFmpeg seeks a lot; s3a streams badly for that.
        File local = File.createTempFile("ff-", ".mp4");
        try (InputStream in = S3Images.open("s3a://" + bucket + "/" + key, hconf)) {
            Files.copy(in, local.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        log.info("downloaded {} ({} bytes)", key, local.length());

        // 2. Decode + sample.
        List<FrameRow> rows = new ArrayList<>();
        Instant videoStart = Instant.now(); // in real deployments: parse from filename or container metadata
        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(local)) {
            grabber.start();
            double srcFps = grabber.getFrameRate() > 0 ? grabber.getFrameRate() : 30.0;
            int step = (int) Math.max(1, Math.round(srcFps / targetFps));
            Java2DFrameConverter conv = new Java2DFrameConverter();
            int n = 0;
            Frame f;
            while ((f = grabber.grabImage()) != null) {
                if (n % step == 0) {
                    BufferedImage img = conv.convert(f);
                    if (img != null) {
                        // copy: converter reuses its buffer between frames
                        BufferedImage copy = new BufferedImage(img.getWidth(), img.getHeight(), BufferedImage.TYPE_3BYTE_BGR);
                        copy.getGraphics().drawImage(img, 0, 0, null);

                        String path = String.format("s3a://warehouse/frames/%s/%s/%06d.png", cameraId, videoId, n);
                        S3Images.writePng(copy, path, hconf);
                        long offsetMs = (long) (n * 1000.0 / srcFps);
                        rows.add(new FrameRow(videoId, cameraId, n,
                                Timestamp.from(videoStart.plusMillis(offsetMs)),
                                path, copy.getWidth(), copy.getHeight(), PHash.of(copy), key, runId));
                        if (maxFrames > 0 && rows.size() >= maxFrames) break;
                    }
                }
                n++;
            }
            log.info("decoded {} frames, kept {} at ~{} fps", n, rows.size(), targetFps);
        } finally {
            local.delete();
        }

        if (rows.isEmpty()) {
            RunLog.write(spark, runId, videoId, "extract", "FAIL", 0, 0, "no frames decoded from " + key);
            throw new IllegalStateException("no frames decoded from " + key);
        }

        // 3. MERGE into bronze. Column names must match the DDL (snake_case).
        Dataset<Row> df = spark.createDataFrame(rows, FrameRow.class)
                .withColumnRenamed("videoId", "video_id")
                .withColumnRenamed("cameraId", "camera_id")
                .withColumnRenamed("frameNo", "frame_no")
                .withColumnRenamed("framePath", "frame_path")
                .withColumnRenamed("sourceKey", "source_key")
                .withColumnRenamed("runId", "run_id");
        df.createOrReplaceTempView("incoming");
        spark.sql("""
                MERGE INTO lakehouse.bronze.frames t
                USING incoming s
                ON t.video_id = s.video_id AND t.frame_no = s.frame_no
                WHEN NOT MATCHED THEN INSERT *
                """);

        long after = spark.table("lakehouse.bronze.frames").filter("video_id = '" + videoId + "'").count();
        RunLog.write(spark, runId, videoId, "extract", "PASS", rows.size(), after, "fps=" + targetFps);
        log.info("bronze now holds {} frames for video {}", after, videoId);
        spark.stop();
    }
}
