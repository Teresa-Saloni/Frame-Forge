package com.frameforge.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Job 3: auto-label unlabelled silver frames with YOLO, MERGE labels back into silver.
 *
 * Executor pattern worth explaining in interviews:
 *   - The ~6 MB ONNX model is read once on the driver and broadcast, so each executor gets one copy,
 *     not one per task.
 *   - mapPartitions creates ONE OrtSession per partition (~200 ms), then runs every frame in that
 *     partition through it. Doing it per row would make session creation dominate runtime.
 *   - Frames are read from S3 inside the executor, so I/O is parallel too.
 *
 * Output: one row per detection. A frame with zero detections gets one row with
 * label_class='__none__' so the gate can still confirm the frame was processed.
 *
 * Args: --videoId --runId [--model=/opt/models/yolov8n.onnx] [--score=0.25] [--iou=0.45] [--partitions=4]
 */
public class LabelFramesJob {

    private static final Logger log = LoggerFactory.getLogger(LabelFramesJob.class);

    public static void main(String[] args) throws Exception {
        JobArgs a = new JobArgs(args);
        String videoId = a.require("videoId");
        String runId = a.require("runId");
        String modelPath = a.get("model", "/opt/models/yolov8n.onnx");
        float score = (float) a.getDouble("score", 0.25);
        float iou = (float) a.getDouble("iou", 0.45);
        int partitions = a.getInt("partitions", 4);

        SparkSession spark = SparkSession.builder().appName("frameforge-label-" + videoId.substring(0, 8)).getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        byte[] model = Files.readAllBytes(Path.of(modelPath));
        Broadcast<byte[]> bModel = jsc.broadcast(model);
        Broadcast<SerializableConfiguration> bConf =
                jsc.broadcast(new SerializableConfiguration(spark.sparkContext().hadoopConfiguration()));
        log.info("broadcast model {} ({} KB)", modelPath, model.length / 1024);

        Dataset<Row> todo = spark.table("lakehouse.silver.frames")
                .filter(col("video_id").equalTo(videoId).and(col("label_class").isNull()))
                .select("video_id", "frame_no", "frame_path")
                .repartition(partitions);
        long framesIn = todo.count();

        StructType outSchema = new StructType()
                .add("video_id", DataTypes.StringType)
                .add("frame_no", DataTypes.IntegerType)
                .add("label_class", DataTypes.StringType)
                .add("bbox_x", DataTypes.FloatType)
                .add("bbox_y", DataTypes.FloatType)
                .add("bbox_w", DataTypes.FloatType)
                .add("bbox_h", DataTypes.FloatType)
                .add("confidence", DataTypes.FloatType)
                .add("model_version", DataTypes.StringType);

        Dataset<Row> labels = todo.mapPartitions((MapPartitionsFunction<Row, Row>) it -> {
            List<Row> out = new ArrayList<>();
            Configuration conf = bConf.value().value();
            try (YoloDetector det = new YoloDetector(bModel.value(), score, iou)) {
                while (it.hasNext()) {
                    Row r = it.next();
                    String vid = r.getString(0);
                    int frameNo = r.getInt(1);
                    BufferedImage img = S3Images.readPng(r.getString(2), conf);
                    List<Detection> ds = det.detect(img);
                    if (ds.isEmpty()) {
                        out.add(RowFactory.create(vid, frameNo, "__none__", 0f, 0f, 0f, 0f, 0f, YoloDetector.MODEL_VERSION));
                    }
                    for (Detection d : ds) {
                        out.add(RowFactory.create(vid, frameNo, d.cls(), d.x(), d.y(), d.w(), d.h(), d.conf(), YoloDetector.MODEL_VERSION));
                    }
                }
            }
            return out.iterator();
        }, RowEncoder.apply(outSchema));

        labels.createOrReplaceTempView("labels");

        // A frame may produce N detections but silver has one unlabelled placeholder row per frame.
        // Strategy: delete the placeholders for this video, then insert all detection rows.
        // Both statements are inside one job; Iceberg commits each atomically and the gate re-checks
        // completeness afterwards, so a crash between them is caught, not silently accepted.
        spark.sql("""
                MERGE INTO lakehouse.silver.frames t
                USING (SELECT DISTINCT video_id, frame_no FROM labels) s
                ON t.video_id = s.video_id AND t.frame_no = s.frame_no AND t.label_class IS NULL
                WHEN MATCHED THEN DELETE
                """);

        spark.sql("""
                INSERT INTO lakehouse.silver.frames
                SELECT b.video_id, b.camera_id, b.frame_no, b.ts, b.frame_path, b.phash,
                       l.label_class, l.bbox_x, l.bbox_y, l.bbox_w, l.bbox_h, l.confidence,
                       l.model_version, 'yolo' AS label_source, '%s' AS run_id
                FROM labels l
                JOIN lakehouse.bronze.frames b ON b.video_id = l.video_id AND b.frame_no = l.frame_no
                """.formatted(runId));

        long labelRows = spark.table("lakehouse.silver.frames")
                .filter(col("video_id").equalTo(videoId).and(col("run_id").equalTo(runId))).count();
        RunLog.write(spark, runId, videoId, "label", "PASS", framesIn, labelRows,
                "model=" + YoloDetector.MODEL_VERSION + " score=" + score + " iou=" + iou);
        log.info("labelled {} frames -> {} label rows", framesIn, labelRows);
        spark.stop();
    }
}
