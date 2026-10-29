package com.frameforge.spark;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * One extracted frame, as written to bronze. A plain Java bean (getters/setters) so Spark can
 * infer the schema with {@code createDataFrame(list, FrameRow.class)}.
 */
public class FrameRow implements Serializable {
    private String videoId;
    private String cameraId;
    private int frameNo;
    private Timestamp ts;
    private String framePath;
    private int width;
    private int height;
    private long phash;
    private String sourceKey;
    private String runId;

    public FrameRow() {}

    public FrameRow(String videoId, String cameraId, int frameNo, Timestamp ts, String framePath,
                    int width, int height, long phash, String sourceKey, String runId) {
        this.videoId = videoId; this.cameraId = cameraId; this.frameNo = frameNo; this.ts = ts;
        this.framePath = framePath; this.width = width; this.height = height; this.phash = phash;
        this.sourceKey = sourceKey; this.runId = runId;
    }

    public String getVideoId() { return videoId; }
    public void setVideoId(String v) { videoId = v; }
    public String getCameraId() { return cameraId; }
    public void setCameraId(String v) { cameraId = v; }
    public int getFrameNo() { return frameNo; }
    public void setFrameNo(int v) { frameNo = v; }
    public Timestamp getTs() { return ts; }
    public void setTs(Timestamp v) { ts = v; }
    public String getFramePath() { return framePath; }
    public void setFramePath(String v) { framePath = v; }
    public int getWidth() { return width; }
    public void setWidth(int v) { width = v; }
    public int getHeight() { return height; }
    public void setHeight(int v) { height = v; }
    public long getPhash() { return phash; }
    public void setPhash(long v) { phash = v; }
    public String getSourceKey() { return sourceKey; }
    public void setSourceKey(String v) { sourceKey = v; }
    public String getRunId() { return runId; }
    public void setRunId(String v) { runId = v; }
}
