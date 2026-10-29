package com.frameforge.spark;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * YOLOv8 inference via ONNX Runtime (CPU). One instance per Spark partition (see LabelFramesJob).
 *
 * Model contract (ultralytics export, imgsz=640, opset 12):
 *   input  "images" : float32 [1, 3, 640, 640], RGB, 0..1, letterboxed
 *   output "output0": float32 [1, 84, 8400]  -> 4 box coords (cx, cy, w, h in pixels) + 80 class scores
 *
 * Pipeline: letterbox -> CHW tensor -> run -> decode (score threshold) -> NMS -> un-letterbox -> normalise.
 *
 * Why this is "off the shelf" and why that is the right call: the project's claim is about data
 * trustworthiness, not detection accuracy. A pretrained COCO model gives usable labels for
 * vehicles, people, bikes etc. Swapping in a fine-tuned .onnx is a one-line change (--model=).
 */
public final class YoloDetector implements AutoCloseable {

    public static final int INPUT = 640;
    public static final String MODEL_VERSION = "yolov8n-coco-1.0";

    private final OrtEnvironment env;
    private final OrtSession session;
    private final float scoreThreshold;
    private final float iouThreshold;

    public YoloDetector(byte[] modelBytes, float scoreThreshold, float iouThreshold) throws OrtException {
        this.env = OrtEnvironment.getEnvironment();
        OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
        opts.setIntraOpNumThreads(2); // executors already run many tasks; don't oversubscribe
        this.session = env.createSession(modelBytes, opts);
        this.scoreThreshold = scoreThreshold;
        this.iouThreshold = iouThreshold;
    }

    public List<Detection> detect(BufferedImage img) throws OrtException {
        int W = img.getWidth(), H = img.getHeight();
        float scale = Math.min((float) INPUT / W, (float) INPUT / H);
        int newW = Math.round(W * scale), newH = Math.round(H * scale);
        int padX = (INPUT - newW) / 2, padY = (INPUT - newH) / 2;

        float[] input = letterbox(img, newW, newH, padX, padY);
        long[] shape = {1, 3, INPUT, INPUT};

        List<Detection> raw = new ArrayList<>();
        try (OnnxTensor t = OnnxTensor.createTensor(env, FloatBuffer.wrap(input), shape);
             OrtSession.Result r = session.run(Map.of("images", t))) {
            float[][][] out = (float[][][]) r.get(0).getValue(); // [1][84][8400]
            float[][] o = out[0];
            int numAnchors = o[0].length;
            int numClasses = o.length - 4;
            for (int i = 0; i < numAnchors; i++) {
                int best = -1; float bestScore = 0;
                for (int c = 0; c < numClasses; c++) {
                    float s = o[4 + c][i];
                    if (s > bestScore) { bestScore = s; best = c; }
                }
                if (bestScore < scoreThreshold) continue;
                float cx = o[0][i], cy = o[1][i], bw = o[2][i], bh = o[3][i];
                // un-letterbox into original pixel space, then normalise to 0..1 and clamp
                float x = ((cx - bw / 2) - padX) / scale;
                float y = ((cy - bh / 2) - padY) / scale;
                float w = bw / scale, h = bh / scale;
                float nx = clamp(x / W), ny = clamp(y / H);
                float nw = clamp(Math.min(w / W, 1f - nx)), nh = clamp(Math.min(h / H, 1f - ny));
                if (nw <= 0 || nh <= 0) continue;
                raw.add(new Detection(CocoClasses.name(best), nx, ny, nw, nh, bestScore));
            }
        }
        return nms(raw);
    }

    /** Greedy class-wise non-max suppression. */
    List<Detection> nms(List<Detection> dets) {
        dets.sort(Comparator.comparingDouble((Detection d) -> d.conf()).reversed());
        List<Detection> keep = new ArrayList<>();
        boolean[] dead = new boolean[dets.size()];
        for (int i = 0; i < dets.size(); i++) {
            if (dead[i]) continue;
            Detection a = dets.get(i);
            keep.add(a);
            for (int j = i + 1; j < dets.size(); j++) {
                if (dead[j]) continue;
                Detection b = dets.get(j);
                if (a.cls().equals(b.cls()) && a.iou(b) > iouThreshold) dead[j] = true;
            }
        }
        return keep;
    }

    private static float[] letterbox(BufferedImage src, int newW, int newH, int padX, int padY) {
        BufferedImage canvas = new BufferedImage(INPUT, INPUT, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = canvas.createGraphics();
        g.setColor(new java.awt.Color(114, 114, 114)); // ultralytics grey padding
        g.fillRect(0, 0, INPUT, INPUT);
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g.drawImage(src, padX, padY, newW, newH, null);
        g.dispose();

        float[] chw = new float[3 * INPUT * INPUT];
        int plane = INPUT * INPUT;
        int[] px = canvas.getRGB(0, 0, INPUT, INPUT, null, 0, INPUT);
        for (int i = 0; i < plane; i++) {
            int p = px[i];
            chw[i]             = ((p >> 16) & 0xFF) / 255f; // R
            chw[plane + i]     = ((p >> 8) & 0xFF) / 255f;  // G
            chw[2 * plane + i] = (p & 0xFF) / 255f;         // B
        }
        return chw;
    }

    private static float clamp(float v) {
        return Math.max(0f, Math.min(1f, v));
    }

    @Override
    public void close() throws OrtException {
        session.close();
    }
}
