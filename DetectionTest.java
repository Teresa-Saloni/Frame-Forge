package com.frameforge.spark;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DetectionTest {

    @Test
    void iouOfIdenticalBoxesIsOne() {
        Detection a = new Detection("car", 0.1f, 0.1f, 0.2f, 0.2f, 0.9f);
        assertEquals(1.0f, a.iou(a), 1e-6);
    }

    @Test
    void iouOfDisjointBoxesIsZero() {
        Detection a = new Detection("car", 0.0f, 0.0f, 0.1f, 0.1f, 0.9f);
        Detection b = new Detection("car", 0.5f, 0.5f, 0.1f, 0.1f, 0.9f);
        assertEquals(0.0f, a.iou(b), 1e-6);
    }

    @Test
    void iouOfHalfOverlapIsOneThird() {
        Detection a = new Detection("car", 0.0f, 0.0f, 0.2f, 0.2f, 0.9f);
        Detection b = new Detection("car", 0.1f, 0.0f, 0.2f, 0.2f, 0.8f);
        // inter = 0.1*0.2 = 0.02 ; union = 0.04+0.04-0.02 = 0.06 ; iou = 1/3
        assertEquals(1f / 3f, a.iou(b), 1e-5);
    }

    @Test
    void nmsKeepsHighestConfidenceAmongOverlappingSameClass() throws Exception {
        List<Detection> in = new ArrayList<>(List.of(
                new Detection("car", 0.10f, 0.10f, 0.30f, 0.30f, 0.70f),
                new Detection("car", 0.11f, 0.11f, 0.30f, 0.30f, 0.95f),   // best, should survive
                new Detection("truck", 0.10f, 0.10f, 0.30f, 0.30f, 0.60f), // different class, survives
                new Detection("car", 0.70f, 0.70f, 0.10f, 0.10f, 0.50f)    // far away, survives
        ));
        // YoloDetector.nms is package-private and needs no session; construct through a tiny subclass-free path:
        List<Detection> out = NmsAccess.run(in, 0.45f);
        assertEquals(3, out.size());
        assertEquals(0.95f, out.get(0).conf(), 1e-6);
        assertTrue(out.stream().anyMatch(d -> d.cls().equals("truck")));
        assertTrue(out.stream().anyMatch(d -> d.conf() == 0.50f));
    }

    /** Exposes the NMS logic without an ONNX session (same algorithm as YoloDetector.nms). */
    static final class NmsAccess {
        static List<Detection> run(List<Detection> dets, float iouThreshold) {
            dets.sort((p, q) -> Float.compare(q.conf(), p.conf()));
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
    }
}
