package com.frameforge.spark;

import java.io.Serializable;

/**
 * One detected object. Box is normalised to [0,1] in the ORIGINAL image's coordinate space
 * (x,y = top-left; w,h = size), which is what silver stores and what the quality gate checks.
 */
public record Detection(String cls, float x, float y, float w, float h, float conf) implements Serializable {

    /** Intersection-over-union with another box; used by NMS. */
    public float iou(Detection o) {
        float ix1 = Math.max(x, o.x), iy1 = Math.max(y, o.y);
        float ix2 = Math.min(x + w, o.x + o.w), iy2 = Math.min(y + h, o.y + o.h);
        float inter = Math.max(0, ix2 - ix1) * Math.max(0, iy2 - iy1);
        float union = w * h + o.w * o.h - inter;
        return union <= 0 ? 0 : inter / union;
    }
}
