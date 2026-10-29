package com.frameforge.spark;

import org.junit.jupiter.api.Test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class PHashTest {

    /** A synthetic "scene": gradient background + a few rectangles, deterministic from a seed. */
    private static BufferedImage scene(long seed, int w, int h) {
        BufferedImage img = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        for (int y = 0; y < h; y++) {
            g.setColor(new Color(y * 255 / h, 120, 255 - y * 255 / h));
            g.drawLine(0, y, w, y);
        }
        Random r = new Random(seed);
        for (int i = 0; i < 5; i++) {
            g.setColor(new Color(r.nextInt(256), r.nextInt(256), r.nextInt(256)));
            g.fillRect(r.nextInt(w / 2), r.nextInt(h / 2), 40 + r.nextInt(w / 3), 40 + r.nextInt(h / 3));
        }
        g.dispose();
        return img;
    }

    @Test
    void identicalImagesHaveZeroDistance() {
        BufferedImage a = scene(1, 640, 360);
        assertEquals(0, PHash.distance(PHash.of(a), PHash.of(a)));
    }

    @Test
    void hashIsDeterministic() {
        assertEquals(PHash.of(scene(7, 640, 360)), PHash.of(scene(7, 640, 360)));
    }

    @Test
    void resizedImageIsNearDuplicate() {
        // same scene downscaled 2x -> structure identical, pixels not
        BufferedImage a = scene(3, 640, 360);
        BufferedImage b = new BufferedImage(320, 180, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = b.createGraphics();
        g.drawImage(a, 0, 0, 320, 180, null);
        g.dispose();
        int d = PHash.distance(PHash.of(a), PHash.of(b));
        assertTrue(d <= 8, "expected near-duplicate, distance was " + d);
    }

    @Test
    void jpegLikeNoiseIsNearDuplicate() {
        BufferedImage a = scene(4, 640, 360);
        BufferedImage b = new BufferedImage(640, 360, BufferedImage.TYPE_INT_RGB);
        Random r = new Random(42);
        for (int y = 0; y < 360; y++)
            for (int x = 0; x < 640; x++) {
                Color c = new Color(a.getRGB(x, y));
                int n = r.nextInt(17) - 8; // +-8 per channel
                b.setRGB(x, y, new Color(clamp(c.getRed() + n), clamp(c.getGreen() + n), clamp(c.getBlue() + n)).getRGB());
            }
        int d = PHash.distance(PHash.of(a), PHash.of(b));
        assertTrue(d <= 8, "pixel noise should not break the hash, distance was " + d);
    }

    private static int clamp(int v) { return Math.max(0, Math.min(255, v)); }

    @Test
    void brightnessShiftIsNearDuplicate() {
        BufferedImage a = scene(5, 640, 360);
        BufferedImage b = new BufferedImage(640, 360, BufferedImage.TYPE_INT_RGB);
        for (int y = 0; y < 360; y++)
            for (int x = 0; x < 640; x++) {
                Color c = new Color(a.getRGB(x, y));
                b.setRGB(x, y, new Color(Math.min(255, c.getRed() + 30), Math.min(255, c.getGreen() + 30),
                        Math.min(255, c.getBlue() + 30)).getRGB());
            }
        int d = PHash.distance(PHash.of(a), PHash.of(b));
        assertTrue(d <= 8, "brightness shift should not break the hash, distance was " + d);
    }

    @Test
    void differentScenesAreFar() {
        int d = PHash.distance(PHash.of(scene(11, 640, 360)), PHash.of(scene(99, 640, 360)));
        assertTrue(d > 8, "different scenes must exceed the dedup threshold, distance was " + d);
    }

    @Test
    void medianOfEvenAndOddLength() {
        assertEquals(2.5, PHash.median(new double[]{4, 1, 3, 2}), 1e-9);
        assertEquals(3.0, PHash.median(new double[]{5, 1, 3}), 1e-9);
    }
}
