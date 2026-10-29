package com.frameforge.spark;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.util.Arrays;

/**
 * 64-bit perceptual hash (pHash) of an image.
 *
 * Algorithm (classic DCT pHash):
 *   1. Resize to 32x32 greyscale.
 *   2. 2-D DCT-II.
 *   3. Keep the top-left 8x8 block of coefficients (lowest frequencies = overall structure).
 *   4. Bit i = 1 if coefficient i is above the median of those 64 values.
 *
 * Two frames that look the same to a human (same scene, slightly different compression noise or
 * brightness) have a Hamming distance of 0-8. Two different scenes land around 32 (random).
 *
 * Why not MD5 of pixels: consecutive video frames are never byte-identical, so MD5 finds nothing.
 * Why not average-hash: aHash is fooled by global brightness changes; DCT compares structure.
 *
 * Pure Java, no native libs, so it runs inside any Spark executor and is trivially unit-testable.
 */
public final class PHash {

    private static final int N = 32;
    private static final int LOW = 8;
    private static final double[][] COS = buildCosTable();

    private PHash() {}

    /** Compute the 64-bit hash of any BufferedImage. */
    public static long of(BufferedImage img) {
        double[][] g = toGray32(img);
        double[][] d = dct2(g);
        double[] low = new double[LOW * LOW];
        for (int y = 0; y < LOW; y++)
            for (int x = 0; x < LOW; x++)
                low[y * LOW + x] = d[y][x];
        double median = median(low);
        long bits = 0L;
        for (int i = 0; i < low.length; i++)
            if (low[i] > median) bits |= 1L << i;
        return bits;
    }

    /** Hamming distance between two hashes; 0 = identical structure. */
    public static int distance(long a, long b) {
        return Long.bitCount(a ^ b);
    }

    // ---- internals ----

    static double[][] toGray32(BufferedImage src) {
        BufferedImage small = new BufferedImage(N, N, BufferedImage.TYPE_BYTE_GRAY);
        Graphics2D g = small.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g.drawImage(src, 0, 0, N, N, null);
        g.dispose();
        double[][] out = new double[N][N];
        for (int y = 0; y < N; y++)
            for (int x = 0; x < N; x++)
                out[y][x] = small.getRaster().getSample(x, y, 0);
        return out;
    }

    /** Separable 2-D DCT-II: rows then columns. O(N^3) is fine for N=32. */
    static double[][] dct2(double[][] in) {
        double[][] tmp = new double[N][N];
        double[][] out = new double[N][N];
        for (int y = 0; y < N; y++)
            for (int u = 0; u < N; u++) {
                double s = 0;
                for (int x = 0; x < N; x++) s += in[y][x] * COS[u][x];
                tmp[y][u] = s;
            }
        for (int u = 0; u < N; u++)
            for (int v = 0; v < N; v++) {
                double s = 0;
                for (int y = 0; y < N; y++) s += tmp[y][u] * COS[v][y];
                out[v][u] = s;
            }
        return out;
    }

    private static double[][] buildCosTable() {
        double[][] t = new double[N][N];
        for (int u = 0; u < N; u++)
            for (int x = 0; x < N; x++)
                t[u][x] = Math.cos((2.0 * x + 1.0) * u * Math.PI / (2.0 * N));
        return t;
    }

    static double median(double[] v) {
        double[] c = v.clone();
        Arrays.sort(c);
        int m = c.length / 2;
        return (c.length % 2 == 0) ? (c[m - 1] + c[m]) / 2.0 : c[m];
    }
}
