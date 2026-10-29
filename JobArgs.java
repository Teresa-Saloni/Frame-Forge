package com.frameforge.spark;

import java.util.HashMap;
import java.util.Map;

/**
 * Minimal --key=value argument parser so every job has the same CLI shape:
 *   spark-submit ... ExtractFramesJob --videoId=... --key=... --cameraId=... --runId=...
 */
public final class JobArgs {
    private final Map<String, String> m = new HashMap<>();

    public JobArgs(String[] args) {
        for (String a : args) {
            if (!a.startsWith("--")) continue;
            int eq = a.indexOf('=');
            if (eq < 0) m.put(a.substring(2), "true");
            else m.put(a.substring(2, eq), a.substring(eq + 1));
        }
    }

    public String require(String k) {
        String v = m.get(k);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("missing --" + k);
        return v;
    }

    public String get(String k, String dflt) {
        return m.getOrDefault(k, dflt);
    }

    public int getInt(String k, int dflt) {
        return Integer.parseInt(get(k, Integer.toString(dflt)));
    }

    public double getDouble(String k, double dflt) {
        return Double.parseDouble(get(k, Double.toString(dflt)));
    }
}
