package com.frameforge.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Persists the bucket watermark across restarts so a redeploy never re-publishes old videos.
 *
 * Stored as a plain file on a Docker volume. In production this would be a Postgres row or
 * the Kafka consumer-offset trick (store watermark in a compacted topic); a file is enough to
 * show the pattern and keeps the service dependency-free.
 *
 * Also keeps an LRU set of the last 2,000 keys seen, which protects against the one gap in the
 * watermark scheme: an upload whose key sorts *below* the watermark (same-day late arrival).
 * Combined with the deterministic videoId + MERGE downstream, a rare duplicate event is harmless.
 */
@Component
public class WatermarkStore {

    private static final Logger log = LoggerFactory.getLogger(WatermarkStore.class);
    private static final int MAX_SEEN = 2_000;

    private final Path file;
    private final Set<String> seen = Collections.newSetFromMap(
            new LinkedHashMap<String, Boolean>(MAX_SEEN, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> e) {
                    return size() > MAX_SEEN;
                }
            });

    public WatermarkStore(@Value("${frameforge.watermark-file:/data/watermark.txt}") String path) {
        this.file = Path.of(path);
    }

    public synchronized String load() {
        try {
            if (Files.exists(file)) return Files.readString(file, StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            log.warn("could not read watermark file {}: {}", file, e.getMessage());
        }
        return "";
    }

    public synchronized void save(String watermark) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, watermark, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("could not persist watermark: {}", e.getMessage());
        }
    }

    public synchronized boolean seen(String key) {
        return seen.contains(key);
    }

    public synchronized void markSeen(String key) {
        seen.add(key);
    }
}
