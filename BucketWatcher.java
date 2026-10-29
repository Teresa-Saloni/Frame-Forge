package com.frameforge.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Polls the raw-video bucket and publishes a {@link VideoIngested} event for every object
 * not yet seen.
 *
 * Why polling and not S3 bucket notifications: MinIO supports notifications, but polling with a
 * {@code startAfter} watermark is portable to every S3-compatible store, needs no extra config,
 * and costs one cheap LIST per interval. The watermark is the highest key seen so far; S3 LIST
 * returns keys in lexicographic order, so {@code startAfter=watermark} returns only new keys.
 *
 * Trade-off: keys must sort after the watermark. The convention camera/YYYY-MM-DD/name.mp4
 * guarantees that for new days; within a day, upload order is not guaranteed, which is why the
 * {@link WatermarkStore} also keeps a small set of recent keys to catch late arrivals.
 */
@Component
public class BucketWatcher {

    private static final Logger log = LoggerFactory.getLogger(BucketWatcher.class);

    private final S3Client s3;
    private final KafkaTemplate<String, VideoIngested> kafka;
    private final WatermarkStore watermarks;
    private final String bucket;
    private final String topic;
    private final AtomicReference<String> watermark;

    public BucketWatcher(S3Client s3,
                         KafkaTemplate<String, VideoIngested> kafka,
                         WatermarkStore watermarks,
                         @Value("${frameforge.raw-bucket:raw-video}") String bucket,
                         @Value("${frameforge.topic:video.ingested}") String topic) {
        this.s3 = s3;
        this.kafka = kafka;
        this.watermarks = watermarks;
        this.bucket = bucket;
        this.topic = topic;
        this.watermark = new AtomicReference<>(watermarks.load());
        log.info("BucketWatcher starting at watermark='{}'", watermark.get());
    }

    @Scheduled(fixedDelayString = "${frameforge.poll-ms:5000}")
    public void poll() {
        String token = null;
        int published = 0;
        do {
            ListObjectsV2Request req = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .startAfter(watermark.get().isEmpty() ? null : watermark.get())
                    .continuationToken(token)
                    .maxKeys(500)
                    .build();
            ListObjectsV2Response resp = s3.listObjectsV2(req);
            for (S3Object obj : resp.contents()) {
                if (!isVideo(obj.key()) || watermarks.seen(obj.key())) continue;
                VideoIngested ev = VideoIngested.from(bucket, obj);
                // key by videoId so all events for one video land on the same partition (ordering)
                kafka.send(topic, ev.videoId(), ev);
                watermarks.markSeen(obj.key());
                if (obj.key().compareTo(watermark.get()) > 0) watermark.set(obj.key());
                published++;
                log.info("published video.ingested id={} key={} size={}B", ev.videoId(), ev.key(), ev.sizeBytes());
            }
            token = resp.isTruncated() ? resp.nextContinuationToken() : null;
        } while (token != null);
        if (published > 0) watermarks.save(watermark.get());
    }

    private static boolean isVideo(String key) {
        String k = key.toLowerCase();
        return k.endsWith(".mp4") || k.endsWith(".mkv") || k.endsWith(".mov") || k.endsWith(".avi");
    }
}
