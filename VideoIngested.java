package com.frameforge.ingest;

import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

/**
 * The event published to Kafka topic {@code video.ingested}, one per raw video object.
 *
 * @param videoId  sha256(bucket + "/" + key). Deterministic: re-uploading the same object
 *                 yields the same id, so downstream MERGE writes are no-ops. This is the root
 *                 of the pipeline's idempotency.
 * @param bucket   source bucket (always raw-video)
 * @param key      object key, e.g. camera-01/2026-10-20/clip.mp4
 * @param cameraId first path segment of the key
 * @param sizeBytes object size
 * @param etag     S3 ETag (MD5 for single-part uploads)
 * @param observedAt when the watcher saw the object
 */
public record VideoIngested(
        String videoId,
        String bucket,
        String key,
        String cameraId,
        long sizeBytes,
        String etag,
        Instant observedAt) {

    public static VideoIngested from(String bucket, S3Object obj) {
        String key = obj.key();
        String cameraId = key.contains("/") ? key.substring(0, key.indexOf('/')) : "unknown";
        return new VideoIngested(
                sha256(bucket + "/" + key),
                bucket,
                key,
                cameraId,
                obj.size(),
                obj.eTag(),
                Instant.now());
    }

    static String sha256(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
