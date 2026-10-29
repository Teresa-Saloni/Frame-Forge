package com.frameforge.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * Consumes {@code video.ingested} and triggers one Airflow DAG run per video.
 *
 * Exactly-once story (what to say in interviews):
 *  - Kafka gives at-least-once delivery.
 *  - We commit the offset ONLY after Airflow returns 200/409, so a crash mid-call replays the event.
 *  - The DAG run id is derived from the videoId, so a replay hits Airflow's 409 Conflict
 *    ("run already exists") and we treat that as success.
 *  - Downstream, every table write is a MERGE keyed on (video_id, frame_no), so even a duplicate
 *    run writes nothing new.
 *  Net effect: at-least-once delivery + idempotent consumer + idempotent sink = effectively exactly-once.
 */
@Component
public class AirflowTriggerConsumer {

    private static final Logger log = LoggerFactory.getLogger(AirflowTriggerConsumer.class);

    private final RestClient airflow;
    private final String dagId;

    public AirflowTriggerConsumer(@Value("${frameforge.airflow.url}") String airflowUrl,
                                  @Value("${frameforge.airflow.user}") String user,
                                  @Value("${frameforge.airflow.password}") String password,
                                  @Value("${frameforge.airflow.dag-id:frameforge_video}") String dagId) {
        this.dagId = dagId;
        String basic = java.util.Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes());
        this.airflow = RestClient.builder()
                .baseUrl(airflowUrl)
                .defaultHeader("Authorization", "Basic " + basic)
                .build();
    }

    @KafkaListener(topics = "${frameforge.topic:video.ingested}", groupId = "frameforge-airflow-trigger")
    public void onVideo(VideoIngested ev, Acknowledgment ack) {
        // deterministic run id: one DAG run per video, ever (re-labelling is the backfill DAG's job)
        String runId = "video__" + ev.videoId().substring(0, 16);
        Map<String, Object> body = Map.of(
                "dag_run_id", runId,
                "conf", Map.of(
                        "videoId", ev.videoId(),
                        "bucket", ev.bucket(),
                        "key", ev.key(),
                        "cameraId", ev.cameraId(),
                        "sizeBytes", ev.sizeBytes()));
        try {
            ResponseEntity<String> resp = airflow.post()
                    .uri("/api/v1/dags/{dag}/dagRuns", dagId)
                    .body(body)
                    .retrieve()
                    .toEntity(String.class);
            log.info("triggered {} run={} status={}", dagId, runId, resp.getStatusCode());
            ack.acknowledge();
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.CONFLICT) {
                log.info("run {} already exists, treating as success", runId);
                ack.acknowledge();
            } else {
                // 4xx other than 409 is a bug in our payload: log loudly and still ack, otherwise the
                // consumer blocks forever on a poison message. The video stays in the bucket for a backfill.
                log.error("Airflow rejected run {}: {} {}", runId, e.getStatusCode(), e.getResponseBodyAsString());
                ack.acknowledge();
            }
        } catch (Exception e) {
            // 5xx / network: do NOT ack. Kafka will redeliver after the listener container backs off.
            log.warn("Airflow unreachable for run {}: {} (will retry)", runId, e.getMessage());
            throw e;
        }
    }
}
