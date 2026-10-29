package com.frameforge.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Ingest service: watches the raw-video bucket, publishes one Kafka event per new video,
 * and consumes those events to trigger the Airflow DAG.
 *
 * Two responsibilities in one small process on purpose: the producer side (bucket -> Kafka)
 * and the consumer side (Kafka -> Airflow) are decoupled by the topic, so either can be
 * restarted, replayed or scaled independently.
 */
@SpringBootApplication
@EnableScheduling
public class IngestApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestApplication.class, args);
    }
}
