package com.frameforge.ingest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class InfraConfig {

    @Bean
    public S3Client s3Client(@Value("${frameforge.s3.endpoint}") String endpoint,
                             @Value("${frameforge.s3.access-key}") String ak,
                             @Value("${frameforge.s3.secret-key}") String sk,
                             @Value("${frameforge.s3.region:us-east-1}") String region) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)))
                .forcePathStyle(true)   // MinIO uses path-style, not virtual-host-style
                .build();
    }

    @Bean
    public ProducerFactory<String, VideoIngested> producerFactory(@Value("${frameforge.kafka.bootstrap}") String bootstrap) {
        Map<String, Object> p = new HashMap<>();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        p.put(ProducerConfig.ACKS_CONFIG, "all");               // durable before we move the watermark
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // broker dedups producer retries
        return new DefaultKafkaProducerFactory<>(p);
    }

    @Bean
    public KafkaTemplate<String, VideoIngested> kafkaTemplate(ProducerFactory<String, VideoIngested> pf) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    public ConsumerFactory<String, VideoIngested> consumerFactory(@Value("${frameforge.kafka.bootstrap}") String bootstrap) {
        Map<String, Object> c = new HashMap<>();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // manual ack after Airflow accepts
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(JsonDeserializer.TRUSTED_PACKAGES, "com.frameforge.ingest");
        c.put(JsonDeserializer.VALUE_DEFAULT_TYPE, VideoIngested.class.getName());
        return new DefaultKafkaConsumerFactory<>(c);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VideoIngested> kafkaListenerContainerFactory(
            ConsumerFactory<String, VideoIngested> cf) {
        var f = new ConcurrentKafkaListenerContainerFactory<String, VideoIngested>();
        f.setConsumerFactory(cf);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        f.setConcurrency(1);
        return f;
    }
}
