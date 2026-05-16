package com.orderflow.common.kafka;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.JacksonMimeTypeModule;

import javax.swing.*;
import java.lang.runtime.ObjectMethods;
import java.util.HashMap;
import java.util.Map;

/**
 * Centralized Kafka configuration for all OrderFlow microservices.
 *
 * Lives in the common module so every service inherits the same producer
 * defaults. Each service's own application.yml provides 'spring.kafka.bootstrap-servers'
 * and 'spring.application.name', which we read via @Value.
 *
 * @EnableKafka activates Spring Kafka's annotation-driven listener infrastructure
 * (the @KafkaListener annotation we'll use for consumers in Session 6).
 *
 * What we're explicitly creating vs. letting auto-config do:
 *   - We define our OWN ProducerFactory and KafkaTemplate with reliability-tuned settings.
 *   - Spring Boot's auto-config has @ConditionalOnMissingBean checks, so our beans win.
 *   - For learning, this is better than auto-config: every setting is visible.
 *   - For production, this is also better: you want to KNOW what your settings are.
 */

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:29092}")
    private String bootstrapServers;

    /**
     * The ObjectMapper used to serialize event payloads to JSON.
     *
     * We register the JavaTimeModule so Jackson knows how to handle Instant
     * (the type we use in EventEnvelope.occurredAt). Without it, Jackson
     * either fails or produces ugly output like {"epochSecond":..., "nano":...}.
     */

    @Bean
    public ObjectMapper kafkaObjectMapper(){
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.findAndRegisterModules();
        return mapper ;

    }

    /**
     * The ProducerFactory creates KafkaProducer instances. We provide the
     * full set of Kafka producer settings here — every choice is deliberate
     * and worth understanding.
     */

    @Bean
    public ProducerFactory<String, Object> producerFactory(ObjectMapper kafkaObjectMapper){
        Map<String , Object> props  = new HashMap<>();

        // connectioon
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // -- Serialization
        // the  key is  a String (we use orderId.toString()).
        // The  value is any object - JsonSerializer turns it into JSON bytes
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , JsonSerializer.class);


        // -- Reliability: the three setting you should never compromise on ---

        // acks=all : leader waits for ALL in-sync replicas to write before acknowledging
        /// this is the durability gold standard. Trade sime latency for gauranteed delivery.
        props.put(ProducerConfig.ACKS_CONFIG ,  "all");

        // enable.idempotence=true: the producer tags every message with a sequence number;
        // the broker dedupes retries automatically. No duplicate messages from network retries.
        // REQUIRES: acks=all, retries>0, max.in.flight<=5. Spring/Kafka enforce these.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // retries=MAX_VALUE: we never want to give up on a transient failure.
        // Combined with idempotence, this is safe — no duplicates.
        // Without idempotence, this would cause duplicates on retried failures.
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // 5 in-flight requests at once. Idempotence preserves ordering even with parallel sends.
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // --- Throughput tuning (sensible defaults — we'd tune these in production load tests) ---

        // Wait up to 10ms to batch messages together before sending.
        // 0 = send immediately (lower latency, lower throughput).
        // Higher = better throughput, slight per-message latency cost.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        // Maximum bytes per batch. When a batch hits this size OR linger.ms expires,
        // the batch is sent. 16KB is the default; fine for our message sizes.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // Compression: snappy is the sweet spot of CPU vs network savings.
        // 'lz4' is also good. 'gzip' is too CPU-heavy. 'zstd' is great but newer.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return  new DefaultKafkaProducerFactory<>(
                props ,
                null , // key serializer constructor — null = use the class we declared

                new org.springframework.kafka.support.serializer.JsonSerializer<>(kafkaObjectMapper)  // value serializer with our ObjectMapper

        );



    }


    /**
     * KafkaTemplate is the high-level send API. Wrapper around KafkaProducer
     * that integrates with Spring's transaction management, error handlers,
     * and metrics.
     *
     * Inject this anywhere you need to publish. It's thread-safe (one instance
     * per app is correct).
     */

    @Bean
    public KafkaTemplate<String , Object >  kafkaTemplate(ProducerFactory<String , Object> pf ){
        return new KafkaTemplate<>(pf) ;

    }




}
