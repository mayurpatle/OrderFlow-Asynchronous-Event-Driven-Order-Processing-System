package com.orderflow.order;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A KafkaTemplate whose VALUE serializer is StringSerializer, used exclusively
 * by the OutboxRelay.
 *
 * Why a separate template:
 *   The outbox stores events ALREADY serialized to JSON. If we published them
 *   through the normal KafkaTemplate<String,Object> (which uses JsonSerializer),
 *   the JSON string would be serialized AGAIN — double-encoded, escaped, wrapped
 *   in quotes. Consumers would fail to deserialize it.
 *
 *   This template sends the stored JSON bytes verbatim: String key, String value,
 *   both via StringSerializer. What's in the outbox is exactly what lands in Kafka.
 *
 * Reliability settings mirror the main producer: acks=all, idempotence on,
 * infinite retries. Same durability guarantees.
 */
@Configuration
public class OutboxKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:29092}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, String> stringProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(name = "stringKafkaTemplate")
    public KafkaTemplate<String, String> stringKafkaTemplate(
            ProducerFactory<String, String> stringProducerFactory) {
        return new KafkaTemplate<>(stringProducerFactory);
    }
}