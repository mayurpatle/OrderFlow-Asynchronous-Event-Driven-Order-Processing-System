package com.orderflow.common.kafka;

//import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.orderflow.common.events.EventEnvelope;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.JacksonMimeTypeModule;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

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

    @Value("${spring.application.name:default-service}")
    private String applicationName;


    // =========================================================================
    // OBJECT MAPPER — shared by both producer and consumer serializers
    // =========================================================================


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

    // =========================================================================
    // PRODUCER (unchanged from Session 5)
    // =========================================================================

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

        // Don't add the __TypeId__ header. Consumers must know what type they expect
        // based on the TOPIC they're subscribed to — not based on a header the producer
        // sets. This decouples producer and consumer class names: the producer could
        // be in Python and have no Java classes at all, and Java consumers still work.
        JsonSerializer<Object> valueSerializer = new JsonSerializer<>(kafkaObjectMapper);
        valueSerializer.setAddTypeInfo(false);

        return  new DefaultKafkaProducerFactory<>(
                props ,
                new StringSerializer() ,
                valueSerializer
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

    // =========================================================================
    // CONSUMER — new in Session 6
    // =========================================================================

    /*
     * The ConsumerFactory creates KafkaConsumer instances. Like the ProducerFactory,
     * we set every important property explicitly so reliability defaults are
     * visible and intentional.
     */

    @Bean
    public ConsumerFactory<String , Object > consumerFactory(ObjectMapper kafkaObjectMapper){
        Map<String , Object> props   = new HashMap<>() ;

        // Connection
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServers);

        // --- Consumer Group ---
        //
        // Default group ID = the service's application.name from application.yml.
        // So notification-service joins the "notification-service" group,
        // order-service joins the "order-service" group, etc.
        //
        // Each @KafkaListener annotation can override this with its own groupId,
        // but the default is sensible: one group per service.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, applicationName ) ;

        // --- Where to start reading ---
        //
        // 'earliest' = if this consumer group has no committed offset (brand new),
        //              start from the beginning of the topic.
        // 'latest'   = start from the end, ignore historical messages.
        //
        // 'earliest' is the right default for our case. If notification-service
        // starts fresh for the first time, we want it to consume any pending
        // events. If it's been running and crashes, it picks up from its last
        // committed offset (this setting only kicks in when there's NO committed
        // offset).
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");





        // --- Reliability ---
        //
        // ENABLE_AUTO_COMMIT=false: we want the consumer to commit offsets only
        // AFTER our @KafkaListener method returns successfully. Auto-commit
        // commits on a timer, which means a message could be marked "consumed"
        // before we actually process it — leading to message loss on crashes.
        //
        // Spring Kafka's default with @KafkaListener is to commit after the method
        // returns, which is what we want. We set it explicitly here so the choice
        // is visible.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // --- Throughput tuning ---
        //
        // Max records returned in a single poll. Higher = better throughput
        // (less polling overhead), lower = lower latency (process less per batch).
        // 500 is the default; reasonable for our case.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);



        // Build the deserializer with the target type (EventEnvelope) explicitly.
        // This is the CRITICAL line: passing the class as the first arg tells
        // JsonDeserializer exactly what type to produce. No header guessing,
        // no string class-name resolution.
        @SuppressWarnings({"rawtypes", "unchecked"})
        JsonDeserializer<Object> valueDeserializer = (JsonDeserializer)
                new JsonDeserializer<>(EventEnvelope.class, kafkaObjectMapper, false)
                        .trustedPackages("com.orderflow.*", "java.util", "java.lang");

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                valueDeserializer
        );



    }

    /*
     * The listener container factory. @KafkaListener annotations look up THIS
     * bean by name (or by default if there's only one). It creates the actual
     * threads that poll Kafka.
     *
     * setConcurrency(3) means: for each @KafkaListener using this factory,
     * Spring creates 3 consumer threads. With a 3-partition topic, each thread
     * gets one partition — full parallelism inside one JVM.
     *
     * If we had 6 partitions, each of the 3 threads would handle 2 partitions
     * (still serialized within each thread, parallel across threads).
     *
     * For multi-instance deployments (multiple JVMs), the partition count caps
     * total parallelism. We'll see this in action with multiple notification-service
     * instances in Part 5.
     */

    @Bean(name = "orderflowKafkaListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> cf) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setConcurrency(3);
        return factory;
    }







}
