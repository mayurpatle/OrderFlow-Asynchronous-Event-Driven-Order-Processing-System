package com.orderflow.order;

import com.orderflow.common.events.EventEnvelope;
import com.orderflow.common.events.Topics;
import com.orderflow.common.events.payloads.OrderCreatedPayload;
//import jakarta.websocket.SendResult;
import org.springframework.kafka.support.SendResult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The order-service's Kafka producer. Publishes order.created (and order.cancelled
 * later) events.
 *
 *
 *
 *  1. The KafkaTemplate is injected via constructor — Spring's preferred way
 *     (see Session 1.5). Final field, immutable, easy to mock in tests.
 *
 *  2. We use the orderId as the partition key. Same key = same partition
 *     (Session 3's lesson). This means all events for one order — created,
 *     potentially cancelled later — land in the same partition and are
 *     processed in order by every consumer.
 *
 *  3. Every event is wrapped in an EventEnvelope. The envelope carries
 *     metadata (eventId for dedup, correlationId for tracing, occurredAt
 *     for business time). Downstream consumers know exactly how to handle
 *     this without needing schema docs.
 *
 *  4. The send is asynchronous. KafkaTemplate.send() returns a CompletableFuture.
 *     We register a callback that logs success or failure, but we don't BLOCK
 *     the calling thread. The HTTP request returns to the customer as soon as
 *     the in-memory queue accepts the message — Kafka handles delivery in
 *     the background.
 *
 *  5. The producer is configured (in KafkaConfig) with acks=all, idempotence=true,
 *     and infinite retries. These guarantee that as long as the message reaches
 *     Kafka's producer queue, it WILL eventually be persisted with no duplicates.
 *     The only failure mode is the JVM crashing before the queue drains — that's
 *     what the outbox pattern solves (Session 12 territory).
 */

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaTemplate<String , Object> kafkaTemplate   ;

    /*
     * Publish an order.created event.
     *
     * Called by OrderService.placeOrder() RIGHT AFTER the order is persisted.
     * The persistence and the event publication are NOT atomic in this version —
     * there's a tiny window where the order is saved but the event isn't
     * published (or vice versa). For now we accept this. Session 12 will solve it
     * properly with the transactional outbox pattern.
     */

    public void publishOrderCreated(OrderCreatedPayload payload ){

        // Build the envelope.
        // correlationId is fresh here because this is the START of a customer flow.
        // In production it would come from a TraceId header on the incoming HTTP request.
        // causationId is null because this event isn't caused by another event.
        EventEnvelope<OrderCreatedPayload> envelope = EventEnvelope.of(
                "order.created",
                payload,
                /* correlationId */ UUID.randomUUID() ,
                /* causationId */ null
        );

        // The KEY is orderId.toString().
        // The VALUE is the envelope (which Jackson will serialize to JSON via our JsonSerializer).
        // The TOPIC is "order.created" — pulled from the constants class.
        send(Topics.ORDER_CREATED , payload.orderId().toString() , envelope) ;


    }

    /*
     * Internal send helper. Wraps the async KafkaTemplate.send() with logging.
     *
     * Why use CompletableFuture.whenComplete instead of .get() or blocking?
     *   - Blocking the HTTP thread on a Kafka send defeats the whole point of
     *     event-driven decoupling. We want sub-50ms HTTP responses.
     *   - The producer's internal queue gives us the durability we need. If the
     *     send hasn't completed by the time the HTTP response goes out, that's
     *     fine — the producer will keep retrying in the background.
     */

    public void send(String topic   , String key  , Object envelope ){
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(topic, key, envelope);

        future.whenComplete((result , ex  ) -> {
            if ( ex == null  ){
                // Success: log at debug to avoid noise in production. The key
                // information (partition + offset) tells us EXACTLY where this
                // event landed. Useful in postmortems.
                log.debug("Sent to {} [partition={}, offset={}, key={}]",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        key);

            }else {
                // Failure: the producer has retried (RETRIES_CONFIG=MAX_VALUE) and
                // STILL failed. This is rare — usually means topic doesn't exist or
                // there's an auth issue. Log loudly and move on.
                //
                // In production we'd route this to a dead-letter mechanism or an
                // outbox table for replay. For our learning project, logging is enough.
                log.error("FAILED to send to {} key={}", topic, key, ex);

            }
        });



    }



}
