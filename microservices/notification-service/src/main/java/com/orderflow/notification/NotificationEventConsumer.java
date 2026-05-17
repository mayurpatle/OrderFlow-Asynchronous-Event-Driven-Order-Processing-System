package com.orderflow.notification;

import com.orderflow.common.events.EventEnvelope;
import com.orderflow.common.events.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

/*
 * The notification-service's Kafka consumer.
 *
 * In production this would call SES, SendGrid, Twilio, etc. For our learning
 * project, we just log "email sent" — the mechanics of Kafka consumption are
 * what matter today, not the side effect.
 *
 * Why this service was the FIRST to be extracted from the monolith (in the
 * strangler-fig migration story):
 *   - No state, no persistence, no migrations needed
 *   - Idempotency is tolerable (duplicate email is annoying but not catastrophic)
 *   - Slow SMTP no longer slows down order placement
 *   - Multiple notification types can be added independently (email, SMS, push)
 *     without touching other services
 *
 * This file is the mirror image of OrderEventPublisher from Session 5.
 * Producer + topic + key on the writing side; consumer group + topic + key
 * extraction on the reading side. Same Kafka, two ends.
 */
@Component
@Slf4j
@RequiredArgsConstructor

public class NotificationEventConsumer {

    /*
     * Listen on the order.created topic, in consumer group "notification-service".
     *
     *   topics = the topic(s) to subscribe to
     *   groupId = the consumer group. Multiple instances of notification-service
     *             share this groupId so they split partitions among themselves
     *             (you saw this in Session 3 with the CLI).
     *
     * The method signature: Spring deserializes the Kafka message value into
     * EventEnvelope using the deserializer we configured. The generic parameter
     * is Map<String, Object> because of how JSON deserialization works — the
     * payload inside the envelope is parsed as a generic Map, and we extract
     * fields by name. In Session 13 (Avro), we'll have type-safe payloads.
     *
     * What Spring is doing under the hood:
     *   1. The listener container has a poll loop running in a thread
     *   2. Every few hundred ms it asks Kafka: "any new messages?"
     *   3. Kafka returns a batch of messages
     *   4. For each message, Spring deserializes it and invokes this method
     *   5. If the method returns normally, Spring commits the offset back to Kafka
     *   6. If the method throws, Spring's default behavior is to log + retry,
     *      and after enough failures, send to a dead-letter topic
     *      (we'll customize this in Session 15)
     */
    @KafkaListener(topics = Topics.ORDER_CREATED, groupId = "notification-service",
            containerFactory = "orderflowKafkaListenerFactory"    )
    public void onOrderCreated(EventEnvelope<Map<String, Object>> envelope) {
        // Log the envelope metadata to prove we received it
        log.info("Received {} event eventId={} correlationId={}",
                envelope.eventType(),
                envelope.eventId(),
                envelope.correlationId());

        // Extract fields from the payload Map
        UUID orderId = UUID.fromString((String) envelope.payload().get("orderId"));
        String customerId = (String) envelope.payload().get("customerId");
        Number totalCents = (Number) envelope.payload().get("totalCents");

        // The "side effect" — in real code this calls SES / SendGrid / etc.
        sendEmail(
                customerId,
                "Order received",
                String.format("Hi! We've received your order %s for $%.2f. We'll email you again once it ships.",
                        orderId, totalCents.longValue() / 100.0)
        );
    }

    /**
     * The fake email sender. In production this is an HTTP call to a transactional
     * email provider. The point of having this method is that the SHAPE of the
     * consumer code is unchanged — Kafka delivery, payload extraction, side effect.
     */
    private void sendEmail(String recipient, String subject, String body) {
        log.info("[EMAIL → {}] Subject: {} | Body: {}", recipient, subject, body);
    }

}
