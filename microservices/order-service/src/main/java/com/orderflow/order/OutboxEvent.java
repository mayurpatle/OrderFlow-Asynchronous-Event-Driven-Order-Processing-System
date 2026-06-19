package com.orderflow.order;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * The Transactional Outbox.
 *
 * This table lives in the SAME database as the orders table — that's the whole
 * point. When we place an order, we INSERT the order row AND an OutboxEvent row
 * in ONE transaction. Either both commit or neither does. No dual-write window.
 *
 * A separate relay (OutboxRelay, built in Part 4) polls this table for rows
 * where publishedAt IS NULL, sends each to Kafka, and stamps publishedAt on
 * success.
 *
 * The single nullable column 'publishedAt' IS the state machine:
 *   NULL      -> pending, the relay still needs to publish it
 *   non-NULL  -> published at this timestamp (also serves as audit record)
 *
 * Why we store the pre-serialized payload string:
 *   The payload is frozen exactly as it was when the business transaction
 *   committed. The relay ships these exact bytes — it never re-serializes.
 *   This guarantees the published event matches what the transaction intended,
 *   even if code or schemas change later.
 */
@Entity
@Table(name = "outbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    private UUID id;

    /**
     * The kind of entity this event concerns, e.g. "Order".
     * Useful for filtering/debugging when one outbox serves multiple aggregates.
     */
    @Column(nullable = false)
    private String aggregateType;

    /**
     * The specific entity's ID, e.g. the orderId as a string.
     * This becomes the Kafka message KEY — guaranteeing per-order ordering and
     * consistent partition routing.
     */
    @Column(nullable = false)
    private String aggregateId;

    /**
     * The event type, e.g. "order.created". Carried for clarity and debugging.
     */
    @Column(nullable = false)
    private String eventType;

    /**
     * The destination Kafka topic. Stored explicitly so the relay doesn't need
     * an event-type-to-topic mapping — the producing code already knows the
     * topic and records it here.
     */
    @Column(nullable = false)
    private String topic;

    /**
     * The fully-serialized event JSON. The relay sends this verbatim.
     * Stored as TEXT because event payloads can exceed the default varchar(255).
     */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    /**
     * When this outbox row was created (i.e. when the business transaction ran).
     * The relay processes rows in this order to preserve event ordering.
     */
    @Column(nullable = false)
    private Instant createdAt;

    /**
     * When this row was successfully published to Kafka.
     * NULL = not yet published. This is the relay's filter condition.
     */
    @Column
    private Instant publishedAt;
}