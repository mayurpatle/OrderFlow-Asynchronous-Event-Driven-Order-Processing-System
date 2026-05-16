package com.orderflow.common.events;


import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

/**
 * The standard envelope wrapping every event published on Kafka.
 *
 * Why an envelope?
 *
 *   Most engineers' first instinct is to publish raw payload objects to Kafka:
 *
 *     kafkaTemplate.send("order.created", new OrderCreatedPayload(...));
 *
 *   This works until you need any of these:
 *     - Idempotency (which event is this exactly?)
 *     - Distributed tracing (what flow does this event belong to?)
 *     - Schema versioning (what version of this event are we receiving?)
 *     - Causation tracking (what caused this event?)
 *
 *   By the time you realize you need them, you have a million events in production
 *   without them, and adding them means a breaking change.
 *
 *   The envelope is the senior engineer's preemptive strike. Every event,
 *   regardless of payload, has the same metadata. Consumers always know how to
 *   extract eventId for dedup, correlationId for tracing, etc.
 *
 * Why a record?
 *
 *   Records are Java's value classes (since Java 14). They give us:
 *     - Immutability (all fields are final automatically)
 *     - Auto-generated equals/hashCode/toString
 *     - Compact syntax
 *
 *   Events should be immutable — once produced, they're a fact in the log.
 *   Records enforce this at the language level.
 *
 * Why parameterized with <T>?
 *
 *   The payload varies per event type (OrderCreatedPayload vs PaymentCompletedPayload
 *   vs ShippingDispatchedPayload), but the envelope shape is constant. Generic <T>
 *   lets us reuse the same class without losing payload type information.
 */

@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope<T>(
        // Globally unique identifier for THIS event. Used as the dedup key
        // by every consumer's idempotency check. Same physical event always
        // has the same eventId. If Kafka redelivers, eventId stays the same.
        UUID eventId,

        // The event type as a string (e.g. "order.created"). Used by consumers
        // that listen on multiple topics or do conditional routing.
        String eventType,

        // Semver version of the event schema. When we evolve the payload,
        // we bump this. Consumers can branch on version if needed.
        String eventVersion,

        // Wall-clock time at the producer when the event was created.
        // Different from Kafka's record timestamp (which is when the broker
        // received it). occurredAt is the BUSINESS time.
        Instant occurredAt,

        // Same value flows across ALL events in one customer flow. Set at
        // the entry point (the order service's HTTP handler), propagated to
        // every event triggered by that flow. Enables distributed tracing
        // without a separate tracing system — just filter by correlationId.
        UUID correlationId,

        // The eventId of the event that CAUSED this one. Forms a causal chain.
        // For the first event in a flow, this is null. For downstream events,
        // it points to the parent. Used for debugging "where did this come from?"
        UUID causationId,

        // The actual business payload. Typed via the generic parameter.
        T payload
){
    public static <T> EventEnvelope<T> of(
            String eventType ,
            T payload,
            UUID correlationId ,
            UUID causationId
    ){
        return new EventEnvelope<>(
                UUID.randomUUID(),
                eventType,
                "1.0.0",
                Instant.now(),
                correlationId != null ? correlationId : UUID.randomUUID()  ,
                causationId,
                payload
        );

    }

}
