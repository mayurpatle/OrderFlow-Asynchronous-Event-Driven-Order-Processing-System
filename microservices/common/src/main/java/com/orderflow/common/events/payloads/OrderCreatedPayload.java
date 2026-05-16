package com.orderflow.common.events.payloads;

import java.util.List;
import java.util.UUID;

/**
 * The payload for the order.created event.
 *
 * This is the BUSINESS content that goes inside an EventEnvelope. The envelope
 * carries the metadata (eventId, correlationId, occurredAt), this carries the
 * actual fact: "this order was created with these items by this customer."
 *
 * Design note — why we don't reuse the Order JPA entity here:
 *
 *   The Order entity is an internal model of the order-service. It might have
 *   fields like 'createdAt', 'updatedAt', 'optimisticLockVersion' that
 *   downstream services have no business knowing about. The payload is the
 *   PUBLIC contract — only fields other services actually need.
 *
 *   Mixing entity and event payload is a classic mistake. The entity evolves
 *   for internal reasons (new columns, new flags); the event payload evolves
 *   for cross-service reasons (a new field other services need). They have
 *   different evolution velocities and different stakeholders. Keep them separate.
 *
 * In Session 13 we'll replace this hand-written class with one generated from
 * an Avro schema. For now, plain Java records work fine.
 */
public record OrderCreatedPayload(
        UUID orderId,
        String customerId,
        List<Item> items,
        long totalCents,
        String currency
) {

    /** Nested record fo line items **/
    public record Item(
            String sku,
            int quantity,
            long unitPriceCents

    ){}

}
