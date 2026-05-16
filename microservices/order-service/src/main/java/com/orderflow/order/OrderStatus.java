package com.orderflow.order;

/**
 * The order lifecycle states.
 *
 * Notice how much richer this is than the monolith's version. In the monolith,
 * the @Transactional method either succeeded fully (CONFIRMED) or rolled back
 * (no record at all). Two real states.
 *
 * In the microservices version, the order's state machine reflects the journey
 * across distributed services:
 *
 *   PENDING            -- just created, waiting for inventory + payment
 *   AWAITING_PAYMENT   -- inventory reserved, charging in progress
 *   PAID               -- payment captured
 *   DISPATCHED         -- shipping handed off to carrier
 *   DELIVERED          -- delivered to customer
 *   CANCELLED          -- by customer or compensation flow (inventory unavailable, payment failed)
 *
 * We won't drive all these transitions in Session 5. We'll add the consumers
 * that drive them later (Session 14). For now we use PENDING as the only state.
 */

public enum OrderStatus {
    PENDING,
    AWAITING_PAYMENT,
    PAID,
    DISPATCHED,
    DELIVERED,
    CANCELLED,
    FAILED
}
