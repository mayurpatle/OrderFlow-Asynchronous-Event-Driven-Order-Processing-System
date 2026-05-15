package com.orderflow.order;

/**
 * The order's lifecycle states. In the monolith we only really use
 * PENDING (just created) and CONFIRMED (after all 5 modules succeed).
 *
 * Notice these will become much richer in the microservices version, because
 * each downstream module will independently confirm via events.
 */
public enum OrderStatus {
    PENDING,
    CONFIRMED,
    CANCELLED,
    FAILED
}
