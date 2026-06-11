package com.orderflow.common.events.payloads;


import java.util.List;
import java.util.UUID;

/**
 * Payload for inventory.reservation_failed.
 *
 * Emitted when stock cannot be reserved for an order (some SKUs are unavailable).
 * Triggers the saga rollback path:
 *   - order-service consumes this and transitions the order to CANCELLED
 *   - notification-service emails the customer that the order can't be fulfilled
 *   - analytics-service records the failure for inventory replenishment signals
 *
 * The 'unavailableSkus' field lets downstream services give the customer specific
 * information ("SKU-001 is out of stock") rather than just a generic failure.
 */


public record InventoryReservationFailedPayload(
        UUID orderId,
        String reason,
        List<String> unavailableSkus

) {


}
