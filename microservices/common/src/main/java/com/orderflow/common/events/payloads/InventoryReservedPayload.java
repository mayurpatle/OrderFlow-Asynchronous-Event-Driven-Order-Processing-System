package com.orderflow.common.events.payloads;

import java.util.List;
import java.util.UUID;

/**
 * Payload for the inventory.reserved event.
 *
 * Emitted by inventory-service when stock has been successfully reserved for
 * an order. Downstream consumers:
 *   - payment-service uses it as the trigger to charge the customer
 *     (we only charge AFTER stock is confirmed available)
 *   - order-service uses it to transition the order to AWAITING_PAYMENT state
 *   - analytics-service records the reservation
 *
 * Design note — why we echo the items back in the payload:
 *
 *   The payment-service could fetch the order details from order-service via REST,
 *   but that's exactly the synchronous coupling we're avoiding. By echoing the
 *   relevant fields back into the event, downstream services have everything
 *   they need without making a call. This pattern — events carry their own
 *   context — is called the "event-carried state transfer" pattern.
 */
public record InventoryReservedPayload(
        UUID orderId ,
        UUID reservationId  ,
        String warehouseId ,
        List<ReservedItem> reservedItems

) {
    public record ReservedItem(String sku , int quantity )  {

    }

}
