package com.orderflow.common.events.payloads;

import java.time.Instant;
import java.util.UUID;

/**
 * Payload for the shipping.dispatched event.
 *
 * Emitted by shipping-service when an order has been handed off to the carrier.
 * Downstream consumers:
 *   - notification-service emails the customer with the tracking number
 *   - order-service (Session 14) transitions the order to DISPATCHED state
 *   - analytics-service records dispatch metrics
 *
 * Why we include the tracking number in the event:
 *   The customer-facing email needs it. The order-service's customer dashboard
 *   needs it. If we didn't include it in the event, every downstream consumer
 *   would have to call shipping-service via REST to get it — exactly the sync
 *   coupling we're avoiding. Self-contained events keep downstream consumers
 *   independent.
 *
 * Why we include the estimated delivery time:
 *   Same reason. The "your order ships by Thursday" line in the email is
 *   right here in the event. Notification-service doesn't need to know how
 *   shipping calculated it.
 */
public record ShippingDispatchedPayload(
        UUID orderId,
        UUID shipmentId,
        String carrier,
        String trackingNumber,
        Instant estimatedDeliveryAt
) {}