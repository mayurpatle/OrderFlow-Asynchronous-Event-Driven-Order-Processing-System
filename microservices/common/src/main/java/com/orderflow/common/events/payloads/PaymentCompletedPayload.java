package com.orderflow.common.events.payloads;

import java.util.UUID;

/**
 * Payload for payment.completed event.
 *
 * Emitted by payment-service when a charge succeeds through the gateway.
 * Downstream consumers:
 *   - shipping-service uses this as the trigger to dispatch the order
 *     (we only ship after payment clears)
 *   - notification-service emails the customer their payment confirmation
 *   - order-service (Session 14) transitions the order to PAID state
 *   - analytics-service records the revenue
 *
 * Design note — why gatewayReference matters:
 *
 *   Real payment gateways (Stripe, Razorpay, Adyen) return a transaction
 *   identifier after every successful charge. You need this for:
 *     - Daily reconciliation: matching your records against the gateway's records
 *     - Refunds: the gateway needs this ID to find the original charge
 *     - Customer service: when a customer disputes a charge, this ID is
 *       what you give to support to look it up
 *     - Compliance: PCI-DSS and audit requirements
 *
 *   Even in a learning project we model this correctly. Skipping it would
 *   teach a bad habit.
 */
public record PaymentCompletedPayload(
        UUID orderId,
        UUID paymentId,
        long amountCents,
        String currency,
        String paymentMethod,
        String gatewayReference
) {}
