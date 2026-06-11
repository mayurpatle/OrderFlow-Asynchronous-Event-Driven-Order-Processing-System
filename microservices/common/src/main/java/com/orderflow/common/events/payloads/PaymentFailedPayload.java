package com.orderflow.common.events.payloads;

import java.util.UUID;

/**
 * Payload for payment.failed event.
 *
 * Emitted when the gateway declines the charge. This is THE event that
 * triggers the saga rollback in Session 9:
 *   - inventory-service consumes payment.failed and RELEASES the reservation,
 *     returning the stock to availableQuantity. The customer's stock is freed
 *     up for other orders.
 *   - order-service (Session 14) transitions the order to CANCELLED.
 *   - notification-service emails the customer about the failed charge.
 *
 * Design note — why two failure fields:
 *
 *   failureCode is for programmatic logic. Different failure codes might trigger
 *   different behaviors:
 *     - INSUFFICIENT_FUNDS → suggest the customer try another card
 *     - EXPIRED_CARD → prompt to update card details
 *     - FRAUD_DECLINED → may trigger a manual review queue
 *     - GATEWAY_TIMEOUT → retry policy might apply
 *
 *   failureReason is the human-readable string for support and customer comms.
 *
 *   Real gateways return distinct codes for distinct conditions. This separation
 *   between machine-readable code and human-readable message is the canonical
 *   payments pattern.
 */
public record PaymentFailedPayload(
        UUID orderId,
        long amountCents,
        String failureCode,
        String failureReason
) {}