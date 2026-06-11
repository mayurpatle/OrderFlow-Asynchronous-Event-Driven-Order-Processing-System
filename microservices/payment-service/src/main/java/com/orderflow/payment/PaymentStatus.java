package com.orderflow.payment;

/**
 * The lifecycle of a payment attempt.
 *
 *   PENDING    - the charge is in progress (we've called the gateway, awaiting response)
 *   COMPLETED  - the gateway confirmed the charge succeeded
 *   FAILED     - the gateway declined the charge, or some error occurred
 *
 * Note we don't have REFUNDED here. In a real system refunds are separate
 * entities — a Refund record linked to a Payment record. The Payment stays
 * COMPLETED even after a refund; the refund is its own row. This keeps audit
 * trails clean and makes "how many refunds happened today" a simple query.
 */
public enum PaymentStatus {
    PENDING,
    COMPLETED,
    FAILED
}

