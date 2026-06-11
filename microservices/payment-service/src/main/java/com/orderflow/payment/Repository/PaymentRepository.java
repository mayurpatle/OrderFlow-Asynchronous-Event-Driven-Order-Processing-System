package com.orderflow.payment.Repository  ;


import com.orderflow.payment.Entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data JPA repository for Payment.
 *
 * The custom finder findByOrderId is useful for two things:
 *   1. Idempotency check — if we receive the same inventory.reserved event twice
 *      (Kafka at-least-once delivery), we don't want to charge the customer twice.
 *      We check "do we already have a payment for this orderId?" before processing.
 *      We'll wire this in during Part 4.
 *   2. Customer service — looking up the payment for a specific order
 *      ("the customer says they paid but the order isn't shipping").
 */
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByOrderId(UUID orderId);
}
