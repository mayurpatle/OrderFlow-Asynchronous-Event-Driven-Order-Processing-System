package com.orderflow.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.common.events.EventEnvelope;
import com.orderflow.common.events.Topics;
import com.orderflow.common.events.payloads.InventoryReservedPayload;
import com.orderflow.common.events.payloads.PaymentCompletedPayload;
import com.orderflow.common.events.payloads.PaymentFailedPayload;
import com.orderflow.payment.Entity.Payment;
import com.orderflow.payment.Repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The payment-service's consumer-that-produces.
 *
 * THE FLOW:
 *   1. Listen to inventory.reserved
 *   2. Check idempotency — have we already paid this order? If yes, skip.
 *   3. Persist a PENDING payment row BEFORE calling the gateway
 *      (so a crash mid-charge leaves an audit trail)
 *   4. Call the (simulated) gateway: 200-800ms latency, 5% failure rate
 *   5. Update the payment row to COMPLETED or FAILED
 *   6. Publish payment.completed or payment.failed
 *
 * The critical design choice: we consume inventory.reserved, NOT order.created.
 * This ENFORCES the ordering "stock first, then charge" through the event
 * topology itself. We can never accidentally charge a customer for an order
 * whose inventory failed — payment-service never sees those events.
 *
 * The simulated gateway uses ThreadLocalRandom for latency (200-800ms) and
 * a 5% random failure rate. Run 20 orders and you'll see ~1 fail. Session 9
 * uses these failures to demonstrate saga rollback.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentEventConsumer {

    private final PaymentRepository paymentRepo;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Consume inventory.reserved.
     *
     * groupId = "payment-service" means all payment-service instances share
     * this group. Multiple instances split the partitions among themselves
     * (the rebalancing you saw in Session 6 with notification-service).
     *
     * containerFactory points at our custom factory (the bug we hit in Session 6
     * if we forget this). Always specify it explicitly to avoid auto-config
     * surprises.
     *
     * @Transactional wraps DB writes in a transaction. The Kafka send is OUTSIDE
     * the transaction — we accept the rare window where DB commits but Kafka
     * send fails. Session 12's outbox pattern closes this gap properly.
     */
    @KafkaListener(
            topics = Topics.INVENTORY_RESERVED,
            groupId = "payment-service",
            containerFactory = "orderflowKafkaListenerFactory"
    )
    @Transactional
    public void onInventoryReserved(EventEnvelope<Map<String, Object>> envelope) {
        // Convert the Map-shaped payload into our typed class
        InventoryReservedPayload reserved = objectMapper.convertValue(
                envelope.payload(), InventoryReservedPayload.class);

        log.info("Processing inventory.reserved for orderId={} reservationId={}",
                reserved.orderId(), reserved.reservationId());

        // STEP 1: Idempotency check
        //
        // Kafka delivers at-least-once. If we processed this event before
        // (say, we crashed after charging but before committing the offset),
        // Kafka will redeliver. We must NOT charge again.
        //
        // The check: does a Payment row already exist for this orderId?
        // If yes, the previous attempt is authoritative — skip this delivery.
        if (paymentRepo.findByOrderId(reserved.orderId()).isPresent()) {
            log.info("Payment already exists for order {}, skipping (idempotent)",
                    reserved.orderId());
            return;
        }

        // STEP 2: Compute the amount
        //
        // The inventory.reserved payload doesn't include the dollar amount —
        // it only includes SKU + quantity. In a real system we'd either:
        //   - Have order-service include the total in order.created, which
        //     propagates through inventory.reserved
        //   - Or have payment-service look up the order via a saved local copy
        //     (built from order.created events it also consumes)
        //
        // For simplicity here, we assume the inventory.reserved event carries
        // the order's total. Let's grab it from the envelope's payload Map
        // (since OrderCreatedPayload had totalCents, and we'd typically forward it).
        //
        // BUT — InventoryReservedPayload doesn't currently have totalCents.
        // We'll fake it: $59.98 (5998 cents) as a flat rate per order. In a
        // real system you'd thread the amount through. We'll discuss this
        // tradeoff in the wrap-up of Part 6.
        long amountCents = 5998L;
        String currency = "USD";

        // STEP 3: Persist a PENDING payment BEFORE calling the gateway.
        // This is the "persist intent before action" pattern. If we crash
        // during the gateway call, we have a record of the attempt for
        // reconciliation.
        Payment payment = Payment.builder()
                .id(UUID.randomUUID())
                .orderId(reserved.orderId())
                .amountCents(amountCents)
                .currency(currency)
                .status(PaymentStatus.PENDING)
                .createdAt(Instant.now())
                .build();
        payment = paymentRepo.save(payment);

        // STEP 4: Call the simulated gateway
        GatewayResult result = callPaymentGateway(amountCents, currency);

        // STEP 5: Update the payment row with the outcome and publish the event
        if (result.success()) {
            payment.setStatus(PaymentStatus.COMPLETED);
            payment.setGatewayReference(result.gatewayReference());
            paymentRepo.save(payment);

            publishPaymentCompleted(payment, envelope);
        } else {
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureCode(result.failureCode());
            payment.setFailureReason(result.failureReason());
            paymentRepo.save(payment);

            publishPaymentFailed(payment, envelope);
        }
    }

    /**
     * Simulated payment gateway call.
     *
     * Real gateways:
     *   - 200-1500ms latency (we use 200-800 to keep things snappy)
     *   - 1-10% failure rate depending on context (we use 5%)
     *   - Return a transaction ID on success
     *   - Return a structured error code on failure
     *
     * This method blocks the calling thread, just like a real HTTP call to
     * a gateway would. With concurrency=3 on our listener container, three
     * payments can be in flight in parallel — matching real-world behavior.
     */
    private GatewayResult callPaymentGateway(long amountCents, String currency) {
        // Simulate gateway latency
        int latencyMs = ThreadLocalRandom.current().nextInt(200, 800);
        try {
            Thread.sleep(latencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new GatewayResult(false, null, "GATEWAY_INTERRUPTED",
                    "Thread interrupted during gateway call");
        }

        // Simulate 5% failure rate.
        // ThreadLocalRandom because each thread gets its own RNG (no contention).
        boolean success = ThreadLocalRandom.current().nextInt(100) >= 5;

        if (success) {
            // Generate a fake transaction reference matching the shape of real
            // gateway responses (Stripe: "ch_3PqXyZ...", we'll just use STRIPE-uuid)
            String txnRef = "STRIPE-" + UUID.randomUUID().toString().substring(0, 12);
            return new GatewayResult(true, txnRef, null, null);
        } else {
            // Simulate a realistic failure code
            return new GatewayResult(false, null,
                    "GATEWAY_DECLINED",
                    "Card declined by issuing bank");
        }
    }

    /**
     * Publish payment.completed.
     *
     * Notice the correlationId and causationId propagation pattern — same as
     * inventory-service in Session 7. We forward correlationId from the source
     * event (preserves the customer flow trace) and set causationId to the
     * source event's eventId (records that this event was caused by that one).
     */
    private void publishPaymentCompleted(Payment payment,
                                         EventEnvelope<Map<String, Object>> sourceEnvelope) {
        PaymentCompletedPayload payload = new PaymentCompletedPayload(
                payment.getOrderId(),
                payment.getId(),
                payment.getAmountCents(),
                payment.getCurrency(),
                "CREDIT_CARD",
                payment.getGatewayReference()
        );

        EventEnvelope<PaymentCompletedPayload> envelope = EventEnvelope.of(
                "payment.completed",
                payload,
                sourceEnvelope.correlationId(),
                sourceEnvelope.eventId()
        );

        kafkaTemplate.send(Topics.PAYMENT_COMPLETED, payment.getOrderId().toString(), envelope);
        log.info("Payment {} COMPLETED for order {} (ref: {})",
                payment.getId(), payment.getOrderId(), payment.getGatewayReference());
    }

    /**
     * Publish payment.failed.
     *
     * In Session 9 this is the event that triggers the saga rollback.
     * inventory-service will consume payment.failed and release the reservation,
     * returning the stock to availableQuantity.
     */
    private void publishPaymentFailed(Payment payment,
                                      EventEnvelope<Map<String, Object>> sourceEnvelope) {
        PaymentFailedPayload payload = new PaymentFailedPayload(
                payment.getOrderId(),
                payment.getAmountCents(),
                payment.getFailureCode(),
                payment.getFailureReason()
        );

        EventEnvelope<PaymentFailedPayload> envelope = EventEnvelope.of(
                "payment.failed",
                payload,
                sourceEnvelope.correlationId(),
                sourceEnvelope.eventId()
        );

        kafkaTemplate.send(Topics.PAYMENT_FAILED, payment.getOrderId().toString(), envelope);
        log.warn("Payment {} FAILED for order {} — {}: {}",
                payment.getId(), payment.getOrderId(),
                payment.getFailureCode(), payment.getFailureReason());
    }

    /**
     * Internal value object representing the gateway's response.
     * Kept private — outside callers shouldn't see the gateway shape.
     */
    private record GatewayResult(
            boolean success,
            String gatewayReference,
            String failureCode,
            String failureReason
    ) {}
}