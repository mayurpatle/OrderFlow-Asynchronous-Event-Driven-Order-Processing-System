package com.orderflow.payment;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The Payment module.
 *
 * The biggest latency offender in the chain. Real payment gateways (Razorpay,
 * Stripe) routinely take 200-1500ms because they're making HTTP calls to
 * card networks and issuing banks. We simulate that range.
 *
 * In the microservices version this is the most important consumer — slow
 * payment can no longer slow down order placement.
 */
@Service
@Slf4j
public class PaymentModule {

    public void charge(UUID orderId, long amountCents) {
        sleep(200, 1500);
        log.info("[PaymentModule] Charged {} cents for order {}", amountCents, orderId);
    }

    private void sleep(int minMs, int maxMs) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}