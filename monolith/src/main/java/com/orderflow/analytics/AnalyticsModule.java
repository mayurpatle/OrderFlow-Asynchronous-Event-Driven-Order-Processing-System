package com.orderflow.analytics;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The Analytics module. Records metrics about every order.
 *
 * Cheap individually (30-150ms) but always present in the hot path.
 * In a microservices design this becomes a pure event consumer — never blocks
 * the order request.
 */
@Service
@Slf4j
public class AnalyticsModule {

    public void recordOrderPlaced(UUID orderId, long totalCents) {
        sleep(30, 150);
        log.info("[AnalyticsModule] Recorded analytics for order {}, value {}", orderId, totalCents);
    }

    private void sleep(int minMs, int maxMs) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}