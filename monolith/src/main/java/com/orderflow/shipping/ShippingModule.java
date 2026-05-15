package com.orderflow.shipping;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The Shipping module. Schedules dispatch with a carrier.
 * Realistic latency: 50-200ms (carrier API lookup, warehouse routing).
 */
@Service
@Slf4j
public class ShippingModule {

    public void scheduleDispatch(UUID orderId) {
        sleep(50, 200);
        log.info("[ShippingModule] Scheduled dispatch for order {}", orderId);
    }

    private void sleep(int minMs, int maxMs) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}