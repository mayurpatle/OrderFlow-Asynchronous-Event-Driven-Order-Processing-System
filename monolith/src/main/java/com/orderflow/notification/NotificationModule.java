package com.orderflow.notification;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The Notification module. Sends order confirmations via email/SMS.
 *
 * Tail latency offender — SMTP servers are notoriously bursty. Real SMTP
 * sends range from 100ms (warm connection) to 2-3 seconds (cold, with TLS
 * handshake and DKIM signing).
 *
 * In the monolith this is bad: slow SMTP slows down order placement itself.
 * In the microservices world this is the FIRST module we'd typically extract
 * because the fan-out nature of notifications maps perfectly to Kafka consumers.
 */
@Service
@Slf4j
public class NotificationModule {

    public void sendOrderConfirmation(UUID orderId, String customerId) {
        sleep(100, 800);
        log.info("[NotificationModule] Emailed customer {} about order {}", customerId, orderId);
    }

    private void sleep(int minMs, int maxMs) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
