package com.orderflow.analytics;

import com.orderflow.common.events.EventEnvelope;
import com.orderflow.common.events.Topics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The fan-out (fan-in) consumer. Subscribes to EVERY topic in the system,
 * aggregates real-time metrics in memory.
 *
 * THE PATTERN: one @KafkaListener with multiple topics. Every event in the
 * system flows through this single method, which branches on eventType to
 * decide what to count.
 *
 * Why this is safe to extract first in a strangler-fig migration:
 *   Analytics is READ-ONLY. No other service consumes its output. If this
 *   service has a bug, the worst case is wrong dashboard numbers — no order
 *   fails, no money is lost, no customer is affected. That zero-blast-radius
 *   property makes it the ideal first migration step.
 *
 * Why metrics are in-memory:
 *   For this learning session, ConcurrentHashMap counters are enough. They're
 *   lost on restart, which is fine for a demo. In production, analytics would
 *   write to a time-series DB (Prometheus, InfluxDB) or a warehouse (Snowflake,
 *   BigQuery). The CONSUME + AGGREGATE pattern is identical; only the storage
 *   target changes.
 *
 * Thread safety:
 *   With concurrency=3 on the listener container, three threads may call this
 *   method simultaneously (one per partition). We use ConcurrentHashMap and
 *   AtomicLong so the counters are safe under concurrent updates. This matters —
 *   a plain HashMap + long would lose increments under concurrency.
 */
@Component
@Slf4j
public class AnalyticsEventConsumer {

    // Counter per event type. ConcurrentHashMap + AtomicLong = thread-safe.
    private final ConcurrentHashMap<String, AtomicLong> eventCounts = new ConcurrentHashMap<>();

    // Running total of revenue from completed payments (in cents).
    private final AtomicLong totalRevenueCents = new AtomicLong(0);

    /**
     * Listen to ALL topics. The single method receives every event in the system.
     *
     * Note: we list every topic explicitly rather than using a wildcard pattern.
     * Spring Kafka supports topic patterns (topicPattern = "...") but explicit
     * lists are clearer and prevent accidentally consuming internal topics like
     * __consumer_offsets.
     */
    @KafkaListener(
            topics = {
                    Topics.ORDER_CREATED,
                    Topics.ORDER_CANCELLED,
                    Topics.INVENTORY_RESERVED,
                    Topics.INVENTORY_RESERVATION_FAILED,
                    Topics.PAYMENT_COMPLETED,
                    Topics.PAYMENT_FAILED,
                    Topics.SHIPPING_DISPATCHED
            },
            groupId = "analytics-service",
            containerFactory = "orderflowKafkaListenerFactory"
    )
    public void onAnyEvent(EventEnvelope<Map<String, Object>> envelope) {
        String eventType = envelope.eventType();

        // Increment the counter for this event type.
        // computeIfAbsent is atomic — if two threads see the same missing key,
        // only one AtomicLong is created, and both increment it safely.
        eventCounts.computeIfAbsent(eventType, k -> new AtomicLong()).incrementAndGet();

        // Special handling: accumulate revenue from completed payments.
        if (Topics.PAYMENT_COMPLETED.equals(eventType) || "payment.completed".equals(eventType)) {
            Object amount = envelope.payload().get("amountCents");
            if (amount instanceof Number n) {
                totalRevenueCents.addAndGet(n.longValue());
            }
        }

        log.debug("Analytics recorded {} (total for type: {})",
                eventType, eventCounts.get(eventType).get());
    }

    /**
     * Snapshot the current metrics. Called by the controller to serve /metrics.
     *
     * We build a fresh map each call so the caller can't mutate our internal state.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> result = new HashMap<>();

        // Per-event-type counts
        Map<String, Long> counts = new HashMap<>();
        eventCounts.forEach((type, counter) -> counts.put(type, counter.get()));
        result.put("eventCounts", counts);

        // Derived business metrics
        result.put("totalRevenueCents", totalRevenueCents.get());
        result.put("totalRevenueDollars", totalRevenueCents.get() / 100.0);

        // A few derived ratios that are useful at a glance
        long ordersCreated = getCount("order.created");
        long paymentsCompleted = getCount("payment.completed");
        long paymentsFailed = getCount("payment.failed");
        long inventoryFailed = getCount("inventory.reservation_failed");

        result.put("ordersCreated", ordersCreated);
        result.put("paymentsCompleted", paymentsCompleted);
        result.put("paymentsFailed", paymentsFailed);
        result.put("inventoryRejections", inventoryFailed);

        // Payment success rate (guard against divide-by-zero)
        long totalPaymentAttempts = paymentsCompleted + paymentsFailed;
        if (totalPaymentAttempts > 0) {
            double successRate = (double) paymentsCompleted / totalPaymentAttempts * 100;
            result.put("paymentSuccessRatePercent", Math.round(successRate * 100.0) / 100.0);
        } else {
            result.put("paymentSuccessRatePercent", null);
        }

        return result;
    }

    private long getCount(String eventType) {
        AtomicLong counter = eventCounts.get(eventType);
        return counter == null ? 0 : counter.get();
    }
}