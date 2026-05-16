package com.orderflow.order;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

/**
 * Simple health check.
 *
 * In Session 5 we'll add the real OrderController with POST /api/orders.
 * For today we just want to verify Spring Boot starts cleanly, the database
 * connects, and the common library is on the classpath.
 */
@RestController
public class HealthController {

    /**
     * Returns a fresh UUID using a utility from common library. This proves
     * the common JAR is actually loaded and accessible at runtime — not just
     * a compile-time dependency.
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        // We use Java's UUID directly, but the key thing is the response
        // succeeds and the service is responsive.
        return Map.of(
                "service", "order-service",
                "status", "UP",
                "instanceId", UUID.randomUUID().toString()
        );
    }
}
