package com.orderflow.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Entry point for analytics-service.
 *
 * No @EntityScan or @EnableJpaRepositories — analytics has no database.
 * Just web (for /metrics) + Kafka (to consume all topics).
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.orderflow.analytics", "com.orderflow.common"})
public class AnalyticsServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(AnalyticsServiceApplication.class, args);
    }
}
