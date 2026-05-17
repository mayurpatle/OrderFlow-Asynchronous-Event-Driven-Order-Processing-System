package com.orderflow.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Entry point for the notification-service.
 *
 * Notice what's missing compared to OrderServiceApplication:
 *   - No @EntityScan (we have no entities)
 *   - No @EnableJpaRepositories (we have no repositories)
 *
 * The notification-service is a pure event consumer with no persistence.
 * Just web + Kafka, kept as simple as the role allows.
 *
 * @ComponentScan still includes com.orderflow.common so that KafkaConfig is
 * picked up — same pattern as order-service.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.orderflow.notification", "com.orderflow.common"})
public class NotificationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}