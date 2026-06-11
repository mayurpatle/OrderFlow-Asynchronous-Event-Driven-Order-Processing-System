package com.orderflow.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Entry point for payment-service.
 *
 * Same pattern as inventory-service: ComponentScan and JPA scanning include
 * com.orderflow.common so KafkaConfig and any shared classes are discovered.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.orderflow.payment", "com.orderflow.common"})
@EntityScan(basePackages = {"com.orderflow.payment", "com.orderflow.common"})
@EnableJpaRepositories(basePackages = {"com.orderflow.payment", "com.orderflow.common"})
public class PaymentServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}