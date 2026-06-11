package com.orderflow.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Entry point for the inventory-service.
 *
 * Same pattern as order-service: ComponentScan and JPA scanning include
 * com.orderflow.common so that KafkaConfig and any common entities/repositories
 * are discovered.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.orderflow.inventory", "com.orderflow.common"})
@EntityScan(basePackages = {"com.orderflow.inventory", "com.orderflow.common"})
@EnableJpaRepositories(basePackages = {"com.orderflow.inventory", "com.orderflow.common"})
public class InventoryServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}