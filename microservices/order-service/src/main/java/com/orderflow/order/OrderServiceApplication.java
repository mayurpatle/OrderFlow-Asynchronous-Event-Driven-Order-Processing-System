package com.orderflow.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Entry point for the order-service.
 *
 * Three things worth understanding here, all related to the common library:
 *
 *   1. @ComponentScan(basePackages = {...})
 *      By default, @SpringBootApplication scans com.orderflow.order and below.
 *      But our common library lives in com.orderflow.common — outside the default
 *      scan path. We explicitly include it so any @Service or @Component in common
 *      gets picked up.
 *
 *   2. @EntityScan
 *      JPA entities (anything with @Entity) need to be found by Hibernate's
 *      entity scanner. By default it scans the main class's package. If we ever
 *      have @Entity classes in common (like a shared 'processed_events' table for
 *      idempotency in Session 12), this annotation lets Hibernate find them.
 *
 *   3. @EnableJpaRepositories
 *      Same idea for Spring Data repositories — extends the scan path.
 *
 *  Right now we only have one module, so this might feel like over-engineering.
 *  But every microservice will need these exact annotations, so we set them up
 *  once and copy-paste for the others.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.orderflow.order", "com.orderflow.common"})
@EntityScan(basePackages = {"com.orderflow.order", "com.orderflow.common"})
@EnableJpaRepositories(basePackages = {"com.orderflow.order", "com.orderflow.common"})
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }

}
