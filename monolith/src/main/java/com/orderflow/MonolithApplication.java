package com.orderflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the monolith.
 *
 * @SpringBootApplication is the trifecta from Session 1.5:
 *   - @Configuration: this class can define beans
 *   - @EnableAutoConfiguration: scan classpath, fire auto-configs
 *   - @ComponentScan: scan this package and below for @Component/@Service/@Repository/@Controller
 *
 * Because this class lives in com.orderflow, the scan picks up everything under
 * com.orderflow.* — order, inventory, payment, shipping, notification, analytics.
 */
@SpringBootApplication
public class MonolithApplication {
    public static void main(String[] args) {
        SpringApplication.run(MonolithApplication.class , args) ;

    }
}
