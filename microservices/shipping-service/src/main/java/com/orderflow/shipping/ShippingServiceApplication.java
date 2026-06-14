package com.orderflow.shipping;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@ComponentScan(basePackages = {"com.orderflow.shipping", "com.orderflow.common"})
@EntityScan(basePackages = {"com.orderflow.shipping", "com.orderflow.common"})
@EnableJpaRepositories(basePackages = {"com.orderflow.shipping", "com.orderflow.common"})
public class ShippingServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShippingServiceApplication.class, args);
    }
}