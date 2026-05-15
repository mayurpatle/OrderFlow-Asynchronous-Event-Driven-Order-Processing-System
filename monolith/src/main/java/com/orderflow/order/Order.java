package com.orderflow.order;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * The Order JPA entity. Mapped to the 'orders' table in Postgres.
 *
 * @Entity tells Hibernate "this Java class corresponds to a database table"
 * @Table sets the explicit table name (defaults to class name, but 'order' is
 *        a reserved keyword in SQL so we use plural)
 *
 * Lombok annotations:
 *   @Data = getters, setters, equals, hashCode, toString
 *   @Builder = generates a builder for fluent construction
 *   @NoArgsConstructor + @AllArgsConstructor = JPA needs the no-arg, builder needs the all-args
 */
@Entity
@Table(name = "orders")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private long totalCents;

    @Enumerated(EnumType.STRING)  // store as 'CONFIRMED' not as ordinal 1
    @Column(nullable = false)
    private OrderStatus status;

    @Column(nullable = false)
    private Instant createdAt;
}