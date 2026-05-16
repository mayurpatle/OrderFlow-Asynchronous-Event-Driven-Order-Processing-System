package com.orderflow.order;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * The Order JPA entity. Internal model of the order-service.
 *
 * Note the name: OrderEntity, not Order. Two reasons:
 *   1. 'Order' is a SQL reserved keyword in some databases (we'd need quoting)
 *   2. It makes the entity vs. payload distinction explicit at the type level
 *
 * This entity is the order-service's PRIVATE data model. Other services should
 * never see this class. They see the event payload (OrderCreatedPayload) instead.
 * This is the key discipline of microservices: hide your data model, publish
 * your contract.
 */
@Entity
@Table(name = "orders")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private long totalCents;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;
}