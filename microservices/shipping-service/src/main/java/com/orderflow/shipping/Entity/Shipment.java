package com.orderflow.shipping.Entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * A single shipment for one order.
 *
 * One-to-one with order — each order ships exactly once. We enforce this with
 * a unique constraint on orderId, which doubles as our idempotency check (if
 * the payment.completed event is redelivered, the second attempt to create
 * a shipment for the same orderId will fail at the database, which is the
 * belt-and-suspenders pattern we've been using throughout).
 *
 * The carrier and trackingNumber fields would come from a real carrier API
 * in production. We simulate them with hardcoded values + a generated tracking
 * string.
 */
@Entity
@Table(name = "shipments")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Shipment {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private UUID orderId;

    @Column(nullable = false)
    private String carrier;

    @Column(nullable = false)
    private String trackingNumber;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ShipmentStatus status;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant estimatedDeliveryAt;
}