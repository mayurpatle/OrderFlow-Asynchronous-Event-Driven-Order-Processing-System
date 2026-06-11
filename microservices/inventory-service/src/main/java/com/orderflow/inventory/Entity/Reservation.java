package com.orderflow.inventory.Entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * A single reservation — links an order to the stock it consumed.
 *
 * Why track this separately instead of just decrementing stock?
 *
 *   Because of cancellation. When an order is cancelled (because payment failed,
 *   inventory in another SKU was unavailable, customer cancelled), we need to
 *   RETURN the stock to availableQuantity. To do that we need to know exactly
 *   how much was reserved and for which SKUs. The Reservation entity records
 *   this — it's the audit trail.
 *
 * Status transitions:
 *   ACTIVE      -- reservation is live; stock is held
 *   RELEASED    -- order was cancelled; stock returned to available
 *   COMMITTED   -- order shipped; stock physically gone, reservation closed
 */
@Entity
@Table(name = "reservations")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Reservation {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ReservationStatus status;

    @Column(nullable = false)
    private Instant createdAt;
}
