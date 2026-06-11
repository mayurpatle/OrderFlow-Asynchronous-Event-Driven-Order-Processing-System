package com.orderflow.inventory.Entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

/**
 * A single line item within a Reservation.
 *
 * One Reservation has many ReservationItems — modeling the fact that one
 * order can reserve quantities across multiple SKUs.
 * This Entity was not  made  at start  this was made when we were doing compensating actions
 * This entity exists specifically to enable saga compensation. Without it,
 * we'd know that a reservation was made but not exactly what was reserved,
 * which means we couldn't undo it correctly when payment fails.
 *
 * Design notes:
 *   - We use a generated UUID id so each line is uniquely addressable.
 *   - We DON'T use a JPA @OneToMany relationship on Reservation. Instead we
 *     store reservationId as a plain foreign key and query separately.
 *     This is intentional: @OneToMany with default settings has subtle
 *     performance traps (N+1 queries, lazy loading surprises, cascade
 *     pitfalls). Explicit queries via the repository are clearer and
 *     more predictable. Many senior teams avoid bidirectional JPA
 *     relationships entirely for exactly this reason.
 */
@Entity
@Table(name = "reservation_items")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReservationItem {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID reservationId;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private int quantity;
}