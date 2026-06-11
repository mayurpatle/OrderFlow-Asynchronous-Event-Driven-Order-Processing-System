package com.orderflow.inventory.Repositories;

import com.orderflow.inventory.Entity.ReservationItem;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/**
 * Spring Data repository for ReservationItem.
 *
 * The custom query findByReservationId is the entry point for compensation —
 * when payment.failed arrives, we look up the items by reservation ID to know
 * what to credit back to stock.
 */
public interface ReservationItemRepository extends JpaRepository<ReservationItem, UUID> {

    List<ReservationItem> findByReservationId(UUID reservationId);
}