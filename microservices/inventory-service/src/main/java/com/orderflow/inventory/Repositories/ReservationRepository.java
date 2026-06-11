package com.orderflow.inventory.Repositories;

import com.orderflow.inventory.Entity.Reservation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface ReservationRepository extends JpaRepository<Reservation, UUID> {

    /**
     * Lookup by orderId. Used during cancellation flow — when a payment fails,
     * we find the reservation by orderId and release it.
     */
    Optional<Reservation> findByOrderId(UUID orderId);
}

