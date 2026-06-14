package com.orderflow.shipping.Repository;

import com.orderflow.shipping.Entity.Shipment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data repository for Shipment.
 *
 * findByOrderId enables the application-level idempotency check before
 * we even attempt to insert (cleaner than catching the unique constraint
 * violation, though both work).
 */
public interface ShipmentRepository extends JpaRepository<Shipment, UUID> {

    Optional<Shipment> findByOrderId(UUID orderId);
}