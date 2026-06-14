package com.orderflow.shipping.Entity;

/**
 * Lifecycle states for a shipment.
 *
 *   CREATED    - shipment record exists, not yet handed to carrier
 *   DISPATCHED - handed to carrier with tracking number assigned
 *   DELIVERED  - carrier confirmed delivery to customer
 *
 * In our simulation we go straight from CREATED to DISPATCHED immediately,
 * because we don't have an actual carrier integration to wait on. In production
 * the CREATED state would last while we wait for the carrier's API to accept
 * the package and return a tracking number.
 */
public enum ShipmentStatus {
    CREATED,
    DISPATCHED,
    DELIVERED
}