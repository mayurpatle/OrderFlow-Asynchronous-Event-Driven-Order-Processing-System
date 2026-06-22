package com.orderflow.inventory.Exception;


/**
 * Thrown when the atomic tryReserve affects 0 rows — i.e. a SKU didn't have
 * enough stock at the moment of reservation. Caught in onOrderCreated to drive
 * the reservation_failed path. Carries the offending SKU for the failure event.
 */
public class InsufficientStockException extends RuntimeException {
    private final String sku;

    public InsufficientStockException(String sku) {
        super("Insufficient stock for SKU: " + sku);
        this.sku = sku;
    }

    public String getSku() {
        return sku;
    }
}