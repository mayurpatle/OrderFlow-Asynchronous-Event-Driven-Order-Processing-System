package com.orderflow.order;

import java.util.List;

/**
 * HTTP request body for POST /api/orders.
 *
 * Java record because it's immutable and we only need value semantics here.
 */
public record OrderRequest(String customerId, List<Item> items) {

    public record Item(String sku, int quantity, long unitPriceCents) {}

    /** Compute the total from line items. */
    public long totalCents() {
        return items.stream()
                .mapToLong(i -> i.unitPriceCents() * i.quantity())
                .sum();
    }
}