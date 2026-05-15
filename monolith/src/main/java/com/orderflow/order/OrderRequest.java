package com.orderflow.order;

import com.orderflow.inventory.InventoryModule.OrderItem;

import java.util.List;

/**
 * The request DTO for placing an order. A Java record because it's immutable
 * and we only need value semantics for an inbound request.
 *
 * Note we reuse InventoryModule.OrderItem — in a real codebase you'd typically
 * have a shared 'common' or 'domain' package. For the monolith we accept this
 * cross-module reference because everything is in one process anyway.
 */
public record OrderRequest(String customerId, List<OrderItem> items) {

    /** Calculate the total from item prices. */
    public long totalCents() {
        return items.stream()
                .mapToLong(i -> i.unitPriceCents() * i.quantity())
                .sum();
    }
}
