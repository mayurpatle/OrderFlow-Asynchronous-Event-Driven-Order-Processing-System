package com.orderflow.inventory;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Thread.sleep;

/**
 * The Inventory module.
 *
 * In the real world, this would write to inventory tables, check warehouses,
 * call an inventory management system, etc. For our simulation, we sleep for
 * a realistic amount of time to model the work.
 *
 * The sleep is the IMPORTANT part — it represents the "slowness" that the
 * synchronous chain has to wait for. When we look at load tests later, these
 * sleeps are what cause the monolith to collapse.
 */
@Service
@Slf4j
public class InventoryModule {

    public void   reserveStock(UUID orderId   , List<OrderItem> items ) throws InterruptedException {
        sleep(80 ,  400) ;
        log.info("[InventoryModule] Reserved {} items for order {}", items.size(), orderId);


    }

    private void sleep(int minMs, int maxMs) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(minMs, maxMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }



    public record OrderItem(String sku, int quantity, long unitPriceCents) {}
}
