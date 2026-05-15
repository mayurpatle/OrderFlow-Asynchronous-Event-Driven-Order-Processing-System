package com.orderflow.order;

import com.orderflow.analytics.AnalyticsModule;
import com.orderflow.inventory.InventoryModule;
import com.orderflow.notification.NotificationModule;
import com.orderflow.payment.PaymentModule;
import com.orderflow.shipping.ShippingModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * THE BEFORE PICTURE.
 *
 * This is the file we are migrating AWAY from. Read it carefully — every design
 * choice here is what microservices + Kafka will undo.
 *
 * placeOrder() makes 5 synchronous in-process calls under a SINGLE @Transactional
 * boundary. The implications:
 *
 *   1. The DB transaction is open for the FULL duration of all 5 calls
 *      (worst case 80+1500+200+800+150 = 2730ms — almost 3 seconds!)
 *   2. The customer's HTTP thread is BLOCKED for that entire duration
 *   3. A DB connection from the pool is HELD for that entire duration
 *   4. If ANY module throws, the whole transaction rolls back. The order is gone.
 *      Even if shipping fails. Even if notification fails.
 *
 * Once you write this you should be a little uncomfortable. Good. That discomfort
 * is what drives the migration.
 */
@Service
@Slf4j
@RequiredArgsConstructor  // Lombok generates a constructor for all final fields
public class OrderService {

    // 6 dependencies. Spring will inject all of them via the constructor
    // that @RequiredArgsConstructor generates.
    private final OrderRepository orderRepository;
    private final InventoryModule inventoryModule;
    private final PaymentModule paymentModule;
    private final ShippingModule shippingModule;
    private final NotificationModule notificationModule;
    private final AnalyticsModule analyticsModule;

    /**
     * Place an order. THE problematic method.
     *
     * @Transactional means: open a DB transaction at method entry, commit on
     * normal return, rollback if any RuntimeException propagates out.
     */
    @Transactional
    public Order placeOrder(OrderRequest request) throws InterruptedException {
        long start = System.currentTimeMillis();

        // STEP 1: Persist the order in PENDING state
        Order order = Order.builder()
                .id(UUID.randomUUID())
                .customerId(request.customerId())
                .totalCents(request.totalCents())
                .status(OrderStatus.PENDING)
                .createdAt(Instant.now())
                .build();
        order = orderRepository.save(order);

        // STEP 2: Reserve inventory — sync, blocks until done
        inventoryModule.reserveStock(order.getId(), request.items());

        // STEP 3: Charge the customer — sync, blocks until the payment gateway responds
        paymentModule.charge(order.getId(), order.getTotalCents());

        // STEP 4: Schedule dispatch — sync
        shippingModule.scheduleDispatch(order.getId());

        // STEP 5: Send confirmation email — sync. SMTP can be slow.
        notificationModule.sendOrderConfirmation(order.getId(), request.customerId());

        // STEP 6: Record analytics — sync
        analyticsModule.recordOrderPlaced(order.getId(), order.getTotalCents());

        // Mark CONFIRMED and persist again
        order.setStatus(OrderStatus.CONFIRMED);
        orderRepository.save(order);

        long elapsed = System.currentTimeMillis() - start;
        log.info("Order {} placed in {}ms via SYNC chain of 5 module calls", order.getId(), elapsed);
        return order;
    }
}