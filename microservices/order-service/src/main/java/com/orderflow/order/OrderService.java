package com.orderflow.order;

import com.orderflow.common.events.payloads.OrderCreatedPayload;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * THE AFTER PICTURE.
 *
 * Compare this with monolith/com/orderflow/order/OrderService.java
 * Five synchronous module calls are gone. placeOrder() does:
 *   - One DB save (the order itself)
 *   - One Kafka event publish (fire-and-forget from the caller's perspective)
 *   - Returns
 *
 * Latency: ~10-30ms instead of 800-2000ms.
 * Resource holding time: milliseconds instead of seconds.
 * Failure isolation: payment can be down for hours, order placement still works.
 *
 *
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {

    private final   OrderRepository orderRepository ;
    private final   OrderEventPublisher eventPublisher ;

    /*
     * Place an order.
     *
     * @Transactional wraps the DB save. The Kafka send happens AFTER the
     * transaction commits — this is intentional. If we published BEFORE
     * commit and the commit failed, downstream services would receive an
     * event for an order that doesn't exist (a phantom event).
     *
     * Publishing AFTER commit means a small window where the order is
     * saved but the event isn't yet published. If the JVM crashes in
     * that window, downstream services don't hear about the order. This
     * is the outbox problem — we'll solve it properly in Session 12.
     */

    @Transactional
    public OrderEntity placeOrder(OrderRequest request ){
        long start  = System.currentTimeMillis();

        // Step 1: persist the  order
        OrderEntity order = OrderEntity.builder()
                .id(UUID.randomUUID())
                .customerId(request.customerId())
                .totalCents(request.totalCents())
                .status(OrderStatus.PENDING)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        order = orderRepository.save(order);

        // STEP 2: publish the event
        //
        // We build the payload from the persisted entity (after save() so we
        // have the canonical state). The publisher wraps this in an envelope
        // and sends it asynchronously.
        OrderCreatedPayload payload = new OrderCreatedPayload(
                order.getId(),
                order.getCustomerId(),
                request.items().stream()
                        .map(i -> new OrderCreatedPayload.Item(i.sku(), i.quantity(), i.unitPriceCents()))
                        .toList(),
                order.getTotalCents(),
                "USD"
        );

        eventPublisher.publishOrderCreated(payload);

        long elapsed = System.currentTimeMillis() - start;
        log.info("Order {} placed in {}ms (1 DB save + 1 async event)", order.getId(), elapsed);
        return order;






    }
}
