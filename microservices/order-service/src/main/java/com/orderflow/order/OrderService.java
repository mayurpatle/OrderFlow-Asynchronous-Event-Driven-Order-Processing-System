package com.orderflow.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.common.events.EventEnvelope;
import com.orderflow.common.events.Topics;
import com.orderflow.common.events.payloads.OrderCreatedPayload;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.Transactional;
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
@EnableScheduling
public class OrderService {

    private final   OrderRepository orderRepository ;
    private final OutboxEventRepository outboxRepository;

    private final   OrderEventPublisher eventPublisher ;
    private final ObjectMapper objectMapper;


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

        // STEP 1: persist the order
        OrderEntity order = OrderEntity.builder()
                .id(UUID.randomUUID())
                .customerId(request.customerId())
                .totalCents(request.totalCents())
                .status(OrderStatus.PENDING)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        order = orderRepository.save(order);

        // STEP 2: build the SAME event envelope we used to publish directly...
        OrderCreatedPayload payload = new OrderCreatedPayload(
                order.getId(),
                order.getCustomerId(),
                request.items().stream()
                        .map(i -> new OrderCreatedPayload.Item(i.sku(), i.quantity(), i.unitPriceCents()))
                        .toList(),
                order.getTotalCents(),
                "USD"
        );

        EventEnvelope<OrderCreatedPayload> envelope = EventEnvelope.of(
                "order.created",
                payload,
                /* correlationId */ UUID.randomUUID(),  // start of a customer flow
                /* causationId   */ null
        );

        // ...and write it to the OUTBOX instead of sending to Kafka.
        // This INSERT is in the same transaction as the order INSERT above,
        // so they commit atomically. The OutboxRelay will publish it shortly.
        outboxRepository.save(toOutbox(order.getId(), envelope));

        long elapsed = System.currentTimeMillis() - start;
        log.info("Order {} placed in {}ms (1 DB save + 1 async event)", order.getId(), elapsed);
        return order;






    }

    /**
     * Serialize the envelope and wrap it in an OutboxEvent row.
     *
     * The aggregateId (orderId) becomes the Kafka message KEY when the relay
     * publishes — preserving the per-order partition routing we've relied on
     * since Session 3.
     */
    private OutboxEvent toOutbox(UUID orderId, EventEnvelope<OrderCreatedPayload> envelope) {
        String json;
        try {
            json = objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            // If serialization fails, throw — this rolls back the whole transaction,
            // including the order save. We will NOT persist an order we can't emit
            // an event for. Fail loud, fail atomic.
            throw new IllegalStateException(
                    "Failed to serialize order.created event for order " + orderId, e);
        }

        return OutboxEvent.builder()
                .id(UUID.randomUUID())
                .aggregateType("Order")
                .aggregateId(orderId.toString())
                .eventType("order.created")
                .topic(Topics.ORDER_CREATED)
                .payload(json)
                .createdAt(Instant.now())
                .publishedAt(null)              // pending — relay will publish
                .build();
    }
}
