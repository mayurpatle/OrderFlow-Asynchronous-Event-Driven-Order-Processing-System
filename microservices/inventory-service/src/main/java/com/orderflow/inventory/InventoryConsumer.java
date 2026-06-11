package com.orderflow.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.common.events.EventEnvelope;
import com.orderflow.common.events.Topics;
import com.orderflow.common.events.payloads.InventoryReservationFailedPayload;
import com.orderflow.common.events.payloads.InventoryReservedPayload;
import com.orderflow.common.events.payloads.OrderCreatedPayload;
import com.orderflow.inventory.Entity.Reservation;
import com.orderflow.inventory.Entity.ReservationStatus;
import com.orderflow.inventory.Entity.StockItem;
import com.orderflow.inventory.Repositories.ReservationRepository;
import com.orderflow.inventory.Repositories.StockItemRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The inventory-service's Kafka handler.
 *
 * THE CRITICAL PATTERN this class demonstrates: consume → decide → produce.
 *
 * Flow:
 *   1. Consume order.created from Kafka
 *   2. For each item, check if stock is available
 *   3. If all items available:
 *        - Atomically decrement availableQuantity, increment reservedQuantity, create Reservation row
 *        - Publish inventory.reserved
 *   4. If any item unavailable:
 *        - Do NOT modify any stock (atomicity within the @Transactional method)
 *        - Publish inventory.reservation_failed
 *
 * The two-path branching here is the seed of the saga pattern.
 */

@Component
@Slf4j
@RequiredArgsConstructor

public class InventoryConsumer {

    private final StockItemRepository stockItemRepository  ;
    private final ReservationRepository reservationRepository  ;
    private final ObjectMapper objectMapper  ;
    private final KafkaTemplate<String , Object>  kafkaTemplate  ;

    // Listener
    /**
     * Listen on order.created in consumer group "inventory-service".
     *
     * @Transactional wraps the entire method. If anything throws, all DB changes
     * roll back AND the Kafka offset is NOT committed (Spring Kafka will redeliver).
     *
     * Note: the @Transactional here only protects the DB writes. The Kafka send is
     * NOT inside the transaction — if the send fails after we've decremented stock,
     * stock stays decremented and we'd need compensation. We accept this risk for
     * now and solve it properly with the outbox pattern in Session 12.
     */
    @KafkaListener(
            topics = Topics.ORDER_CREATED,
            groupId = "inventory-service",
            containerFactory = "orderflowKafkaListenerFactory"


    )
    @Transactional
    public void  onOderCreated(EventEnvelope<Map<String,Object>> envelope){
        // Convert the Map-shaped payload into our typed OrderCreatedPayload.
        // Jackson's convertValue does the JSON->object conversion using our
        // ObjectMapper's configured modules (incl. JavaTimeModule).
        OrderCreatedPayload  order = objectMapper.convertValue(envelope.payload() , OrderCreatedPayload.class)  ;

        log.info("Processing order.created for orderId={} with {} items",
                order.orderId(), order.items().size());


        // STEP 1: Check availability for ALL items BEFORE reserving any.
        // This is the atomicity guarantee — we don't half-reserve.
        List<String> unavailableSkus = checkAvailability(order.items());

        if (!unavailableSkus.isEmpty()) {
            // The failure path. Publish reservation_failed and exit.
            // Note we did NOT modify the DB — no stock was decremented.
            publishReservationFailed(order, unavailableSkus, envelope);
            return;
        }

        // STEP 2: Stock is available for all items. Reserve them.
        Reservation reservation = reserveStock(order);


        // if the  data  is  updated  in the  reservation DB and the event fails  to send then  this  will be  handled  by he outbox  pattern


        // STEP 3: Publish the success event.
        publishReserved(order, reservation, envelope);




    }
    /**
     * Check every SKU in the order. Return list of SKUs that don't have enough stock.
     * Empty list = all available.
     *
     * For SKUs that don't exist in our DB yet, we treat them as having 1000 units
     * available — this is a demo-mode convenience. In production this would be a
     * hard error: ordering a non-existent SKU should fail validation upstream.
     */
    public List<String> checkAvailability(List<OrderCreatedPayload.Item> items){
        List<String> unavailable  = new ArrayList<>()  ;

        for   ( OrderCreatedPayload.Item item  : items ){
            StockItem  stock   = stockItemRepository.findById(item.sku()).orElseGet(() -> StockItem.builder()
                    .sku(item.sku())
                    .availableQuantity(1000)  // seed
                    .reservedQuantity(0) // seed
                    .build()
            );
            if( stock.getAvailableQuantity()  <  item.quantity()){
                unavailable.add(item.sku());

            }

        }

        return unavailable  ;

    }

    /**
     * Decrement stock, create the Reservation row. All within the @Transactional
     * boundary of the caller — so if anything fails partway, everything rolls back.
     */
    private Reservation reserveStock(OrderCreatedPayload order) {
        for (OrderCreatedPayload.Item item : order.items()) {
            StockItem stock = stockItemRepository.findById(item.sku())
                    .orElseGet(() -> StockItem.builder()
                            .sku(item.sku())
                            .availableQuantity(1000)
                            .reservedQuantity(0)
                            .build());

            stock.setAvailableQuantity(stock.getAvailableQuantity() - item.quantity());
            stock.setReservedQuantity(stock.getReservedQuantity() + item.quantity());
            stockItemRepository.save(stock);
        }

        Reservation reservation = Reservation.builder()
                .id(UUID.randomUUID())
                .orderId(order.orderId())
                .status(ReservationStatus.ACTIVE)
                .createdAt(Instant.now())
                .build();
        return reservationRepository.save(reservation);
    }

    /**
     * Publish the success event.
     *
     * Notice: we pass the original envelope's correlationId and eventId forward.
     *   - correlationId: same value across the entire customer flow (order →
     *     reservation → payment → ...). Set by the first event and propagated.
     *   - causationId: this NEW event's causationId is the eventId of the
     *     event that triggered it (the order.created we just consumed).
     *
     * This is how we maintain distributed traceability without a separate tracing
     * system. Grep logs by correlationId to see the entire flow for one customer.
     */

    private void publishReserved(
            OrderCreatedPayload order,
            Reservation reservation,
            EventEnvelope<Map<String, Object>> sourceEnvelope) {

        InventoryReservedPayload payload = new InventoryReservedPayload(
                order.orderId(),
                reservation.getId(),
                "WH-EAST-01",
                order.items().stream()
                        .map(i -> new InventoryReservedPayload.ReservedItem(i.sku(), i.quantity()))
                        .toList()
        );

        EventEnvelope<InventoryReservedPayload> envelope = EventEnvelope.of(
                "inventory.reserved",
                payload,
                sourceEnvelope.correlationId(),   // propagate correlation
                sourceEnvelope.eventId()           // this event was CAUSED by the source event
        );

        kafkaTemplate.send(Topics.INVENTORY_RESERVED, order.orderId().toString(), envelope);
        log.info("Reserved inventory for order {} (reservation {})",
                order.orderId(), reservation.getId());
    }

    /**
     * Publish the failure event.
     */
    private void publishReservationFailed(
            OrderCreatedPayload order,
            List<String> unavailableSkus,
            EventEnvelope<Map<String, Object>> sourceEnvelope) {

        InventoryReservationFailedPayload payload = new InventoryReservationFailedPayload(
                order.orderId(),
                "Insufficient stock for SKUs: " + unavailableSkus,
                unavailableSkus
        );

        EventEnvelope<InventoryReservationFailedPayload> envelope = EventEnvelope.of(
                "inventory.reservation_failed",
                payload,
                sourceEnvelope.correlationId(),
                sourceEnvelope.eventId()
        );

        kafkaTemplate.send(Topics.INVENTORY_RESERVATION_FAILED, order.orderId().toString(), envelope);
        log.warn("Failed to reserve inventory for order {} — unavailable SKUs: {}",
                order.orderId(), unavailableSkus);
    }











}
