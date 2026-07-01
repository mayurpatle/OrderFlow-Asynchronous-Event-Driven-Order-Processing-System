package com.orderflow.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.common.events.EventEnvelope;
import com.orderflow.common.events.Topics;
import com.orderflow.common.events.payloads.InventoryReservationFailedPayload;
import com.orderflow.common.events.payloads.InventoryReservedPayload;
import com.orderflow.common.events.payloads.OrderCreatedPayload;
import com.orderflow.common.events.payloads.PaymentFailedPayload;
import com.orderflow.inventory.Entity.Reservation;
import com.orderflow.inventory.Entity.ReservationItem;
import com.orderflow.inventory.Entity.ReservationStatus;
import com.orderflow.inventory.Entity.StockItem;
import com.orderflow.inventory.Exception.InsufficientStockException;
import com.orderflow.inventory.Repositories.ReservationItemRepository;
import com.orderflow.inventory.Repositories.ReservationRepository;
import com.orderflow.inventory.Repositories.StockItemRepository;
import com.orderflow.inventory.Service.StockReservationService;
import org.springframework.transaction.annotation.Transactional;
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

    private final ReservationItemRepository reservedItemRepo;   // ← NEW

    private final ObjectMapper objectMapper  ;

    private final StockReservationService stockReservationService ;

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
    public void  onOrderCreated(EventEnvelope<Map<String,Object>> envelope){
        // Convert the Map-shaped payload into our typed OrderCreatedPayload.
        // Jackson's convertValue does the JSON->object conversion using our
        // ObjectMapper's configured modules (incl. JavaTimeModule).
        OrderCreatedPayload  order = objectMapper.convertValue(envelope.payload() , OrderCreatedPayload.class)  ;

        log.info("Processing order.created for orderId={} with {} items",
                order.orderId(), order.items().size());

        // This baisc checking of availability will result in concurrency probelms
        // STEP 1: Check availability for ALL items BEFORE reserving any.
        // This is the atomicity guarantee — we don't half-reserve.
        // List<String> unavailableSkus = checkAvailability(order.items());

        // if (!unavailableSkus.isEmpty()) {
            // The failure path. Publish reservation_failed and exit.
            // Note we did NOT modify the DB — no stock was decremented.
            // publishReservationFailed(order, unavailableSkus, envelope);
            // return;
        // }


        // Reservation reservation = reserveStock(order);





        // publishReserved(order, reservation, envelope);
        // Dont use  try catch will result in the  UnexpectedRollbackException
        /* try {
            // STEP 2: Stock is available for all items. Reserve them.
            // reserveStock runs in its own transaction (see @Transactional on it).
            // The atomic tryReserve enforces the stock ceiling; if any item can't
            // be reserved, it throws InsufficientStockException and the whole
            // reservation rolls back.
            Reservation reservation = stockReservationService.reserve(order);
            // if the  data  is  updated  in the  reservation DB and the event fails  to send then  this  will be  handled  by he outbox  pattern
            // STEP 3: Publish the success event.
            publishReserved(order, reservation, envelope);

        } catch (InsufficientStockException e) {
            // Rejection path: one or more SKUs didn't have enough stock.
            // The reservation transaction rolled back — no stock was decremented.
            publishReservationFailed(order, List.of(e.getSku()), envelope);
        } */
        // Handle the  concurrency problem my making transactional methods in the seperate service and  adding Atomic SQL UPDATE method in stock item repository.
        StockReservationService.ReserveResult result = stockReservationService.reserve(order);

        if (result.success()) {
            publishReserved(order, result.reservation(), envelope);
        } else {
            publishReservationFailed(order, List.of(result.failedSku()), envelope);
        }




    }
    /*
     * Check every SKU in the order. Return list of SKUs that don't have enough stock.
     * Empty list = all available.
     *
     * For SKUs that don't exist in our DB yet, we treat them as having 1000 units
     * available — this is a demo-mode convenience. In production this would be a
     * hard error: ordering a non-existent SKU should fail validation upstream.
     */
    /* public List<String> checkAvailability(List<OrderCreatedPayload.Item> items){
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

    } */

    /*
     * Decrement stock, create the Reservation row. All within the @Transactional
     * boundary of the caller — so if anything fails partway, everything rolls back.
     */



    /*
     * Reserve stock for every item using the ATOMIC conditional UPDATE.
     *
     * For each item:
     *   1. Ensure the SKU row exists (idempotent seed)
     *   2. Run tryReserve — atomic check-and-decrement, returns rows affected
     *   3. If 0 rows affected, this SKU didn't have enough stock
     *
     * If ANY item can't be reserved, we throw to roll back the whole transaction
     * (including any items we already reserved in this loop) and signal the caller
     * to take the rejection path. This preserves the all-or-nothing guarantee:
     * an order either reserves ALL its items or NONE.
     *
     * No read-modify-write anywhere. No lost-update window. The database enforces
     * the stock ceiling atomically.
     */

    // Will Result in Spring AOP Self Invocation therefore making it in another service : StockItemReservationService

     /* @Transactional
    private Reservation reserveStockTransactional(OrderCreatedPayload order) {
        for (OrderCreatedPayload.Item item : order.items()) {
            stockItemRepository.seedIfAbsent(item.sku());

            int affected = stockItemRepository.tryReserve(item.sku(), item.quantity());
            if (affected == 0) {
                // Atomic check failed: not enough stock for this SKU RIGHT NOW.
                // Throw to roll back everything reserved so far in this order,
                // and let onOrderCreated catch it and publish reservation_failed.
                throw new InsufficientStockException(item.sku());
            }

        }

        Reservation reservation = Reservation.builder()
                .id(UUID.randomUUID())
                .orderId(order.orderId())
                .status(ReservationStatus.ACTIVE)
                .createdAt(Instant.now())
                .build();

        // Persist one ReservationItem per ordered SKU.
        // These are what onPaymentFailed will read during saga compensation.
        for (OrderCreatedPayload.Item item : order.items()) {
            ReservationItem ri = ReservationItem.builder()
                    .id(UUID.randomUUID())
                    .reservationId(reservation.getId())
                    .sku(item.sku())
                    .quantity(item.quantity())
                    .build();
            reservedItemRepo.save(ri);
        }



        return reservationRepository.save(reservation);



    } */

    /*
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

    // Session  9 : Saga Compensation for failed Payments

    /*
     * Compensation handler — releases reserved stock when a payment fails.
     *
     * THIS IS THE SAGA ROLLBACK in action. When payment-service publishes
     * payment.failed for an order, we look up the matching reservation, mark it
     * RELEASED, and return the reserved quantities to availableQuantity.
     *
     * Idempotency: the reservation's status field IS our idempotency key. If a
     * duplicate payment.failed arrives (Kafka at-least-once), we'll see status
     * is already RELEASED and skip silently.
     *
     * Missing reservation: if we receive payment.failed but there's no
     * reservation for that orderId, we log a warning and move on rather than
     * throwing. Throwing would cause Spring Kafka to retry forever, blocking
     * the consumer thread. There's nothing meaningful to retry.
     *
     * Why we're NOT publishing an "inventory.released" event yet:
     *   In production we'd typically publish one so that notification-service
     *   could email the customer, and order-service could transition the order
     *   to CANCELLED. We'll add this in later sessions — for today the focus
     *   is on the compensation mechanic itself.
     */
    @KafkaListener(
            topics = Topics.PAYMENT_FAILED,
            groupId = "inventory-service",
            containerFactory = "orderflowKafkaListenerFactory"
    )
    @Transactional
    public void onPaymentFailed(EventEnvelope<Map<String, Object>> envelope) {
        // Convert payload to typed form
        /* PaymentFailedPayload failed = objectMapper.convertValue(
                envelope.payload(), PaymentFailedPayload.class);

        log.info("Processing payment.failed for orderId={} reason={}",
                failed.orderId(), failed.failureReason());

        // Find the reservation that needs to be compensated
        var reservationOpt = reservationRepository.findByOrderId(failed.orderId());

        if (reservationOpt.isEmpty()) {
            // No reservation found. Either we never created one, or it was
            // already cleaned up by some other path. Either way, nothing to do.
            // We log and return — DO NOT throw, because throwing would cause
            // Spring Kafka to redeliver forever and block this consumer.
            log.warn("No reservation found for order {} — nothing to compensate", failed.orderId());
            return;
        }

        Reservation reservation = reservationOpt.get();

        // Idempotency check: if already released, this is a duplicate delivery.
        // Silently skip.
        if (reservation.getStatus() != ReservationStatus.ACTIVE) {
            log.info("Reservation {} for order {} is already {}, skipping (idempotent)",
                    reservation.getId(), failed.orderId(), reservation.getStatus());
            return;
        }

        // ====================================================================
        // THE COMPENSATION ITSELF
        // ====================================================================

        // Step 1: Find which items this reservation locked, and how much.
        //
        // We don't store the reserved items directly on the Reservation entity
        // (look at Reservation.java — it just has id, orderId, status, createdAt).
        // So we need another way to know which SKUs to credit back.
        //
        // We have two options:
        //   (a) Add a reserved_items table linked to Reservation, populated when
        //       we reserve in onOrderCreated
        //   (b) Read the original order.created event from Kafka and use its items
        //
        // Option (b) requires re-reading Kafka and breaks encapsulation. Option (a)
        // is the right long-term answer. For Session 9 we'll do option (a) properly —
        // see ReservationItem entity in the next step.
        //
        // For now, this method assumes ReservationItem exists. If you're reading
        // this from a fresh checkout: see the ReservationItem entity that the
        // session adds alongside this consumer method.

        var reservedItems = reservedItemRepo.findByReservationId(reservation.getId());

        if (reservedItems.isEmpty()) {
            log.warn("Reservation {} has no reserved items recorded — cannot restore stock. " +
                    "Marking as RELEASED anyway.", reservation.getId());
        }

        // Step 2: Restore stock for each item
        for (ReservationItem item : reservedItems) {
            StockItem stock = stockItemRepository.findById(item.getSku())
                    .orElseThrow(() -> new IllegalStateException(
                            "StockItem missing for SKU " + item.getSku() +
                                    " — this should never happen for an existing reservation"));

            stock.setAvailableQuantity(stock.getAvailableQuantity() + item.getQuantity());
            stock.setReservedQuantity(stock.getReservedQuantity() - item.getQuantity());
            stockItemRepository.save(stock);

            log.info("Restored {} units of {} to available stock", item.getQuantity(), item.getSku());
        }

        // Step 3: Mark the reservation as RELEASED
        reservation.setStatus(ReservationStatus.RELEASED);
        reservationRepository.save(reservation);

        log.info("SAGA COMPENSATION: Released reservation {} for order {} (payment failure code: {})",
                reservation.getId(), failed.orderId(), failed.failureCode()); */

        // moved the  core logic of  decrementing  the Q to seperate service  and  annotated it  with transactional


        PaymentFailedPayload failed = objectMapper.convertValue(
                envelope.payload(), PaymentFailedPayload.class);

        log.info("Processing payment.failed for orderId={} reason={}",
                failed.orderId(), failed.failureReason());

        boolean released = stockReservationService.release(failed.orderId());
        if (released) {
            log.info("SAGA COMPENSATION: released reservation for order {} (failure code: {})",
                    failed.orderId(), failed.failureCode());
        } else {
            log.info("No active reservation to compensate for order {} (already released or missing)",
                    failed.orderId());
        }
    }











}
