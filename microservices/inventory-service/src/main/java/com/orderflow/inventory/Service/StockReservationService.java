package com.orderflow.inventory.Service;

import com.orderflow.common.events.payloads.OrderCreatedPayload;
import com.orderflow.inventory.Entity.Reservation;
import com.orderflow.inventory.Entity.ReservationItem;
import com.orderflow.inventory.Entity.ReservationStatus;
import com.orderflow.inventory.Exception.InsufficientStockException;
import com.orderflow.inventory.Repositories.ReservationItemRepository;
import com.orderflow.inventory.Repositories.ReservationRepository;
import com.orderflow.inventory.Repositories.StockItemRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Owns the transactional stock-mutation logic for inventory-service.
 *
 * Separated from InventoryConsumer specifically so that @Transactional works:
 * Spring's transaction proxy is bypassed on self-invocation (a method calling
 * another @Transactional method in the SAME class). By putting the transactional
 * work in its own bean and injecting it, every call is a real proxied call and
 * the transaction boundary is honored.
 *
 * CONCURRENCY: reservation uses the atomic conditional UPDATE (tryReserve), which
 * does check-and-decrement in one indivisible SQL statement. No read-modify-write,
 * no lost-update window. Overselling is impossible regardless of thread count.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class StockReservationService {

    private final StockItemRepository stockItemRepository;
    private final ReservationRepository reservationRepository;
    private final ReservationItemRepository reservedItemRepo;

    /**
     * Reserve stock for all items in the order, atomically.
     *
     * For each item: seed the SKU if absent, then run the atomic tryReserve.
     * If tryReserve affects 0 rows, stock was insufficient — throw
     * InsufficientStockException, which rolls back THIS transaction (undoing any
     * items already reserved in this loop) and signals the caller to reject.
     *
     * All-or-nothing: an order reserves ALL its items or NONE.
     *
     * @Transactional here is honored because this is a proxied bean method called
     * from a DIFFERENT bean (InventoryConsumer). The transaction opens on entry
     * and commits/rolls back on return — BEFORE the caller's catch block runs.
     */
    @Transactional
    public Reservation reserve(OrderCreatedPayload order) {
        for (OrderCreatedPayload.Item item : order.items()) {
            stockItemRepository.seedIfAbsent(item.sku());

            int affected = stockItemRepository.tryReserve(item.sku(), item.quantity());
            if (affected == 0) {
                // Atomic check failed: not enough stock for this SKU right now.
                // Throwing rolls back everything reserved so far in this txn.
                throw new InsufficientStockException(item.sku());
            }
        }

        // All items reserved atomically. Record the reservation + its line items.
        Reservation reservation = Reservation.builder()
                .id(UUID.randomUUID())
                .orderId(order.orderId())
                .status(ReservationStatus.ACTIVE)
                .createdAt(Instant.now())
                .build();
        reservationRepository.save(reservation);

        for (OrderCreatedPayload.Item item : order.items()) {
            ReservationItem ri = ReservationItem.builder()
                    .id(UUID.randomUUID())
                    .reservationId(reservation.getId())
                    .sku(item.sku())
                    .quantity(item.quantity())
                    .build();
            reservedItemRepo.save(ri);
        }

        return reservation;
    }

    /**
     * Release a reservation (saga compensation when payment fails).
     *
     * Restores stock atomically via restoreStock, then marks the reservation
     * RELEASED. Idempotent: if the reservation is not ACTIVE, does nothing.
     *
     * Returns true if it actually released, false if it was a no-op (already
     * released / missing) — lets the caller log appropriately.
     */
    @Transactional
    public boolean release(UUID orderId) {
        var reservationOpt = reservationRepository.findByOrderId(orderId);
        if (reservationOpt.isEmpty()) {
            return false;  // nothing to compensate
        }

        Reservation reservation = reservationOpt.get();
        if (reservation.getStatus() != ReservationStatus.ACTIVE) {
            return false;  // already released — idempotent skip
        }

        // Restore each reserved item's stock atomically.
        var reservedItems = reservedItemRepo.findByReservationId(reservation.getId());
        for (ReservationItem item : reservedItems) {
            stockItemRepository.restoreStock(item.getSku(), item.getQuantity());
            log.info("Restored {} units of {} to available stock", item.getQuantity(), item.getSku());
        }

        reservation.setStatus(ReservationStatus.RELEASED);
        reservationRepository.save(reservation);
        return true;
    }
}