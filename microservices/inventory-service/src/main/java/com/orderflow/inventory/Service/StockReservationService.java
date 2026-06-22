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
import java.util.ArrayList;
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
     * Result of a reservation attempt. Either success (with the reservation) or
     * failure (with the offending SKU). We return this instead of throwing, so the
     * rejection path never marks the Kafka listener's transaction rollback-only.
     */
    public record ReserveResult(boolean success, Reservation reservation, String failedSku) {
        static ReserveResult ok(Reservation r) { return new ReserveResult(true, r, null); }
        static ReserveResult fail(String sku)  { return new ReserveResult(false, null, sku); }
    }

    /**
     * Reserve stock atomically. Returns a ReserveResult rather than throwing on
     * insufficient stock.
     *
     * If a later item fails after earlier items were reserved, we explicitly restore
     * the earlier ones (manual compensation) so the order is all-or-nothing WITHOUT
     * relying on a thrown-exception rollback — which would mark the surrounding
     * Kafka transaction rollback-only and cause redelivery storms.
     */
    @Transactional
    public ReserveResult reserve(OrderCreatedPayload order) {
        List<OrderCreatedPayload.Item> reservedSoFar = new ArrayList<>();

        for (OrderCreatedPayload.Item item : order.items()) {
            stockItemRepository.seedIfAbsent(item.sku());
            int affected = stockItemRepository.tryReserve(item.sku(), item.quantity());

            if (affected == 0) {
                // Insufficient stock for this SKU. Manually restore anything we
                // already reserved in THIS order, so it's all-or-nothing — without
                // throwing across the transaction boundary.
                for (OrderCreatedPayload.Item done : reservedSoFar) {
                    stockItemRepository.restoreStock(done.sku(), done.quantity());
                }
                return ReserveResult.fail(item.sku());
            }
            reservedSoFar.add(item);
        }

        Reservation reservation = Reservation.builder()
                .id(UUID.randomUUID())
                .orderId(order.orderId())
                .status(ReservationStatus.ACTIVE)
                .createdAt(Instant.now())
                .build();
        reservationRepository.save(reservation);

        for (OrderCreatedPayload.Item item : order.items()) {
            reservedItemRepo.save(ReservationItem.builder()
                    .id(UUID.randomUUID())
                    .reservationId(reservation.getId())
                    .sku(item.sku())
                    .quantity(item.quantity())
                    .build());
        }

        return ReserveResult.ok(reservation);
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