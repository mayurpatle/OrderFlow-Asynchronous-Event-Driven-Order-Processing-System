package com.orderflow.inventory.Repositories;

import com.orderflow.inventory.Entity.StockItem;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StockItemRepository extends JpaRepository<StockItem, String> {

   // The below  two methods are for to fix Concurrency issue ny Atomic SQL UPDATE QUERY


    /**
     * Atomic conditional reserve. Decrements available and increments reserved
     * in a SINGLE SQL statement, but ONLY if there's enough available stock.
     *
     * This is the heart of the oversell fix. There is no read-modify-write window:
     * the database evaluates "available_quantity >= :qty" (the check) and applies
     * "available_quantity - :qty" (the decrement) as one indivisible operation.
     * Two concurrent calls cannot both decrement past zero — the database
     * serializes the row updates internally, and the second call sees the first
     * call's committed effect.
     *
     * Returns the number of rows affected:
     *   1 -> stock was available; the reservation succeeded
     *   0 -> the WHERE condition failed (not enough stock); reservation rejected
     *
     * @Modifying tells Spring Data this query changes data (not a SELECT).
     * clearAutomatically/flushAutomatically keep the persistence context in sync
     * so a later read in the same transaction sees the updated values.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE StockItem s
            SET s.availableQuantity = s.availableQuantity - :qty,
                s.reservedQuantity  = s.reservedQuantity  + :qty
            WHERE s.sku = :sku AND s.availableQuantity >= :qty
            """)
    int tryReserve(@Param("sku") String sku, @Param("qty") int qty);

    /**
     * Atomic restore — the inverse, for saga compensation when a payment fails.
     * Returns reserved stock back to available. No conditional needed (we're
     * always allowed to give stock back), but we keep it as a single atomic
     * statement for the same lost-update safety as tryReserve.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE StockItem s
            SET s.availableQuantity = s.availableQuantity + :qty,
                s.reservedQuantity  = s.reservedQuantity  - :qty
            WHERE s.sku = :sku
            """)
    int restoreStock(@Param("sku") String sku, @Param("qty") int qty);

    // The Aboce two method will only  work if the row exist in the DB
    // Use this seed method
    /**
     * Seed a SKU with default stock if it doesn't already exist. Idempotent and
     * concurrency-safe via ON CONFLICT DO NOTHING — if two threads race to seed
     * the same SKU, one inserts and the other's insert is silently ignored.
     *
     * Native query because ON CONFLICT is Postgres-specific syntax.
     *
     * In production you would NOT auto-seed — SKUs would be provisioned by a
     * catalog service. This exists only so our demo can accept orders for SKUs
     * we never explicitly created.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            INSERT INTO stock_items (sku, available_quantity, reserved_quantity)
            VALUES (:sku, 1000, 0)
            ON CONFLICT (sku) DO NOTHING
            """, nativeQuery = true)
    void seedIfAbsent(@Param("sku") String sku);
}