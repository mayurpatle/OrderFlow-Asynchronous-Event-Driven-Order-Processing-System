package com.orderflow.inventory.Entity;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;

/**
 * Stock level for one SKU.
 *
 * Two fields track quantities:
 *   - availableQuantity: count physically in the warehouse and unallocated
 *   - reservedQuantity: count allocated to in-flight orders but not yet shipped
 *
 * When an order is created:
 *   available -= ordered_quantity
 *   reserved  += ordered_quantity
 *
 * When the order ships:
 *   reserved  -= shipped_quantity
 *   (the items are physically gone — no longer counted in either)
 *
 * When the order is cancelled (saga rollback):
 *   reserved  -= cancelled_quantity
 *   available += cancelled_quantity
 *
 * This double-bookkeeping prevents oversell. Two customers can't both reserve
 * the last unit because the first reservation reduces availableQuantity.
 */

@Entity
@Table(name = "stock_items")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockItem {

    @Id
    private String sku  ;

    @Column(nullable = false)
    private int availableQuantity  ;

    @Column(nullable = false)
    private int  reservedQuantity   ;



}
