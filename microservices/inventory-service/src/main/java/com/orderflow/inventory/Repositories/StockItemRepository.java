package com.orderflow.inventory.Repositories;

import com.orderflow.inventory.Entity.StockItem;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StockItemRepository extends JpaRepository<StockItem, String> {
}