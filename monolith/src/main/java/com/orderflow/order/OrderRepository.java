package com.orderflow.order;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * Spring Data JPA repository. We extend JpaRepository<Entity, IdType>
 * and Spring generates the implementation at runtime — save(), findById(),
 * findAll(), deleteById(), and dozens more come for free.
 *
 * No implementation class. Spring scans for interfaces extending JpaRepository
 * and creates a proxy bean. This is one of those things that feels magical
 * until you remember Spring is just generating an implementation.
 */
public interface OrderRepository extends JpaRepository<Order, UUID> {
}