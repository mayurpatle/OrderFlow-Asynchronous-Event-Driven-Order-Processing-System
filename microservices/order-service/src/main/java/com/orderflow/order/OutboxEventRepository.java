package com.orderflow.order;

import com.orderflow.order.OutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

/**
 * Repository for the outbox.
 *
 * The relay uses findUnpublished() to fetch pending rows in creation order,
 * capped by a page size so one poll cycle doesn't try to publish a massive
 * backlog all at once.
 */
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Fetch unpublished rows (publishedAt IS NULL), oldest first, capped to a
     * batch size via Pageable. Oldest-first preserves event ordering.
     *
     * In a multi-instance deployment you'd add FOR UPDATE SKIP LOCKED here so
     * that concurrent relays don't both grab the same rows. We run single-
     * instance for now, so a plain query is fine — but this is the line that
     * would change for horizontal scaling.
     */
    @Query("SELECT o FROM OutboxEvent o WHERE o.publishedAt IS NULL ORDER BY o.createdAt ASC")
    List<OutboxEvent> findUnpublished(Pageable pageable);
}