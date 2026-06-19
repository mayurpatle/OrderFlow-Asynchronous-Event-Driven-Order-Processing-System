package com.orderflow.order;



import com.orderflow.order.OutboxEvent;
import com.orderflow.order.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * The Outbox Relay.
 *
 * Scheduled poller that drains the outbox to Kafka. Every poll:
 *   1. Fetch a batch of unpublished rows (publishedAt IS NULL), oldest first
 *   2. Publish each to its topic, keyed by aggregateId
 *   3. Stamp publishedAt on success
 *
 * Guarantees, combined with the atomic write in OrderService.placeOrder():
 *   - No order exists without its event eventually reaching Kafka (no loss)
 *   - May publish a duplicate if we crash after send but before marking
 *     (at-least-once) — harmless because consumers are idempotent
 *
 * Per-row marking (not batch): a single failed row stays unpublished and
 * retries next cycle; its neighbors are unaffected.
 */
@Component
@Slf4j
public class OutboxRelay {

    private static final int BATCH_SIZE = 100;

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> stringKafkaTemplate;

    public OutboxRelay(OutboxEventRepository outboxRepository,
                       @Qualifier("stringKafkaTemplate") KafkaTemplate<String, String> stringKafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.stringKafkaTemplate = stringKafkaTemplate;
    }

    /**
     * Runs every 500ms. fixedDelay means the next run starts 500ms AFTER the
     * previous one finishes (not every 500ms regardless) — so a slow poll won't
     * pile up overlapping executions.
     */
    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishOutbox() {
        List<OutboxEvent> batch =
                outboxRepository.findUnpublished(PageRequest.of(0, BATCH_SIZE));

        if (batch.isEmpty()) {
            return;  // nothing to do; stay quiet
        }

        log.info("Outbox relay: publishing {} pending event(s)", batch.size());

        for (OutboxEvent event : batch) {
            try {
                // Send the stored JSON VERBATIM. Key = aggregateId (orderId),
                // preserving per-order partition routing. Value = the exact JSON
                // string we serialized at write-time.
                stringKafkaTemplate
                        .send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get();  // block until broker acks — we only mark published on real success
                // the .get() returns  future

                event.setPublishedAt(Instant.now());
                outboxRepository.save(event);

                log.debug("Outbox relay: published {} for {} to {}",
                        event.getEventType(), event.getAggregateId(), event.getTopic());

            } catch (Exception e) {
                // Send failed (Kafka down, broker error). Leave publishedAt NULL
                // so this row retries next cycle. Don't rethrow — we want the
                // remaining rows in the batch to still get their chance.
                log.warn("Outbox relay: failed to publish event {} for {} — will retry next cycle",
                        event.getId(), event.getAggregateId(), e);
            }
        }
    }
}