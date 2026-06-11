# Kafka Internals

## Core Concepts

- Kafka is an **append-only log** — partitions are independent ordered logs
- **Ordering is per-partition**, not global — same key always lands on same partition
- Producers append, consumers read by offset, broker keeps everything until retention expires
- **Consumer groups** split partition work — partitions are the unit of parallelism
- Two groups consuming the same topic don't interfere — each maintains its own offsets
- **Rebalancing** happens automatically when consumers join/leave
- Offsets are stored in `__consumer_offsets`, an internal Kafka topic — durable and resettable
- You can **replay** any time by resetting offsets — the key superpower over traditional queues
- **Consumer lag** = `LOG-END-OFFSET - CURRENT-OFFSET`, the most important metric to monitor

---

## Interview Q&A

### How does Kafka guarantee message ordering?

Kafka guarantees ordering **within a partition**, not across partitions. Messages with the same partition key always land on the same partition because Kafka hashes the key modulo partition count.

So if I'm building a payment system and I key events by `orderId`, all events for one order are in one partition and processed in order. Cross-order ordering isn't guaranteed but also isn't needed.

### What happens when a Kafka consumer crashes?

The broker stops receiving heartbeats from that consumer. After `session.timeout.ms`, the broker marks it dead and triggers a **rebalance** — the consumer group pauses, partitions are redistributed among surviving consumers, and consumption resumes.

The committed offset is preserved, so the surviving consumer picks up exactly where the dead one left off. No messages are lost, but in-flight messages may be redelivered — which is why consumers need to be idempotent.

### What's the difference between Kafka and a traditional message queue?

Traditional queues delete messages after they're consumed. Kafka keeps messages in the log until retention expires, regardless of who's read them.

This enables **replay** — you can reset a consumer to an earlier offset and re-consume. It also enables multiple independent consumer groups to read the same topic without affecting each other.

Kafka is fundamentally a **distributed log**; queues are FIFO data structures. Different tools for different problems.
