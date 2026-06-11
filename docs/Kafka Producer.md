# Kafka Producer

## The Producer's Job

A Kafka producer is responsible for:

- Serializing your Java object into bytes (the broker only speaks bytes)
- Picking a partition — either you specify, or it hashes the key, or it picks round-robin
- Batching messages before sending (for throughput)
- Sending the batch to the broker via TCP
- Awaiting acknowledgment — depending on `acks` setting
- Retrying on transient failures

---

## The Three Producer Settings You Must Know

### `acks` — How durable must the write be before we consider it sent?

| Setting | Behavior |
|---------|----------|
| **`acks=0`** | Fire and forget. Producer doesn't wait for any ack. Fastest, least durable. Messages can be lost on broker crashes. |
| **`acks=1`** | Wait for the leader broker to write to its log. Lost only if the leader crashes before replication. (Default in older clients.) |
| **`acks=all`** | Wait for the leader **and** all in-sync replicas to confirm. Slowest, most durable. What we'll use. |

For financial events like orders, `acks=all` is the only acceptable choice. Yes, it's slower. Yes, the latency cost is real. But losing an order event because a broker crashed is catastrophic.

### `enable.idempotence` — Guarantee no duplicates from producer retries

**Without this:** if the producer sends a message, the broker writes it but the ack is lost on the network, the producer retries, the broker writes it again. Now you have two copies of the same message. The `order.created` event fires twice, two emails sent, customer charged twice.

**With `enable.idempotence=true`:** the producer attaches a sequence number to every message. The broker tracks the producer's sequence numbers and deduplicates retries. This combined with `acks=all` gives you exactly-once producer-to-broker semantics. (End-to-end exactly-once needs more, which we'll cover in Session 12.)

---

## The Key Choice — Why `orderId`?

You saw this in Session 3. Messages with the same key always go to the same partition, and partition ordering is preserved. So for OrderFlow:

| Key strategy | Effect |
|--------------|--------|
| **Key by `orderId`** | Every event for one order (`created`, then potentially `cancelled`) lands on one partition. The downstream consumer processes them in causal order, always. |
| **Key by `customerId`** | Every event for one customer lands on one partition. But events for different orders of the same customer also serialize through that partition — unnecessary contention. |
| **No key** | Events distribute round-robin. We lose ordering guarantees. Catastrophic for our case. |

**`orderId` it is.** We'll see the producer code make this choice explicit.
