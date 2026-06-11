# Kafka Interview Questions

## Producer Setup

### Walk me through your Kafka producer setup.

I use `acks=all` with idempotent producers and infinite retries. That gives me exactly-once producer-to-broker semantics — no message loss, no duplicates from network retries. I key every event by the business entity ID — `orderId` in this case — so all events for one entity land on the same partition and consumers process them in causal order.

I wrap every payload in an envelope with `eventId`, `correlationId`, and `occurredAt`:

- **eventId** — consumer-side dedup
- **correlationId** — distributed tracing across services
- **occurredAt** — business-time analytics

The send is async; I never block the calling thread. The producer is fire-and-forget from the HTTP handler's perspective, with the durability guarantee coming from `acks=all` + idempotence + retries. For absolute guarantees against producer-side crashes, the outbox pattern is the right answer — save the event to the DB in the same transaction as the entity, then a separate process publishes from the outbox.

### What's the difference between `acks=1` and `acks=all`?

**`acks=1`** means the partition leader writes to its log and acknowledges. If the leader crashes before the message replicates, the message is lost.

**`acks=all`** means the leader waits for all in-sync replicas to also write. The trade-off is latency — `acks=all` adds a round trip to replicas — for durability. For anything money-related, like orders or payments, `acks=all` is the only acceptable choice.

---

## Order Flow & Sagas

### Walk me through what happens when a customer places an order.

The HTTP request hits `order-service`, which saves the order in `PENDING` state and publishes `order.created` to Kafka — total time under 10ms. The `order-service` responds `202 Accepted` to the customer immediately.

Asynchronously, multiple consumers process the same event in parallel:

- **notification-service** emails the customer
- **inventory-service** checks stock and either reserves it and emits `inventory.reserved`, or fails and emits `inventory.reservation_failed`

Each event carries a `correlationId` that's propagated from the original `order.created`, so I can trace the full flow for any customer through the logs. The two-path pattern in `inventory-service` — succeed vs fail — is the start of the saga. Downstream services subscribe to whichever outcome they care about: `payment-service` waits for `inventory.reserved` before charging, `order-service` listens for `inventory.reservation_failed` to cancel the order.

### What's the saga pattern and how does this implement it?

A saga is a sequence of local transactions across multiple services, where each step publishes an event that triggers the next step. Instead of one distributed transaction with a coordinator, each service makes its own decision locally and publishes the outcome. If a step fails, downstream services react by publishing compensating events — releasing reserved stock, refunding charges, cancelling orders.

This is the **choreography** approach to sagas. The alternative is **orchestration**, where a central coordinator drives the flow.

| Approach | Pros | Cons |
|----------|------|------|
| **Choreography** | Scales better; each service owns its logic | Harder to reason about |
| **Orchestration** | Easier to reason about | Creates a coordination bottleneck |

OrderFlow uses choreography because each service owns its own logic and the coordination emerges from the event protocol.
