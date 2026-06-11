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


### Why does payment-service consume inventory.reserved instead of order.created?

Because of ordering enforcement through the event topology. If payment-service consumed order.created directly, it could charge a customer for an order whose inventory was unavailable — and refunds are expensive, both in real cost and in customer trust. By subscribing to inventory.reserved, we make it architecturally impossible to charge before stock is confirmed. The workflow ordering — stock first, then payment — is enforced by which topics each service subscribes to. No central coordinator decides this. The choice of subscription topics is the workflow design. This is choreography in practice.

### How do you handle duplicate payment processing if Kafka redelivers

Two layers. At the application layer, I check for an existing Payment row by orderId at the start of the consumer method — if one exists, I skip processing. At the database layer, the orderId column has a unique constraint, so even if the application check is bypassed somehow, the database rejects a second insert. This belt-and-suspenders pattern means either layer alone would prevent duplicate charges, and together they make the consumer truly idempotent. For more granular idempotency across all consumers, the canonical solution is a separate processed_events table keyed by event ID, which I'd build into a shared service for production — that's a separate concern from this domain check.

### What if the gateway call succeeded but you crashed before saving COMPLETED status? The next delivery would find the PENDING row and not re-charge, but you'd also never confirm the charge.

Correct, that's a real gap. The fix is a reconciliation job that periodically scans for PENDING payments older than some threshold — say, 30 minutes — and queries the gateway to find out what actually happened. That's how Stripe, Razorpay, and every real payment system handles it. The reconciliation job is the safety net for any stuck state in the database. I haven't built that here, but the design pattern is: never trust the in-memory state of a gateway call; trust only the gateway's own records as ground truth.

### Walk me through what happens when a payment fails.

Payment-service writes the FAILED status to its payments table and publishes a payment.failed event to Kafka. Right now in my project, that event has no consumer — which means the stock reserved for that failed order stays locked up in inventory-service. That's exactly the kind of inconsistency saga compensation is designed to fix. The next thing I'd build, and what I'm about to build in the next session, is an inventory-service consumer for payment.failed that finds the matching reservation by orderId and releases the stock back to availableQuantity. That's compensation in choreographed sagas — each service's failure event triggers compensating actions in upstream services. There's no central coordinator; the rollback happens because each service knows how to undo its own work in response to the right downstream event.


