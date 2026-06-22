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

### Q1: "What is the saga pattern and when would you use it?"

"The saga pattern is the answer to 'how do you maintain consistency across microservices without a distributed transaction.' A saga is a sequence of local transactions across services where each step publishes an event that triggers the next step. When a downstream step fails, upstream services react by publishing compensating events that undo their own work. There's no central coordinator and no two-phase commit. Each service knows how to do its forward action and how to undo it when the right downstream failure event arrives. I'd use this anywhere I have a multi-step business workflow spanning service boundaries — order processing, account opening, refund flows, anything where each step needs to either fully complete or be rolled back across multiple services. The alternative — orchestrated sagas with a central coordinator — is simpler to reason about but creates a single point of bottleneck and forces every service to know about the coordinator. Choreographed sagas like the one I built scale better and keep services independent, at the cost of being harder to debug because the workflow is implicit in event subscriptions."

### Q2: "How do you handle a payment failure in your system?"

"Payment-service publishes a payment.failed event when the gateway declines. Inventory-service subscribes to that topic. When it receives the event, it looks up the reservation by orderId — every reservation is keyed by orderId because we partition Kafka events by orderId too. If the reservation status is ACTIVE, it transitions it to RELEASED and returns the reserved quantities back to available stock, one entry per SKU. The whole thing runs in one DB transaction so it's atomic. If the reservation is already RELEASED, it's a duplicate event delivery from Kafka's at-least-once semantics, so we skip silently. If the reservation doesn't exist at all, we log a warning and move on rather than throwing — throwing would cause Spring Kafka to retry forever, blocking the consumer thread. Critically, the only service that changed for this compensation logic was inventory-service. Order-service, payment-service, and notification-service all stayed exactly the same. That localization of failure handling is the win of choreography."

### Q3: "What happens if inventory-service crashes during compensation?"

"Kafka offers at-least-once delivery. If inventory-service consumed the payment.failed event, started processing the compensation, and crashed mid-way before committing the Kafka offset — when it restarts, Kafka redelivers the same event. The whole compensation runs again. That's safe because we made it idempotent: the first thing the handler does is check whether the reservation status is already RELEASED. If yes, skip. If not, proceed. The idempotency key is the reservation's state itself, not a separate dedup table — that's cleaner because the business state and the idempotency state are the same thing. I verified this in practice: I killed inventory-service mid-flight while payment failures were happening, restarted it 30 seconds later, and the backlog of unprocessed payment.failed events got compensated automatically when the service came back. Zero data loss, no manual intervention. The combination of Kafka's durability and idempotent consumers gives you operational resilience you can't get from synchronous architectures."

If any of those felt awkward to say aloud, rehearse them again. Senior interviews are won by candidates who can deliver these answers fluently, not by candidates who know the technology better.

What's NOT yet handled (be ready for the follow-up)
A senior interviewer will push further. Be ready for these — they're worth knowing the answers to even though we won't build them all:
Q: "What if the compensation itself fails?"n  


"Spring Kafka retries the failed message a configurable number of times, then by default sends it to a dead-letter queue. I haven't customized this yet — that's Session 15. In production I'd have a DLQ topic for payment.failed events that couldn't be compensated after N retries. An operations runbook would specify how to manually inspect and replay these — typically the DLQ has very low volume because compensation logic is simple and rarely fails, so manual handling is acceptable. If volume were higher I'd build an admin dashboard for DLQ inspection."

Q: "What about the publish-after-DB-commit risk in your forward path? If you save the Reservation but the Kafka send fails, you've reserved stock for an order that no other service will hear about."

"Correct, that's the dual-write problem. My current implementation has a small window where DB commits but the Kafka send fails — the producer's retry logic catches most cases, but a crash after DB commit and before the send completes would leave that gap. The clean fix is the transactional outbox pattern: write the event to an outbox table in the same DB transaction as the entity, then a separate worker reads from the outbox and publishes to Kafka. That makes the DB the source of truth for what should be published, and pushes the publish concern to a process that can retry independently. I'm building that in Session 12."

Q: "How do you debug a flow that goes wrong in production?"

"Every event carries a correlationId that's set at the entry point and propagated through every downstream event. So if a customer reports an issue, I take the order ID, find the original correlationId, and grep my logs for that correlationId across all services. I get the full causal chain in one query — what events fired, in what order, with what payloads. The same correlationId would feed into a distributed tracing system like Jaeger in production. The pattern is the foundation; the tooling is how you scale it."

Q. "Walk me through how a customer's order flows through your system from button-click to ship-confirmation email."

"Customer hits POST /api/orders. Order-service persists the order in PENDING status and publishes order.created to Kafka — total HTTP latency around 5ms. From there it's async: inventory-service consumes order.created, checks stock for all items in one transaction, decrements available quantity, persists reservation records keyed by orderId, and publishes inventory.reserved. Payment-service consumes inventory.reserved — note we subscribe to inventory.reserved not order.created so we only charge after stock is confirmed — calls the gateway, persists the payment record, publishes payment.completed. Shipping-service consumes payment.completed — again, we only ship after payment, not after inventory reservation — creates the shipment with a tracking number, publishes shipping.dispatched. Notification-service consumes events from multiple topics in parallel, sending the customer 'order received,' 'payment confirmed,' and 'shipped' emails. Every event carries a correlationId that's propagated from the first order.created, so I can trace a single customer's flow across all five services with one grep. If payment fails — which I simulated at 5% — payment-service publishes payment.failed, and inventory-service has a separate listener that consumes that event and releases the reservation, returning stock to available. The saga rolls back without any central coordinator."

### Questions on Transactional Outbox pattern 

Q: "How do you make sure you don't lose events when publishing to Kafka?"

"The naive approach saves the entity to the database and then publishes to Kafka as two separate operations — that's a dual-write, and it has a failure window: if the process crashes or Kafka is unreachable after the DB commit but before the publish, you've got an entity with no event, and no way to recover because nothing knows the event is missing. I solved it with the transactional outbox. Instead of publishing directly, I write the event to an outbox table in the same database transaction as the entity — so they commit atomically, no window. A separate scheduled relay polls the outbox for unpublished rows and ships them to Kafka, marking each one published on success. I verified it by killing Kafka entirely, placing orders — which still succeeded — and watching the events sit durably in the outbox, then drain automatically when Kafka came back. Zero loss. Order intake stayed available through a full broker outage."

Q2: "Doesn't the relay risk publishing duplicates?"

"Yes, and that's by design. If the relay sends to Kafka but crashes before marking the row published, next cycle it republishes — a duplicate. The outbox guarantees at-least-once, not exactly-once. I handle the duplicates downstream: every consumer is idempotent, deduping by a business key like orderId, backed by a unique database constraint. At-least-once delivery plus idempotent consumers gives you effectively exactly-once processing. Trying to make the relay itself exactly-once would require distributed transactions across the database and Kafka, which is exactly the complexity the outbox pattern exists to avoid."

Q3: "How would you scale this relay, or reduce its latency?"

"Two axes. For latency: my relay polls every 500ms, so there's up to half a second of publish lag. To cut that, I'd move from polling to Change Data Capture — a tool like Debezium tails the database's write-ahead log and streams outbox inserts in near-real-time, no polling. For horizontal scaling: if I run multiple order-service instances each with a relay, two relays could grab the same outbox rows and double-publish. The fix is SELECT ... FOR UPDATE SKIP LOCKED — each relay locks the rows it claims and others skip them, so the work partitions cleanly across instances. I built the simple polling version because it's correct and easy to reason about; I'd graduate to CDC or SKIP LOCKED when measured latency or throughput demanded it."