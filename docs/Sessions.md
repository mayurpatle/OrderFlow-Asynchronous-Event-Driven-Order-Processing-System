# The 18-Session Plan

Each session is roughly 60–90 minutes of focused work on your end. Each one ends with a checkpoint — something runnable that proves the concept landed.

---

## Phase 1: Foundations (Sessions 1–3)

### Session 1 — Prerequisites & Mental Model ✅

Install JDK 17, Maven, Docker. Set up the project skeleton. Draw the architecture diagram together so you carry the "before/after" picture in your head. No Kafka yet — just the lay of the land.

### Session 2 — Build the Monolith (the "before" picture) ✅

Spring Boot project, Postgres, the Order entity, and `OrderService.placeOrder()` with all 5 sync module calls inside one `@Transactional`. You'll feel why this is a problem. Hit it with a load test at the end and watch it collapse.

### Session 3 — Kafka Internals: Deep Dive ✅

Your bonus session. No code — just Kafka. We'll start a single Kafka broker in Docker, then use the CLI to:

- Create a topic with 3 partitions, see how messages distribute
- Watch what happens when you produce with no key vs with a key
- Start two consumers in the same group, kill one, watch rebalancing
- Read the consumer offsets directly (`__consumer_offsets` topic)
- Replay from the beginning by resetting offset
- See lag accumulate in real time

You'll walk away knowing exactly what partitions, consumer groups, offsets, rebalancing, and ordering guarantees mean — not as words but as things you've poked with a stick.

---

## Phase 2: First Producer & First Consumer (Sessions 4–6)

### Session 4 — Bootstrap the Order Service & Common Library ✅

Multi-module Maven setup. The `EventEnvelope<T>` record (and why it exists). The `Topics` constants class. Your first standalone Spring Boot service that doesn't yet talk to Kafka.

### Session 5 — First Kafka Producer ✅

Configure `KafkaTemplate`. Write `OrderEventPublisher`. Send your first event. Inspect it in Kafka UI. We unpack `acks=all`, `enable.idempotence`, partition keys, and why `orderId` is the right key here.

### Session 6 — First Kafka Consumer

Build the `notification-service` end-to-end. `@KafkaListener`. Consumer group ID. Deserialization. Watch the event flow from producer → broker → consumer in real time.

---

## Phase 3: The Domain (Sessions 7–11)

### Session 7 — Inventory Service: Two-Path Logic ✅

Producer + consumer in the same service. Reserve stock or fail. This is where "events trigger more events" clicks.

> At this point, the inventory service works as both a producer and consumer. If the POST API is called from the orders API, then according to item availability the event is created for the reservation topic or reservation-failed topic only — the event is made and passed to the specific topic.

**Known issue:** If the current items are not available (i.e. not reserved), then the notification service should not send the notification email.

### Session 8 — Payment Service & The Saga Begins

Simulated gateway with a 5% failure rate. Now you have a real failure path to compensate for.

### Session 9 — Saga Compensation: Distributed Transactions Without 2PC

The crucial session. When payment fails, who releases the inventory? You'll wire up the compensating consumer in `inventory-service`. Diagram the choreography on paper.

### Session 10 — Shipping Service

Quick session — by now you know the pattern. Cements it.

### Session 11 — Analytics Service: Fan-out

Consuming 7 topics from one service. The "easiest to extract first" service of the strangler-fig migration.

---

## Phase 4: Production Concerns (Sessions 12–15)

### Session 12 — Idempotency in Depth

Events get redelivered. Why? Walk through the exact scenarios. Build the `processed_events` table and `IdempotencyService`. Test it by killing a consumer mid-process.

### Session 13 — Avro Schemas & Schema Registry

Replace JSON with Avro. Register schemas. Try a breaking change, watch Schema Registry reject it. Try a backward-compatible change, watch it succeed.

### Session 14 — Order State Machine: Closing the Loop

Order service consumes its own downstream events. State transitions. Now the order's status reflects reality across services without any sync calls.

### Session 15 — Error Handling, DLQs & Retries

What happens when a consumer throws? Retries, backoff, dead letter queues. Spring Kafka's `DefaultErrorHandler`.

---

## Phase 5: Proof (Sessions 16–18)

### Session 16 — Load Testing Both Architectures

k6 scripts. Run monolith, watch p99 climb. Run microservices, watch it stay flat. Capture the numbers — these are the ones you'll quote in interviews.

### Session 17 — Migration Strategy: The Strangler Fig

We discuss (and document as ADRs) the order in which you'd migrate the 6 modules in real life, why analytics goes first and order-service goes last, and what "dual writing" looks like.

### Session 18 — Interview Prep & The Story

We rehearse the bullet point. I throw FAANG-level follow-up questions at you. You defend every claim with code and numbers from your own project.
