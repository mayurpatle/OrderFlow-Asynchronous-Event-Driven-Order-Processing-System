# OrderFlow — Design Document

**An event-driven e-commerce order pipeline: monolith → six Kafka-choreographed microservices.**

> *A hand-built study of what actually breaks when you split a synchronous order flow into services that talk only through events — and the reliability patterns that put it back together.*

**Status:** Sessions 1–13 complete (monolith baseline → outbox → oversell hardening). Avro, order state machine, DLQ/retries, and k6 load comparison pending.
**Last updated:** after Phase 1 · Session 13.

---

1. [Problem & scope](#1-problem--scope)
2. [Requirements](#2-requirements)
3. [Capacity estimation](#3-capacity-estimation)
4. [Architecture](#4-architecture)
5. [Decision log](#5-decision-log) ← *the core of this document*
6. [Data model](#6-data-model)
7. [Failure modes](#7-failure-modes)
8. [Configuration reference](#8-configuration-reference)
9. [Build status](#9-build-status)
10. [Chaos engineering plan](#10-chaos-engineering-plan)
11. [Interview talking points](#11-interview-talking-points)

---

## 1. Problem & scope

### The one-line pitch

Placing an order should take milliseconds and never oversell the last item — even when a thousand people buy it in the same second and the payment gateway is slow. **The hard part isn't placing the order; it's keeping six independent services consistent without a database transaction spanning all of them.**

### The reframe that drives everything

The naive model is one method that does everything, in order:

- save the order, reserve stock, charge the card, schedule shipping, email the customer, record analytics — five synchronous calls inside one `@Transactional` method;
- the customer's HTTP thread is held for the sum of all five latencies (worst case ~3 seconds);
- if the email server is slow, *order placement* is slow; if analytics throws, the order rolls back;
- every module redeploys together, scales together, fails together.

So this is **not a faster monolith.** It's **a decoupling problem**:

- the order must be *accepted* in milliseconds and its consequences unfold asynchronously;
- each service owns its own data and reacts to events, never calls another service directly;
- correctness (no oversell, no lost events, no money charged for stock that doesn't exist) has to hold *without* a distributed transaction.

> **The one idea:** *The choice of which topics a service subscribes to IS the workflow. There is no orchestrator — the business process is an emergent property of event subscriptions.*

### Out of scope (deliberately)

- Real payment gateway — simulated with latency + a 5% failure rate.
- Authentication, authorization, multi-tenancy.
- A product catalog / real SKU provisioning — SKUs self-seed at 1000 units for the demo.
- Horizontal scale-out (multi-broker Kafka, Postgres sharding, Redis reservation layer) — discussed as the scaling path, not built.
- A UI — the system is exercised via REST + Kafka UI + psql.

---

## 2. Requirements

### Functional

| # | Requirement | Phase |
|---|---|---|
| F1 | Accept an order and return immediately without waiting on downstream effects | 1 (S5) |
| F2 | Reserve inventory per order; reject when stock is insufficient | 1 (S7) |
| F3 | Charge payment only after stock is confirmed | 1 (S8) |
| F4 | On payment failure, release the reserved stock (compensation) | 1 (S9) |
| F5 | Dispatch a shipment once payment completes | 1 (S10) |
| F6 | Notify the customer at each lifecycle step | 1 (S6/S10) |
| F7 | Aggregate cross-service metrics for observability | 1 (S11) |
| F8 | Never lose an order-created event, even if the broker is down at publish time | 1 (S12) |
| F9 | Never oversell a SKU under concurrent orders | 1 (S13) |
| F10 | Advance order status from downstream events (PENDING→…→DISPATCHED/CANCELLED) | 2 (S15) |

### Non-functional

| # | Requirement | Consequence |
|---|---|---|
| N1 | Order intake p99 ≪ downstream latency | Intake does one DB write + hands off; no synchronous downstream calls |
| N2 | No oversell under concurrency | Reservation must be a single atomic DB operation, not read-modify-write |
| N3 | At-least-once delivery with no duplicate side effects | Every consumer idempotent; dedup on `eventId` |
| N4 | No dual-write gap between DB and broker | Transactional outbox: DB write + event enqueue in one local transaction |
| N5 | A slow/failing service must not stall others | Temporal decoupling via Kafka; per-service consumer groups |
| N6 | Every order's journey must be traceable across services | `correlationId` propagated on every event |
| N7 | Services independently deployable | Database-per-service; no shared tables |

> **N2 is the keystone requirement.** "Never oversell" is what forces the atomic conditional UPDATE over the obvious read-modify-write, which in turn exposes the transaction-vs-lock distinction and the rollback-only redelivery trap. It is the answer to "why not just wrap it in `@Transactional`?" — because a transaction is not a lock. Every hard decision in §5.4 traces back to N2.

### SLOs

| Metric | Target |
|---|---|
| Order intake response | **202 in < 50 ms** (local, p99) |
| Oversell under load | **exactly 0** |
| Event loss on broker outage | **0** |
| Reserved quantity vs ceiling | **never exceeds stock** |
| Idempotency | **duplicate delivery → no duplicate effect** |

---

## 3. Capacity estimation

**Assumptions:** single-node local dev — 1 Kafka broker (KRaft), 1 Postgres, 6 service JVMs, 3 partitions per topic, listener concurrency 3.

```
Load test run        2000 orders   fired concurrently (50 in flight)   =  2000 order.created
Stock ceiling        1 SKU         × 1000 units                        =  1000 reservable
Expected outcome     1000 reserved + ~1000 rejected                    =  2000 resolved
Payment failure      ~5% of reserved                                   ≈  50 compensations
Partitions           3 per topic   × concurrency 3                     =  3-way parallel consume
```

| Metric | Figure | What it forces |
|---|---|---|
| Concurrent orders on one hot SKU | 2000 | **Atomic single-row UPDATE** — any read-modify-write loses updates |
| Reserved must never exceed | 1000 | **DB-enforced ceiling**, not application check |
| Partitions per topic | 3 | **orderId as partition key** so one order's events stay ordered |
| Payment latency (simulated) | 200–800 ms | **Async** — this latency must never reach the intake thread |

> ⭐ **The key asymmetry:** intake is cheap and must stay cheap; the expensive, failure-prone work (payment, the hot-row reservation) is exactly what must be moved off the request path and made individually safe.

**Scoping to reality:** this runs on one laptop. 2000 concurrent orders against one SKU is enough to *reproduce* the lost-update race 100% of the time and prove the fix — which is the point. The design holds at higher scale (the atomic UPDATE is lock-free on the hot row), but the *next* bottleneck at real flash-sale volume is the single Postgres row itself, which is where a Redis reservation layer would come in (P-02). Nothing here claims measured throughput numbers; the only hard numbers are the oversell counts, which are real.

---

## 4. Architecture

```
                          POST /api/orders (8071)
                                   │
                                   ▼
                        ┌────────────────────────┐
                        │     order-service      │  save order + outbox row
                        │   (8071, db: orders)   │  (one local transaction)
                        │                        │
                        │  OutboxRelay @Scheduled │──────► order.created
                        └───────────┬────────────┘            │
                                    ▲                          │
             consumes downstream    │                          │
             events to advance      │        ┌─────────────────┼───────────────┐
             order status           │        ▼                 ▼               ▼
                                    │  ┌────────────┐   ┌────────────┐  ┌──────────────┐
                                    │  │ inventory  │   │ analytics  │  │ notification │
                                    │  │ (8072)     │   │ (8076)     │  │ (8075)       │
                                    │  │ db:inventory│  │ in-memory  │  │ no db        │
                                    │  └─────┬──────┘   └────────────┘  └──────────────┘
                                    │        │ inventory.reserved /
                                    │        │ inventory.reservation_failed
                                    │        ▼
                                    │  ┌────────────┐
                                    │  │ payment    │  charge AFTER stock confirmed
                                    │  │ (8073)     │
                                    │  │ db:payments│
                                    │  └─────┬──────┘
                                    │        │ payment.completed / payment.failed
                                    │        ▼
                                    │  ┌────────────┐
                                    └──│ shipping   │── shipping.dispatched ─► (notif, analytics)
                                       │ (8074)     │
                                       │ db:shipping│
                                       └────────────┘

   Infra: Kafka KRaft (29092 host listener), Postgres (5432), Kafka UI (8090)
```

**Happy path:** `order.created → inventory.reserved → payment.completed → shipping.dispatched`, with order-service consuming each back to move status `PENDING → AWAITING_PAYMENT → PAID → DISPATCHED`.

**Failure paths:** `inventory.reservation_failed → order CANCELLED`; or `payment.failed → inventory releases reservation + order CANCELLED`.

| Service | Port | Store | Responsibility |
|---|---|---|---|
| order-service | 8071 | Postgres `orders` | Intake, outbox, order state machine |
| inventory-service | 8072 | Postgres `inventory` | Atomic reservation + saga compensation |
| payment-service | 8073 | Postgres `payments` | Charge simulation (after stock) |
| shipping-service | 8074 | Postgres `shipping` | Dispatch |
| notification-service | 8075 | none | Email sink (fan-in) |
| analytics-service | 8076 | in-memory | Metrics fan-in (`GET /metrics`) |

**Data ownership is the seam.** Each service's database is private; the only thing crossing a service boundary is an event on a topic. That single rule is what makes the services independently deployable, enables polyglot persistence later, and forces every consistency problem to be solved with events instead of a shared transaction.

> 🔸 **Gray zone:** the transactional outbox (D-09) is built only on order-service. The other producers (inventory, payment, shipping) still publish directly inside their consumer's `@Transactional` method, which has the same dual-write gap D-09 closes. Current leaning: the pattern is proven; rolling it out to the rest is mechanical and deferred, and this is stated honestly rather than implied to be everywhere.

---

## 5. Decision log

> Format: **what we chose · over what · because · and what we gave up.**
> The tradeoff line is the important one — a decision without a stated cost is a decision you haven't finished making.

### 5.1 Platform & transport

**D-01 · Apache Kafka (KRaft mode) over RabbitMQ or async REST**
- **Because:** the log is replayable — analytics (and any future consumer) can rebuild state from history; partition-per-key gives **per-order ordering for free**; fan-out to N consumers needs zero producer changes.
- **Over:** RabbitMQ (no replay, queue semantics), async REST with `@Async` (no durability across restart, no fan-out, N×N coupling).
- **Tradeoff:** ⚠️ operational weight — a broker to run, consumer lag to watch, and eventual consistency now visible to the user.

**D-02 · orderId as the partition key on every event** ⭐
- **Because:** Kafka guarantees ordering *within a partition*, and partition = hash(key). Keying by orderId means **every event for one order lands on one partition and is processed in order** by every consumer — no out-of-order reservation-after-cancellation.
- **Over:** random/round-robin keys (better balance, no ordering) or no key.
- **Tradeoff:** ⚠️ a hot order (or hot SKU mapped through orderId) can skew one partition; acceptable because order events are short-lived.

### 5.2 Serialization

**D-03 · Hand-rolled `EventEnvelope<T>` wrapping every payload**
- **Because:** separates metadata (eventId, correlationId, causationId, occurredAt, version) from the business payload, so **every consumer reads the same metadata shape regardless of event type** — dedup and tracing are uniform.
- **Over:** publishing raw payload objects (no dedup key, no trace id, no version).
- **Tradeoff:** ⚠️ it's a bespoke envelope, not a schema-registry contract — to be replaced by Avro in Phase 2 (P-01).

**D-04 · JSON with `JsonSerializer.setAddTypeInfo(false)` over type-header-driven deserialization** 🔥
- **Because:** the consumer must know its type from the *topic it subscribes to*, not a `__TypeId__` header the producer sets. This **decouples producer and consumer class names** — the producer could be in Python.
- **What we hit:** 🔥 the consumer side originally relied on auto-config and silently fell back to `StringDeserializer`, producing `Cannot convert from [String] to [EventEnvelope]`. Root cause: the `consumerFactory` bean wasn't being picked up, so Spring used its default.
- **Fix:** construct `new JsonDeserializer<>(EventEnvelope.class, mapper, false)` explicitly with trusted packages, and name the listener factory `orderflowKafkaListenerFactory` so every `@KafkaListener` references it.
- **Tradeoff:** ⚠️ every listener must remember `containerFactory = "orderflowKafkaListenerFactory"`; forget it and you get the default factory again.

### 5.3 Reliability — the outbox

**D-09 · Transactional outbox on order-service over direct publish** ⭐
- **Context:** `placeOrder()` originally saved the order and then called `kafkaTemplate.send()`. Two systems, no shared transaction — a crash between them loses the event (or publishes one for an order that rolled back). The classic dual-write gap.
- **Because:** writing the order row **and** an `outbox_event` row in **one local Postgres transaction** makes the event as durable as the order. A `@Scheduled` `OutboxRelay` polls unpublished rows, sends them, and stamps `published_at`. **The broker being down no longer blocks or loses intake.**
- **Over:** direct publish (dual-write gap); full CDC/Debezium (correct, heavier infra than a learning project warrants).
- **What we proved:** 🔥 stopped Kafka, placed 3 orders (all returned 202), saw 3 rows with `published_at IS NULL`, restarted Kafka, watched the relay drain 3 → 0 automatically. Zero loss.
- **Tradeoff:** ⚠️ polling adds latency (fixed-delay 500 ms) and the relay is single-instance; at multi-instance you'd need `SELECT … FOR UPDATE SKIP LOCKED` so two relays don't double-send.
- **Revisit when:** rolling the pattern to the other producers, or moving to CDC.

**D-10 · Dedicated `stringKafkaTemplate` (StringSerializer) for the relay**
- **Because:** the outbox stores the envelope as already-serialized JSON `TEXT`. Re-serializing it through the normal `JsonSerializer` would double-encode it. A String-valued template sends the stored JSON **verbatim**.
- **Over:** reusing the object `KafkaTemplate` (double-encoding) or deserializing-then-reserializing (wasteful, lossy).
- **Tradeoff:** ⚠️ two KafkaTemplate beans now exist; the relay must use the right one.

### 5.4 Concurrency & correctness — the oversell fix

**D-11 · Atomic conditional UPDATE over read-modify-write for reservation** ⭐🔥
- **Context:** reservation was `findById(sku) → check available ≥ qty → setAvailable(−qty) → save`. Under listener concurrency 3, two threads read the same `available`, both pass the check, both write — a lost update.
- **What we hit:** 🔥 a 2000-concurrent-order load test against 1000 units produced **100% oversell** — ~1908 reservations, 0 rejections, `reserved_quantity` corrupted to 637.
- **Because:** a single statement — `UPDATE stock_items SET available = available − :qty, reserved = reserved + :qty WHERE sku = :sku AND available >= :qty` — evaluates the check and applies the decrement **atomically**; the DB serializes concurrent row updates. Rows-affected = 1 → reserved, 0 → reject. **Overselling becomes impossible regardless of thread count.**
- **Over:** pessimistic `SELECT … FOR UPDATE` (serializes the hot SKU — throughput bottleneck under 50-way contention); optimistic `@Version` (retry storm on one hot row).

  | Strategy | Hot-row behaviour | Chosen? |
  |---|---|---|
  | Atomic conditional UPDATE | lock-free, DB serializes | ✅ |
  | Pessimistic `FOR UPDATE` | blocks, serializes | ✗ bottleneck |
  | Optimistic `@Version` | retry storm | ✗ |

- **Insight:** ⭐ **`@Transactional` is not a lock.** At READ_COMMITTED, a transaction prevents reading *uncommitted* data; it does not stop two transactions from both reading a value and both updating it.
- **Tradeoff:** ⚠️ the logic must fit a single conditional SQL statement; anything needing multiple dependent reads can't use this and would need a lock.

**D-12 · `seedIfAbsent` via `INSERT … ON CONFLICT (sku) DO NOTHING`**
- **Because:** an `UPDATE` only touches existing rows — a never-seeded SKU returns 0 rows-affected, indistinguishable from "out of stock." A concurrency-safe upsert guarantees the row exists before `tryReserve`.
- **Over:** `findById().orElseGet(build)` (the old read path — not safe under concurrent seed) or seeding at startup (demo orders any SKU).
- **Tradeoff:** ⚠️ Postgres-specific syntax (native query); not portable. In production SKUs come from a catalog service, not auto-seed.

**D-13 · Return a `ReserveResult` object over throwing `InsufficientStockException`** ⭐🔥
- **Context:** the first version of D-11 *threw* to signal out-of-stock, to roll the reservation back.
- **What we hit:** 🔥 the thrown exception crossed the `@Transactional` boundary and marked the transaction **rollback-only**; the Kafka listener then failed its offset commit and Spring did "seek to current after exception" → **redelivery storm**. Rejections inflated to 2128 for ~1000 rejected orders, with `UnexpectedRollbackException` in the logs.
- **Because:** out-of-stock is an **expected business outcome, not an error.** `reserve()` now returns `ReserveResult(success, reservation, failedSku)` and always returns normally; partial multi-item reservations are unwound manually with `restoreStock`. No exception crosses the boundary → clean offset commit → no redelivery.
- **Over:** exception-as-control-flow (the bug), or catching inside the same transaction (fights Spring's rollback-only machinery).
- **Insight:** ⭐ **Don't use exceptions to model expected outcomes — an out-of-stock order is a normal result.** Verified clean: exactly 1000 reserved, `reservation_failed ≈ 995`, no ERROR logs.
- **Tradeoff:** ⚠️ the caller must branch on the result rather than relying on transaction rollback to undo partial work — the unwind is now explicit code.

**D-14 · Extract `StockReservationService` as a separate `@Service` bean**
- **Because:** Spring's `@Transactional` proxy is **bypassed on self-invocation** (`this.method()`). Putting the transactional reserve/release logic in its own injected bean makes every call a real proxied call, so the transaction boundary actually exists where intended.
- **Over:** keeping it inline in the consumer (self-invocation trap — `@Transactional` silently ignored).
- **Tradeoff:** ⚠️ one more class and an injection; worth it for correct transaction semantics.

### 5.5 Saga & idempotency

**D-07 · Choreographed saga over an orchestrator**
- **Because:** each service owns its compensating action (inventory releases on `payment.failed`); the workflow emerges from subscriptions, no central coordinator to deploy or bottleneck.
- **Over:** an orchestration engine (central control, central failure point, more infra).
- **Tradeoff:** ⚠️ no single place shows the whole flow; a new failure path means remembering to add compensation to the right service. Easy to miss one.

**D-08 · Idempotent consumers keyed on `eventId`; reservation `status` as the dedup key for compensation**
- **Because:** Kafka is at-least-once. A duplicate `payment.failed` finds the reservation already `RELEASED` and skips. At-least-once + idempotent consumers = **effectively exactly-once** without distributed transactions.
- **Over:** trusting the broker for exactly-once (it can't, across your side effects).
- **Tradeoff:** ⚠️ every consumer carries dedup logic; a missed check means a double side effect.

### 5.6 Persistence

**D-05 · Database-per-service over a shared schema**
- **Because:** private data is what makes services independently deployable and enables **polyglot persistence** later (inventory + Redis, analytics → columnar) without touching other services.
- **Over:** one shared Postgres schema (fast to build, but recreates the monolith's coupling).
- **Tradeoff:** ⚠️ no cross-service JOIN; no atomic "reset the whole system" — which forced the `reset.ps1` tooling (D-06).

**D-06 · Money as `long` cents everywhere**
- **Because:** floating point can't represent currency exactly; integer cents is exact.
- **Over:** `double`/`BigDecimal` (lossy / heavier).
- **Tradeoff:** ⚠️ every boundary must agree on cents; a stray `/100` or float creeps error back in.

### 5.7 Proposed — designed, not yet built

| ID | Decision | Phase |
|---|---|---|
| P-01 | Avro + Confluent Schema Registry, BACKWARD compatibility, replacing the JSON envelope | 2 (S14) |
| P-02 | Redis reservation layer (atomic `DECR`) in front of Postgres for flash-sale hot SKUs | post-S18 |
| P-03 | Dead-letter queue + bounded retries for poison messages | 2 (S16) |
| P-04 | Roll the transactional outbox to inventory/payment/shipping producers | 2 |
| P-05 | Order state machine: consume all downstream events back into order-service, publish `order.cancelled` | 2 (S15) |
| P-06 | k6 load comparison (monolith vs microservices) to produce real latency/throughput numbers | 2 (S16) |

---

## 6. Data model

### Domain records (common library)

```java
EventEnvelope<T>(
    UUID   eventId,        // dedup key — unique per physical event
    String eventType,      // "order.created"
    String eventVersion,   // semver of the payload schema
    Instant occurredAt,    // producer wall-clock
    UUID   correlationId,  // same across one customer flow — the trace id
    UUID   causationId,    // eventId of the event that caused this one (null at root)
    T      payload         // the typed business body
)
```

> `correlationId` is set once at order placement and propagated on every downstream event; `causationId` points to the direct parent. Together they reconstruct the causal chain by grepping one id.

### Tables per store

| Store | Table | Key | Contents |
|---|---|---|---|
| orders | orders | id (UUID) | customerId, totalCents, status, created/updatedAt |
| orders | outbox_event | id | aggregateId(=orderId, Kafka key), eventType, topic, payload (TEXT JSON), createdAt, **publishedAt (nullable = state)** |
| inventory | stock_items | sku | availableQuantity, reservedQuantity |
| inventory | reservations | id (UUID) | orderId, status (ACTIVE/RELEASED/COMMITTED), createdAt |
| inventory | reservation_items | id (UUID) | reservationId, sku, quantity |
| payments | payments | id (UUID) | orderId (unique), amountCents, status, gatewayReference, createdAt |
| shipping | shipments | id (UUID) | orderId, carrier, trackingNumber, status, created/estimatedDeliveryAt |

> `outbox_event.publishedAt IS NULL` is the entire relay state machine — unpublished vs relayed. No separate status column needed.

### Topics

| Topic | Key | Producer | Consumers | Phase |
|---|---|---|---|---|
| order.created | orderId | order | inventory, payment, analytics, notification | 1 |
| order.cancelled | orderId | order | inventory, payment, notification, analytics | 2 |
| inventory.reserved | orderId | inventory | order, payment, analytics | 1 |
| inventory.reservation_failed | orderId | inventory | order, notification, analytics | 1 |
| payment.completed | orderId | payment | shipping, notification, analytics | 1 |
| payment.failed | orderId | payment | order, inventory, notification, analytics | 1 |
| shipping.dispatched | orderId | shipping | notification, analytics | 1 |

### API contract

| Method | Path | Status | Service |
|---|---|---|---|
| POST | /api/orders | 202 Accepted | order (8071) |
| GET | /api/orders/{id} | 200 / 404 | order (8071) |
| GET | /metrics | 200 | analytics (8076) |

---

## 7. Failure modes

| Failure | Behaviour | Recovery | Data loss? |
|---|---|---|---|
| **Kafka down at publish** 🔥 | Intake still returns 202; order + outbox row committed, `publishedAt` stays NULL | Relay drains backlog on broker return | No — outbox guarantees it (D-09) |
| **Topic missing** 🔥 | Producer logs `UNKNOWN_TOPIC_OR_PARTITION`, retries | Create topics (`reset.ps1`) | No — producer keeps retrying |
| **Concurrent oversell** 🔥 | Atomic UPDATE rejects excess; rows-affected 0 → reservation_failed | None needed — prevented by design (D-11) | No |
| **Payment fails after reserve** | `payment.failed` → inventory releases stock, order CANCELLED | Automatic saga compensation (D-07) | No |
| **Duplicate event delivery** | Consumer sees already-processed state, skips | Idempotency on eventId / reservation status (D-08) | No |
| **Postgres volume wiped on unclean shutdown** 🔥 | Service fails to start (`Could not obtain connection`) or DBs missing | `postgres-init.sql` on fresh init; else recreate DBs manually | ⚠️ Yes — committed data in the wiped volume is gone; code/schema survive in git |
| **Relay double-send (multi-instance)** | Two relays could publish the same outbox row | ⚠️ **Not yet handled** — needs `SELECT … FOR UPDATE SKIP LOCKED` (D-09 revisit) | No — consumers idempotent, but wasteful |
| **Poison message (unparseable)** | Listener throws, redelivers indefinitely | ⚠️ **Not yet handled — Phase 2 (DLQ, P-03)** | No — but blocks the partition |

---

## 8. Configuration reference

### 🔬 Provenance — measured vs assumed

> ⭐ **When you write a number in a config, know whether it's measured, derived, defaulted, or guessed — and say which.** Most tuning-related incidents come from treating a guess as a measurement.

| Value | Source |
|---|---|
| Oversell = 0, reserved = 1000 under 2000 orders | **Measured** — load test, Session 13 |
| `reservation_failed ≈ 995` after the fix | **Measured** — same run |
| Outbox 3 → 0 drain on Kafka restart | **Measured** — Session 12 demo |
| Listener concurrency = 3 | **Chosen** — matches 3 partitions per topic |
| Partitions = 3 | **Chosen** — small-scale default for parallel consume |
| Producer `acks=all`, idempotence, retries=MAX, in-flight≤5 | **Industry guideline** — durability gold standard |
| Relay `fixedDelay = 500 ms` | ⚠️ **GUESS** — not tuned against any latency target |
| SKU seed = 1000 units | ⚠️ **Dev only** — real SKUs come from a catalog |
| Payment failure 5%, latency 200–800 ms | **Chosen** — simulated to exercise compensation |
| Intake p99 < 50 ms | ⚠️ **ESTIMATE** — not yet measured with k6 (P-06) |

### Port plan (807x — revised from 808x; Docker Desktop held 8081)

| Service | Port |
|---|---|
| monolith | 8070 |
| order-service | 8071 |
| inventory-service | 8072 |
| payment-service | 8073 |
| shipping-service | 8074 |
| notification-service | 8075 |
| analytics-service | 8076 |
| Kafka UI | 8090 |
| Postgres | 5432 |
| Kafka host listener | 29092 |

> 🔹 Services talk only over Kafka, so renumbering every HTTP port touched zero integration code — only `server.port` and the two REST entry points. That decoupling is itself the proof the architecture works.

### Version notes & traps

| Item | Note |
|---|---|
| **`@Transactional` import** 🐛 | Must be `org.springframework.transaction.annotation.Transactional`, **not** `jakarta.transaction.Transactional`. Wrong one silently changes rollback semantics. |
| **Postgres driver** 🐛 | `org.postgresql.Driver`, not `org.postgres.Driver` — the typo cost a debug session. |
| **Kafka bootstrap** ⚠️ | Apps connect on **29092** (host listener), not 9092 (internal). |
| **Docker volumes** 🔥 | Unclean Windows/Docker Desktop shutdown reinitializes the Postgres/Kafka volume. Confirm `postgres-data` and `kafka-data` are **named** volumes, not anonymous — anonymous volumes are discarded on hard restart. This is the root of both the DB-missing and topic-missing incidents. |
| **Self-invocation** 🐛 | `@Transactional` on a method called via `this.` is ignored — extract to a separate bean (D-14). |
| **Package casing** 🔹 | inventory uses `Entity`/`Repositories` (capitalized, plural) — non-conventional but intentional in this codebase. |

---

## 9. Build status

| Phase | Status | Delivered |
|---|---|---|
| Monolith baseline (S1–S2) | ✅ | Synchronous "before" app, 5 inline calls |
| Kafka foundations (S3–S6) | ✅ | Multi-module Maven, first producer/consumer, envelope, rebalancing |
| Core services (S7–S11) | ✅ | Inventory, payment, saga compensation, shipping, analytics |
| Outbox (S12) | ✅ | Transactional outbox on order-service, zero-loss verified |
| Concurrency (S13) | ✅ | Atomic reservation, result-object fix, oversell = 0 verified |
| Avro + Schema Registry (S14) | ⬜ | P-01 |
| Order state machine (S15) | ⬜ | P-05 |
| DLQ/retries, k6 compare (S16+) | ⬜ | P-03, P-06 |

### Current pipeline

```
POST /api/orders → order saved + outbox row (1 txn) → relay publishes order.created
  → inventory atomic-reserves → inventory.reserved
    → payment charges → payment.completed
      → shipping dispatches → shipping.dispatched
  (failure: reservation_failed OR payment.failed → saga compensation + CANCELLED)
analytics consumes all; notification emails on each step
```

### Known gaps

- ⚠️ Outbox only on order-service; other producers keep the dual-write gap (P-04).
- ⚠️ No DLQ — a poison message blocks its partition (P-03, Phase 2).
- ⚠️ Order status not yet driven fully by downstream events (P-05, Phase 2).
- ⚠️ Intake latency never measured under load — no k6 numbers yet (P-06).
- ⚠️ Payment charges a flat amount — `inventory.reserved` doesn't carry the order total (known simplification).
- 🗑️ `OrderEventPublisher` is dead code since the outbox replaced it on the placeOrder path — delete once confirmed unused.

---

## 10. Chaos engineering plan

> To be executed after Phase 2, under k6 load. **The premise:** every row in §7 is a *claim*. Chaos engineering is how you find out which ones are true.

### Method

Hypothesis first, name the blast radius, run under sustained load, record actual behaviour against §7. ⭐ **An experiment that confirms what you already believed teaches you nothing — the value is in the surprises.**

### Planned experiments

**C-01 · Kill the broker mid-load** ✅ Observed accidentally
*Hypothesis (D-09):* intake keeps returning 202; outbox backlog grows; relay drains on recovery with zero loss.
*status:* Done ✅ — ran deliberately in S12 (3 → 0 drain). Repeat under continuous load.

**C-02 · Oversell under concurrency** ⭐ ✅
*Hypothesis (D-11):* 2000 orders / 1000 units → exactly 1000 reserved, 0 oversell.
*Control case:* the pre-fix read-modify-write, which oversold 100%.
*status:* Done ✅ — the before/after is the headline result.

**C-03 · Kill payment-service after some reservations**
*Hypothesis (D-07):* reserved-but-unpaid orders stay ACTIVE; on restart payment resumes from committed offset; no stock leaked.
*Watch:* ACTIVE reservations with no matching payment during the outage.

**C-04 · Inject a poison message**
*Hypothesis (§7):* without a DLQ (P-03), the partition stalls on infinite redelivery.
*Watch:* consumer lag on that partition climbing while others flow.
*status:* expected to fail — that's the point; it justifies P-03.

**C-05 · Duplicate delivery storm**
*Hypothesis (D-08):* replaying the same events produces no duplicate reservations, shipments, or revenue.

### Tooling

k6 for load; `docker stop`/`kafka-topics` for fault injection; psql + `/metrics` for invariant checks.

---

## 11.Points worth talking

The soundbites worth having ready:

1. **The one idea:** *"There's no orchestrator. The business workflow is literally which topics each service subscribes to — choosing subscriptions IS designing the process. That's what 'choreographed saga' means in practice."*

2. **Transaction is not a lock (N2/D-11):** *"I load-tested inventory with 2000 concurrent orders against 1000 units and got 100% oversell. The trap is that it was already inside a `@Transactional` method — but at READ_COMMITTED a transaction stops you reading uncommitted data, it doesn't stop two threads both reading a value and both writing. The fix is a single atomic `UPDATE … WHERE available >= qty`; rows-affected tells you if it worked. Lock-free, and overselling becomes impossible."*

3. **Exceptions aren't control flow (D-13):** *"My first fix threw an exception to signal out-of-stock. That crossed the transaction boundary, marked it rollback-only, and made Kafka redeliver — my rejection count inflated to 2128 for 1000 rejected orders. Out-of-stock is an expected outcome, not an error, so I returned a result object instead. Cleaner semantics and the redelivery storm vanished."*

4. **The dual-write gap (D-09):** *"Saving to the DB and publishing to Kafka are two systems with no shared transaction — crash in between and you lose the event. The transactional outbox writes the order and the event row in one local transaction, and a relay publishes asynchronously. I proved it by killing Kafka, placing orders, and watching the relay drain the backlog on restart. Zero loss."*

5. **Effectively-once (D-08):** *"Kafka is at-least-once; you get exactly-once end-to-end by making consumers idempotent. My dedup key is the eventId, and for compensation the reservation's own status is the key — a duplicate payment-failed just finds it already released and skips."*

6. **Why not just Postgres oversell to scale (P-02):** *"The atomic UPDATE is correct and lock-free, but at real flash-sale volume the single hot row becomes the bottleneck. The next step is a Redis reservation counter with atomic DECR in front of Postgres as the durable record — which my database-per-service design lets me add to inventory without touching any other service."*

7. **Non-technical version:** *"Imagine a shop where the order counter just takes your order and hands slips to the stockroom, the till, and the packing desk — instead of one clerk walking the whole order through every step while you wait. Everyone works in parallel, and if the packing desk is slow, your order still gets taken instantly. My job was making sure that even when a thousand people grab the last item at once, exactly one of them gets it."*