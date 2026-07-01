# OrderFlow — Official Engineering Documentation

> An event-driven e-commerce platform demonstrating a monolith-to-microservices
> migration using **Java 17**, **Spring Boot 3.2**, **Apache Kafka (KRaft)**, and
> **PostgreSQL**. This document covers the system as built through Session 13:
> architecture, services, event contracts, REST APIs, and the engineering
> concepts introduced to mitigate each class of failure encountered along the way.

**Status:** Sessions 1–13 complete (monolith baseline through concurrency/oversell hardening)
**Audience:** Engineers onboarding to the codebase; interviewers reviewing the design
**Last updated:** Session 13 (concurrency & oversell prevention)

---

## Table of contents

1. [System overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [Services reference](#3-services-reference)
4. [Event catalog (Kafka topics)](#4-event-catalog-kafka-topics)
5. [The event envelope & distributed tracing](#5-the-event-envelope--distributed-tracing)
6. [REST API reference](#6-rest-api-reference)
7. [Data model](#7-data-model)
8. [Saga & compensation flows](#8-saga--compensation-flows)
9. [Reliability patterns](#9-reliability-patterns)
10. [Concurrency & correctness](#10-concurrency--correctness)
11. [Session-by-session concept log](#11-session-by-session-concept-log)
12. [Error-mitigation playbook](#12-error-mitigation-playbook)
13. [System invariants](#13-system-invariants)
14. [Local development & operations](#14-local-development--operations)

---

## 1. System overview

OrderFlow models the lifecycle of an e-commerce order — placement, inventory
reservation, payment, shipping, customer notification, and analytics — first as
a synchronous monolith (the "before"), then as six independently deployable,
event-driven microservices (the "after").

The migration goal is to replace a chain of in-process synchronous calls with
asynchronous Kafka events, so that:

- the customer's order-placement request returns in milliseconds rather than
  waiting on the slowest downstream dependency;
- a slow or failing downstream service (e.g. email/SMTP) cannot take down the
  order-intake path;
- each service deploys, scales, and fails independently.

The platform is a learning/portfolio system run locally on Docker Desktop. It is
deliberately built by hand, session by session, so that every design decision is
understood and defensible rather than generated.

### The "before": synchronous monolith

The monolith's `placeOrder()` performs five sequential blocking calls inside one
`@Transactional` boundary (reserve inventory → charge payment → schedule shipping
→ send confirmation → record analytics). Worst-case latency stacks to several
seconds while holding a database connection and an HTTP thread, and any single
failure rolls the whole order back — including the confirmation email.

### The "after": event-driven microservices

Order placement persists the order and publishes a single `order.created` event,
returning `202 Accepted` in milliseconds. Inventory, payment, shipping,
notification, and analytics consume events independently and emit their own
events in turn. Order state is then advanced by consuming those downstream events
back into the order-service.

---

## 2. Architecture

### Component diagram (textual)

```
                        ┌──────────────────────┐
   POST /api/orders ───►│    order-service     │  persists order,
                        │      (port 8081)     │  writes outbox row,
                        │       db: orders     │  relay publishes event
                        └──────────┬───────────┘
                                   │ order.created
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                     ▼
     ┌─────────────────┐  ┌─────────────────┐   ┌──────────────────┐
     │ inventory-svc   │  │  payment-svc    │   │  analytics-svc   │
     │  (8082)         │  │   (8083)        │   │   (8086, no db)  │
     │  db: inventory  │  │  db: payments   │   │  in-memory       │
     └───────┬─────────┘  └───────┬─────────┘   └──────────────────┘
             │                    │
    inventory.reserved /   payment.completed /
    inventory.reservation_ payment.failed
    failed                       │
             │                   ▼
             │           ┌─────────────────┐
             │           │  shipping-svc   │
             │           │   (8084)        │
             │           │  db: shipping   │
             │           └───────┬─────────┘
             │                   │ shipping.dispatched
             ▼                   ▼
        ┌──────────────────────────────┐
        │     notification-service     │  fan-in sink:
        │        (8085, no db)         │  emails on order.created,
        │                              │  payment.completed,
        │                              │  shipping.dispatched,
        │                              │  order.cancelled
        └──────────────────────────────┘
```

### Key architectural principles

- **Database per service.** Each stateful service owns a private PostgreSQL
  database (`orders`, `inventory`, `payments`, `shipping`). No service reads
  another's tables; integration happens exclusively through events. This is what
  later makes polyglot persistence possible (a service can swap its own store
  without touching any other).
- **Choreographed saga.** There is no central orchestrator. Each service knows
  which events it consumes and which it emits; the workflow emerges from those
  subscriptions. *"The choice of subscription topics is the workflow design."*
- **Event-carried state transfer.** Events carry enough payload for consumers to
  act without calling back to the source.
- **Partition key = `orderId`.** Every event is keyed by `orderId`, so all events
  for one order land on the same partition and are processed in order.
- **At-least-once delivery + idempotent consumers.** Kafka guarantees
  at-least-once; consumers deduplicate, yielding effectively-once processing.

---

## 3. Services reference

| Service | Port | Database | Role | Produces | Consumes |
|---|---|---|---|---|---|
| order-service | 8081 | `orders` | Order intake + state machine | `order.created`, `order.cancelled` | `inventory.reserved`, `inventory.reservation_failed`, `payment.completed`, `payment.failed` |
| inventory-service | 8082 | `inventory` | Stock reservation + compensation | `inventory.reserved`, `inventory.reservation_failed` | `order.created`, `payment.failed` |
| payment-service | 8083 | `payments` | Charge simulation (gateway) | `payment.completed`, `payment.failed` | `inventory.reserved` |
| shipping-service | 8084 | `shipping` | Dispatch shipment | `shipping.dispatched` | `payment.completed` |
| notification-service | 8085 | none | Customer notifications (sink) | — | `order.created`, `payment.completed`, `shipping.dispatched`, `order.cancelled` |
| analytics-service | 8086 | none (in-memory) | Metrics fan-in | — | all topics |

> The monolith baseline runs on **port 8080** with database `monolith` for
> before/after comparison.

### Notes on individual services

**order-service** is the entry point and the only service exposing a customer
REST API. It uses the **transactional outbox pattern** (Session 12): order writes
and event publication happen in one local transaction, and a scheduled relay
publishes to Kafka. It also consumes downstream events to advance order status.

**inventory-service** is the most logic-rich service. It reserves stock on
`order.created`, releases it on `payment.failed` (compensation), and is the locus
of the Session 13 concurrency work. The reservation logic lives in a dedicated
`StockReservationService` bean so that `@Transactional` is honored (avoiding the
self-invocation proxy trap).

**payment-service** consumes `inventory.reserved` (not `order.created`) — it only
charges *after* stock is confirmed, so it never charges a customer whose order
can't be fulfilled. It simulates gateway latency (200–800 ms) and a ~5% failure
rate to exercise the compensation path.

**analytics-service** consumes every topic and maintains in-memory counters
(`ConcurrentHashMap` + `AtomicLong`), exposed at `GET /metrics`. It was the
easiest module to extract because nothing depends on it downstream.

---

## 4. Event catalog (Kafka topics)

All topic names are centralized in a `Topics` constants class in the `common`
library — no string literals scattered across services.

| Topic | Producer | Consumers | Partition key |
|---|---|---|---|
| `order.created` | order-service | inventory, payment, analytics, notification | orderId |
| `order.cancelled` | order-service | inventory, payment, notification, analytics | orderId |
| `inventory.reserved` | inventory-service | order, payment, analytics | orderId |
| `inventory.reservation_failed` | inventory-service | order, notification, analytics | orderId |
| `payment.completed` | payment-service | shipping, notification, analytics | orderId |
| `payment.failed` | payment-service | order, inventory, notification, analytics | orderId |
| `shipping.dispatched` | shipping-service | notification, analytics | orderId |
| `shipping.delivered` | shipping-service | notification, analytics | orderId |
| `notification.sent` | notification-service | analytics | orderId |

### Payload shapes

Payloads are Java records in `common/.../events/payloads/`. Money is always
represented as `long` cents (never floating point).

```
OrderCreatedPayload(
    UUID orderId, String customerId,
    List<Item> items, long totalCents, String currency)
  Item(String sku, int quantity, long unitPriceCents)

InventoryReservedPayload(
    UUID orderId, UUID reservationId, String warehouseId,
    List<ReservedItem> reservedItems)
  ReservedItem(String sku, int quantity)

InventoryReservationFailedPayload(
    UUID orderId, String reason, List<String> unavailableSkus)

PaymentCompletedPayload(
    UUID orderId, UUID paymentId, long amountCents,
    String currency, String paymentMethod, String gatewayReference)

PaymentFailedPayload(
    UUID orderId, long amountCents, String failureCode, String failureReason)

ShippingDispatchedPayload(
    UUID orderId, UUID shipmentId, String carrier,
    String trackingNumber, Instant estimatedDeliveryAt)

OrderCancelledPayload(UUID orderId, String reason)
```

> **Note on payment amount:** payment currently charges a flat amount because the
> `inventory.reserved` event does not carry the order total. This is a known,
> documented simplification, not an oversight.

---

## 5. The event envelope & distributed tracing

Every event is wrapped in a common envelope that separates metadata (who, when,
why) from payload (what):

```java
EventEnvelope<T>(
    UUID   eventId,        // unique per physical event — the idempotency dedup key
    String eventType,      // e.g. "order.created"
    String eventVersion,   // semver of the payload schema
    Instant occurredAt,    // wall-clock at the producer
    UUID   correlationId,  // same value across every event in one customer flow
    UUID   causationId,    // the eventId that directly caused this event (or null at root)
    T      payload
)
```

- **`eventId`** is the deduplication key. Idempotent consumers record processed
  eventIds and skip repeats.
- **`correlationId`** is set once at the root (order placement) and propagated
  through every downstream event, so a whole customer journey can be traced by a
  single id.
- **`causationId`** points to the direct parent event, letting you reconstruct
  the exact causal chain (`order.created` → `inventory.reserved` →
  `payment.completed` → `shipping.dispatched`).

### Serialization configuration (important)

The shared `KafkaConfig` configures:

- **Producer:** `acks=all`, `enable.idempotence=true`, `retries=MAX_VALUE`,
  `max.in.flight=5`, `linger.ms=10`, snappy compression, `JsonSerializer` with
  `addTypeInfo(false)`.
- **Consumer:** `auto.offset.reset=earliest`, manual ack (auto-commit off),
  `JsonDeserializer` constructed with the target type `EventEnvelope.class` and
  trusted packages `com.orderflow.*`.
- A named listener container factory (`orderflowKafkaListenerFactory`,
  concurrency = 3) that every `@KafkaListener` references via
  `containerFactory=`.

> The explicit `JsonDeserializer` construction is deliberate — see the
> error-mitigation playbook entry on the silent `StringDeserializer` fallback.

---

## 6. REST API reference

Only **order-service** and **analytics-service** expose HTTP endpoints.

### `POST /api/orders` — place an order

Creates an order and (via the outbox relay) publishes `order.created`. Returns
immediately; downstream processing is asynchronous.

**Request**

```http
POST http://localhost:8081/api/orders
Content-Type: application/json

{
  "customerId": "cust-42",
  "items": [
    { "sku": "SKU-001", "quantity": 2, "unitPriceCents": 2999 }
  ]
}
```

**Response** — `202 Accepted`

```json
{
  "id": "8f3b...-uuid",
  "customerId": "cust-42",
  "totalCents": 5998,
  "status": "PENDING",
  "createdAt": "2026-06-23T00:00:00Z",
  "updatedAt": "2026-06-23T00:00:00Z"
}
```

`202 Accepted` (not `200 OK`) communicates that the order has been accepted for
processing but the downstream effects (reservation, payment, shipping) have not
yet completed. The total is computed server-side from the line items.

### `GET /api/orders/{id}` — fetch order status

**Response** — `200 OK` with the order, or `404 Not Found`.

```json
{
  "id": "8f3b...-uuid",
  "customerId": "cust-42",
  "totalCents": 5998,
  "status": "PAID",
  "createdAt": "2026-06-23T00:00:00Z",
  "updatedAt": "2026-06-23T00:00:03Z"
}
```

The `status` advances asynchronously as downstream events arrive:
`PENDING → AWAITING_PAYMENT → PAID → DISPATCHED → DELIVERED`, or `CANCELLED` on a
failure path.

### `GET /metrics` — analytics snapshot

**Response** — `200 OK`

```json
{
  "totalRevenueCents": 5716094,
  "totalRevenueDollars": 57160.94,
  "ordersCreated": 2000,
  "paymentSuccessRatePercent": 94.83,
  "inventoryRejections": 995,
  "paymentsCompleted": 953,
  "paymentsFailed": 52,
  "eventCounts": {
    "order.created": 2000,
    "inventory.reserved": 1005,
    "shipping.dispatched": 953,
    "payment.completed": 953,
    "payment.failed": 52,
    "inventory.reservation_failed": 995
  }
}
```

Used to verify system invariants after load tests (every order resolves to
exactly one of reserved/rejected; reserved never exceeds stock ceiling; revenue
ties to completed payments).

---

## 7. Data model

Each service owns its schema. Money is stored as `long` cents.

### order-service (`orders` db)

- **orders** — `id (UUID PK)`, `customer_id`, `total_cents`, `status (enum)`,
  `created_at`, `updated_at`.
  - `OrderStatus`: `PENDING, AWAITING_PAYMENT, PAID, DISPATCHED, DELIVERED, CANCELLED, FAILED`.
- **outbox_event** — `id (PK)`, `aggregate_type`, `aggregate_id` (= orderId, used
  as the Kafka key), `event_type`, `topic`, `payload (TEXT, serialized JSON)`,
  `created_at`, `published_at (nullable)`. The nullable `published_at` is the
  state machine: `NULL` = unpublished, timestamp = relayed.

### inventory-service (`inventory` db)

- **stock_items** — `sku (PK)`, `available_quantity`, `reserved_quantity`.
- **reservations** — `id (UUID PK)`, `order_id`, `status (enum)`, `created_at`.
  - `ReservationStatus`: `ACTIVE, RELEASED, COMMITTED`.
- **reservation_items** — `id (UUID PK)`, `reservation_id`, `sku`, `quantity`.
  Added in Session 9 because compensation needs to know exactly what to restore.

### payment-service (`payments` db)

- **payments** — `id (UUID PK)`, `order_id`, `amount_cents`, `status (enum)`,
  `gateway_reference`, `created_at`. Idempotency via unique `order_id` +
  `findByOrderId`. The PENDING intent is persisted *before* the gateway call.

### shipping-service (`shipping` db)

- **shipments** — `id (UUID PK)`, `order_id`, `carrier`, `tracking_number`,
  `status (enum)`, `created_at`, `estimated_delivery_at`.

---

## 8. Saga & compensation flows

OrderFlow uses a **choreographed saga** — no central coordinator. Each service
owns its compensating action.

### Happy path

```
order.created → inventory.reserved → payment.completed → shipping.dispatched
```

Order status advances `PENDING → AWAITING_PAYMENT → PAID → DISPATCHED` as the
order-service consumes each downstream event.

### Failure path: payment fails after reservation

```
order.created → inventory.reserved → payment.failed
                                          │
                ┌─────────────────────────┴───────────────────────┐
                ▼                                                  ▼
   inventory-service consumes payment.failed         order-service consumes
   and RELEASES the reservation (restores stock)      payment.failed and
                                                       CANCELS the order
```

This is the core saga insight: there is no distributed transaction, so a
downstream failure is handled by reacting to a rollback *signal* and undoing
local work.

### Failure path: inventory unavailable

```
order.created → inventory.reservation_failed
                       │
                       ▼
          order-service cancels the order;
          notification-service emails the customer
```

---

## 9. Reliability patterns

### Transactional outbox (Session 12)

**Problem solved:** the dual-write gap. Writing the order to the database and
publishing to Kafka are two separate systems; a crash between them loses the
event (or publishes an event for an order that rolled back).

**Mechanism:**

1. `placeOrder()` writes the order row **and** an `outbox_event` row in one local
   `@Transactional` unit. No direct Kafka send.
2. An `OutboxRelay` (`@Scheduled(fixedDelay=500)`) polls unpublished rows, sends
   each to Kafka via a dedicated `stringKafkaTemplate` (StringSerializer — the
   payload is already serialized JSON, avoiding double-encoding), and stamps
   `published_at`.

Because the order and the outbox row commit atomically, the event can never be
lost: if Kafka is down, intake still succeeds and the relay drains the backlog
when Kafka returns.

**Verified:** with Kafka stopped, three orders were placed (all succeeded, three
rows with `published_at = NULL`); on restart the relay drained 3 → 0
automatically. Zero loss.

> Currently the outbox is implemented on order-service only. The pattern is
> proven; rolling it out to the other producers is mechanical. This scope is
> stated honestly rather than implied to be everywhere.

### Idempotent consumers

Kafka is at-least-once, so every consumer must tolerate redelivery. The dedup key
is `eventId`. Combined with at-least-once delivery, idempotent consumers yield
**effectively exactly-once** processing without distributed transactions.

### Producer hardening

`acks=all` + `enable.idempotence=true` + bounded `max.in.flight` means producer
retries don't create duplicates or reorder within a partition.

### Coordinated reset tooling

Because there is no atomic "reset the whole system" across N databases, a
committed `reset.ps1` truncates all four service databases and recreates all
topics. It was born from a real cross-service data-drift incident; the event log
is the source of truth when state diverges.

---

## 10. Concurrency & correctness

This section documents the Session 13 work — the deepest correctness problem in
the system.

### The bug: lost-update race / oversell

The original reservation logic was read-modify-write:

```
findById(sku) → check available >= qty → setAvailable(available - qty) → save
```

Under concurrency (listener container concurrency = 3), two threads both read the
same `available`, both pass the check, and both write — the second overwrites the
first's decrement. A load test of **2000 concurrent orders against 1000 units of
stock** produced **100% oversell**: ~1908 reservations succeeded, 0 rejections,
and the `reserved_quantity` counter was corrupted to an incoherent value.

**Critical insight:** the operation was already inside an `@Transactional` method,
and it did not help. At `READ_COMMITTED` isolation a transaction prevents reading
*uncommitted* data, but it does **not** prevent two transactions from both reading
a value and both updating it. **A transaction is not a lock.**

### The fix: atomic conditional UPDATE

Replace read-modify-write with a single atomic statement that does check-and-
decrement indivisibly:

```sql
UPDATE stock_items
SET available_quantity = available_quantity - :qty,
    reserved_quantity  = reserved_quantity  + :qty
WHERE sku = :sku AND available_quantity >= :qty
```

The database evaluates the `WHERE available >= qty` condition and applies the
decrement as one operation, serializing concurrent updates internally. The rows-
affected count is the decision:

- `1` → reservation succeeded;
- `0` → insufficient stock, reject.

Overselling becomes impossible regardless of thread count. Implemented as
`tryReserve(sku, qty)` (`@Modifying`, `clearAutomatically`, `flushAutomatically`),
with companion `restoreStock(sku, qty)` for compensation and a concurrency-safe
`seedIfAbsent(sku)` using Postgres `INSERT ... ON CONFLICT (sku) DO NOTHING`.

### Why atomic-update over the alternatives

| Strategy | How it works | Best for | Why not chosen here |
|---|---|---|---|
| **Atomic conditional UPDATE** *(chosen)* | Check + decrement in one SQL statement; DB serializes rows | Hot single-row counters like stock | — |
| Pessimistic lock (`SELECT … FOR UPDATE`) | Locks the row; others block | Multiple dependent reads/writes under one consistency view | Serializes the hot SKU — throughput bottleneck under 50-way contention |
| Optimistic lock (`@Version`) | Write fails if version changed; catch + retry | Many rows, rare contention | One hot row under heavy contention → retry storm |

### The second bug: exception across the transaction boundary

The first version of the fix *threw* `InsufficientStockException` to signal
out-of-stock. That exception crossed the `@Transactional` boundary and marked the
transaction **rollback-only**; the Kafka listener then failed its offset commit
and the message was **redelivered** ("seek to current after exception"), so
rejected orders were processed multiple times and the rejection metric inflated
(e.g. 2128 instead of ~1000).

**The fix:** stop using exceptions for an expected business outcome. Out-of-stock
is a normal result, not an error. `reserve()` now returns a `ReserveResult`
record (`success` / `failedSku`) and always returns normally; partial reservations
within a multi-item order are unwound manually with `restoreStock`. No exception
crosses the boundary, the listener commits cleanly, and no redelivery occurs.

**Verified clean:** 2000 concurrent orders → `reserved_quantity` exactly 1000,
totals reconcile to 1000, `inventory.reservation_failed ≈ 995`, no `ERROR` logs.

> Design note: this is also simply better design. Exceptions should not model
> expected control flow.

---

## 11. Session-by-session concept log

| Session | Theme | Key concepts introduced |
|---|---|---|
| 1–2 | Foundations & monolith baseline | Project skeleton, Spring Boot mental model, the synchronous "before" with 5 in-process calls under one `@Transactional` |
| 3–4 | Kafka CLI & multi-module Maven | KRaft (no Zookeeper), topics/partitions, parent POM + `common` shared library |
| 5 | First producer | `OrderEventPublisher`, partition-by-`orderId`, async send with callback, `202 Accepted` |
| 6 | First consumer | `@KafkaListener`, consumer groups, partition rebalancing across instances |
| 7 | Inventory: consumer-that-produces | Two outcome paths (reserved / failed), database-per-service, own `StockItem`/`Reservation` model |
| 8 | Payment | Consume `inventory.reserved` ("stock first, then charge"), simulated gateway latency + 5% failure, idempotency, money as `long` cents, persist intent before gateway call |
| 9 | Saga compensation | Release reservation on `payment.failed`, `ReservationItem` model (know what to undo), choreographed saga |
| 10 | Shipping | Single-path consumer of `payment.completed`, emits `shipping.dispatched` |
| 11 | Analytics | Fan-in consumer of all topics, in-memory counters, `GET /metrics`, invariant reconciliation |
| 12 | Transactional outbox | Dual-write problem, outbox table as state machine, scheduled relay, zero-loss verified with Kafka down |
| 13 | Concurrency & oversell prevention | Lost-update race, transaction ≠ lock, atomic conditional UPDATE vs pessimistic/optimistic, rollback-only redelivery, result-object over exception-control-flow |

---

## 12. Error-mitigation playbook

Each entry is a real failure encountered during the build, its root cause, and
the durable mitigation — the kind of thing worth recognizing instantly.

### 12.1 Silent `StringDeserializer` fallback (Session 6)

- **Symptom:** `Cannot convert String to EventEnvelope`.
- **Root cause:** a missing `@Bean` on the consumer factory caused Spring to
  silently fall back to auto-configured `StringDeserializer`.
- **Mitigation:** explicitly construct `JsonDeserializer` with the target class
  and `addTypeInfo(false)`, wire a named container factory, and reference it from
  every listener. **Lesson:** Spring beans fail silently — verify what is actually
  in the context rather than assuming.

### 12.2 Wrong `@Transactional` import (Session 9)

- **Symptom:** transactional behavior (rollback) not applied as expected.
- **Root cause:** imported `jakarta.transaction.Transactional` instead of
  `org.springframework.transaction.annotation.Transactional`.
- **Mitigation:** always use the Spring annotation. **Lesson:** two annotations
  share a name and behave differently; check the import.

### 12.3 Postgres driver typo (Session 7)

- **Symptom:** driver/connection failure on startup.
- **Root cause:** `org.postgres.Driver` instead of `org.postgresql.Driver`.
- **Mitigation:** correct the driver class; the fully-qualified name is
  `org.postgresql.Driver`.

### 12.4 Missing topic → `UNKNOWN_TOPIC_OR_PARTITION` (Session 11)

- **Symptom:** consumer error for `order.cancelled`.
- **Root cause:** the topic had not been created.
- **Mitigation:** create the topic; added topic creation to `reset.ps1`.

### 12.5 Dual-write gap (Session 12)

- **Symptom (latent):** a crash between DB commit and Kafka send loses the event.
- **Root cause:** writing to two systems without a shared transaction.
- **Mitigation:** transactional outbox — write order + outbox row atomically, relay
  publishes asynchronously.

### 12.6 Docker volume reinitialization on unclean shutdown (recurring)

- **Symptom:** service databases disappear after an unclean Docker Desktop /
  power-loss shutdown; tables gone.
- **Root cause:** Postgres data volume reinitializes on unclean shutdown in this
  environment.
- **Mitigation:** a mounted `postgres-init.sql` recreates the per-service
  databases on a fresh init; durable Kafka volume added so topics/messages
  survive restarts. **Lesson:** code and schema live in git; only data needs
  durability — keep services stateless and make initialization self-healing.

### 12.7 Lost-update race / oversell (Session 13)

- **Symptom:** 100% oversell under 2000 concurrent orders; corrupted
  `reserved_quantity`.
- **Root cause:** unguarded read-modify-write; `@Transactional` at
  `READ_COMMITTED` does not prevent lost updates. **A transaction is not a lock.**
- **Mitigation:** atomic conditional `UPDATE … WHERE available >= qty`, returning
  rows-affected as the success signal.

### 12.8 Rollback-only redelivery storm (Session 13)

- **Symptom:** rejection metric inflated (e.g. 2128 vs ~1000); `ERROR` logs
  ("seek to current after exception", `UnexpectedRollbackException`).
- **Root cause:** throwing an exception across a `@Transactional` boundary marked
  the transaction rollback-only, failing the Kafka offset commit and triggering
  redelivery.
- **Mitigation:** return a `ReserveResult` value object instead of throwing; unwind
  partial reservations manually. **Lesson:** don't use exceptions for expected
  business outcomes; an expected result is not an error.

### 12.9 Self-invocation proxy trap (Session 13)

- **Symptom (avoided):** `@Transactional` silently not applied when a method calls
  another `@Transactional` method in the same class.
- **Root cause:** Spring's transaction proxy is bypassed on `this.method()`
  self-invocation.
- **Mitigation:** extract transactional logic into a separate `@Service` bean
  (`StockReservationService`) so calls are genuinely proxied.

---

## 13. System invariants

These must hold after any run; analytics + database queries are used to verify
them.

- **COMPLETED payments == DISPATCHED shipments.**
- **FAILED payments == RELEASED reservations.**
- **ACTIVE reservations == COMPLETED payments** (steady state).
- **Total orders == reservations + inventory rejections.**
- **`reserved_quantity` never exceeds the stock ceiling** (the oversell
  guarantee).
- **Every order resolves to exactly one outcome:** reserved or rejected.

---

## 14. Local development & operations

### Infrastructure (`infrastructure/docker-compose.yml`)

- **Kafka** — Confluent 7.6.0, KRaft mode (no Zookeeper). Internal listener on
  `9092`, host listener on `29092` (applications connect to `29092`). Durable
  `kafka-data` volume so topics/messages survive restarts.
- **PostgreSQL** — 16-alpine, port `5432`, user/password `orderflow`/`orderflow`,
  durable `postgres-data` volume. `postgres-init.sql` mounted at
  `/docker-entrypoint-initdb.d/` auto-creates the per-service databases on a fresh
  data directory.
- **Kafka UI** — provectuslabs, port `8090`, for inspecting topics and messages.
- **Schema Registry** — defined for future Avro work (Session 14), intended on
  port `8087` to avoid collisions.

### Operational scripts

- **`infrastructure/reset.ps1`** — coordinated reset: truncates all four service
  databases and deletes/recreates all topics. Use between load tests for a clean
  baseline.

### Running a load test (concurrency verification)

On Windows PowerShell 5.x, `ForEach-Object -Parallel` is unavailable (it is a
PowerShell 7+ feature). Either install PowerShell 7 (`winget install --id
Microsoft.PowerShell`) and run under `pwsh`, or use a runspace pool to achieve
true concurrency. Fire 2000 concurrent orders against a 1000-unit SKU, let the
pipeline drain (watch `GET /metrics` until counts stabilize), then verify the
`stock_items` row shows `reserved_quantity` never exceeded 1000 and all counters
reconcile.

### Conventions & gotchas

- Money is always `long` cents — never floating point.
- Driver class is `org.postgresql.Driver`.
- `@Transactional` is always `org.springframework.transaction.annotation.Transactional`.
- Applications connect to Kafka on `29092` (host listener), not `9092`.
- Some inventory packages use capitalized/plural names (`Entity`, `Repositories`)
  — non-conventional but intentional within this codebase.

---

## Roadmap (post–Session 13)

- **Session 14:** Avro + Confluent Schema Registry (replace hand-rolled JSON;
  enforce backward-compatible schema evolution).
- **Session 15:** Order state machine refinements (close the loop on status
  transitions, publish `order.cancelled`).
- **Session 16+:** DLQ/retries, k6 load testing of both architectures,
  strangler-fig migration ADRs, interview-prep consolidation.

*Future scaling direction:* polyglot persistence enabled by database-per-service
— transactional core stays on PostgreSQL; inventory gains a Redis reservation
layer for flash sales; analytics moves to a columnar store; search is fed from
Kafka into Elasticsearch.
