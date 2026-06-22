# Transactional outbox pattern 


### Architechture


![img.png](D:\Desktop\EDA\OrderFlow\Assets\Transactional-Outbox-pattern.png)

### The Dual-Write Problem and the Outbox Solution

@Transactional
public Order placeOrder(...) {
Order order = buildOrder(...);
orderRepository.save(order);           // (1) write to database
kafkaTemplate.send("order.created", ...); // (2) publish to Kafka
return order;
}

These are two separate writes to two separate systems — your PostgreSQL database and your Kafka broker. The @Transactional annotation only covers the database. Kafka is outside the transaction. It has no idea a database transaction is happening.
Now ask: what happens if the process crashes, or Kafka is unreachable, after step 1 commits but before step 2 completes?
You get an order in the database that no downstream service will ever hear about. Inventory never reserves stock. Payment never charges. The customer's order is silently stuck in PENDING forever. There is no event, so there is no recovery — the system doesn't even know anything is wrong.
This is the dual-write problem, and it is one of the most important failure modes in all of distributed systems. Any time you write to two systems without a shared transaction, you have a window where one write succeeds and the other doesn't.

The solution: the Transactional Outbox
The insight is simple and elegant: make the event part of the same database transaction as the entity.
Instead of writing to Kafka directly, order-service writes the event into an outbox table in its own database — in the same transaction as the order:

@Transactional
public Order placeOrder(...) {
Order order = buildOrder(...);
orderRepository.save(order);        // (1) write the order
outboxRepository.save(outboxRow);   // (2) write the event to the outbox table
return order;                        // both commit together, atomically
}

Now both writes go to the same database, inside one transaction. Either both commit or neither does. There is no window. If the process crashes mid-way, the transaction rolls back and there's neither an order nor an outbox row — consistent. If it commits, there's both an order and an outbox row — consistent. The atomicity you needed is now guaranteed by the database's own transaction machinery, which is exactly what databases are good at.
But the event isn't in Kafka yet — it's sitting in the outbox table. So we need a second piece: a relay that reads unpublished rows from the outbox and publishes them to Kafka.

Why this is bulletproof
Trace every failure case:

Crash after order commits, before relay runs? The outbox row is safely in the database. When the relay next runs, it finds the row and publishes it. Recovered.
Kafka is down when the relay tries? The send fails, the row stays unpublished, the relay tries again next cycle. When Kafka recovers, the backlog drains. Recovered.
Relay publishes to Kafka but crashes before marking the row published? Next cycle, the row still looks unpublished, so the relay publishes it again. A duplicate. And this is where all your idempotency work pays off — your consumers already dedupe by orderId. The duplicate is harmless.

That last point is the beautiful part. The outbox pattern guarantees at-least-once delivery — it might publish a duplicate, but it will never lose an event. And "at-least-once + idempotent consumers = effectively exactly-once" is the foundational reliability recipe of event-driven systems. You built the idempotency half over the last several sessions. The outbox is the other half clicking into place.
Polling vs CDC — the design fork
There are two ways to build the relay, and an interviewer will absolutely ask you to compare them.
Polling publisher (what we'll build): a scheduled job runs SELECT * FROM outbox WHERE published_at IS NULL ORDER BY created_at every N milliseconds, publishes each row, marks it published. Simple, no extra infrastructure, easy to reason about. The downside is polling latency (events wait up to one poll interval) and database load from constant polling.
Change Data Capture (CDC): a tool like Debezium tails the database's write-ahead log (the transaction log the DB already maintains), detects inserts into the outbox table, and streams them to Kafka. No polling — it reacts to the log in near-real-time. Lower latency, no polling load. The downside is significant operational complexity: you run Debezium, configure connectors, manage the WAL, handle schema changes. It's the production-grade answer at scale.
The interview line:

"The outbox relay can be polling-based or CDC-based. Polling is a scheduled query for unpublished rows — simple, no extra infrastructure, but adds latency and database load. CDC with something like Debezium tails the database's transaction log and streams outbox inserts in near-real-time — lower latency and no polling overhead, but it's a whole piece of infrastructure to operate. I'd start with polling because it's simple and correct, and move to CDC only when polling latency or database load became a measured problem. The pattern — atomic write of entity plus event, then async relay — is identical either way; only the relay's mechanism changes."

We build polling because it teaches the concept cleanly without burying it under Debezium config. When you can articulate why you'd graduate to CDC, you've demonstrated you understand both.
One production wrinkle worth knowing now
If you run multiple instances of order-service, each with its own relay, two relays could poll the same unpublished rows and both publish them — lots of duplicates. The production fix is SELECT ... FOR UPDATE SKIP LOCKED: each relay locks the rows it claims, and other relays skip locked rows. We'll run single-instance so it won't bite us, but I'll show you where it goes. Mentioning SKIP LOCKED in an interview signals you've actually thought about running this at scale.

## Outbox Table 


One entity, one repository, plus a design conversation about the columns. The outbox table lives in order-service's own database (the orders database) — because the whole point is that it commits in the same transaction as the order, and that only works if it's in the same database.
This is the critical design constraint to internalize: the outbox table must live in the same database as the entity whose writes it's making atomic. If the outbox were in a separate database, you'd be back to a dual-write across two systems — exactly the problem you're solving. The outbox works precisely because the order INSERT and the outbox INSERT are two rows in two tables in one database, covered by one transaction.

### Why we store the serialized payload rather than re-deriving it

A subtle but important choice. We serialize the event to JSON at the moment we write the outbox row, and store that string. The relay sends the stored string verbatim. Why not store structured fields and serialize in the relay?
Because the payload captured at write-time is the authoritative representation of what happened at that moment. If we re-derived it later in the relay, and the code or schema had changed in between, we might publish something subtly different from what the transaction intended. Storing the serialized bytes means the event is frozen exactly as it was when the business transaction committed. This is the same principle as the "persist intent before action" pattern from payment-service — capture the truth at the decision point, don't reconstruct it later.

### The Outbox Relay

The relay is the second half of the pattern. The outbox table now fills up with unpublished events (every placeOrder writes one), but nothing sends them to Kafka yet. The relay is a scheduled background job that:

Wakes up every N milliseconds
Queries the outbox for rows where publishedAt IS NULL, oldest first, capped to a batch
For each row, sends the stored JSON to its topic, keyed by the aggregateId
On successful send, stamps publishedAt = now()

That's the whole thing. It's a loop: poll, publish, mark. The elegance is in what it guarantees — combined with Part 3's atomic write, no order can ever exist without its event eventually reaching Kafka.
The three design decisions worth understanding before you type
Decision 1: We send the raw stored JSON string, not a re-serialized object.
The outbox stored the event already serialized to JSON at write-time. The relay must publish those exact bytes. This means we can't use your existing KafkaTemplate<String, Object> with the JsonSerializer — that serializer would take our String and JSON-encode it again, wrapping it in quotes and escaping it. We'd get a double-serialized mess.
So the relay needs to send the String as-is. The cleanest way: publish the already-serialized payload as a String value. I'll show you how to handle this — it's the one genuinely fiddly part of the session, and getting it right is exactly the kind of detail that separates "works" from "subtly broken."
Decision 2: Each row is published and marked in its own transaction.
We don't wrap the whole batch in one transaction. If we did, and row 50 of 100 failed, the rows 1-49 we'd already marked published would roll back — and we'd re-publish them next cycle, even though they actually reached Kafka. Per-row marking keeps the blast radius of any single failure to that one row. A failed row simply stays unpublished and gets retried next cycle; its neighbors are unaffected.
Decision 3: The relay tolerates duplicates by design.
If the relay sends to Kafka successfully but crashes before stamping publishedAt, next cycle it'll send that row again — a duplicate. That's fine, and it's the whole reason you built idempotent consumers. The outbox guarantees at-least-once; your consumers' dedup makes it effectively exactly-once. We don't try to make the relay itself exactly-once (that's impossible without distributed transactions, which is what we're avoiding). We lean on the idempotency you already built.

