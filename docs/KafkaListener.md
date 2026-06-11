# Kafka Listener

## What `@KafkaListener` Actually Does

When Spring Boot starts a service that has `spring-kafka` on the classpath, it scans for methods annotated with `@KafkaListener`. For each one, it:

1. Creates a **listener container** — a long-running thread (or pool of threads) dedicated to this listener
2. The container creates a `KafkaConsumer` instance (the low-level client from Kafka's library)
3. The consumer joins the consumer group specified in the annotation
4. The broker assigns partitions to this consumer (you saw this in Session 3)
5. The container polls the broker in a loop: "give me messages from my assigned partitions"
6. When messages arrive, the container deserializes them and invokes your method
7. After your method returns successfully, the container commits the offset back to Kafka

Every consumer in your system is doing this exact dance. Spring hides the polling loop and the offset commit, but they're still happening underneath. You can see them in your logs if you turn up Kafka's logging.

---

## The Consumer Group Is Your Scaling Unit

```java
@KafkaListener(topics = "order.created", groupId = "notification-service")
```

- The `groupId` here is `notification-service`. If we run two instances of `notification-service`, both will use this same `groupId`. Kafka treats them as one group and splits the topic's partitions between them.
- If we run a different service (say, `analytics-service`) with `groupId = "analytics-service"` consuming the same topic, that's a different group. Both groups consume independently — each gets its own copy of every message.

This is publish/subscribe built on top of consumer groups. Producer publishes once. Each consumer group gets a complete copy.

---

## The Polling Model — At-Least-Once Delivery

Here's the part most engineers don't internalize. Kafka consumers poll the broker on a regular interval. They say "give me messages from offset X onward" and the broker responds with a batch. The consumer processes the batch, then commits offset `X + batch_size` back to Kafka. Next poll asks from there.

**What happens if the consumer crashes between processing and committing?**

When the consumer restarts (or another consumer in the group takes over), it resumes from the last committed offset. The messages it processed but didn't commit get redelivered. This is **at-least-once delivery**. Messages might be delivered more than once, never less.

This is why idempotency is the consumer's responsibility, not Kafka's. We'll build the idempotency machinery in Session 12. For today, the `notification-service` is naturally tolerant of duplicate emails (sending the same email twice is annoying but not catastrophic), so we ignore the concern.

---

## Fan-Out Pattern

Same event triggers different side effects in different services. Notification gets the event, analytics gets the event, inventory gets the event — all from the same publish.
