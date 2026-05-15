# OrderFlow

A monolith-to-microservices migration demonstration project built to deeply
understand event-driven architecture with Apache Kafka, the saga pattern,
and Spring Boot microservices.



## Architecture (target state)

Six microservices communicating asynchronously through Kafka topics:

1. **Order Service** — accepts orders, publishes `order.created`
2. **Inventory Service** — reserves stock, publishes `inventory.reserved` or `.reservation_failed`
3. **Payment Service** — charges customer, publishes `payment.completed` or `.failed`
4. **Shipping Service** — dispatches on payment success
5. **Notification Service** — fan-out emails on every state change
6. **Analytics Service** — consumes all topics, builds metrics

## Stack

- Java 17, Spring Boot 3.2
- Apache Kafka (KRaft mode, no Zookeeper)
- PostgreSQL 16 (one DB per service)
- Confluent Schema Registry + Avro
- Docker Compose for local infra
- k6 for load testing

## Quick start

_To be added as we build it._

## Author

Mayur Patle — built as part of structured deep-dive learning toward senior backend roles.