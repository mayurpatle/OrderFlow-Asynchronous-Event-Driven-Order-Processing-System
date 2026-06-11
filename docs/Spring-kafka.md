# Spring Kafka

## Auto-Configuration

When you add the `spring-kafka` dependency, Spring Boot's auto-configuration fires. Remember from Session 1.5: auto-config = `@Configuration` classes with `@Conditional` annotations.

`KafkaAutoConfiguration` checks `@ConditionalOnClass(KafkaTemplate.class)`. Since adding the dependency put `KafkaTemplate` on the classpath, it fires. The auto-config reads properties under `spring.kafka.*` from your `application.yml` and creates:

- A **`ProducerFactory<K, V>`** — knows how to construct Kafka producer connections
- A **`KafkaTemplate<K, V>`** — the high-level API you call to send messages
- A **`ConsumerFactory<K, V>`** — for consumers (we'll use this in Session 6)
- A **`ConcurrentKafkaListenerContainerFactory`** — manages consumer threads

## Custom `KafkaConfig`

For learning purposes — and to keep production-grade control — we're going to write our own `KafkaConfig` class instead of letting auto-configuration do everything implicitly. The auto-config defaults aren't bad, but they hide what's happening. By writing it ourselves, you see every knob and understand every setting.

Plus, `@ConditionalOnMissingBean` on the auto-config means our beans win — so this is the canonical way to customize.
