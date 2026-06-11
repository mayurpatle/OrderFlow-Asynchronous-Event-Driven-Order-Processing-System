# Kafka Docker Basic Commands

All commands assume the container name `orderflow-kafka` and bootstrap server `kafka:9092`.

## Topics

### Describe a topic

```bash
docker exec orderflow-kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic playground
```

### Delete a topic

```bash
docker exec orderflow-kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic playground
```

### List topics (verify deletion)

```bash
docker exec orderflow-kafka kafka-topics --bootstrap-server kafka:9092 --list
```

---

## Producing Messages

### Console producer (interactive)

```bash
docker exec -it orderflow-kafka kafka-console-producer --bootstrap-server kafka:9092 --topic playground
```

### Console producer with key support

Use `key:value` format when sending messages:

```bash
docker exec -it orderflow-kafka kafka-console-producer --bootstrap-server kafka:9092 --topic playground --property "parse.key=true" --property "key.separator=:"
```

---

## Consuming Messages

### Consume from beginning (with partition and offset)

```bash
docker exec orderflow-kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic playground --from-beginning --property print.partition=true --property print.offset=true --timeout-ms 5000
```

### Consumer with group `demo-group`

```bash
docker exec -it orderflow-kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic playground --group demo-group --property print.partition=true
```

### Consumer with group `another-group`

```bash
docker exec -it orderflow-kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic playground --group another-group --property print.partition=true
```

---

## Consumer Groups

### Describe a consumer group

```bash
docker exec orderflow-kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group another-group
```
