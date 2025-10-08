# kmon

A Kafka monitoring tool written in Go to provide insights into Kafka cluster performance and behavior.

## Properties

- **Static Partitions:** Each `Monitor` instance assumes that the set of partitions it finds for a topic at startup is static and will not change throughout its lifetime. The monitor does not currently handle dynamic partition changes.
- **Message Self-Processing:** Each `Monitor` instance only processes messages that it has created. This is verified by checking the UUID in the message key, which is unique to each `Monitor` instance.

## Testing

```sh
# Unit tests (no Kafka required)
make unit_tests

# Full suite – requires Kafka
make all_tests
```

## Kafka Cluster Management

The project includes commands to start and stop a 3-node Kafka cluster using Docker Compose:

```sh
# Start a 3-node Kafka cluster
make start_kafka

# Stop the Kafka cluster
make stop_kafka
```
