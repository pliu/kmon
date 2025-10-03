# kmon

A Kafka monitoring tool written in Go to provide insights into Kafka cluster performance and behavior.

## TODO

- Add logging

## Properties

- **Static Partitions:** Each `Monitor` instance assumes that the set of partitions it finds for a topic at startup is static and will not change throughout its lifetime. The monitor does not currently handle dynamic partition changes.
- **Message Self-Processing:** Each `Monitor` instance only processes messages that it has created. This is verified by checking the UUID in the message key, which is unique to each `Monitor` instance.

## Getting Started

### Prerequisites

- Go (version 1.23 or later)
- A running Kafka cluster

## Usage

Build the binary and start collecting metrics via the provided `Makefile` targets.

```sh
# format, lint, test, then compile to build/kmon
make build

# run the compiled binary
make run
```

```sh
./kmon
```

For more detailed logging, you can use the `-debug` flag:

```sh
./kmon -debug
```

## Testing

The Kafka client integration test (`pkg/clients/kafka_test.go`) expects a Kafka broker at `localhost:9092`. Start a local cluster (Docker compose, local install, etc.) before running the full suite or disable that package when Kafka is unavailable:

```sh
# Unit tests (no Kafka required)
make unit_tests

# Full suite – requires Kafka
make all_tests
```
