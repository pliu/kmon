# kmon

A Kafka monitoring tool written in Go to provide insights into Kafka cluster performance and behavior.

## Features

- **Sliding-window stats** powered by the `Stats` structure, giving efficient rolling averages and percentiles for any metric stream (latencies, sizes, etc.). `Stats.Merge` lets you compose results from multiple producers or consumers while keeping data windowed correctly.
- **Order-statistic tree** (`SortedList`) to support fast `O(log n)` inserts and percentile lookups, even with large duplicate sets.
- **Kafka read/write exercises** via the Franz-go client to validate throughput and round-trip timings across multiple topics.
- **Clean separation** between reusable utility packages (`pkg/utils`) and Kafka-specific orchestration (`pkg/clients`, `pkg/kmon`).

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
