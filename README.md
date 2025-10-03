# kmon

A Kafka monitoring tool written in Go to provide insights into Kafka cluster performance and behavior.

## Getting Started

### Prerequisites

- Go (version 1.23 or later)
- A running Kafka cluster

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/pliu/kmon.git
   cd kmon
   ```

2. Build the application:
   ```sh
   make build
   ```

## Usage

Run the `kmon` application from the root of the project directory:

```sh
./kmon
```

For more detailed logging, you can use the `-debug` flag:

```sh
./kmon -debug
```

## Testing

To run the test suite, a Kafka cluster must be running and accessible at `localhost:9092`.

Run the following command:

```sh
make test
```
