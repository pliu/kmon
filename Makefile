GO_CMD = go
GO_BUILD_FLAGS = -v
GO_TEST_FLAGS = -v -count=1 -cover

.PHONY: build run unit_tests all_tests clean fmt lint start_kafka stop_kafka

build: unit_tests
	$(GO_CMD) build $(GO_BUILD_FLAGS) -o build/kmon .

unit_tests: lint
	$(GO_CMD) test $(GO_TEST_FLAGS) ./...

all_tests: lint
	$(GO_CMD) test $(GO_TEST_FLAGS) -tags integration ./...

clean:
	$(GO_CMD) clean -cache
	$(GO_CMD) clean
	rm -rf ./build

fmt:
	$(GO_CMD) fmt ./...

lint: fmt
	golangci-lint run

run: build
	./build/kmon -config.path test_config.json

start_kafka:
	docker compose -f docker-compose.yml up -d

stop_kafka:
	docker compose -f docker-compose.yml down
