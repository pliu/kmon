GO_CMD = go
GO_BUILD_FLAGS = -v
GO_TEST_FLAGS = -v -count=1

.PHONY: all build run unit_tests test_all clean fmt lint

build: unit_tests
	$(GO_CMD) build $(GO_BUILD_FLAGS) -o build/kmon .

run:
	./kmon

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

lint:
	golangci-lint run

start_kafka:
	docker compose -f docker-compose.yml up -d

stop_kafka:
	docker compose -f docker-compose.yml down
