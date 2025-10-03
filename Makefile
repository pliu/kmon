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
	$(GO_CMD) test $(GO_TEST_FLAGS) -tags kafka ./...

clean:
	$(GO_CMD) clean -cache
	$(GO_CMD) clean
	rm -rf ./build

fmt:
	$(GO_CMD) fmt ./...

lint:
	golangci-lint run
