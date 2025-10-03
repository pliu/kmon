GO_CMD = go
GO_BUILD_FLAGS = -v
GO_TEST_FLAGS = -v

.PHONY: all build run test clean fmt lint

all: build

build: test
	$(GO_CMD) build $(GO_BUILD_FLAGS) -o build/kmon .

run:
	./kmon

test: lint
	$(GO_CMD) test $(GO_TEST_FLAGS) ./...

clean:
	$(GO_CMD) clean
	rm -f build/kmon

fmt:
	$(GO_CMD) fmt ./...

lint:
	golangci-lint run
