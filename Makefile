.PHONY: build test test-integration generate install-paramgen

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-databricks.version=${VERSION}'" -o conduit-connector-databricks cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

generate:
	go generate ./...

download:
	@echo Download go.mod dependencies
	@go mod download

install-tools: download
	@echo Installing tools from tools.go
	@go list -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy
