.PHONY: test lint build clean install-tools

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=ueba-generator
BINARY_PATH=./cmd/$(BINARY_NAME)

# Build the binary
build:
	$(GOBUILD) -o $(BINARY_NAME) -v $(BINARY_PATH)

# Run tests
test:
	$(GOTEST) -v -race -coverprofile=coverage.out ./...

# Generate test coverage
test-coverage: test
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run tests with coverage threshold check
test-coverage-check: test
	@COVERAGE=$$(go tool cover -func=coverage.out | grep total | grep -Eo '[0-9]+\.[0-9]+'); \
	echo "Total coverage: $${COVERAGE}%"; \
	if (( $$(echo "$${COVERAGE} < 80.0" | bc -l) )); then \
		echo "Coverage $${COVERAGE}% is below threshold 80%"; \
		exit 1; \
	fi

# Run linter
lint:
	golangci-lint run --config .golangci.yml

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f coverage.out coverage.html

# Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Install development tools
install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest

# Run all checks (like CI)
ci: deps lint test-coverage-check build

# Development workflow
dev: clean deps lint test build

# Cross-platform build
build-all:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o build/$(BINARY_NAME)-linux-amd64 $(BINARY_PATH)
	GOOS=windows GOARCH=amd64 $(GOBUILD) -o build/$(BINARY_NAME)-windows-amd64.exe $(BINARY_PATH)
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -o build/$(BINARY_NAME)-darwin-amd64 $(BINARY_PATH)
	GOOS=darwin GOARCH=arm64 $(GOBUILD) -o build/$(BINARY_NAME)-darwin-arm64 $(BINARY_PATH)
