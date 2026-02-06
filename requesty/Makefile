# Makefile for Requesty - The friendly load tester for Loggy

# Variables
BINARY_NAME=requesty
MAIN_PATH=cmd/requesty/main.go
BUILD_DIR=build
GO=go
GOFLAGS=-v
LDFLAGS=-ldflags "-s -w"

# Version info
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Build flags with version info
FULL_LDFLAGS=-ldflags "-s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"

# Platforms for cross-compilation
PLATFORMS=darwin/amd64 darwin/arm64 linux/amd64 linux/arm64 windows/amd64

# Colors for pretty output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
NC=\033[0m # No Color

.PHONY: all build clean test deps run install help fmt vet lint cross-compile

## Default target
all: deps fmt vet build

## Display help
help:
	@echo "$(GREEN)Requesty Makefile$(NC)"
	@echo "$(YELLOW)Available targets:$(NC)"
	@grep -E '^##' $(MAKEFILE_LIST) | sed 's/## /  /'
	@echo ""
	@echo "$(YELLOW)Examples:$(NC)"
	@echo "  make build        # Build for current platform"
	@echo "  make test         # Run tests"
	@echo "  make install      # Install to GOPATH/bin"
	@echo "  make cross        # Build for all platforms"
	@echo "  make clean        # Clean build artifacts"

## Build the binary for current platform
build:
	@echo "$(GREEN)Building $(BINARY_NAME)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) $(FULL_LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "$(GREEN)✅ Build complete: $(BUILD_DIR)/$(BINARY_NAME)$(NC)"

## Build a minimal binary (no debug info)
build-minimal:
	@echo "$(GREEN)Building minimal $(BINARY_NAME)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 $(GO) build -a -installsuffix cgo $(FULL_LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-minimal $(MAIN_PATH)
	@echo "$(GREEN)✅ Minimal build complete: $(BUILD_DIR)/$(BINARY_NAME)-minimal$(NC)"

## Install dependencies
deps:
	@echo "$(GREEN)Installing dependencies...$(NC)"
	$(GO) mod download
	$(GO) mod tidy
	@echo "$(GREEN)✅ Dependencies installed$(NC)"

## Run tests
test:
	@echo "$(GREEN)Running tests...$(NC)"
	$(GO) test -v -cover ./...
	@echo "$(GREEN)✅ Tests complete$(NC)"

## Run tests with race detection
test-race:
	@echo "$(GREEN)Running tests with race detection...$(NC)"
	$(GO) test -v -race ./...
	@echo "$(GREEN)✅ Race tests complete$(NC)"

## Run benchmarks
bench:
	@echo "$(GREEN)Running benchmarks...$(NC)"
	$(GO) test -bench=. -benchmem ./...

## Format code
fmt:
	@echo "$(GREEN)Formatting code...$(NC)"
	$(GO) fmt ./...
	@echo "$(GREEN)✅ Code formatted$(NC)"

## Run go vet
vet:
	@echo "$(GREEN)Running vet...$(NC)"
	$(GO) vet ./...
	@echo "$(GREEN)✅ Vet complete$(NC)"

## Run linter (requires golangci-lint)
lint:
	@echo "$(GREEN)Running linter...$(NC)"
	@which golangci-lint > /dev/null || (echo "$(RED)golangci-lint not installed$(NC)" && exit 1)
	golangci-lint run ./...
	@echo "$(GREEN)✅ Linting complete$(NC)"

## Clean build artifacts
clean:
	@echo "$(YELLOW)Cleaning build artifacts...$(NC)"
	@rm -rf $(BUILD_DIR)
	@rm -f $(BINARY_NAME)
	@echo "$(GREEN)✅ Clean complete$(NC)"

## Install to GOPATH/bin
install: build
	@echo "$(GREEN)Installing $(BINARY_NAME) to GOPATH/bin...$(NC)"
	@cp $(BUILD_DIR)/$(BINARY_NAME) $(GOPATH)/bin/
	@echo "$(GREEN)✅ Installed to $(GOPATH)/bin/$(BINARY_NAME)$(NC)"

## Build for all platforms
cross-compile: deps
	@echo "$(GREEN)Cross-compiling for multiple platforms...$(NC)"
	@mkdir -p $(BUILD_DIR)
	@for platform in $(PLATFORMS); do \
		platform_split=($${platform//\// }); \
		GOOS=$${platform_split[0]}; \
		GOARCH=$${platform_split[1]}; \
		output_name=$(BUILD_DIR)/$(BINARY_NAME)-$${GOOS}-$${GOARCH}; \
		if [ $${GOOS} = "windows" ]; then \
			output_name=$${output_name}.exe; \
		fi; \
		echo "  Building for $${GOOS}/$${GOARCH}..."; \
		CGO_ENABLED=0 GOOS=$${GOOS} GOARCH=$${GOARCH} $(GO) build $(LDFLAGS) -o $${output_name} $(MAIN_PATH); \
	done
	@echo "$(GREEN)✅ Cross-compilation complete$(NC)"
	@echo "$(YELLOW)Built binaries:$(NC)"
	@ls -lh $(BUILD_DIR)/

## Run the tool with example parameters
run: build
	@echo "$(GREEN)Running $(BINARY_NAME) with test parameters...$(NC)"
	$(BUILD_DIR)/$(BINARY_NAME) -url https://lfoms1829vm-loggy.azurewebsites.net/api/CustomLog -duration 10s -rps 5

## Quick build and run
quick: build
	@echo "$(GREEN)Quick test run...$(NC)"
	$(BUILD_DIR)/$(BINARY_NAME) -url https://lfoms1829vm-loggy.azurewebsites.net/api/HttpTrigger -duration 5s -rps 2

## Docker build
docker:
	@echo "$(GREEN)Building Docker image...$(NC)"
	docker build -t requesty:latest .
	@echo "$(GREEN)✅ Docker image built: requesty:latest$(NC)"

## Generate test coverage report
coverage:
	@echo "$(GREEN)Generating coverage report...$(NC)"
	@mkdir -p coverage
	$(GO) test -coverprofile=coverage/coverage.out ./...
	$(GO) tool cover -html=coverage/coverage.out -o coverage/coverage.html
	@echo "$(GREEN)✅ Coverage report: coverage/coverage.html$(NC)"

## Check for updates
update-deps:
	@echo "$(GREEN)Checking for dependency updates...$(NC)"
	$(GO) list -u -m all
	@echo ""
	@echo "$(YELLOW)To update all dependencies, run:$(NC)"
	@echo "  go get -u ./..."
	@echo "  go mod tidy"

## Show version info
version:
	@echo "$(GREEN)Requesty Version Info$(NC)"
	@echo "  Version:    $(VERSION)"
	@echo "  Build Time: $(BUILD_TIME)"
	@echo "  Git Commit: $(GIT_COMMIT)"
	@echo "  Go Version: $(shell go version)"

.DEFAULT_GOAL := help
