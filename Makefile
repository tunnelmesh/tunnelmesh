.PHONY: all build build-force test test-verbose test-coverage test-js test-js-coverage test-all clean install lint fmt hooks hooks-install vendor \
        dev-server dev-peer gen-keys release release-all push-release \
        docker-build docker-up docker-down docker-logs docker-clean docker-test \
        ghcr-login ghcr-build ghcr-push deploy deploy-plan deploy-destroy deploy-taint-coordinator \
        deploy-update deploy-update-node deploy-token \
        service-install service-uninstall service-start service-stop service-status

# Build variables
BINARY_NAME=tunnelmesh
SD_GENERATOR_NAME=tunnelmesh-prometheus-sd-generator
S3BENCH_NAME=tunnelmesh-s3bench
BUILD_DIR=bin
GO=go

# Version info (use short commit ID)
VERSION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-s -w -X main.Version=$(VERSION) -X main.Commit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)"

# Platforms for cross-compilation
PLATFORMS=linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64

# Platform-specific release binary paths
S3BENCH_BIN=./bin/release/tunnelmesh-s3bench-$(shell go env GOOS)-$(shell go env GOARCH)

all: build

# Build for current platform
build:
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/tunnelmesh

# Force rebuild even if binary exists
build-force: clean build

# Run tests (use -short to skip slow simulator tests)
test:
	TUNNELMESH_TEST=1 $(GO) test -short ./...

# Run all tests including slow simulator stress tests (used in CI merge jobs)
test-full:
	TUNNELMESH_TEST=1 $(GO) test ./...

test-race:
	TUNNELMESH_TEST=1 $(GO) test -short -race ./...

test-verbose:
	TUNNELMESH_TEST=1 $(GO) test -short -v ./...

test-coverage:
	TUNNELMESH_TEST=1 $(GO) test -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# JavaScript tests with Bun
JS_DIR = internal/coord/web

test-js:
	@if command -v bun >/dev/null 2>&1; then \
		cd $(JS_DIR) && bun test; \
	else \
		echo "Bun not installed. Install with: curl -fsSL https://bun.sh/install | bash"; \
		exit 1; \
	fi

test-js-coverage:
	@if command -v bun >/dev/null 2>&1; then \
		cd $(JS_DIR) && bun test --coverage; \
		echo "Coverage report: $(JS_DIR)/coverage/lcov.info"; \
	else \
		echo "Bun not installed. Install with: curl -fsSL https://bun.sh/install | bash"; \
		exit 1; \
	fi

# Run all tests (Go + JavaScript)
test-all: test test-js

# Clean build artifacts
clean:
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	$(GO) clean -cache >/dev/null 2>&1 || true

# Install to /usr/local/bin
install: build
	cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/

# Linting and formatting
lint:
	golangci-lint run ./...

fmt:
	$(GO) fmt ./...
	goimports -w .

# Vendoring
vendor:
	go mod vendor
	@echo "Vendor directory updated. Commit the changes."

# Git hooks with lefthook
LEFTHOOK=$(shell $(GO) env GOPATH)/bin/lefthook

hooks-install:
	@if ! command -v lefthook &> /dev/null && [ ! -f "$(LEFTHOOK)" ]; then \
		echo "Installing lefthook..."; \
		$(GO) install github.com/evilmartians/lefthook@latest; \
	fi
	$(LEFTHOOK) install
	@echo "Git hooks installed"

hooks:
	$(LEFTHOOK) run pre-commit

# Development helpers
dev-server: build
	@echo "=== Join command for peers ==="
	@TOKEN=$$(grep 'auth_token:' server.yaml 2>/dev/null | head -1 | sed 's/.*"\(.*\)".*/\1/'); \
	if [ -n "$$TOKEN" ]; then \
		echo "tunnelmesh join --server http://localhost:8080 --token $$TOKEN --context dev"; \
	else \
		echo "tunnelmesh join --server http://localhost:8080 --token <your-token> --context dev"; \
	fi
	@echo ""
	./$(BUILD_DIR)/$(BINARY_NAME) serve --config server.yaml

dev-peer: build
	sudo ./$(BUILD_DIR)/$(BINARY_NAME) join --config peer.yaml

# Generate SSH keys for testing
gen-keys:
	@mkdir -p ~/.tunnelmesh
	@ssh-keygen -t ed25519 -f ~/.tunnelmesh/id_ed25519 -N "" -C "tunnelmesh"
	@echo "Keys generated in ~/.tunnelmesh/"

# Build release binaries for all platforms
release-all:
	@mkdir -p $(BUILD_DIR)/release
	@for platform in $(PLATFORMS); do \
		GOOS=$${platform%/*} GOARCH=$${platform#*/} \
		$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/release/$(BINARY_NAME)-$${platform%/*}-$${platform#*/}$$([ "$${platform%/*}" = "windows" ] && echo ".exe") \
		./cmd/tunnelmesh; \
		echo "Built: $(BINARY_NAME)-$${platform%/*}-$${platform#*/}"; \
	done
	@for platform in $(PLATFORMS); do \
		GOOS=$${platform%/*} GOARCH=$${platform#*/} \
		$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/release/$(SD_GENERATOR_NAME)-$${platform%/*}-$${platform#*/}$$([ "$${platform%/*}" = "windows" ] && echo ".exe") \
		./cmd/$(SD_GENERATOR_NAME); \
		echo "Built: $(SD_GENERATOR_NAME)-$${platform%/*}-$${platform#*/}"; \
	done
	@for platform in $(PLATFORMS); do \
		GOOS=$${platform%/*} GOARCH=$${platform#*/} \
		$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/release/$(S3BENCH_NAME)-$${platform%/*}-$${platform#*/}$$([ "$${platform%/*}" = "windows" ] && echo ".exe") \
		./cmd/$(S3BENCH_NAME); \
		echo "Built: $(S3BENCH_NAME)-$${platform%/*}-$${platform#*/}"; \
	done
	@echo "Release binaries in $(BUILD_DIR)/release/"
	@$(MAKE) release-checksums

# Generate SHA256 checksums for release binaries
release-checksums:
	@echo "Generating checksums..."
	@cd $(BUILD_DIR)/release && shasum -a 256 tunnelmesh-darwin-* tunnelmesh-linux-* tunnelmesh-windows-* tunnelmesh-prometheus-sd-generator-* tunnelmesh-s3bench-* 2>/dev/null | sort -u > checksums.txt
	@echo "Checksums written to $(BUILD_DIR)/release/checksums.txt"

# Build release for current platform
release: build
	@mkdir -p $(BUILD_DIR)/release
	@cp $(BUILD_DIR)/$(BINARY_NAME) $(BUILD_DIR)/release/$(BINARY_NAME)-$(VERSION)
	@echo "Release binary: $(BUILD_DIR)/release/$(BINARY_NAME)-$(VERSION)"

# Tag defaults to commit ID, or override with TAG=v1.0.0
TAG ?= $(COMMIT)

# Create and push a release tag
push-release:
	@echo "Creating tag $(TAG)..."
	git tag -a $(TAG) -m "Release $(TAG)"
	git push origin $(TAG)
	@echo "Tag $(TAG) pushed"

# Create GitHub release (requires gh CLI)
github-release: release-all
	@if ! command -v gh &> /dev/null; then \
		echo "GitHub CLI (gh) not installed. Install with: brew install gh"; \
		exit 1; \
	fi
	gh release create $(TAG) $(BUILD_DIR)/release/* --title "$(TAG)" --generate-notes

# Show version info
version:
	@echo "Version: $(VERSION)"
	@echo "Commit:  $(COMMIT)"
	@echo "Build:   $(BUILD_TIME)"

# Docker Compose targets
DOCKER_COMPOSE=docker compose -f docker/docker-compose.yml --profile monitoring

docker-build:
	$(DOCKER_COMPOSE) build
	@docker system prune -f --filter "until=1h" >/dev/null 2>&1 || true

docker-up: docker-build
	@TUNNELMESH_TOKEN=$$(openssl rand -hex 32); \
	echo "Generated auth token: $$TUNNELMESH_TOKEN"; \
	printf "%s" "$$TUNNELMESH_TOKEN" > /tmp/tunnelmesh-docker-token; \
	chmod 600 /tmp/tunnelmesh-docker-token; \
	TUNNELMESH_TOKEN=$$TUNNELMESH_TOKEN $(DOCKER_COMPOSE) up -d; \
	echo "TunnelMesh Docker environment started"; \
	echo ""; \
	echo "  Coordinator:  http://localhost:8081"; \
	echo "  Grafana:      http://localhost:3000  (admin/admin)"; \
	echo "  Prometheus:   http://localhost:8081/prometheus/"; \
	echo ""; \
	echo "Use 'make docker-logs' to follow all logs"; \
	echo "Use 'make docker-logs-coords' to follow coordinator logs"; \
	echo "Use 'make docker-logs-monitoring' to follow monitoring logs"; \
	echo ""; \
	echo "=== Join from this machine ==="; \
	read -p "Run 'sudo tunnelmesh join --context docker'? [Y/n] " answer </dev/tty; \
	if [ "$$answer" != "n" ] && [ "$$answer" != "N" ]; then \
		sudo tunnelmesh context rm docker 2>/dev/null || true; \
		echo "Joining mesh in background..."; \
		sudo env "TUNNELMESH_TOKEN=$$(cat /tmp/tunnelmesh-docker-token)" tunnelmesh join http://localhost:8081 --context docker & \
		JOIN_PID=$$!; \
		echo "Waiting for mesh connection to establish..."; \
		sleep 8; \
		if ! kill -0 $$JOIN_PID 2>/dev/null; then \
			wait $$JOIN_PID && echo "Warning: tunnelmesh join exited early (possible error)" || echo "Warning: tunnelmesh join failed (exit $$?)"; \
		fi; \
	fi; \
	echo ""; \
	echo "=== Run s3bench against this environment ==="; \
	if [ "$${S3BENCH:-1}" = "0" ]; then \
		echo "Skipping s3bench (S3BENCH=0)"; \
	else \
		echo "Waiting 30 seconds for environment to stabilize... (skip with S3BENCH=0 make docker-up)"; \
		sleep 30; \
		$(S3BENCH_BIN) run alien_invasion \
			--coordinator http://localhost:8081 \
			--auth-token "$$TUNNELMESH_TOKEN" \
			--time-scale 500 \
			--json results.json \
			--accordion || true; \
	fi

docker-logs:
	$(DOCKER_COMPOSE) logs -f

# View coordinator logs only
docker-logs-coords:
	$(DOCKER_COMPOSE) logs -f coord-1 coord-2 coord-3

# View monitoring stack logs only (Prometheus, Grafana, Loki, sd-generator)
docker-logs-monitoring:
	$(DOCKER_COMPOSE) logs -f prometheus grafana loki sd-generator

docker-down:
	$(DOCKER_COMPOSE) down
	@rm -f /tmp/tunnelmesh-docker-token

docker-clean: docker-down
	$(DOCKER_COMPOSE) down -v --rmi local
	@echo "Docker environment cleaned"

docker-test: docker-build
	@echo "Starting Docker test environment..."
	@TUNNELMESH_TOKEN=$$(openssl rand -hex 32); \
	printf "%s" "$$TUNNELMESH_TOKEN" > /tmp/tunnelmesh-docker-token; \
	chmod 600 /tmp/tunnelmesh-docker-token; \
	TUNNELMESH_TOKEN=$$TUNNELMESH_TOKEN $(DOCKER_COMPOSE) up -d
	@echo "Waiting for mesh to stabilize (20s)..."
	@sleep 20
	@echo "\n=== Mesh Status ==="
	$(DOCKER_COMPOSE) logs --tail=50 alice bob charlie 2>&1 | grep -E "(SUCCESS|FAILED|Pinging)" || true
	@echo "\n=== Running containers ==="
	$(DOCKER_COMPOSE) ps

# GitHub Container Registry targets
GHCR_REPO ?= ghcr.io/zombar/tunnelmesh
GHCR_TAG ?= $(COMMIT)

# Terraform targets
TF_DIR = terraform

deploy-token:
	@cd $(TF_DIR) && terraform output -raw auth_token

deploy-init:
	cd $(TF_DIR) && terraform init

deploy-plan:
	cd $(TF_DIR) && terraform plan

deploy:
	@echo "Deploying TunnelMesh infrastructure to DigitalOcean..."
	cd $(TF_DIR) && terraform apply -auto-approve
	@echo ""
	@echo "=== Deployment Complete ==="
	@cd $(TF_DIR) && terraform output -json nodes | jq -r 'to_entries[] | "\(.key): \(.value.ip) - \(.value.hostname)"'
	@echo ""
	@echo "=== Join Command ==="
	@TOKEN=$$(cd $(TF_DIR) && terraform output -raw auth_token); \
	URL=$$(cd $(TF_DIR) && terraform output -raw coordinator_url | sed 's|https://||'); \
	PORT=$$(cd $(TF_DIR) && terraform output -raw external_api_port 2>/dev/null || echo 8443); \
	echo "sudo env TUNNELMESH_TOKEN=$$TOKEN tunnelmesh join $$URL:$$PORT"

# Update tunnelmesh binary on all deployed nodes using built-in update command
# Usage: make deploy-update [BINARY_VERSION=latest]
BINARY_VERSION ?= latest

deploy-update:
	@echo "Updating tunnelmesh on all deployed nodes..."
	@cd $(TF_DIR) && terraform output -json node_ips 2>/dev/null | jq -r 'to_entries[] | "\(.key) \(.value)"' | \
	while read -r NAME IP; do \
		echo ""; \
		echo "=== Updating $$NAME ($$IP) ==="; \
		if [ "$(BINARY_VERSION)" = "latest" ]; then \
			ssh -n -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$$IP \
				'/usr/local/bin/tunnelmesh update' || echo "Failed to update $$NAME"; \
		else \
			ssh -n -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$$IP \
				'/usr/local/bin/tunnelmesh update $(BINARY_VERSION)' || echo "Failed to update $$NAME"; \
		fi; \
	done; \
	echo ""; \
	echo "=== Update complete ==="

# Update a single node by name using built-in update command
# Usage: make deploy-update-node NODE=tunnelmesh [BINARY_VERSION=latest]
deploy-update-node:
	@if [ -z "$(NODE)" ]; then \
		echo "Error: NODE not specified. Usage: make deploy-update-node NODE=tunnelmesh"; \
		exit 1; \
	fi
	@IP=$$(cd $(TF_DIR) && terraform output -json node_ips 2>/dev/null | jq -r '.["$(NODE)"]'); \
	if [ -z "$$IP" ] || [ "$$IP" = "null" ]; then \
		echo "Error: Node '$(NODE)' not found in terraform state"; \
		exit 1; \
	fi; \
	echo "=== Updating $(NODE) ($$IP) ==="; \
	if [ "$(BINARY_VERSION)" = "latest" ]; then \
		ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$$IP \
			'/usr/local/bin/tunnelmesh update'; \
	else \
		ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@$$IP \
			'/usr/local/bin/tunnelmesh update $(BINARY_VERSION)'; \
	fi

ghcr-login:
	@echo "Logging in to GitHub Container Registry..."
	@echo "Use: echo \$$GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin"

ghcr-build:
	@echo "Building Docker image for ghcr.io (linux/amd64)..."
	docker buildx build \
		--platform linux/amd64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(GHCR_REPO):$(GHCR_TAG) \
		-t $(GHCR_REPO):latest \
		-f docker/Dockerfile \
		--load .
	@echo "Built: $(GHCR_REPO):$(GHCR_TAG)"
	@docker system prune -f --filter "until=1h" >/dev/null 2>&1 || true

ghcr-push:
	@echo "Building and pushing to GitHub Container Registry (linux/amd64)..."
	docker buildx build \
		--platform linux/amd64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(GHCR_REPO):$(GHCR_TAG) \
		-t $(GHCR_REPO):latest \
		-f docker/Dockerfile \
		--push .
	@echo "Pushed: $(GHCR_REPO):$(GHCR_TAG) and :latest"
	@docker system prune -f --filter "until=1h" >/dev/null 2>&1 || true

# Service management targets (require sudo on Linux/macOS)
SERVICE_MODE ?= join
SERVICE_CONFIG ?= $(if $(filter serve,$(SERVICE_MODE)),/etc/tunnelmesh/server.yaml,/etc/tunnelmesh/peer.yaml)

service-install: build
	@echo "Installing TunnelMesh as system service (mode: $(SERVICE_MODE))..."
	sudo ./$(BUILD_DIR)/$(BINARY_NAME) service install --mode $(SERVICE_MODE) --config $(SERVICE_CONFIG)

service-uninstall:
	@echo "Uninstalling TunnelMesh service..."
	sudo ./$(BUILD_DIR)/$(BINARY_NAME) service uninstall

service-start:
	@echo "Starting TunnelMesh service..."
	sudo ./$(BUILD_DIR)/$(BINARY_NAME) service start

service-stop:
	@echo "Stopping TunnelMesh service..."
	sudo ./$(BUILD_DIR)/$(BINARY_NAME) service stop

service-status:
	./$(BUILD_DIR)/$(BINARY_NAME) service status

service-logs:
	./$(BUILD_DIR)/$(BINARY_NAME) service logs --follow

# Help
help:
	@echo "Available targets:"
	@echo "  build          - Build for current platform"
	@echo "  build-force    - Clean and rebuild"
	@echo "  test           - Run Go tests with race detector"
	@echo "  test-verbose   - Run Go tests with verbose output"
	@echo "  test-coverage  - Run Go tests and generate coverage report"
	@echo "  test-js        - Run JavaScript tests with Bun"
	@echo "  test-js-coverage - Run JS tests with coverage (lcov for Codecov)"
	@echo "  test-all       - Run all tests (Go + JavaScript)"
	@echo "  clean          - Remove build artifacts"
	@echo "  install        - Install to /usr/local/bin"
	@echo "  lint           - Run golangci-lint"
	@echo "  fmt            - Format code"
	@echo "  hooks-install  - Install lefthook git hooks"
	@echo "  hooks          - Run pre-commit hooks manually"
	@echo "  release        - Build release binary for current platform"
	@echo "  release-all    - Build release binaries for all platforms"
	@echo "  push-release   - Create and push git tag (default: commit ID, or TAG=v1.0.0)"
	@echo "  github-release - Create GitHub release (default: commit ID, or TAG=v1.0.0)"
	@echo "  dev-server     - Build and run coordination server"
	@echo "  dev-peer       - Build and run peer (with sudo)"
	@echo "  gen-keys       - Generate SSH keys for testing"
	@echo "  version        - Show version info"
	@echo ""
	@echo "Service targets (require sudo):"
	@echo "  service-install   - Install as system service (SERVICE_MODE=join|serve)"
	@echo "  service-uninstall - Remove system service"
	@echo "  service-start     - Start the service"
	@echo "  service-stop      - Stop the service"
	@echo "  service-status    - Show service status"
	@echo "  service-logs      - Follow service logs"
	@echo ""
	@echo "Docker targets:"
	@echo "  docker-build        - Build Docker images"
	@echo "  docker-up           - Start 2 coordinators + 5 client mesh environment"
	@echo "  docker-down         - Stop and remove containers"
	@echo "  docker-logs         - Follow container logs"
	@echo "  docker-logs-coords  - Follow coordinator logs only"
	@echo "  docker-scale-coords - Scale coordinators to N replicas"
	@echo "  docker-clean        - Remove containers and images"
	@echo "  docker-test         - Build, start, and show mesh status"
	@echo ""
	@echo "GitHub Container Registry targets:"
	@echo "  ghcr-login     - Show login instructions for ghcr.io"
	@echo "  ghcr-build     - Build image for ghcr.io (GHCR_TAG=version)"
	@echo "  ghcr-push      - Build and push to ghcr.io"
	@echo ""
	@echo "Deployment targets:"
	@echo "  deploy-init            - Initialize terraform"
	@echo "  deploy-plan            - Show terraform plan for deployment"
	@echo "  deploy                 - Deploy infrastructure to DigitalOcean"
	@echo "  deploy-destroy         - Destroy deployed infrastructure"
	@echo "  deploy-taint-coordinator - Taint coordinator droplet for recreation"
	@echo "  deploy-update          - Update tunnelmesh on all nodes (BINARY_VERSION=latest)"
	@echo "  deploy-update-node     - Update single node (NODE=name BINARY_VERSION=latest)"
	@echo "  deploy-token           - Print the mesh auth token"
