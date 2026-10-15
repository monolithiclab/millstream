include common.mk
include common.go.mk

VULNCHECK_PACKAGES = ./cmd/... ./internal/...

.PHONY: docs
docs: ## Check Markdown links and 120-column prose
	@scripts/check-docs

.PHONY: lint-shell
lint-shell:
	@shellcheck scripts/check-docs

lint: lint-shell
ci: docs

# The Go SDK is its own module (sdk/go), outside the main module's ./...: test and lint it too.
SDK_DIR = sdk/go

.PHONY: test-sdk
test-sdk:
	@cd $(SDK_DIR) && go test -race -cover ./...

.PHONY: lint-sdk
lint-sdk:
	@cd $(SDK_DIR) && go tool -modfile=$(CURDIR)/tools/go.mod golangci-lint run --allow-serial-runners ./...
	@cd $(SDK_DIR) && go mod tidy -diff

test: test-sdk
lint: lint-sdk

# The e2e suite runs the built binary, so the test cache cannot see changes to the code it exercises.
.PHONY: test-e2e
test-e2e: ## Run the end-to-end and crash suite without the test cache
	@go test -race -count=1 ./internal/e2e/...

test: test-e2e
