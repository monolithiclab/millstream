include common.mk
include common.go.mk

VULNCHECK_PACKAGES = ./cmd/... ./internal/...
# The default build's coverage: the code only real processes reach (scaffold, conformance) is covered by test-e2e.
COVERAGE_MIN = 87

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

# make test runs the default build: unit tests, in-process, in a few hundred milliseconds of test time. Tests that
# build binaries, start processes or Docker, or run long (the internal/e2e package, the real-process twins of the
# in-process tests, long property runs) are behind the e2e build tag. They run the built binaries, so the test cache
# cannot see changes to the code they exercise: -count=1.
.PHONY: test-e2e
test-e2e: ## Run every test, the e2e-tagged ones included (slow: before closing a milestone, and in CI)
	@go test -race -count=1 -tags e2e ./...
	@cd $(SDK_DIR) && go test -race -count=1 -tags e2e ./...

# golangci-lint sees one build: lint-golangci lints the default one, lint-e2e the e2e-tagged files.
.PHONY: lint-e2e
lint-e2e:
	@$(TOOLS) golangci-lint run --allow-serial-runners --build-tags e2e ./...
	@cd $(SDK_DIR) && go tool -modfile=$(CURDIR)/tools/go.mod golangci-lint run --allow-serial-runners --build-tags e2e ./...

lint: lint-e2e

# A race-built test binary sleeps 1 s at exit (atexit_sleep_ms), a second per package; checkptr instrumentation of
# the transpiled SQLite (modernc.org) triples the cost of every store test under -race and checks none of our code.
export GORACE ?= atexit_sleep_ms=0
export GOFLAGS ?= -gcflags=modernc.org/...=-d=checkptr=0

# Harness recordings are made against a fake model server, never the network.
.PHONY: recordings
recordings: ## Rewrite the agent harness recordings: internal/agent/testdata and the M2 fixture's
	@go test ./internal/agent ./examples/m2-fixture -run TestRecordings -count=1 -update
