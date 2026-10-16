include common.mk
include common.go.mk

VULNCHECK_PACKAGES = ./cmd/... ./internal/...
# The default build's coverage (make test enforces it); test-e2e prints the whole suite's, real processes included.
COVERAGE_MIN = 89

# make test runs the default build without -race, to stay within its budget (scripts/test-times): the race detector
# runs in test-e2e. golangci-lint sees one build: the e2e-tagged one holds every file.
TEST_FLAGS =
GOLANGCI_ARGS = --build-tags e2e

.PHONY: docs
docs: ## Check Markdown links and 120-column prose
	@scripts/check-docs

.PHONY: lint-shell
lint-shell:
	@shellcheck scripts/check-docs scripts/test-times

lint: lint-shell
ci: docs

# The Go SDK is its own module (sdk/go), outside the main module's ./...: test and lint it too.
SDK_DIR = sdk/go

.PHONY: test-sdk
test-sdk:
	@cd $(SDK_DIR) && go test -cover ./...

.PHONY: lint-sdk
lint-sdk:
	@cd $(SDK_DIR) && go tool -modfile=$(CURDIR)/tools/go.mod golangci-lint run --allow-serial-runners $(GOLANGCI_ARGS) ./...
	@cd $(SDK_DIR) && go mod tidy -diff

test: test-sdk
lint: lint-sdk

# make test runs the default build: in-process unit tests within the budget scripts/test-times checks. Tests that
# build binaries, start processes or Docker, drive the CLI or the engine end to end, or run long are behind the e2e
# build tag. test-e2e runs them all, tagged and untagged, under -race, and prints the coverage of the whole suite.
# They run the built binaries, so the test cache cannot see changes to the code they exercise: -count=1.
.PHONY: test-e2e
test-e2e: ## Run every test with -race, the e2e-tagged ones included, and print the full coverage (slow: CI, milestones)
	@mkdir -p build
	@go test -race -count=1 -tags e2e -coverprofile=build/cover-e2e.out ./...
	@if [ -f .covignore ]; then \
		patterns=$$(grep -vE '^[[:space:]]*(#|$$)' .covignore || true); \
		if [ -n "$$patterns" ]; then \
			grep -vE -f <(printf '%s\n' "$$patterns") build/cover-e2e.out > build/cover-e2e.tmp && \
				mv build/cover-e2e.tmp build/cover-e2e.out; \
		fi; \
	fi
	@go tool cover -func=build/cover-e2e.out | awk '/^total:/ {print "Total coverage, e2e build: " $$NF}'
	@cd $(SDK_DIR) && go test -race -count=1 -tags e2e ./...

.PHONY: test-times
test-times: ## Measure the default suite against its test-time budget (serial-equivalent, per test, per package)
	@scripts/test-times

# checkptr instrumentation of the transpiled SQLite (modernc.org), on under -race, triples the cost of every store
# test and checks none of our code.
export GOFLAGS ?= -gcflags=modernc.org/...=-d=checkptr=0

# Harness recordings are made against a fake model server, never the network.
.PHONY: recordings
recordings: ## Rewrite the agent harness recordings: internal/agent/testdata and the M2 fixture's
	@go test ./internal/agent ./examples/m2-fixture -run TestRecordings -count=1 -update
