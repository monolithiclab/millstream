include common.mk
include common.go.mk

VULNCHECK_PACKAGES = ./cmd/... ./internal/...
# The default build's coverage (make test enforces it); test-e2e prints the whole suite's, real processes included.
# 88 since M5: the Docker and git paths of publishing and the sandbox (internal/publish, internal/runtime/sandbox)
# are covered by the e2e build only.
COVERAGE_MIN = 88

# make test runs the default build without -race, to stay within its budget (scripts/test-times): the race detector
# runs in test-e2e. Coverage counts every package's statements in every test binary (-coverpkg): the store's
# conformance suite runs from packages of its own (internal/store/sqlstore/conformance/...), not from the package
# whose code it covers. golangci-lint sees one build: the e2e-tagged one holds every file.
TEST_FLAGS = -coverpkg=./...
GOLANGCI_ARGS = --build-tags e2e

.PHONY: docs
docs: ## Check Markdown links and 120-column prose
	@scripts/check-docs

.PHONY: lint-shell
lint-shell:
	@shellcheck scripts/check-docs scripts/test-times scripts/test-image scripts/test-deploy scripts/test-install
	@shellcheck -s sh scripts/install.sh

# No store query binds a bare LIMIT parameter, which has SQLite compile the statement again at every run: the
# queries use limitParam (internal/store/sqlstore/read.go).
.PHONY: lint-sql
lint-sql:
	@if grep -nEi 'LIMIT[[:space:]]+\?([[:space:]]|`|$$)' $(filter-out %_test.go,$(wildcard internal/store/*/*.go)); then \
		echo "a bare LIMIT ? above: use limitParam"; exit 1; fi

lint: lint-shell lint-sql
CI_EXTRA = docs

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
# build tag. test-e2e runs them all, tagged and untagged, under -race, and prints the coverage of the whole suite,
# except internal/e2e: its crash matrix, chaos suites and milestone scenarios take about ten minutes on ten cores, far
# past a CI runner's budget, so they are test-scenarios, run by hand and from the manual Scenarios workflow.
# They run the built binaries, so the test cache cannot see changes to the code they exercise: -count=1.
# PKG=<packages> runs those packages' tests alone, tagged and untagged, without -race or coverage: the iteration tier.
# The heavy suites (test-e2e, test-scenarios, test-ui-e2e, soak) run under a machine-wide lock, a file in $TMPDIR
# taken by scripts/e2elock (macOS has no flock(1)): several checkouts' runs queue, each saying whom it waits for,
# instead of overloading the machine together. Each public target takes the lock and runs its *-locked twin.
locked = @go run ./scripts/e2elock -- $(MAKE) --no-print-directory $@-locked

.PHONY: test-e2e test-e2e-locked
test-e2e: ## Run every test with -race, the e2e-tagged ones included, and print the full coverage; PKG=./internal/foo/... runs those alone, without -race
ifdef PKG
	@go test -tags e2e -count=1 $(PKG)
else
	$(locked)
endif

test-e2e-locked:
	@mkdir -p build
	@docker version >/dev/null 2>&1 || echo "Docker is unavailable: the Postgres tests skip (internal/store/postgres/pgtest)"
	@go test -race -count=1 -tags e2e -coverpkg=./... -coverprofile=build/cover-e2e.out $$(go list -tags e2e ./... | grep -v '/internal/e2e$$')
	@if [ -f .covignore ]; then \
		patterns=$$(grep -vE '^[[:space:]]*(#|$$)' .covignore || true); \
		if [ -n "$$patterns" ]; then \
			grep -vE -f <(printf '%s\n' "$$patterns") build/cover-e2e.out > build/cover-e2e.tmp && \
				mv build/cover-e2e.tmp build/cover-e2e.out; \
		fi; \
	fi
	@go tool cover -func=build/cover-e2e.out | awk '/^total:/ {print "Total coverage, e2e build: " $$NF}'
	@cd $(SDK_DIR) && go test -race -count=1 -tags e2e ./...

# internal/e2e alone (docs/development/testing.md, End-to-end tests), under -race. No go test timeout: a CI
# runner takes far longer than Go's default ten minutes, and the workflow's own timeout bounds it.
.PHONY: test-scenarios test-scenarios-locked
test-scenarios: ## Run internal/e2e under -race: the crash matrix, the failover and node chaos suites, the milestone scenarios
	$(locked)

test-scenarios-locked:
	@docker version >/dev/null 2>&1 || echo "Docker is unavailable: the Postgres halves and the M5 exit scenario skip"
	@go test -race -count=1 -tags e2e -timeout 0 ./internal/e2e

# The M5 exit scenario alone (internal/e2e/m5_exit_test.go, docs/development/m5-exit.md), verbose so that it prints what it
# measured: production evidence to accepted lessons and distill proposals in development. It needs Docker (the
# distill's sandbox).
.PHONY: test-exit test-exit-locked
test-exit: ## Run the M5 exit scenario alone and print its measures (docs/development/m5-exit.md; Docker for the sandbox)
	$(locked)

test-exit-locked:
	@go test -tags e2e -count=1 -run '^TestM5Exit$$' -v ./internal/e2e

# The soak (internal/e2e/soak_test.go): the Compose deployment of deploy/compose, as its own project, runs three
# pipelines for SOAK_DURATION while a chaos driver kills the active engine and the nodes every SOAK_CHAOS on average,
# then checks the store's invariants and writes build/soak/report.md. Not in make ci or test-e2e (it skips without
# SOAK_DURATION). A week: make soak SOAK_DURATION=168h.
SOAK_DURATION ?= 30m
SOAK_CHAOS ?= 2m

.PHONY: soak soak-locked
soak: ## Run the soak on the Compose deployment for SOAK_DURATION (30m), chaos every SOAK_CHAOS (2m); report in build/soak/report.md
	$(locked)

soak-locked: image
	@SOAK_DURATION=$(SOAK_DURATION) SOAK_CHAOS=$(SOAK_CHAOS) go test -tags e2e -count=1 -timeout 0 -v -run '^TestSoak$$' ./internal/e2e

.PHONY: test-times
test-times: ## Measure the default suite against its test-time budget (serial-equivalent, per test, per package)
	@scripts/test-times

# checkptr instrumentation of the transpiled SQLite (modernc.org), on under -race, triples the cost of every store
# test and checks none of our code.
export GOFLAGS ?= -gcflags=modernc.org/...=-d=checkptr=0

# Harness recordings are made against a fake model server, never the network.
.PHONY: recordings
recordings: ## Rewrite the agent harness recordings: internal/agent/testdata, the M2 fixture's, the listings example's and the improve and distill drafts'
	@go test ./internal/agent ./examples/m2-fixture ./examples/fakemodel -run TestRecordings -count=1 -update
	@go test -tags e2e ./internal/e2e -run 'TestImprove|TestDistill' -count=1 -update

# The web UI (ui/) builds and tests only in containers (D35): ui/images.mk pins the images by digest. node_modules
# and the npm cache live in named Docker volumes, so repeated runs skip the install; containers run as the host uid.
include ui/images.mk

UI_MODULES_VOLUME = millstream-ui-node-modules
UI_CACHE_VOLUME = millstream-ui-npm-cache
UI_DIST = internal/ui/dist
UI_DOCKER = docker run --rm --user $$(id -u):$$(id -g) -e HOME=/tmp -e CI -e npm_config_cache=/npm-cache \
	-e npm_config_update_notifier=false -v $(CURDIR):/work -v $(UI_MODULES_VOLUME):/work/ui/node_modules \
	-v $(UI_CACHE_VOLUME):/npm-cache -w /work/ui

# New volumes are created root-owned: hand them to the host uid once. npm ci runs only when package-lock.json
# differs from the one the volume was installed from.
.PHONY: ui-deps
ui-deps:
	@docker volume inspect $(UI_MODULES_VOLUME) $(UI_CACHE_VOLUME) >/dev/null 2>&1 || \
		docker run --rm -v $(UI_MODULES_VOLUME):/m -v $(UI_CACHE_VOLUME):/c $(UI_NODE_IMAGE) chown $$(id -u):$$(id -g) /m /c
	@$(UI_DOCKER) $(UI_NODE_IMAGE) sh -c 'cmp -s package-lock.json node_modules/.installed-lock 2>/dev/null || \
		{ npm ci --no-audit --no-fund && cp package-lock.json node_modules/.installed-lock; }'

.PHONY: ui
ui: ui-deps ## Build the web UI into internal/ui/dist, embedded by the next go build (node container)
	@find $(UI_DIST) -mindepth 1 ! -name placeholder.html -exec rm -rf {} +
	@$(UI_DOCKER) $(UI_NODE_IMAGE) npm run build

# The UI's API types, generated from the OpenAPI document and formatted by the repo's prettier into the container's
# /tmp: ui-types installs them, test-ui fails when the committed copy differs.
UI_TYPES = src/api/openapi.d.ts
UI_TYPES_GEN = npx openapi-typescript ../internal/api/openapi.yaml --empty-objects-unknown -o /tmp/openapi.d.ts >/dev/null && \
	npx prettier --stdin-filepath $(UI_TYPES) </tmp/openapi.d.ts >/tmp/openapi.fmt.d.ts

.PHONY: ui-types
ui-types: ui-deps ## Generate ui/src/api/openapi.d.ts from internal/api/openapi.yaml (openapi-typescript, node container)
	@$(UI_DOCKER) $(UI_NODE_IMAGE) sh -c '$(UI_TYPES_GEN) && cp /tmp/openapi.fmt.d.ts $(UI_TYPES)'

# FILES narrows a run to some paths under ui/ while working (make test-ui FILES='src/screens/improve src/api'): the
# whole project's typecheck, then ESLint, Prettier and Vitest on those paths alone, without coverage.
.PHONY: test-ui
test-ui: ui-deps ## Typecheck, lint, format check and Vitest (coverage, i18n checks) for the web UI; FILES= narrows it (node container)
ifeq ($(strip $(FILES)),)
	@$(UI_DOCKER) $(UI_NODE_IMAGE) sh -c '$(UI_TYPES_GEN) && { cmp -s /tmp/openapi.fmt.d.ts $(UI_TYPES) || \
		{ echo "ui/$(UI_TYPES) is stale: run make ui-types" >&2; exit 1; }; } && \
		npm run typecheck && npm run lint && npm run format:check && npm test'
else
	@$(UI_DOCKER) $(UI_NODE_IMAGE) sh -c 'npm run typecheck && npx eslint --max-warnings 0 $(FILES) && \
		npx prettier --check $(FILES) && npx vitest run --passWithNoTests $(FILES)'
endif

# The Playwright tests reach the build through the binary: millstream, cross-compiled for the container's Linux with
# the UI embedded, serves the API and the UI over a package.
UI_E2E_GOENV = GOOS=linux GOARCH=$$(docker version --format '{{.Server.Arch}}') CGO_ENABLED=0
UI_E2E_PLATFORM = linux-$$(docker version --format '{{.Server.Arch}}')
UI_PLAYWRIGHT = $(UI_DOCKER) --ipc=host $(UI_PLAYWRIGHT_IMAGE) npx playwright test

.PHONY: ui-e2e-bin
ui-e2e-bin: ui
	@mkdir -p .build
	@$(UI_E2E_GOENV) go build -o .build/millstream ./cmd/millstream

# The M3 reference demo's package, assembled in .build/demo from ui/e2e/demo-fixture/pipeline.yaml and the M2
# fixture's actions and lessons, its exec actions compiled for the container; .build/demo-recordings holds the M2
# fixture's recordings but L002's under the committed lessons, so that listing dead-letters (as crashRecordings does
# for the M2 crash reference, internal/e2e/m2_build_test.go).
DEMO = .build/demo
DEMO_ACTIONS = crawl:./examples/m2-fixture/actions/crawl rentability:./examples/m1-fixture/actions/rentability \
	report:./examples/m1-fixture/actions/report

.PHONY: ui-demo-package
ui-demo-package:
	@rm -rf $(DEMO) $(DEMO)-recordings && mkdir -p $(DEMO) $(DEMO)-recordings
	@cp -R examples/m2-fixture/actions examples/m2-fixture/lessons ui/e2e/demo-fixture/pipeline.yaml $(DEMO)/
	@for a in $(DEMO_ACTIONS); do name=$${a%%:*}; \
		$(UI_E2E_GOENV) go build -o $(DEMO)/actions/$$name/bin/$(UI_E2E_PLATFORM)/$$name $${a#*:} || exit 1; done
	@cp examples/m2-fixture/recordings/*.json $(DEMO)-recordings/
	@withheld=$$(grep -l 'Listing L002:' $(DEMO)-recordings/*.json | xargs grep -l 'by 10 points'); \
		[ $$(echo "$$withheld" | wc -w) -eq 1 ] || { echo "want one recording of L002, got: $$withheld" >&2; exit 1; }; \
		rm $$withheld

# test-ui-e2e runs every suite: every screen over ui/e2e/fixture (playwright.config.ts), the same server with auth.token
# set (playwright.auth.config.ts), then the reference demo (playwright.demo.config.ts); test-ui-demo runs the demo
# alone.
.PHONY: test-ui-e2e test-ui-e2e-locked
test-ui-e2e: ## Playwright and axe: every screen in both themes, languages and widths, then the M3 reference demo (Playwright container)
	$(locked)

test-ui-e2e-locked: ui-e2e-bin ui-demo-package
	@$(UI_PLAYWRIGHT)
	@$(UI_PLAYWRIGHT) --config playwright.auth.config.ts
	@$(UI_PLAYWRIGHT) --config playwright.demo.config.ts

.PHONY: test-ui-demo
test-ui-demo: ui-e2e-bin ui-demo-package ## The M3 reference demo alone, in English and French, axe on every state (Playwright container)
	@$(UI_PLAYWRIGHT) --config playwright.demo.config.ts

.PHONY: ui-shell
ui-shell: ui-deps ## Open a shell in the node container, ui/ mounted, Vite's port 5173 published
	@$(UI_DOCKER) -it -p 5173:5173 $(UI_NODE_IMAGE) bash

# The container image (Dockerfile): the UI built in the pinned node image, the binary with it embedded, distroless
# static at runtime. One image is the engine and the node alike. test-image smoke-tests it, test-deploy checks
# deploy/ (Compose and systemd); neither is in make ci, both run in CI's image job.
IMAGE = millstream:dev

.PHONY: image
image: ## Build the container image millstream:dev from source (UI and binary built inside, distroless runtime)
	@docker build --build-arg VERSION=$(VERSION) --build-arg REVISION=$$(git rev-parse HEAD) -t $(IMAGE) .

.PHONY: test-image
test-image: image ## Smoke-test the image: version, and millstream run serving the built UI over the e2e fixture
	@scripts/test-image $(IMAGE) $(UI_NODE_IMAGE)

.PHONY: test-deploy
test-deploy: ## Check deploy/: docker compose config, and systemd-analyze verify of the units in a container
	@scripts/test-deploy

# GoReleaser and syft are pinned in their own tools module (tools/release/go.mod), so their dependencies never move
# the linters'. syft is built onto the PATH GoReleaser runs it from. No signing and no publishing in a snapshot.
RELEASE_TOOLS = .build/release-tools
GORELEASER = PATH=$(CURDIR)/$(RELEASE_TOOLS):$$PATH goreleaser

.PHONY: release-tools
release-tools:
	@go build -modfile=tools/release/go.mod -o $(RELEASE_TOOLS)/ github.com/goreleaser/goreleaser/v2 github.com/anchore/syft/cmd/syft

.PHONY: release-snapshot
release-snapshot: release-tools ## Build a GoReleaser snapshot into dist/: archives, SBOMs, checksums, local images; publishes nothing
	@$(GORELEASER) release --snapshot --clean --skip=sign

# What CI checks on every pull request (its release job), so a tag only repeats it: the snapshot builds, its formula
# is written, and its amd64 image serves the built UI at the snapshot version (snapshot.version_template).
SNAPSHOT_VERSION = 0.0.0-snapshot
SNAPSHOT_IMAGE = ghcr.io/monolithiclab/millstream:$(SNAPSHOT_VERSION)-amd64

.PHONY: test-release
test-release: release-snapshot ## Build the GoReleaser snapshot and smoke-test it: Homebrew formula, image at its version
	@test -s dist/homebrew/millstream.rb || { echo "test-release: no Homebrew formula in dist/" >&2; exit 1; }
	@scripts/test-image $(SNAPSHOT_IMAGE) $(UI_NODE_IMAGE) $(SNAPSHOT_VERSION)
	@scripts/test-install $(SNAPSHOT_VERSION)

# The release itself, run only by .github/workflows/release.yml on a v* tag, with its credentials in the environment.
.PHONY: release
release: release-tools
	@$(GORELEASER) release --clean
