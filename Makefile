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
