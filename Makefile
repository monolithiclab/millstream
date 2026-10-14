include common.mk

# Docs-only until the first implementation plan lands; the Go targets arrive with cmd/strand (see lab-go-app).

.PHONY: build
build: ## Check that every relative Markdown link resolves and prose wraps at 120 columns
	@scripts/check-docs

.PHONY: lint
lint: ## Shellcheck the scripts
	@shellcheck scripts/check-docs

.PHONY: ci
ci: lint build ## The gate: lint and build (what CI runs)

.PHONY: clean
clean: ## Remove build output
	@rm -rf .build
