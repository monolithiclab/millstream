# The UI toolchain images, pinned by digest (D35): the one place they are named, for make and CI alike.
# Refresh: docker pull <tag> && docker inspect --format '{{index .RepoDigests 0}}' <tag>, then update the tag comment.
# The Playwright image must match the @playwright/test version in ui/package.json.

# node:22-bookworm-slim (Node 22.23.3, npm 10.9.9)
UI_NODE_IMAGE = node@sha256:c3de60bf2f9dd0ac6370e6117950ff62d6e339527e7472301c9c78a017978392
# mcr.microsoft.com/playwright:v1.64.1-noble
UI_PLAYWRIGHT_IMAGE = mcr.microsoft.com/playwright@sha256:7852dc5a7d72f5fe458a597d690f6c42fbc8e9e26ddfa95f8e6ef552558d9bbf
