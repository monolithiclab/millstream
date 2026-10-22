# The millstream image, built from source: make image (tag millstream:dev) and the CI image job. The same binary is
# the engine (serve, run) and a node (node --join), so one image serves both roles.
#
# Three stages: the web UI built in the node image make ui uses, the Go binary cross-compiled with that UI embedded,
# and distroless static as the runtime: no shell, no package manager, uid 65532. Every base is pinned by digest
# (lint-pins); the tag beside each is for reading only. Refresh: docker buildx imagetools inspect <tag>.
# GoReleaser does not use this file: it packages its own binaries with Dockerfile.release.

# node:22-bookworm-slim, the digest of UI_NODE_IMAGE in ui/images.mk: keep the two equal. The UI stage runs on the
# build platform, since its output is the same for every target.
FROM --platform=$BUILDPLATFORM node:22-bookworm-slim@sha256:c3de60bf2f9dd0ac6370e6117950ff62d6e339527e7472301c9c78a017978392 AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

# golang:1.27.3-trixie, go.mod's toolchain line exactly: GOTOOLCHAIN=local refuses to download another.
FROM --platform=$BUILDPLATFORM golang:1.27.3-trixie@sha256:fbb7ae40b62ff8afe31662df570d39b4f5052ac32b90dae8e60d7657a20a74f4 AS build
ARG TARGETOS TARGETARCH
ARG VERSION=dev
ENV GOTOOLCHAIN=local CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
COPY sdk/go/go.mod ./sdk/go/
RUN --mount=type=cache,target=/go/pkg/mod go mod download && go mod verify
COPY . .
COPY --from=ui /src/internal/ui/dist/ ./internal/ui/dist/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/millstream ./cmd/millstream

# gcr.io/distroless/static-debian13:nonroot
FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="millstream" \
      org.opencontainers.image.description="Millstream pipeline platform: engine, node and web UI in one binary" \
      org.opencontainers.image.source="https://github.com/monolithiclab/millstream" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=build /out/millstream /millstream
# The working directory, the nonroot user's home, holds what a command keeps beside it: run's .millstream/ store.
WORKDIR /home/nonroot
# run and the API record review decisions under $USER when no token names a reviewer (spec section 12.1).
ENV USER=millstream
USER 65532:65532
EXPOSE 7070
ENTRYPOINT ["/millstream"]
