# syntax=docker/dockerfile:1.7
###############################################################################
# sbw-server — the SBW control-plane GLOBAL BRAIN: owns YugabyteDB/etcd/Redpanda,
# does placement + the failover decision, and serves the rpc.ServerCoverer gRPC to
# the coverers. Pure Go, NO cgo, NO dataplane, NO GoBGP (the RIB tap is the
# COVERER's) → a tiny static binary on distroless.
#
# go.mod has ONE path replace, so the builder clones the sibling repo:
#   replace github.com/fivetime/sbw-contract => ../sbw-contract
# Refs are build-args; CI passes the triggering SHA. For private repos pass GH_TOKEN
# (a short-lived token) and it is used in the clone URLs.
###############################################################################
FROM golang:1.25-alpine AS builder

ARG SERVER_REPO=github.com/fivetime/sbw-server
ARG SERVER_REF=main
ARG CONTRACT_REPO=github.com/fivetime/sbw-contract
ARG CONTRACT_REF=main
ARG GH_TOKEN=""
ARG VERSION=dev
# CACHEBUST (the triggering commit SHA, passed as --build-arg CACHEBUST=${{ github.sha }}
# from CI) is referenced in the clone RUN below so that RUN's cache key changes every
# commit. Without it buildx caches the `git clone -b main` layer (the command string is
# constant) and every build re-serves the FIRST clone's STALE source — producing an
# identical, perpetually-old image no matter what is pushed.
ARG CACHEBUST=dev

RUN apk add --no-cache git ca-certificates
WORKDIR /src
RUN set -eux; \
    : "cachebust ${CACHEBUST}"; \
    auth=""; [ -n "$GH_TOKEN" ] && auth="x-access-token:${GH_TOKEN}@"; \
    git clone --depth 1 -b "$SERVER_REF"   "https://${auth}${SERVER_REPO}.git"   sbw-server; \
    git clone --depth 1 -b "$CONTRACT_REF" "https://${auth}${CONTRACT_REPO}.git" sbw-contract

WORKDIR /src/sbw-server
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/sbw-server ./cmd/sbw-server

###############################################################################
# runtime — distroless static (no shell, nonroot). The server needs the binary +
# CA certs (already in the base) + a config file mounted at runtime.
###############################################################################
FROM gcr.io/distroless/static:nonroot
ARG VERSION=dev
LABEL org.opencontainers.image.title="sbw-server" \
      org.opencontainers.image.description="Shared-bandwidth-pool server (global brain: store + placement + ServerCoverer gRPC)" \
      org.opencontainers.image.source="https://github.com/fivetime/sbw-server" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=builder /out/sbw-server /usr/local/bin/sbw-server
# ServerCoverer gRPC (coverers dial) 1792 · admin HTTP (BSS) 8080 · metrics 9101
EXPOSE 1792 8080 9101
ENTRYPOINT ["/usr/local/bin/sbw-server"]
CMD ["-config", "/etc/sbw/server.json"]
