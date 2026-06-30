#!/usr/bin/env bash
# CI pipeline: the single source of truth for checks. The GitHub Actions workflow
# and local `bash scripts/ci.sh` both run this, so green local means green CI.
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n==> %s\n' "$*"; }

step "gofmt"
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
  echo "files need gofmt:" >&2
  echo "$unformatted" >&2
  exit 1
fi

step "go vet"
go vet ./...

step "golangci-lint formatters"
if command -v golangci-lint >/dev/null 2>&1; then
  fmt_diff=$(golangci-lint fmt --diff 2>&1) || {
    echo "$fmt_diff" >&2
    exit 1
  }
  if [ -n "$fmt_diff" ]; then
    echo "files need gofumpt/goimports:" >&2
    echo "$fmt_diff" >&2
    exit 1
  fi
elif [ "${CI:-}" = "true" ]; then
  echo "golangci-lint is required in CI but not installed" >&2
  exit 1
else
  echo "golangci-lint not installed locally; skipping formatters (CI enforces them)" >&2
fi

step "golangci-lint"
if command -v golangci-lint >/dev/null 2>&1; then
  golangci-lint run --timeout 5m
elif [ "${CI:-}" = "true" ]; then
  echo "golangci-lint is required in CI but not installed" >&2
  exit 1
else
  echo "golangci-lint not installed locally; skipping (CI enforces it)" >&2
fi

step "go test (race + coverage)"
go test -race -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | tail -1

step "go build"
go build ./...

step "ARM64 release artifact"
mkdir -p dist
commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
ldflags="-X github.com/fivetime/sbw-contract/buildinfo.Commit=${commit}"
GOOS=linux GOARCH=arm64 go build -ldflags "$ldflags" -o dist/sbw-server-linux-arm64 ./cmd/sbw-server
ls -l dist/

printf '\nCI pipeline passed.\n'
