# yoke-sdk-go — repository justfile
#
# Go SDK (role: framework). Conventional targets per
# yoke-meta/docs/justfile-conventions.md. The wire bindings come from
# yoke-proto, resolved locally via the replace directive in go.mod.

set shell := ["bash", "-euo", "pipefail", "-c"]

# ---- Default ---------------------------------------------------------------

# List available recipes
default:
    @just --list

# ---- Conventional targets --------------------------------------------------

# Build all packages
build:
    go build ./...

# Run the tests
test:
    go test ./...

# Run the integration tests: drive the plugin SDK against a real Core process.
# Requires the sibling ../yoke-core to be built (or set YOKE_CORE_BIN); the tag
# keeps these out of the default `test` run.
test-integration:
    go test -tags=integration ./...

# Run static analysis
lint:
    go vet ./...

# Format the Go sources
fmt:
    gofmt -l -w .
