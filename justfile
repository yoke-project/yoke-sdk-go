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

# Run static analysis
lint:
    go vet ./...

# Format the Go sources
fmt:
    gofmt -l -w .
