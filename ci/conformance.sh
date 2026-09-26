#!/usr/bin/env bash
# The conformance suite against this family's plugin harness: L2, which blocks.
#
# The suite and the Core it drives come from `yoke` through the module proxy, never from a sibling;
# the harness is built from this checkout. Usage: conformance.sh [yoke version, default main]
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="${1:-main}"
bin="$(mktemp -d)"
trap 'rm -rf "$bin"' EXIT

# One version for both, resolved once, so the suite and the Core come from one commit.
# A branch is resolved at its source, since the proxy may still hold an older answer for it.
resolved="$(cd "$bin" && GOPROXY=direct GOFLAGS=-mod=mod go list -m -f '{{.Version}}' "github.com/yoke-project/yoke@$version")"
GOBIN="$bin" go install "github.com/yoke-project/yoke/cmd/yoke-core@$resolved"
GOBIN="$bin" go install "github.com/yoke-project/yoke/cmd/yoke-conformance@$resolved"
echo "conformance: yoke $resolved"
(cd "$root" && go build -o "$bin/yoke-go-plugin-harness" ./cmd/yoke-go-plugin-harness)

"$bin/yoke-conformance" --core "$bin/yoke-core" --harness "$bin/yoke-go-plugin-harness"
