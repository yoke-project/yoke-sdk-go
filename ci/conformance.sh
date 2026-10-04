#!/usr/bin/env bash
# The conformance suite against each of this family's harnesses: L2, which blocks.
#
# The suite and the Core it drives come from `yoke` through the module proxy, never from a sibling;
# the harness is built from this checkout. Usage: conformance.sh [yoke version, default main]
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="${1:-main}"
bin="$(mktemp -d)"
trap 'rm -rf "$bin"' EXIT

# One version for both, resolved once, so the suite and the Core come from one commit.
# A branch is resolved at its source, since the proxy may still hold an older answer for it, with git's
# automatic collection off, which would otherwise rewrite the shallow clone under go's second fetch.
resolved="$(cd "$bin" && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=gc.auto GIT_CONFIG_VALUE_0=0 GOPROXY=direct GOFLAGS=-mod=mod go list -m -f '{{.Version}}' "github.com/yoke-project/yoke@$version")"
GOBIN="$bin" go install "github.com/yoke-project/yoke/cmd/yoke-core@$resolved"
GOBIN="$bin" go install "github.com/yoke-project/yoke/cmd/yoke-conformance@$resolved"
echo "conformance: yoke $resolved"
(cd "$root" && go build -o "$bin/yoke-go-plugin-harness" ./cmd/yoke-go-plugin-harness)
(cd "$root" && go build -o "$bin/yoke-go-admin-harness" ./cmd/yoke-go-admin-harness)
(cd "$root" && go build -o "$bin/yoke-go-interface-harness" ./cmd/yoke-go-interface-harness)

# Each contract the family offers, against its own harness; every one runs, and any failing fails.
status=0
"$bin/yoke-conformance" --core "$bin/yoke-core" --harness "$bin/yoke-go-plugin-harness" || status=1
"$bin/yoke-conformance" --core "$bin/yoke-core" --harness "$bin/yoke-go-admin-harness" || status=1
"$bin/yoke-conformance" --core "$bin/yoke-core" --harness "$bin/yoke-go-interface-harness" || status=1
exit "$status"
