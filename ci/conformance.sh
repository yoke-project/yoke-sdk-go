#!/usr/bin/env bash
# The conformance suite against each of this family's harnesses: L2, which blocks.
#
# The suite and the Core it drives come from `yoke` through the module proxy, never from a sibling;
# the harness is built from this checkout. With CONFORMANCE_SUITE naming a directory holding
# `yoke-conformance` and `yoke-core`, that pair is run instead, and with CONFORMANCE_REPORTS naming a
# directory, each harness's run writes its report there as <harness>.json — which is how the release of
# `yoke` runs this script at this family's newest publication.
# Usage: conformance.sh [yoke version, default main]
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="${1:-main}"
bin="$(mktemp -d)"
trap 'rm -rf "$bin"' EXIT

suite="$bin"
if [[ -n "${CONFORMANCE_SUITE:-}" ]]; then
  # A caller that brings its own suite and Core — the release of `yoke`, with the pair of its own commit —
  # is answerable for them, and nothing is installed.
  suite="$CONFORMANCE_SUITE"
  echo "conformance: the suite at $suite"
else
  # One version for both, resolved once, so the suite and the Core come from one commit.
  # A branch is resolved at its source, since the proxy may still hold an older answer for it, with git's
  # automatic collection off, which would otherwise rewrite the shallow clone under go's second fetch.
  resolved="$(cd "$bin" && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=gc.auto GIT_CONFIG_VALUE_0=0 GOPROXY=direct GOFLAGS=-mod=mod go list -m -f '{{.Version}}' "github.com/yoke-project/yoke@$version")"
  GOBIN="$bin" go install "github.com/yoke-project/yoke/cmd/yoke-core@$resolved"
  GOBIN="$bin" go install "github.com/yoke-project/yoke/cmd/yoke-conformance@$resolved"
  echo "conformance: yoke $resolved"
fi

# run_suite <harness>: the suite against one harness, writing its report where a caller asks for one.
run_suite() {
  local report=()
  if [[ -n "${CONFORMANCE_REPORTS:-}" ]]; then
    mkdir -p "$CONFORMANCE_REPORTS"
    report=(--report "$CONFORMANCE_REPORTS/$(basename "$1").json")
  fi
  "$suite/yoke-conformance" --core "$suite/yoke-core" "${report[@]}" --harness "$1"
}
(cd "$root" && go build -o "$bin/yoke-go-plugin-harness" ./cmd/yoke-go-plugin-harness)
(cd "$root" && go build -o "$bin/yoke-go-admin-harness" ./cmd/yoke-go-admin-harness)
(cd "$root" && go build -o "$bin/yoke-go-interface-harness" ./cmd/yoke-go-interface-harness)

# Each contract the family offers, against its own harness; every one runs, and any failing fails.
status=0
run_suite "$bin/yoke-go-plugin-harness" || status=1
run_suite "$bin/yoke-go-admin-harness" || status=1
run_suite "$bin/yoke-go-interface-harness" || status=1
exit "$status"
