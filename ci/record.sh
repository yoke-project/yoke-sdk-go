#!/usr/bin/env bash
# Assembles the record of a run from what that run left behind, and writes it to standard output.
# It runs nothing: a record is evidence of a run that already happened.
#
# The tool comes from the module proxy and never from a sibling: no clone, no checkout, and the
# record names the version the proxy resolved. From 0.2 it is the binary `develop` installs.
# Usage: record.sh [results directory]
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
results="${1:-$root/.results}"
module=github.com/yoke-project/yoke

for each in checks.txt started finished; do
  [[ -f "$results/$each" ]] || { echo "record: the run left no $each in $results" >&2; exit 1; }
done

# The architecture as the environment's dimension names it.
case "$(uname -m)" in
  x86_64) architecture=amd64 ;;
  aarch64 | arm64) architecture=arm64 ;;
  *) architecture="$(uname -m)" ;;
esac

# A run that tested Go code left its runner's output beside the checks'.
go_results=()
[[ -f "$results/go.json" ]] && go_results=(--results "$results/go.json")

# The version the proxy resolves carries the commit, so a record says exactly what wrote it.
version="$(go list -m -f '{{.Version}}' "$module@main" 2>/dev/null)" || version=main

go run "$module/cmd/yoke-verify@main" record \
  --level L1 \
  --tier reference \
  --repository yoke-sdk-go \
  --environment "architecture=$architecture" \
  --started "$(tr -d '[:space:]' < "$results/started")" \
  --finished "$(tr -d '[:space:]' < "$results/finished")" \
  --ran "yoke-verify=$version@${version##*-}" \
  --results "$results/checks.txt" \
  ${go_results[@]+"${go_results[@]}"} \
  "$root"
