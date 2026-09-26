#!/usr/bin/env bash
# The checks described by the-release-run.std.md, one function per case.

rr_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
rr_workflow="$rr_root/.github/workflows/release.yml"

# std: yoke-sdk-go:the-release-run.01
check_the_verb_publishes_with_the_published_release_verb() {
  local recipe
  recipe="$(cd "$rr_root" && just --show release 2>/dev/null)"
  grep -q 'github.com/yoke-project/yoke/cmd/yoke-release@' <<<"$recipe" || { echo "the verb does not run yoke-release from the proxy: $recipe"; return 1; }
  grep -q -- '-modules-only' <<<"$recipe" || { echo "the verb does not publish modules only"; return 1; }
  grep -qE '\.\./|replace ' <<<"$recipe" && { echo "the verb reaches a sibling"; return 1; }
  return 0
}

# std: yoke-sdk-go:the-release-run.02
check_a_release_tag_runs_the_verb_and_keeps_its_lines() {
  [[ -f "$rr_workflow" ]] || { echo "no release workflow"; return 1; }
  grep -qE "tags: \['v\*'\]" "$rr_workflow" || { echo "it does not run on a tag v*"; return 1; }
  grep -qE 'fetch-depth: 0' "$rr_workflow" || { echo "it does not read the whole history"; return 1; }
  grep -qE 'just release > manifest-lines\.jsonl' "$rr_workflow" || { echo "it does not keep the verb's lines"; return 1; }
  grep -qE 'name: manifest-lines' "$rr_workflow" || { echo "the lines are not the artifact manifest-lines"; return 1; }
}

# std: yoke-sdk-go:the-release-run.03
check_an_untagged_commit_publishes_nothing() {
  local out errs code
  errs="$(mktemp)"
  out="$(cd "$rr_root" && just release 2> "$errs")"; code=$?
  local said; said="$(cat "$errs")"; rm -f "$errs"
  (( code == 0 )) || { echo "the verb exited $code: $said"; return 1; }
  [[ -z "$out" ]] || { echo "the verb emitted: $out"; return 1; }
  grep -q "nothing is published" <<<"$said" || { echo "the verb did not say so: $said"; return 1; }
}
