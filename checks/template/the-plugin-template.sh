#!/usr/bin/env bash
# The checks described by the-plugin-template.std.md. The tree is generated as the composer generates it:
# the archive the release verb makes, unpacked, its two values substituted in contents and in names.

pt_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# pt_generate <dir>: the template, archived from this checkout's tree and generated into <dir> as the
# greeter.
pt_generate() {
  local dir="$1" path
  mkdir -p "$dir"
  # The tree as it stands, staged or not, archived as git archives a tag's.
  (cd "$pt_root" && git ls-files -z --cached --others --exclude-standard template \
    | tar --null -T - -cf -) | tar -xf - -C "$dir" || return 1
  # Names first, deepest first, then contents.
  while IFS= read -r -d '' path; do
    local name; name="$(basename "$path")"
    local renamed="${name//@YOKE_ID@/com.example.greeter}"; renamed="${renamed//@YOKE_PROGRAM@/greeter}"
    [[ "$renamed" != "$name" ]] && mv "$path" "$(dirname "$path")/$renamed"
  done < <(find "$dir/template" -depth -print0)
  find "$dir/template" -type f -print0 | xargs -0 sed -i 's/@YOKE_ID@/com.example.greeter/g; s/@YOKE_PROGRAM@/greeter/g'
}

# std: yoke-sdk-go:the-plugin-template.01
check_the_template_holds_the_plugin_projects_tree() {
  local t="$pt_root/template" missing="" each
  for each in yoke-plugin.yaml plugin/go.mod plugin/main.go 'plugin/@YOKE_ID@.yaml' try .github/workflows/manifest.yml; do
    [[ -e "$t/$each" ]] || missing+=" $each"
  done
  [[ -z "$missing" ]] || { echo "the template has no$missing"; return 1; }
  grep -qx 'language: go' "$t/yoke-plugin.yaml" || { echo "yoke-plugin.yaml does not name language: go"; return 1; }
  [[ ! -e "$t/try/yoke.yaml" ]] || { echo "try/ holds a descriptor, which new plugin makes"; return 1; }
  grep -q -- '--manifest' "$t/.github/workflows/manifest.yml" && grep -q 'diff' "$t/.github/workflows/manifest.yml" \
    || { echo "the workflow does not compare the Manifest printed with the one kept"; return 1; }
  local others
  others="$( { grep -rhoE '@YOKE_[A-Z_]+@' "$t"; find "$t" | grep -oE '@YOKE_[A-Z_]+@'; } | sort -u | grep -vxE '@YOKE_(ID|PROGRAM)@')"
  [[ -z "$others" ]] || { echo "the template writes values nothing substitutes: $others"; return 1; }
}

# std: yoke-sdk-go:the-plugin-template.02
check_the_template_requires_this_releases_library() {
  local line required
  line="$(sed -n 's/^const SDKLine = "yoke-sdk-go \(.*\)"$/\1/p' "$pt_root/base/base.go")"
  required="$(awk '$1 == "require" && $2 == "github.com/yoke-project/yoke-sdk-go" {print $3}' "$pt_root/template/plugin/go.mod")"
  [[ -n "$line" && "$required" == "v$line" ]] || { echo "the template requires ${required:-nothing} and the libraries state $line"; return 1; }
}

# pt_built <dir>: the generated greeter built for this machine into <dir>/out, with this checkout's library
# standing in for the release it requires.
pt_built() {
  local dir="$1" plugin goarch
  pt_generate "$dir" || { echo "the template does not generate"; return 1; }
  plugin="$dir/template/plugin"
  (cd "$plugin" && go mod edit -replace "github.com/yoke-project/yoke-sdk-go=$pt_root") || return 1
  case "$(uname -m)" in x86_64) goarch=amd64 ;; aarch64) goarch=arm64 ;; *) echo "no spelling for $(uname -m)"; return 1 ;; esac
  mkdir -p "$dir/out"
  local run; run="$(sed -n 's/^  run: //p' "$dir/template/yoke-plugin.yaml")"
  (cd "$plugin" && YOKE_OUT="$dir/out" YOKE_GOARCH="$goarch" sh -c "$run") > "$dir/build.log" 2>&1 \
    || { echo "the build fails: $(tail -5 "$dir/build.log")"; return 1; }
}

# std: yoke-sdk-go:the-plugin-template.03
check_generated_the_plugin_builds_and_prints_the_manifest_it_keeps() {
  local dir verdict=0; dir="$(mktemp -d)"
  pt_built "$dir" || { rm -rf "$dir"; return 1; }
  [[ -x "$dir/out/greeter" ]] || { echo "the build left no executable greeter"; verdict=1; }
  if [[ "$verdict" == 0 ]] && ! "$dir/out/greeter" --manifest | cmp -s - "$dir/template/plugin/com.example.greeter.yaml"; then
    echo "the Manifest printed differs from the one kept: $("$dir/out/greeter" --manifest)"; verdict=1
  fi
  rm -rf "$dir"; return "$verdict"
}

# std: yoke-sdk-go:the-plugin-template.04
check_the_generated_plugin_is_admitted_under_a_core() {
  local dir verdict=0 bin resolved program; dir="$(mktemp -d)"; bin="$dir/bin"
  pt_built "$dir" || { rm -rf "$dir"; return 1; }
  mkdir -p "$bin"
  resolved="$(cd "$bin" && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=gc.auto GIT_CONFIG_VALUE_0=0 GOPROXY=direct GOFLAGS=-mod=mod \
    go list -m -f '{{.Version}}' github.com/yoke-project/yoke@main 2>/dev/null)" || { echo "yoke's main cannot be resolved"; rm -rf "$dir"; return 1; }
  for program in yoke yoke-compose yoke-guardian yoke-core; do
    GOBIN="$bin" go install "github.com/yoke-project/yoke/cmd/$program@$resolved" > "$dir/install.log" 2>&1 \
      || { echo "$program@$resolved does not install: $(tail -3 "$dir/install.log")"; rm -rf "$dir"; return 1; }
  done
  # An account of the check's own: its directories, its configuration, its runtime directory.
  local apps="$dir/apps" config="$dir/config" state="$dir/state" run
  run="$(mktemp -d /tmp/yt.XXXX)"
  mkdir -p "$apps" "$config/yoke" "$state"; chmod 2775 "$apps"
  printf 'install_root: %s\ncore: %s\n' "$apps" "$bin/yoke-core" > "$config/yoke/manager.conf"
  y() { XDG_RUNTIME_DIR="$run" XDG_STATE_HOME="$state" XDG_CONFIG_HOME="$config" PATH="$bin:$PATH" "$@"; }
  local try="$dir/template/try" out
  if ! out="$(cd "$try" && y yoke-compose init com.example.greeter-try --dir . --core '>=0.0.0-0' 2>&1 \
      && y yoke-compose add plugin .. --yes 2>&1 && y yoke-compose build --out "$dir" 2>&1)"; then
    echo "the trial application is not made: $out"; verdict=1
  fi
  if [[ "$verdict" == 0 ]]; then
    local pkg; pkg="$(ls "$dir"/*.yoke 2>/dev/null | head -1)"
    if ! out="$(y yoke app install "$pkg" 2>&1 && y yoke instance create com.example.greeter-try --name try 2>&1 \
        && y yoke instance start try 2>&1)"; then
      echo "the instance does not start: $out"; verdict=1
    else
      local i state_of=""
      for (( i = 0; i < 120; i++ )); do
        state_of="$(y yoke unit show greeter --instance try 2>/dev/null | tr -d ' \n')"
        [[ "$state_of" == *'"state":"Running"'* ]] && break
        sleep 1
      done
      [[ "$state_of" == *'"state":"Running"'* ]] \
        || { echo "the greeter never ran: ${state_of:-no answer}; $(tail -5 "$state/yoke/guardian.log" 2>/dev/null)"; verdict=1; }
      y yoke instance stop try > /dev/null 2>&1
    fi
  fi
  local guardian; guardian="$(cat "$run/yoke/guardian.lock" 2>/dev/null)"
  [[ -n "$guardian" ]] && kill "$guardian" 2>/dev/null
  rm -rf "$dir" "$run"; return "$verdict"
}
