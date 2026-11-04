# Go's Plugin template

| | |
| --- | --- |
| **Feature** | `template/`, the starting tree of a Plugin project in Go that this family hands over with each of its releases: `yoke-plugin.yaml`, `plugin/` with the Manifest kept beside the source, `try/` holding the trial application's companions and no descriptor, and the continuous-integration step that has the program print its Manifest and fails where it differs; written with two values to substitute, `@YOKE_ID@` and `@YOKE_PROGRAM@`, and no other; the library it requires is this release's; generated, it builds, prints the Manifest it keeps, and is admitted under a Core |
| **Planning item** | yoke-project/yoke-sdk-go#76 |

## yoke-sdk-go:the-plugin-template.01 — the template holds the Plugin project's tree, with two values to substitute

| Field | Value |
| --- | --- |
| **Cites** | arch/20-composer/07 §The template · arch/20-composer/07 §The tree · prj_structure/97 §A family's Plugin template |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | read `template/` |
| **Expected** | it holds `yoke-plugin.yaml` naming `language: go`, `plugin/` with `go.mod`, the program's source and `@YOKE_ID@.yaml`, `try/` with no `yoke.yaml`, and a workflow that builds the program, has it print its Manifest and compares it with the one kept; every `@YOKE_…@` it writes, in contents and in names, is `@YOKE_ID@` or `@YOKE_PROGRAM@` |

## yoke-sdk-go:the-plugin-template.02 — the template requires this release's library

| Field | Value |
| --- | --- |
| **Cites** | prj_structure/97 §A family's Plugin template |
| **Level** | L1 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | a clean checkout |
| **Action** | read `template/plugin/go.mod` and the line the libraries state |
| **Expected** | the template requires `github.com/yoke-project/yoke-sdk-go` at the version the libraries state, so that a tag publishes a template of its own library |

## yoke-sdk-go:the-plugin-template.03 — generated, the Plugin builds and prints the Manifest it keeps

| Field | Value |
| --- | --- |
| **Cites** | arch/20-composer/07 §The template · arch/20-composer/07 §The build contract · specs/95.39 |
| **Level** | L1 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | the template archived as the release verb archives it, unpacked, `@YOKE_ID@` given as `com.example.greeter` and `@YOKE_PROGRAM@` as `greeter` in contents and names; the library this checkout holds standing in for the release it requires |
| **Action** | run the file's `build.run` in `build.in` with `YOKE_OUT` and `YOKE_GOARCH` set for this machine; run the program with `--manifest` |
| **Expected** | the build leaves `$YOKE_OUT/greeter`, executable; what it prints is byte for byte `plugin/com.example.greeter.yaml` |

## yoke-sdk-go:the-plugin-template.04 — the generated Plugin, taken into its trial application, is admitted under a Core

| Field | Value |
| --- | --- |
| **Cites** | arch/20-composer/07 §The template · specs/95.49 |
| **Level** | L3 |
| **Method** | test |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | the tree of `.03`; `yoke`, `yoke-compose`, `yoke-guardian` and `yoke-core` from `yoke`'s default branch, through the module proxy |
| **Action** | make `try/` with `init`, any Core admitted; take the Plugin from `..` with `--yes`; build the package, install it, create an instance and start it |
| **Expected** | the unit `greeter` reaches `Running`: its Manifest matches its declaration and it is admitted |
