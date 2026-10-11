# yoke-sdk-go

## What it is

The Go family of Yoke's SDKs: the libraries a Go program uses to take part in a Yoke instance — as a
Plugin unit, as an interface client, or as tooling on the administrative surface — and the harnesses
that hold each library to the contract under the conformance suite. It is published as one Go module,
`github.com/yoke-project/yoke-sdk-go`.

It is not the Core, and it holds no contract definitions of its own: it takes them from the `proto`
module of the `yoke` repository, at the version its `go.mod` names.

## What is where

| Path | What it holds |
| --- | --- |
| `base/` | What the three libraries share: the connection, the envelope, a refusal as an error |
| `plugin/` | The library a Plugin unit is written with |
| `iface/` | The library an interface client is written with |
| `admin/` | The library tooling and a person's session are written with |
| `harness/`, `ifaceharness/`, `adminharness/` | Each library's harness for the conformance suite |
| `cmd/` | The three harnesses as programs the suite launches |
| `checks/`, `ci/` | This repository's own checks, and the scripts continuous integration runs |

A test's description sits beside the test, as a `.std.md` file.

## Working on it

Go and [`just`](https://just.systems) 1.58.0 or newer. `just develop` also puts the published
verification tool, `yoke-verify`, on the `PATH`, which `just test` needs. The `Containerfile` builds a
container with the toolchain.

| Command | What it does |
| --- | --- |
| `just build` | Builds every package |
| `just test` | Runs the checks and every test, and checks each description against its tests |
| `just lint` | Parses every shell script and runs `go vet` |
| `just fmt` | Fails, naming each file, when a Go file is not formatted |
| `just develop` | Verifies the installed `just` against a floor, given as `just develop 1.58.0`, and installs `yoke-verify` |
| `just release` | Publishes the module at the tag that names this commit; run by the release, not by hand |

## Where the depth is

The documentation at [docs.yoke-project.dev](https://docs.yoke-project.dev/): the
[reference](https://docs.yoke-project.dev/reference/) for the contracts the libraries speak, and the
[contributors' section](https://docs.yoke-project.dev/contributing/) for how the work is done. The
package documentation is on [pkg.go.dev](https://pkg.go.dev/github.com/yoke-project/yoke-sdk-go).

## Licence

Apache 2.0, in [`LICENSE`](LICENSE), with a [`NOTICE`](NOTICE).
