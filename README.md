# yoke-sdk-go

Go SDK for the Yoke framework. Provides the language-idiomatic surface
through which Go components participate in the Yoke protocol.

This repository is part of the Yoke project. The project-wide context
and the canonical list of repositories live in `yoke-meta`.

## Role in the project

Every component of the Yoke project that speaks the protocol does so
through an SDK in its host language. This repository is the Go SDK. It
wraps the wire definitions from `yoke-proto` and exposes the
protocol's capabilities as Go APIs idiomatic to the language.

Per §4 of `yoke-architecture.md`, SDKs mirror each other in protocol
coverage but not in API shape. The Go SDK is one expression of the same
underlying contract that every other SDK respects.

## Surfaces exposed

Every Yoke SDK exposes a **plugin-side** surface: the capabilities a
plugin needs to register, handle commands, emit streams, and report
health. This is the surface every SDK must cover; the symmetry rule of
§4 of `yoke-architecture.md` applies to it.

In addition to the plugin-side surface, this SDK exposes other surfaces
because Go components of the framework consume them:

- **Core-side bindings** — used by `yoke-core`, which is implemented in
  Go and exposes the Gateway, the Operator API, and the plugin RPC
  channel through these bindings.
- **Operator-side bindings** — used by `yoke-cli`, the reference
  command-line client, which is implemented in Go and reaches the
  Operator API through these bindings.
- **Shell-side bindings** — for tools that talk to the Core's shell
  socket.
- **Storage bindings** — for components that read from the registry's
  persistence layer in read-only mode.

The presence of these additional surfaces in the Go SDK does not break
the symmetry rule: the symmetry applies to the plugin-side, which every
SDK exposes equally. Non-plugin-side surfaces exist in a given SDK only
where a tool written in that language consumes them.

## Dependencies

Upstream:

- `yoke-proto` for the wire definitions and generated bindings.

Downstream:

- `yoke-core` consumes the Core-side and the plugin RPC bindings.
- `yoke-cli` consumes the Operator-side bindings.
- Any Go plugin or third-party Go client consumes the plugin-side
  bindings.

## Local operations

| Command       | Purpose                                            |
| ------------- | -------------------------------------------------- |
| `just build`  | Build the SDK                                      |
| `just test`   | Run the test suite                                 |
| `just lint`   | Static analysis                                    |
| `just fmt`    | Format sources                                     |

The conventions are documented in
`yoke-meta/docs/justfile-conventions.md`.

## Genesis

This repository is derived from a refactoring of an earlier incarnation
of the Yoke project. The git history starts from the refactoring;
prior code is reauthored against the current specification and the
multi-repository structure rather than imported. Background on the
refactoring lives in ADR 0001 and ADR 0004 in
`yoke-meta/notes/decisions/`.
