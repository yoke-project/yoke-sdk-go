# yoke-sdk-go

The Go SDK for the Yoke project. It turns the wire bindings published by
`yoke-proto` into ergonomic Go surfaces: the protocol servers the Core
embeds, the protocol client a plugin uses, and read-only clients for the
Core databases.

This repository is part of the Yoke project. The project-wide context
and the canonical list of repositories live in `yoke-meta`.

## Role in the project

Per the repository roles in `yoke-meta/docs/repo-roles.md`, this is a
Layer 3 SDK (`framework: SDKs`). SDKs sit between the wire contract
(`yoke-proto`) and the implementations that speak it. They mirror each
other in protocol coverage but not in API shape: the plugin-side surface
is identical in coverage across all SDKs; the other surfaces (Core-side,
Operator-side, Shell-side) appear in a given SDK only when a tool written
in that language needs them.

`yoke-sdk-go` depends on `yoke-proto` and on nothing else in the project.
The Core (`yoke-core`) depends on this SDK for its protocol-facing
scaffolding.

## What lives here

| Package       | Surface                                                          |
| ------------- | --------------------------------------------------------------- |
| `coreapi`     | Core-side server of the plugin protocol (registration + session) |
| `operatorapi` | server of the Operator API                                      |
| `shellapi`    | server and client of the shell protocol                         |
| `pluginapi`   | plugin-side client of the protocol                              |
| `dbapi`       | read-only clients for the registry and log store databases      |

The gRPC surfaces wrap the generated bindings from
`yoke-proto/gen/go/...`; they expose Go-native request/result types and
handler interfaces rather than raw protobuf messages. `dbapi` is
independent of the wire protocol: it opens the SQLite files in `mode=ro`
and exposes a stable, read-only view, decoupled from the Core's internal
write-path types.

## What does not live here

- **Wire shape.** The message and service definitions are in
  `yoke-proto`; this SDK consumes them.
- **Protocol semantics.** What messages mean and how they are sequenced
  is authoritative in `yoke-specs`.
- **Core logic.** Supervision, the registry, routing, and the gateway
  live in `yoke-core`; this SDK only provides the protocol scaffolding
  those subsystems plug into.

## Dependencies

Upstream:

- `yoke-proto` for the generated wire bindings, resolved locally during
  development via a `replace` directive in `go.mod`.
- `google.golang.org/grpc` and `google.golang.org/protobuf` for the gRPC
  surfaces; `modernc.org/sqlite` (pure-Go driver) for `dbapi`.

Downstream:

- `yoke-core` embeds `coreapi`, `operatorapi`, and `shellapi`, and reads
  the databases through `dbapi` (in `yoke-admin` and its integration
  tests).
- Plugins written in Go use `pluginapi`.

## Structure

```
coreapi/      Core-side plugin-protocol server
operatorapi/  Operator API server
shellapi/     shell protocol server + client
pluginapi/    plugin-side protocol client
dbapi/        read-only DB clients
├── logstore/
└── registry/
```

## Local operations

| Command       | Purpose                          |
| ------------- | -------------------------------- |
| `just build`  | Build all packages               |
| `just test`   | Run the tests                    |
| `just lint`   | Static analysis (`go vet`)       |
| `just fmt`    | Format the Go sources            |

The conventions are documented in
`yoke-meta/docs/justfile-conventions.md`.

## Genesis

This repository is derived from a refactoring of an earlier incarnation
of the Yoke project. The git history starts from the refactoring; the
prior SDK is reauthored against the current `yoke-proto` bindings and the
multi-repository structure rather than imported. Background on the
refactoring lives in ADR 0001 and ADR 0004 in
`yoke-meta/notes/decisions/`.
