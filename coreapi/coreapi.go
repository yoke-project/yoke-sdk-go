// Package coreapi exposes the Core-side surface of the Yoke Go SDK.
//
// Core consumes this package for all protocol interactions with plugins:
// session management, registration handling, heartbeat supervision,
// command dispatch, and DataMessage routing.
//
// Neither Core nor the CLI construct protobuf messages directly; all
// protocol mechanics are encapsulated here.
package coreapi
