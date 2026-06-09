// Package pluginapi exposes the Plugin-side surface of the Yoke Go SDK.
//
// A Go plugin consumes this package to register with Core, open and close
// sessions, send heartbeats, emit typed stream data, and register command
// handlers — without touching gRPC or protobuf directly.
package pluginapi
