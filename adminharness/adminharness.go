// Package adminharness is the Go administrative library's harness: the thinnest translation between the
// suite's directives and the library. It judges nothing and speaks no wire of its own.
package adminharness

import "context"

// Serve runs the harness until it is told to finish, and returns its exit status.
func Serve(ctx context.Context, getenv func(string) string) int { return 1 }
