// Package ifaceharness is the Go interface library's harness.
package ifaceharness

import "context"

// Serve runs the harness until it is told to finish, and returns its exit status.
func Serve(ctx context.Context, getenv func(string) string) int { return 1 }
