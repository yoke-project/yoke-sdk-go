// Command yoke-go-admin-harness is the Go administrative library's harness, which the conformance suite
// drives through the socket CONFORMANCE_SOCKET names, against the instance CONFORMANCE_INSTANCE names.
package main

import (
	"context"
	"os"

	"github.com/yoke-project/yoke-sdk-go/adminharness"
)

func main() { os.Exit(adminharness.Serve(context.Background(), os.Getenv)) }
