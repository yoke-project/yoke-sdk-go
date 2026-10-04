// Command yoke-go-interface-harness is the Go interface library's harness, which the conformance suite
// drives through the socket CONFORMANCE_SOCKET names, against the instance CONFORMANCE_INSTANCE names.
package main

import (
	"context"
	"os"

	"github.com/yoke-project/yoke-sdk-go/ifaceharness"
)

func main() { os.Exit(ifaceharness.Serve(context.Background(), os.Getenv)) }
