// Command yoke-go-plugin-harness is the Go plugin library's harness, which the conformance suite drives
// through the socket CONFORMANCE_SOCKET names.
package main

import (
	"context"
	"os"

	"github.com/yoke-project/yoke-sdk-go/harness"
)

func main() { os.Exit(harness.Serve(context.Background(), os.Getenv)) }
