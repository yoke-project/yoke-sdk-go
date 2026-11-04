// @YOKE_PROGRAM@: a Yoke Plugin. Its Manifest is kept beside this file, as @YOKE_ID@.yaml, and is what
// `@YOKE_PROGRAM@ --manifest` prints; the continuous integration of this project compares the two.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yoke-project/yoke-sdk-go/plugin"
)

// declaration is what this Plugin says about itself: its identity, and whatever it publishes and answers.
var declaration = plugin.Declaration{ID: "@YOKE_ID@"}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--manifest" {
		os.Stdout.Write(declaration.Manifest())
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	u, err := plugin.Start(ctx, declaration)
	if err != nil {
		fmt.Fprintln(os.Stderr, "@YOKE_PROGRAM@:", err)
		os.Exit(1)
	}
	u.Health(0, "ready")
	<-ctx.Done()
	u.Close()
	<-u.Done()
}
