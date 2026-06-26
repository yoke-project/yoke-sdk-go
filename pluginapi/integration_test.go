//go:build integration

// Integration test: drives the plugin SDK against a real yoke-core process.
//
// It is gated behind the `integration` build tag so the normal `go test ./...`
// stays hermetic. Run with:
//
//	go test -tags=integration ./pluginapi/
//
// The test starts Core (the sibling ../../yoke-core/build/yoke-core binary, or
// $YOKE_CORE_BIN), registers a plugin through pluginapi, opens the bidirectional
// session, lets Core auto-start the declared stream, and asserts both the
// SDK-side outcome (registration accepted, producer emitted) and the Core-side
// receipt (Core's log shows the registration, the session, and stream data).
package pluginapi_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/yoke-project/yoke-sdk-go/pluginapi"
)

func coreBinPath(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("YOKE_CORE_BIN"); p != "" {
		return p
	}
	_, file, _, _ := runtime.Caller(0) // .../yoke-sdk-go/pluginapi/integration_test.go
	dir := filepath.Dir(file)
	return filepath.Join(dir, "..", "..", "yoke-core", "build", "yoke-core")
}

// startCore writes a temp config + manifest, launches Core, and returns the
// core.sock path and a function returning Core's captured log so far.
func startCore(t *testing.T, pluginID, streamID string) (string, func() string) {
	t.Helper()

	coreBin := coreBinPath(t)
	if _, err := os.Stat(coreBin); err != nil {
		t.Skipf("core binary not built: %s (build ../yoke-core)", coreBin)
	}

	base := t.TempDir()
	for _, d := range []string{"run", "lib", filepath.Join("etc", "plugins.d", pluginID)} {
		if err := os.MkdirAll(filepath.Join(base, d), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	manifest := fmt.Sprintf(`{
  "plugin_id": %q,
  "name": "Integration Test Plugin",
  "version": "0.1.0",
  "protocol_version": "1.0",
  "language": "go",
  "capabilities": ["it"],
  "streams": [{"stream_id": %q, "schema_family": "it", "schema_version": "v1", "direction": "plugin_to_core"}],
  "commands": ["StartStream", "StopStream"]
}`, pluginID, streamID)
	manifestPath := filepath.Join(base, "etc", "plugins.d", pluginID, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	coreSock := filepath.Join(base, "run", "core.sock")
	cfg := fmt.Sprintf(`core: {id: itest, log_level: info, log_format: text, log_path: stderr}
transport:
  sock_dir: %[1]s/run/
  core_sock: %[2]s
  operator_sock: %[1]s/run/operator.sock
  shell_sock: %[1]s/run/shell.sock
  frontend_sock: %[1]s/run/frontend.sock
registry: {db_path: %[1]s/lib/registry.db}
log_store: {db_path: %[1]s/lib/logs.db}
discovery: {manifest_dir: %[1]s/etc/plugins.d/}
`, base, coreSock)
	cfgPath := filepath.Join(base, "core.test.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	logPath := filepath.Join(base, "core.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}

	cmd := exec.Command(coreBin, "--config", cfgPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start core: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		_ = logFile.Close()
	})

	// Wait for the plugin socket to appear.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(coreSock); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := os.Stat(coreSock); err != nil {
		t.Fatalf("core.sock did not appear: %v", err)
	}

	readLog := func() string {
		b, _ := os.ReadFile(logPath)
		return string(b)
	}
	return coreSock, readLog
}

func TestPluginLifecycleAgainstCore(t *testing.T) {
	const pluginID = "itest-go"
	const streamID = "it.tick"

	coreSock, coreLog := startCore(t, pluginID, streamID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := pluginapi.RegistrationConfig{
		PluginID:       pluginID,
		PluginVersion:  "0.1.0",
		Language:       "go",
		SDKVersion:     "yoke-sdk-go",
		BootstrapToken: "dev",
		PID:            uint32(os.Getpid()),
		Capabilities:   []string{"it"},
		Streams:        []string{streamID},
		Commands:       []string{"StartStream", "StopStream"},
	}

	client, err := pluginapi.Dial(ctx, coreSock, cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	if err := client.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	if client.SessionID == "" {
		t.Fatal("register did not set SessionID")
	}

	// The producer signals each successful emit; Core auto-starts the stream on
	// session open, which drives OnStart.
	emitted := make(chan struct{}, 16)
	client.RegisterStreamProducer(streamID, "it", "v1", pluginapi.StreamProducer{
		OnStart: func(ctx context.Context, emitter pluginapi.StreamEmitter) {
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := emitter.EmitData([]byte(`{"tick":true}`)); err != nil {
						return
					}
					select {
					case emitted <- struct{}{}:
					default:
					}
				}
			}
		},
	})

	if err := client.OpenSession(ctx); err != nil {
		t.Fatalf("open session: %v", err)
	}

	// Expect the producer to emit at least 3 ticks (Core started the stream).
	for i := 0; i < 3; i++ {
		select {
		case <-emitted:
		case <-time.After(8 * time.Second):
			t.Fatalf("timed out waiting for emit #%d; core log:\n%s", i+1, coreLog())
		}
	}

	// Give Core a moment to log the received data, then assert the Core side.
	time.Sleep(500 * time.Millisecond)
	logText := coreLog()
	for _, want := range []string{pluginID, "registered", "session opened", "seq="} {
		if !strings.Contains(logText, want) {
			t.Errorf("core log missing %q; full log:\n%s", want, logText)
		}
	}
}
