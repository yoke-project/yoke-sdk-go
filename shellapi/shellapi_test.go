package shellapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	shellpb "github.com/yoke-project/yoke-proto/gen/go/yoke/shell/v1"
	"github.com/yoke-project/yoke-sdk-go/shellapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// stubHandler is a CommandHandler that echoes command text as a StreamActionResult.
type stubHandler struct{}

func (stubHandler) Handle(_ context.Context, _ string, cmdText string) (shellapi.ShellResult, error) {
	if cmdText == "fail" {
		return shellapi.ShellResult{}, fmt.Errorf("command failed")
	}
	return shellapi.ShellResult{
		Kind: shellapi.KindStreamAction,
		StreamAction: &shellapi.StreamActionResult{
			Action:    "echo",
			StreamRef: cmdText,
		},
	}, nil
}

// startTestServer starts a ShellService server on a temporary Unix socket.
// It returns the server, its socket path, and a cleanup function.
func startTestServer(t *testing.T, handler shellapi.CommandHandler) (*shellapi.Server, string) {
	t.Helper()
	sock := t.TempDir() + "/shell.sock"
	srv := shellapi.NewServer(handler)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = srv.ListenAndServe(ctx, sock)
	}()
	// Wait until the socket is ready.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := net.Dial("unix", sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not start in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return srv, sock
}

// dialTestClient connects a Client to the given socket path.
func dialTestClient(t *testing.T, sock string) *shellapi.Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c, err := shellapi.Dial(ctx, sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestAuthSuccess verifies that a valid username produces a non-empty session ID.
func TestAuthSuccess(t *testing.T) {
	_, sock := startTestServer(t, stubHandler{})
	c := dialTestClient(t, sock)

	if err := c.Auth("operator", "pass"); err != nil {
		t.Fatalf("Auth: %v", err)
	}
	if c.SessionID == "" {
		t.Fatal("expected non-empty SessionID after successful auth")
	}
}

// TestAuthEmptyUsername verifies that an empty username is rejected.
func TestAuthEmptyUsername(t *testing.T) {
	_, sock := startTestServer(t, stubHandler{})
	c := dialTestClient(t, sock)

	if err := c.Auth("", ""); err == nil {
		t.Fatal("expected error for empty username, got nil")
	}
}

// TestCommandSuccess verifies that a command returns output and success.
func TestCommandSuccess(t *testing.T) {
	_, sock := startTestServer(t, stubHandler{})
	c := dialTestClient(t, sock)

	if err := c.Auth("operator", "pass"); err != nil {
		t.Fatalf("Auth: %v", err)
	}
	lines, ok, code, err := c.Command(context.Background(), "cmd-1", "hello")
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if !ok {
		t.Fatalf("expected success, got error_code=%q", code)
	}
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d: %v", len(lines), lines)
	}
	var result shellapi.ShellResult
	if err := json.Unmarshal([]byte(lines[0]), &result); err != nil {
		t.Fatalf("failed to decode ShellResult: %v", err)
	}
	if result.Kind != shellapi.KindStreamAction {
		t.Fatalf("expected kind %q, got %q", shellapi.KindStreamAction, result.Kind)
	}
	if result.StreamAction == nil || result.StreamAction.StreamRef != "hello" {
		t.Fatalf("unexpected StreamAction: %+v", result.StreamAction)
	}
}

// TestCommandFailure verifies that a failing command produces success=false and an error code.
func TestCommandFailure(t *testing.T) {
	_, sock := startTestServer(t, stubHandler{})
	c := dialTestClient(t, sock)

	if err := c.Auth("operator", "pass"); err != nil {
		t.Fatalf("Auth: %v", err)
	}
	_, ok, code, err := c.Command(context.Background(), "cmd-2", "fail")
	if err != nil {
		t.Fatalf("Command: unexpected transport error: %v", err)
	}
	if ok {
		t.Fatal("expected failure, got success")
	}
	if code == "" {
		t.Fatal("expected non-empty error_code on failure")
	}
}

// TestPingPong verifies that a Ping returns with the matching pong ID.
func TestPingPong(t *testing.T) {
	_, sock := startTestServer(t, stubHandler{})
	c := dialTestClient(t, sock)

	if err := c.Auth("operator", "pass"); err != nil {
		t.Fatalf("Auth: %v", err)
	}
	if err := c.Ping(context.Background(), "ping-42"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// TestEventPush verifies that PushEvent delivers events to active sessions.
func TestEventPush(t *testing.T) {
	srv, sock := startTestServer(t, stubHandler{})
	c := dialTestClient(t, sock)

	received := make(chan shellapi.Event, 4)
	// SetOnEvent before Auth so the background reader goroutine sees the handler immediately.
	c.SetOnEvent(func(eventType, pluginID string, tsMs int64, payload string) {
		received <- shellapi.Event{
			Type:            eventType,
			PluginID:        pluginID,
			TimestampUnixMS: tsMs,
			PayloadJSON:     payload,
		}
	})

	if err := c.Auth("operator", "pass"); err != nil {
		t.Fatalf("Auth: %v", err)
	}

	want := shellapi.Event{
		Type:            "plugin.crashed",
		PluginID:        "my-plugin",
		TimestampUnixMS: 1234567890,
		PayloadJSON:     `{"exit_code":1}`,
	}
	srv.PushEvent(want)

	select {
	case got := <-received:
		if got != want {
			t.Fatalf("event mismatch:\n got  %+v\n want %+v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event push")
	}
}

// TestMultipleSessions verifies that two concurrent sessions each receive PushEvent independently.
func TestMultipleSessions(t *testing.T) {
	srv, sock := startTestServer(t, stubHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c1, err := shellapi.Dial(ctx, sock)
	if err != nil {
		t.Fatalf("Dial c1: %v", err)
	}
	defer c1.Close()

	c2, err := shellapi.Dial(ctx, sock)
	if err != nil {
		t.Fatalf("Dial c2: %v", err)
	}
	defer c2.Close()

	recv1 := make(chan string, 4)
	recv2 := make(chan string, 4)
	// SetOnEvent before Auth so the background reader goroutine sees handlers immediately.
	c1.SetOnEvent(func(et, _ string, _ int64, _ string) { recv1 <- et })
	c2.SetOnEvent(func(et, _ string, _ int64, _ string) { recv2 <- et })

	if err := c1.Auth("op1", "pass"); err != nil {
		t.Fatalf("Auth c1: %v", err)
	}
	if err := c2.Auth("op2", "pass"); err != nil {
		t.Fatalf("Auth c2: %v", err)
	}
	if c1.SessionID == c2.SessionID {
		t.Fatalf("expected distinct session IDs, both got %q", c1.SessionID)
	}

	srv.PushEvent(shellapi.Event{Type: "plugin.registered", PluginID: "p"})

	for _, pair := range []struct {
		ch   chan string
		name string
	}{
		{recv1, "c1"},
		{recv2, "c2"},
	} {
		select {
		case got := <-pair.ch:
			if got != "plugin.registered" {
				t.Fatalf("%s: got event type %q, want plugin.registered", pair.name, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s: timed out waiting for event", pair.name)
		}
	}
}

// TestFirstMessageMustBeAuth verifies that sending a command before auth produces ShellError.
func TestFirstMessageMustBeAuth(t *testing.T) {
	_, sock := startTestServer(t, stubHandler{})

	ctx := context.Background()
	conn, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	svc := shellpb.NewShellServiceClient(conn)
	stream, err := svc.OpenShell(ctx)
	if err != nil {
		t.Fatalf("OpenShell: %v", err)
	}

	// Send a command without authenticating first.
	err = stream.Send(&shellpb.ShellClientMessage{
		Payload: &shellpb.ShellClientMessage_Command{
			Command: &shellpb.ShellCommandRequest{CommandId: "x", CommandText: "hello"},
		},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	errMsg := msg.GetError()
	if errMsg == nil {
		t.Fatalf("expected ShellError, got %T", msg.Payload)
	}
	if errMsg.ErrorCode == "" {
		t.Fatal("expected non-empty error_code")
	}
}
