package harness_test

import (
	"bufio"
	"context"
	"encoding/json"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"

	"github.com/yoke-project/yoke-sdk-go/harness"
	"github.com/yoke-project/yoke-sdk-go/plugin"
)

// suite is the suite's side of the control socket, as far as these tests need it.
type suite struct {
	conn  net.Conn
	lines *bufio.Scanner
	next  int
}

func (s *suite) read(t *testing.T) map[string]any {
	t.Helper()
	s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if !s.lines.Scan() {
		t.Fatal("the harness sent nothing more")
	}
	var m map[string]any
	if err := json.Unmarshal(s.lines.Bytes(), &m); err != nil {
		t.Fatalf("a line that is not JSON: %s", s.lines.Text())
	}
	return m
}

func (s *suite) directive(t *testing.T, verb string, args map[string]any) (string, map[string]any) {
	t.Helper()
	s.next++
	id := "d-" + string(rune('0'+s.next))
	b, _ := json.Marshal(map[string]any{"type": "directive", "id": id, "verb": verb, "args": args})
	s.conn.Write(append(b, '\n'))
	for {
		m := s.read(t)
		if m["type"] == "result" {
			return id, m
		}
	}
}

// channel is a plugin channel answering as a test says.
type channel struct {
	pluginv1.UnimplementedRegisterServer
	pluginv1.UnimplementedSessionServer
	answer func(*pluginv1.RegisterRequest) *pluginv1.RegisterResponse
	mu     sync.Mutex
	send   func(*pluginv1.Envelope)
	opened chan struct{}
}

func (c *channel) Register(_ context.Context, req *pluginv1.RegisterRequest) (*pluginv1.RegisterResponse, error) {
	return c.answer(req), nil
}

func (c *channel) Open(stream pluginv1.Session_OpenServer) error {
	c.mu.Lock()
	c.send = func(e *pluginv1.Envelope) { stream.Send(e) }
	c.mu.Unlock()
	close(c.opened)
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

func restricted(req *pluginv1.RegisterRequest) *pluginv1.RegisterResponse {
	return &pluginv1.RegisterResponse{Outcome: pluginv1.RegisterResponse_OUTCOME_ACCEPTED_WITH_RESTRICTIONS, SessionId: "sid-1",
		Granted: &pluginv1.Surface{}, Withheld: req.Declared, Heartbeat: &pluginv1.HeartbeatTerms{Interval: durationpb.New(10 * time.Second), Tolerance: 3}}
}

// started starts a harness against a plugin channel and a suite's socket, and returns the suite's side
// once the hello has been read, the hello, and the channel. done closes when the harness exits.
func started(t *testing.T, c *channel) (*suite, map[string]any, <-chan int) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "yh")
	t.Cleanup(func() { os.RemoveAll(dir) })
	if c.answer == nil {
		c.answer = restricted
	}
	c.opened = make(chan struct{})
	plugins, _ := net.Listen("unix", filepath.Join(dir, "plugin.sock"))
	server := grpc.NewServer()
	pluginv1.RegisterRegisterServer(server, c)
	pluginv1.RegisterSessionServer(server, c)
	go server.Serve(plugins)
	t.Cleanup(server.Stop)
	control, _ := net.Listen("unix", filepath.Join(dir, "control.sock"))
	t.Cleanup(func() { control.Close() })
	env := map[string]string{"CONFORMANCE_SOCKET": filepath.Join(dir, "control.sock"), "YOKE_PLUGIN": harness.Declaration().ID,
		"YOKE_UNIT": "harness", "YOKE_SOCKET": filepath.Join(dir, "plugin.sock"), "YOKE_BIND": filepath.Join(dir, "bind.sock"), "YOKE_TOKEN": "t"}
	done := make(chan int, 1)
	go func() { done <- harness.Serve(context.Background(), func(k string) string { return env[k] }) }()
	conn, err := control.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	s := &suite{conn: conn, lines: bufio.NewScanner(conn)}
	return s, s.read(t), done
}

// std: yoke-sdk-go:the-harness.01
func TestHelloFirst(t *testing.T) {
	_, hello, _ := started(t, &channel{})
	if hello["type"] != "hello" || hello["contract"] != "plugin" || hello["language"] != "go" || hello["sdk"] != plugin.SDKLine ||
		hello["version"] != float64(1) || hello["unit"] != "harness" {
		t.Errorf("the hello was %v", hello)
	}
}

// std: yoke-sdk-go:the-harness.02
func TestDescribeAnswersWithTheGeneratedManifest(t *testing.T) {
	s, _, _ := started(t, &channel{})
	id, result := s.directive(t, "describe", nil)
	value, _ := result["value"].(map[string]any)
	if result["id"] != id || value["manifest"] != string(harness.Declaration().Manifest()) {
		t.Errorf("describe gave %v", result)
	}
}

// std: yoke-sdk-go:the-harness.03
func TestStartReportsWhatAdmissionAnswered(t *testing.T) {
	s, _, _ := started(t, &channel{})
	_, result := s.directive(t, "start", nil)
	value, _ := result["value"].(map[string]any)
	withheld, _ := value["withheld"].(map[string]any)
	if value["outcome"] != "accepted with restrictions" || len(withheld["streams"].([]any)) == 0 {
		t.Errorf("start gave %v", result)
	}

	refusing := &channel{answer: func(*pluginv1.RegisterRequest) *pluginv1.RegisterResponse {
		return &pluginv1.RegisterResponse{Outcome: pluginv1.RegisterResponse_OUTCOME_REFUSED, Stage: pluginv1.Stage_STAGE_AUTHENTICATION,
			Code: "admission.auth.consumed", Message: "the library's own words"}
	}}
	s, _, _ = started(t, refusing)
	_, result = s.directive(t, "start", nil)
	value, _ = result["value"].(map[string]any)
	if result["refusal"] != "admission.auth.consumed" || value["stage"] != "authentication" || strings.Contains(s.lines.Text(), "own words") {
		t.Errorf("a refused start gave %v", result)
	}
}

// std: yoke-sdk-go:the-harness.04
func TestAVerbItDoesNotKnowIsUnrecognised(t *testing.T) {
	s, _, _ := started(t, &channel{})
	id, result := s.directive(t, "subscribe", nil)
	if result["id"] != id || result["unrecognised"] != true {
		t.Errorf("an unknown verb gave %v", result)
	}
}

// std: yoke-sdk-go:the-harness.05
func TestWhatTheLibrarySurfacesIsAnObservation(t *testing.T) {
	c := &channel{}
	s, _, done := started(t, c)
	s.directive(t, "start", nil)
	<-c.opened
	c.mu.Lock()
	c.send(&pluginv1.Envelope{MessageId: "c-1", SessionId: "sid-1", Payload: &pluginv1.Envelope_Control{Control: &pluginv1.Control{
		Kind: &pluginv1.Control_Command_{Command: &pluginv1.Control_Command{Type: "calibrate"}}}}})
	c.send(&pluginv1.Envelope{MessageId: "c-2", SessionId: "sid-1", Payload: &pluginv1.Envelope_Session{Session: &pluginv1.SessionMessage{
		Kind: &pluginv1.SessionMessage_Revoked_{Revoked: &pluginv1.SessionMessage_Revoked{Cause: pluginv1.SessionMessage_Revoked_CAUSE_PLUGIN_DISABLED}}}}})
	c.mu.Unlock()
	first, second := s.read(t), s.read(t)
	if first["type"] != "observation" || first["kind"] != "command" || second["kind"] != "session-ended" {
		t.Fatalf("observed %v, then %v", first, second)
	}
	if fields, _ := second["fields"].(map[string]any); fields["cause"] != "plugin disabled" || fields["closed"] != false {
		t.Errorf("the end was observed as %v", second)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the harness did not exit after its Session ended")
	}
}

// std: yoke-sdk-go:the-harness.06
func TestFinishEndsTheHarnessWhichHoldsNoWire(t *testing.T) {
	s, _, done := started(t, &channel{})
	s.conn.Write([]byte(`{"type":"finish"}` + "\n"))
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("the harness exited %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("finish did not end the harness")
	}
	files, _ := filepath.Glob("*.go")
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, _ := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		for _, imp := range parsed.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if strings.HasPrefix(path, "google.golang.org/") || strings.Contains(path, "/proto/") {
				t.Errorf("%s imports %s", file, path)
			}
		}
	}
}
