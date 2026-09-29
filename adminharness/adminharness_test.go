package adminharness_test

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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	administrativev1 "github.com/yoke-project/yoke/proto/yoke/administrative/v1"

	"github.com/yoke-project/yoke-sdk-go/adminharness"
	"github.com/yoke-project/yoke-sdk-go/base"
)

// core answers the operator projection as a test tells it to.
type core struct {
	administrativev1.UnimplementedOperatorServer
}

func (core) Call(_ context.Context, r *administrativev1.Request) (*administrativev1.Response, error) {
	if r.GetUnitStop() != nil {
		st, _ := status.New(codes.FailedPrecondition, "no unit nobody is declared").WithDetails(&administrativev1.Refusal{Code: "subject.unknown",
			Message: "no unit nobody is declared", Detail: &administrativev1.Refusal_Subject{Subject: &administrativev1.Subject{Kind: "unit", Identity: "nobody"}}})
		return nil, st.Err()
	}
	return &administrativev1.Response{Answer: &administrativev1.Response_PluginDisable{PluginDisable: &administrativev1.Changed{
		Previously: &administrativev1.Previously{Value: &administrativev1.Previously_Enabled{Enabled: true}},
		Effective:  administrativev1.Changed_EFFECTIVE_IMMEDIATELY,
		Consequences: []*administrativev1.Consequence{{Unit: "acquire", Incarnation: 3, What: "its Session was revoked"}},
	}}}, nil
}

func (core) Watch(r *administrativev1.Request, stream administrativev1.Operator_WatchServer) error {
	for _, s := range []*administrativev1.Subscribed{
		{Carries: &administrativev1.Subscribed_Snapshot{Snapshot: &administrativev1.Snapshot{At: 4}}},
		{Carries: &administrativev1.Subscribed_Event{Event: &administrativev1.Event{Seq: 5, Type: "plugin.policy.changed",
			Subject: &administrativev1.Subject{Kind: "plugin", Identity: "com.example.station"}}}},
		{Carries: &administrativev1.Subscribed_Overflow{Overflow: &administrativev1.Snapshot{At: 90}}},
	} {
		stream.Send(&administrativev1.Response{Answer: &administrativev1.Response_Subscribe{Subscribe: s}})
	}
	<-stream.Context().Done()
	return nil
}

type session struct {
	conn  net.Conn
	lines *bufio.Scanner
	next  int
}

func (s *session) read(t *testing.T) map[string]any {
	t.Helper()
	s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if !s.lines.Scan() {
		t.Fatalf("the harness said nothing more: %v", s.lines.Err())
	}
	var m map[string]any
	json.Unmarshal(s.lines.Bytes(), &m)
	return m
}

func (s *session) directive(t *testing.T, verb string, args map[string]any) map[string]any {
	t.Helper()
	s.next++
	b, _ := json.Marshal(map[string]any{"type": "directive", "id": s.next, "verb": verb, "args": args})
	s.conn.Write(append(b, '\n'))
	for {
		if m := s.read(t); m["type"] == "result" {
			return m
		}
	}
}

// started serves the fake Core, starts the harness against it and returns the harness's connection, its
// hello, and its exit status once it exits.
func started(t *testing.T) (*session, map[string]any, <-chan int) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "yah")
	t.Cleanup(func() { os.RemoveAll(dir) })
	l, err := net.Listen("unix", filepath.Join(dir, "operator.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	administrativev1.RegisterOperatorServer(server, core{})
	go server.Serve(l)
	t.Cleanup(server.Stop)
	control, err := net.Listen("unix", filepath.Join(dir, "control.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { control.Close() })
	env := map[string]string{"CONFORMANCE_SOCKET": filepath.Join(dir, "control.sock"), "CONFORMANCE_INSTANCE": dir}
	done := make(chan int, 1)
	go func() { done <- adminharness.Serve(context.Background(), func(k string) string { return env[k] }) }()
	control.(*net.UnixListener).SetDeadline(time.Now().Add(5 * time.Second))
	conn, err := control.Accept()
	if err != nil {
		t.Fatalf("the harness did not connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	s := &session{conn: conn, lines: bufio.NewScanner(conn)}
	return s, s.read(t), done
}

// std: yoke-sdk-go:the-administrative-harness.01
func TestHelloFirstForTheAdministrativeContract(t *testing.T) {
	_, hello, _ := started(t)
	if hello["type"] != "hello" || hello["contract"] != "administrative" || hello["language"] != "go" || hello["sdk"] != base.SDKLine ||
		hello["version"] != float64(1) || hello["unit"] != nil {
		t.Errorf("the harness said %v first", hello)
	}
}

// std: yoke-sdk-go:the-administrative-harness.02
func TestAChangeAndARefusalAreReported(t *testing.T) {
	s, _, _ := started(t)
	changed := s.directive(t, "disable", map[string]any{"plugin": "com.example.station"})
	value, _ := changed["value"].(map[string]any)
	previously, _ := value["previously"].(map[string]any)
	consequences, _ := value["consequences"].([]any)
	if previously["enabled"] != true || value["effective"] != "immediately" || len(consequences) != 1 {
		t.Errorf("the disable was reported as %v", changed)
	}
	refused := s.directive(t, "stop-unit", map[string]any{"unit": "nobody"})
	subject, _ := refused["value"].(map[string]any)["subject"].(map[string]any)
	if refused["refusal"] != "subject.unknown" || subject["kind"] != "unit" || subject["identity"] != "nobody" {
		t.Errorf("the stop was reported as %v", refused)
	}
	if strings.Contains(s.lines.Text(), "declared") {
		t.Errorf("the refusal carries the library's words: %s", s.lines.Text())
	}
}

// std: yoke-sdk-go:the-administrative-harness.03
func TestWhatASubscriptionDeliversIsAnObservation(t *testing.T) {
	s, _, _ := started(t)
	if r := s.directive(t, "subscribe", map[string]any{"subject_kind": "plugin"}); r["refusal"] != nil || r["unrecognised"] != nil {
		t.Fatalf("the subscription was reported as %v", r)
	}
	var kinds []string
	for range 3 {
		o := s.read(t)
		kinds = append(kinds, o["kind"].(string))
		if o["kind"] == "event" {
			fields, _ := o["fields"].(map[string]any)
			if fields["type"] != "plugin.policy.changed" || fields["subject"] != "com.example.station" {
				t.Errorf("the event was observed as %v", o)
			}
		}
	}
	if strings.Join(kinds, ",") != "snapshot,event,overflow" {
		t.Errorf("the observations are %v", kinds)
	}
}

// std: yoke-sdk-go:the-administrative-harness.04
func TestAnUnknownVerbFinishAndNoWire(t *testing.T) {
	s, _, done := started(t)
	if r := s.directive(t, "emit", nil); r["unrecognised"] != true {
		t.Errorf("emit was reported as %v", r)
	}
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
			if path := strings.Trim(imp.Path.Value, `"`); strings.HasPrefix(path, "google.golang.org/grpc") {
				t.Errorf("%s imports %s", file, path)
			}
		}
	}
}

// std: yoke-sdk-go:the-administrative-harness.05
func TestTheSuiteIsRunAgainstIt(t *testing.T) {
	script, err := os.ReadFile("../ci/conformance.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(script), "./cmd/yoke-go-admin-harness") || !strings.Contains(string(script), `--harness "$bin/yoke-go-admin-harness"`) {
		t.Error("ci/conformance.sh does not build the administrative harness and run the suite against it")
	}
}
