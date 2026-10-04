package ifaceharness_test

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

	interfacev1 "github.com/yoke-project/yoke/proto/yoke/interface/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
	"github.com/yoke-project/yoke-sdk-go/ifaceharness"
)

// core is a channel's typed projection that opens, carries two events, and refuses as the cases need.
type core struct {
	interfacev1.UnimplementedInterfaceServer
}

func (core) Attach(stream interfacev1.Interface_AttachServer) error {
	stream.Send(&interfacev1.CoreFrame{Carries: &interfacev1.CoreFrame_Opening{Opening: &interfacev1.Opening{Version: 1, Subscription: "standing",
		Picture: &interfacev1.Snapshot{At: 4, Records: []*interfacev1.Record{{Subject: &interfacev1.Record_Channel{Channel: &interfacev1.ChannelRecord{
			Declared: &interfacev1.ChannelRecord_Declared{Name: "panel"}}}}}}}}})
	for _, n := range []uint64{5, 6} {
		stream.Send(&interfacev1.CoreFrame{Call: "standing", Carries: &interfacev1.CoreFrame_Event{Event: &interfacev1.Event{Seq: n, Type: "channel.attached",
			Subject: &interfacev1.Subject{Kind: "channel", Identity: "panel"}}}})
	}
	for {
		f, err := stream.Recv()
		if err != nil {
			return nil
		}
		ref := &interfacev1.Refusal{Code: "subject.unknown", Message: "no unit nobody is declared",
			Detail: &interfacev1.Refusal_Subject{Subject: &interfacev1.Subject{Kind: "unit", Identity: "nobody"}}}
		if f.GetRequest().GetCommand() != nil {
			ref = &interfacev1.Refusal{Code: "channel.suspended", Message: "the channel is suspended",
				Detail: &interfacev1.Refusal_Suspension{Suspension: &interfacev1.Suspension{Grade: "read-only", By: "bench"}}}
		}
		stream.Send(&interfacev1.CoreFrame{Call: f.GetCall(), Carries: &interfacev1.CoreFrame_Refusal{Refusal: ref}})
	}
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

// directive issues a directive and returns its result, and the observations read before it.
func (s *session) directive(t *testing.T, verb string, args map[string]any) (map[string]any, []map[string]any) {
	t.Helper()
	s.next++
	b, _ := json.Marshal(map[string]any{"type": "directive", "id": s.next, "verb": verb, "args": args})
	s.conn.Write(append(b, '\n'))
	var before []map[string]any
	for {
		m := s.read(t)
		if m["type"] == "result" {
			return m, before
		}
		before = append(before, m)
	}
}

// started serves the fake channel, starts the harness against it and returns the harness's connection,
// its hello, and its exit status once it exits.
func started(t *testing.T) (*session, map[string]any, <-chan int) {
	t.Helper()
	dir, _ := os.MkdirTemp("", "yih")
	t.Cleanup(func() { os.RemoveAll(dir) })
	os.MkdirAll(filepath.Join(dir, "interfaces"), 0o755)
	l, err := net.Listen("unix", filepath.Join(dir, "interfaces", "panel.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	interfacev1.RegisterInterfaceServer(server, core{})
	go server.Serve(l)
	t.Cleanup(server.Stop)
	control, err := net.Listen("unix", filepath.Join(dir, "control.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { control.Close() })
	env := map[string]string{"CONFORMANCE_SOCKET": filepath.Join(dir, "control.sock"), "CONFORMANCE_INSTANCE": dir}
	done := make(chan int, 1)
	go func() { done <- ifaceharness.Serve(context.Background(), func(k string) string { return env[k] }) }()
	control.(*net.UnixListener).SetDeadline(time.Now().Add(5 * time.Second))
	conn, err := control.Accept()
	if err != nil {
		t.Fatalf("the harness did not connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	s := &session{conn: conn, lines: bufio.NewScanner(conn)}
	return s, s.read(t), done
}

// std: yoke-sdk-go:the-interface-harness.01
func TestHelloFirstForTheInterfaceContract(t *testing.T) {
	_, hello, _ := started(t)
	if hello["type"] != "hello" || hello["contract"] != "interface" || hello["language"] != "go" || hello["sdk"] != base.SDKLine ||
		hello["version"] != float64(1) || hello["unit"] != nil {
		t.Errorf("the harness said %v first", hello)
	}
}

// std: yoke-sdk-go:the-interface-harness.02
func TestAttachingReportsTheOpeningAndTheStandingSubscriptionIsObserved(t *testing.T) {
	s, _, _ := started(t)
	r, before := s.directive(t, "attach", map[string]any{"channel": "panel"})
	value, _ := r["value"].(map[string]any)
	records, _ := value["records"].([]any)
	if value["version"] != float64(1) || value["subscription"] != "standing" || value["at"] != float64(4) || len(records) != 1 {
		t.Fatalf("the attachment was reported as %v", r)
	}
	observed := before
	for len(observed) < 2 {
		observed = append(observed, s.read(t))
	}
	for i, o := range observed {
		fields, _ := o["fields"].(map[string]any)
		if o["kind"] != "event" || fields["seq"] != float64(5+i) || fields["type"] != "channel.attached" || fields["subject"] != "panel" {
			t.Errorf("observation %d is %v", i, o)
		}
	}
}

// std: yoke-sdk-go:the-interface-harness.03
func TestARefusalIsReportedWithWhatItNames(t *testing.T) {
	s, _, _ := started(t)
	s.directive(t, "attach", map[string]any{"channel": "panel"})
	read, _ := s.directive(t, "read", map[string]any{"kind": "unit", "identity": "nobody"})
	subject, _ := read["value"].(map[string]any)["subject"].(map[string]any)
	if read["refusal"] != "subject.unknown" || subject["kind"] != "unit" || subject["identity"] != "nobody" {
		t.Errorf("the read was reported as %v", read)
	}
	command, _ := s.directive(t, "command", map[string]any{"unit": "acquire", "type": "calibrate"})
	suspension, _ := command["value"].(map[string]any)["suspension"].(map[string]any)
	if command["refusal"] != "channel.suspended" || suspension["grade"] != "read-only" || suspension["by"] != "bench" {
		t.Errorf("the command was reported as %v", command)
	}
	if strings.Contains(s.lines.Text(), "suspended in") || strings.Contains(s.lines.Text(), "declared") {
		t.Errorf("a refusal carries the library's words: %s", s.lines.Text())
	}
}

// std: yoke-sdk-go:the-interface-harness.04
func TestTheInterfaceHarnessKnowsNoOtherVerbFinishesAndHoldsNoWire(t *testing.T) {
	s, _, done := started(t)
	if r, _ := s.directive(t, "emit", nil); r["unrecognised"] != true {
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

// std: yoke-sdk-go:the-interface-harness.05
func TestTheSuiteIsRunAgainstTheInterfaceHarness(t *testing.T) {
	script, err := os.ReadFile("../ci/conformance.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(script), "./cmd/yoke-go-interface-harness") || !strings.Contains(string(script), `--harness "$bin/yoke-go-interface-harness"`) {
		t.Error("ci/conformance.sh does not build the interface harness and run the suite against it")
	}
}
