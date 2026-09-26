package plugin_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
	"github.com/yoke-project/yoke-sdk-go/plugin"
)

func acquire() plugin.Declaration {
	return plugin.Declaration{
		ID:    "com.yoke.station.acquire",
		Needs: []string{"device:instrument"},
		Streams: []plugin.Stream{
			{ID: "station.spectra"},
			{ID: "station.preview", ToleratesLoss: true, ToleratesReorder: true},
		},
		Commands:    []string{"calibrate"},
		Queries:     []string{"head-status"},
		Occurrences: []string{"calibration.drift"},
		Capabilities: []plugin.Capability{
			{Name: "stream.spectra.publish", Governs: plugin.Object{Stream: "station.spectra"}},
			{Name: "stream.preview.publish", Governs: plugin.Object{Stream: "station.preview"}},
			{Name: "command.calibrate.accept", Governs: plugin.Object{Command: "calibrate"}},
			{Name: "query.head-status.answer", Governs: plugin.Object{Query: "head-status"}},
			{Name: "event.calibration-drift.report", Governs: plugin.Object{Occurrence: "calibration.drift"}},
		},
	}
}

// channel is a plugin channel that records what arrives and answers as a test tells it to.
type channel struct {
	pluginv1.UnimplementedRegisterServer
	pluginv1.UnimplementedSessionServer

	dir     string
	answer  func(*pluginv1.RegisterRequest) *pluginv1.RegisterResponse
	onOpen  func(send func(*pluginv1.Envelope))
	boundAt bool

	mu        sync.Mutex
	requests  []*pluginv1.RegisterRequest
	sessions  int
	received  []*pluginv1.Envelope
	receivedT []time.Time
	send      func(*pluginv1.Envelope)
	end       chan struct{}
}

func (c *channel) Register(_ context.Context, req *pluginv1.RegisterRequest) (*pluginv1.RegisterResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	_, err := os.Stat(filepath.Join(c.dir, "bind.sock"))
	c.boundAt = err == nil
	c.mu.Unlock()
	return c.answer(req), nil
}

func (c *channel) Open(stream pluginv1.Session_OpenServer) error {
	c.mu.Lock()
	c.sessions++
	var sendMu sync.Mutex
	c.send = func(e *pluginv1.Envelope) { sendMu.Lock(); defer sendMu.Unlock(); stream.Send(e) }
	c.mu.Unlock()
	go func() {
		for {
			e, err := stream.Recv()
			if err != nil {
				return
			}
			c.mu.Lock()
			c.received = append(c.received, e)
			c.receivedT = append(c.receivedT, time.Now())
			c.mu.Unlock()
		}
	}()
	select {
	case <-c.end:
	case <-stream.Context().Done():
	}
	return nil
}

func (c *channel) got() []*pluginv1.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*pluginv1.Envelope(nil), c.received...)
}

func accept(req *pluginv1.RegisterRequest) *pluginv1.RegisterResponse {
	return &pluginv1.RegisterResponse{Outcome: pluginv1.RegisterResponse_OUTCOME_ACCEPTED, SessionId: "sid-1",
		Granted: req.Declared, Heartbeat: &pluginv1.HeartbeatTerms{Interval: durationpb.New(10 * time.Second), Tolerance: 3}}
}

// serve starts a channel and returns the environment a unit launched against it would have.
func serve(t *testing.T, c *channel) func(string) string {
	t.Helper()
	dir, _ := os.MkdirTemp("", "yp")
	t.Cleanup(func() { os.RemoveAll(dir) })
	c.dir, c.end = dir, make(chan struct{})
	if c.answer == nil {
		c.answer = accept
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "plugin.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pluginv1.RegisterRegisterServer(server, c)
	pluginv1.RegisterSessionServer(server, c)
	go server.Serve(listener)
	t.Cleanup(func() { close(c.end); server.Stop() })
	env := map[string]string{"YOKE_PLUGIN": "com.yoke.station.acquire", "YOKE_UNIT": "acquire-1",
		"YOKE_SOCKET": filepath.Join(dir, "plugin.sock"), "YOKE_BIND": filepath.Join(dir, "bind.sock"), "YOKE_TOKEN": "the-token"}
	return func(k string) string { return env[k] }
}

func start(t *testing.T, c *channel) *plugin.Unit {
	t.Helper()
	u, err := plugin.StartWith(context.Background(), acquire(), serve(t, c))
	if err != nil {
		t.Fatalf("the unit did not start: %v", err)
	}
	t.Cleanup(func() { u.Close() })
	return u
}

// until waits for the condition.
func until(t *testing.T, what string, holds func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if holds() {
			return
		}
	}
	t.Fatalf("%s did not happen", what)
}

// std: yoke-sdk-go:the-plugin-library.01
func TestADeclarationGeneratesTheManifest(t *testing.T) {
	var got map[string]any
	if err := yaml.Unmarshal(acquire().Manifest(), &got); err != nil {
		t.Fatal(err)
	}
	var want map[string]any
	yaml.Unmarshal([]byte(`
manifest: 1
id: com.yoke.station.acquire
protocol: 1
needs: [ device:instrument ]
streams:
  - id: station.spectra
  - { id: station.preview, tolerates_loss: true, tolerates_reorder: true }
commands: [ { id: calibrate } ]
queries: [ { id: head-status } ]
occurrences: [ { id: calibration.drift } ]
capabilities:
  - { name: stream.spectra.publish, governs: { stream: station.spectra } }
  - { name: stream.preview.publish, governs: { stream: station.preview } }
  - { name: command.calibrate.accept, governs: { command: calibrate } }
  - { name: query.head-status.answer, governs: { query: head-status } }
  - { name: event.calibration-drift.report, governs: { occurrence: calibration.drift } }
`), &want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the Manifest is\n%s", acquire().Manifest())
	}
}

// std: yoke-sdk-go:the-plugin-library.02
func TestNothingTheModelDoesNotHaveCanBeDeclared(t *testing.T) {
	for _, value := range []any{plugin.Declaration{}, plugin.Stream{}, plugin.Capability{}, plugin.Object{}} {
		typ := reflect.TypeOf(value)
		for i := range typ.NumField() {
			name := strings.ToLower(typ.Field(i).Name)
			for _, absent := range []string{"endpoint", "autostart", "digest", "description", "label"} {
				if strings.Contains(name, absent) {
					t.Errorf("%s offers %s", typ.Name(), typ.Field(i).Name)
				}
			}
		}
	}
}

// std: yoke-sdk-go:the-plugin-library.03
func TestTheRegistrationClaimsWhatTheManifestDeclares(t *testing.T) {
	c := &channel{}
	start(t, c)
	req := c.requests[0]
	if req.Plugin != "com.yoke.station.acquire" || req.Unit != "acquire-1" || req.Token != "the-token" || req.Protocol != 1 ||
		req.Language != "go" || !strings.HasPrefix(req.SdkLine, "yoke-sdk-go ") {
		t.Errorf("the request is %v", req)
	}
	var m struct {
		Streams      []struct{ ID string }
		Commands     []struct{ ID string }
		Queries      []struct{ ID string }
		Capabilities []struct{ Name string }
	}
	yaml.Unmarshal(acquire().Manifest(), &m)
	var streams, commands, queries, capabilities []string
	for _, s := range m.Streams {
		streams = append(streams, s.ID)
	}
	for _, s := range m.Commands {
		commands = append(commands, s.ID)
	}
	for _, s := range m.Queries {
		queries = append(queries, s.ID)
	}
	for _, s := range m.Capabilities {
		capabilities = append(capabilities, s.Name)
	}
	d := req.Declared
	if !slices.Equal(d.Streams, streams) || !slices.Equal(d.Commands, commands) || !slices.Equal(d.Queries, queries) || !slices.Equal(d.Capabilities, capabilities) {
		t.Errorf("the request declares %v, and the Manifest %v %v %v %v", d, capabilities, streams, commands, queries)
	}
}

// std: yoke-sdk-go:the-plugin-library.04
func TestTheUnitsSocketIsBoundBeforeItRegisters(t *testing.T) {
	c := &channel{}
	start(t, c)
	if !c.boundAt {
		t.Error("YOKE_BIND was not bound when the registration arrived")
	}
}

// std: yoke-sdk-go:the-plugin-library.05
func TestARefusalIsSurfacedAndNeverRetried(t *testing.T) {
	c := &channel{answer: func(*pluginv1.RegisterRequest) *pluginv1.RegisterResponse {
		return &pluginv1.RegisterResponse{Outcome: pluginv1.RegisterResponse_OUTCOME_REFUSED, Stage: pluginv1.Stage_STAGE_AUTHENTICATION,
			Code: "admission.auth.consumed", Message: "already spent"}
	}}
	_, err := plugin.StartWith(context.Background(), acquire(), serve(t, c))
	var refusal *base.Refusal
	if !errors.As(err, &refusal) || refusal.Code != "admission.auth.consumed" || refusal.Stage != "authentication" {
		t.Fatalf("starting gave %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) != 1 || c.sessions != 0 {
		t.Errorf("the channel saw %d registrations and %d Sessions", len(c.requests), c.sessions)
	}
}

// std: yoke-sdk-go:the-plugin-library.06
func TestAnAcceptanceWithRestrictionsNamesWhatWasWithheld(t *testing.T) {
	c := &channel{answer: func(req *pluginv1.RegisterRequest) *pluginv1.RegisterResponse {
		r := accept(req)
		r.Outcome = pluginv1.RegisterResponse_OUTCOME_ACCEPTED_WITH_RESTRICTIONS
		r.Granted = &pluginv1.Surface{Capabilities: []string{"stream.spectra.publish"}, Streams: []string{"station.spectra"}}
		r.Withheld = &pluginv1.Surface{Capabilities: []string{"stream.preview.publish"}, Streams: []string{"station.preview"}}
		return r
	}}
	a := start(t, c).Admission()
	if !a.Restricted || !slices.Equal(a.Granted.Streams, []string{"station.spectra"}) || !slices.Equal(a.Withheld.Streams, []string{"station.preview"}) ||
		!slices.Equal(a.Withheld.Capabilities, []string{"stream.preview.publish"}) {
		t.Errorf("the admission is %+v", a)
	}
}

// std: yoke-sdk-go:the-plugin-library.07
func TestTheSessionOpensAndBeatsOnTheCoresTerms(t *testing.T) {
	c := &channel{answer: func(req *pluginv1.RegisterRequest) *pluginv1.RegisterResponse {
		r := accept(req)
		r.Heartbeat.Interval = durationpb.New(100 * time.Millisecond)
		return r
	}}
	start(t, c)
	time.Sleep(450 * time.Millisecond)
	got := c.got()
	if len(got) == 0 || got[0].GetSession().GetOpen() == nil || got[0].SessionId != "sid-1" {
		t.Fatalf("the Session's first envelope is %v", got)
	}
	var beats []time.Time
	c.mu.Lock()
	for i, e := range c.received {
		if e.GetHealth() != nil {
			beats = append(beats, c.receivedT[i])
		}
	}
	c.mu.Unlock()
	if len(beats) < 3 {
		t.Fatalf("%d heartbeats in 450 ms at an interval of 100 ms", len(beats))
	}
	for i := 1; i < len(beats); i++ {
		if beats[i].Sub(beats[i-1]) < 50*time.Millisecond {
			t.Errorf("two heartbeats %v apart", beats[i].Sub(beats[i-1]))
		}
	}
	if _, has := reflect.TypeOf(plugin.Declaration{}).FieldByName("HeartbeatInterval"); has {
		t.Error("the author can choose the interval")
	}
}

// std: yoke-sdk-go:the-plugin-library.08
func TestTheEndOfASessionIsSurfacedAndNothingReconnects(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	c.send(&pluginv1.Envelope{MessageId: "c-1", SessionId: "sid-1", Payload: &pluginv1.Envelope_Session{Session: &pluginv1.SessionMessage{
		Kind: &pluginv1.SessionMessage_Revoked_{Revoked: &pluginv1.SessionMessage_Revoked{Cause: pluginv1.SessionMessage_Revoked_CAUSE_PLUGIN_DISABLED, Line: "disabled"}}}}})
	var ended plugin.Ended
	for e := range u.Events() {
		if end, ok := e.(plugin.Ended); ok {
			ended = end
			break
		}
	}
	if ended.Closed || ended.Cause != "plugin disabled" || ended.Line != "disabled" {
		t.Errorf("the end surfaced as %+v", ended)
	}
	select {
	case <-u.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the unit did not report itself done")
	}
	time.Sleep(200 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) != 1 || c.sessions != 1 {
		t.Errorf("after the end the channel saw %d registrations and %d Sessions", len(c.requests), c.sessions)
	}
}

// std: yoke-sdk-go:the-plugin-library.09
func TestAnOrderlyCloseIsTheUnits(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	if err := u.Close(); err != nil {
		t.Fatal(err)
	}
	until(t, "the CLOSE arriving", func() bool {
		for _, e := range c.got() {
			if e.GetSession().GetClose() != nil {
				return true
			}
		}
		return false
	})
	for e := range u.Events() {
		if end, ok := e.(plugin.Ended); ok {
			if !end.Closed {
				t.Errorf("the end surfaced as %+v", end)
			}
			return
		}
	}
	t.Fatal("no end was surfaced")
}

// std: yoke-sdk-go:the-plugin-library.10
func TestWhatTheCoreSendsIsSurfacedAndAnswered(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	c.send(&pluginv1.Envelope{MessageId: "c-1", SessionId: "sid-1", Payload: &pluginv1.Envelope_Control{Control: &pluginv1.Control{
		Kind: &pluginv1.Control_Command_{Command: &pluginv1.Control_Command{Type: "calibrate"}}}}})
	c.send(&pluginv1.Envelope{MessageId: "c-2", SessionId: "sid-1", Payload: &pluginv1.Envelope_Query{Query: &pluginv1.Query{
		Kind: &pluginv1.Query_Question_{Question: &pluginv1.Query_Question{Type: "head-status"}}}}})
	command, ok := (<-u.Events()).(plugin.Command)
	if !ok || command.Type != "calibrate" {
		t.Fatalf("first surfaced %#v", command)
	}
	question, ok := (<-u.Events()).(plugin.Question)
	if !ok || question.Type != "head-status" {
		t.Fatalf("second surfaced %#v", question)
	}
	u.Ack(command, plugin.Done, "")
	u.Answer(question, []byte("ok"))
	until(t, "both answers", func() bool {
		var ack, answer bool
		for _, e := range c.got() {
			ack = ack || (e.GetAck() != nil && e.CorrelationId == "c-1")
			answer = answer || (e.GetQuery().GetAnswer() != nil && e.CorrelationId == "c-2")
		}
		return ack && answer
	})
}

// std: yoke-sdk-go:the-plugin-library.11
func TestAnOccurrenceCarriesTheAuthorsSeverity(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	if err := u.Report("calibration.drift", plugin.Severity{}, "drifting", nil); err == nil {
		t.Error("an occurrence with no severity was accepted")
	}
	if err := u.Report("calibration.drift", plugin.SeverityOf(40), "drifting", nil); err != nil {
		t.Fatal(err)
	}
	until(t, "the event", func() bool {
		events := 0
		for _, e := range c.got() {
			if ev := e.GetEvent(); ev != nil {
				events++
				if ev.Severity != 40 || ev.Occurrence != "calibration.drift" {
					t.Errorf("the event is %v", ev)
				}
			}
		}
		return events == 1
	})
}

// std: yoke-sdk-go:the-plugin-library.12
func TestNothingIsEmittedOnAStreamNotActivated(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	before := len(c.got())
	err := u.Emit("station.spectra", []byte("frame"))
	var refusal *base.Refusal
	if !errors.As(err, &refusal) || refusal.Code != "stream.inactive" {
		t.Errorf("emitting gave %v", err)
	}
	entries, _ := os.ReadDir(c.dir)
	for _, e := range entries {
		if strings.Contains(e.Name(), "spectra") {
			t.Errorf("a socket was created for the stream: %s", e.Name())
		}
	}
	time.Sleep(100 * time.Millisecond)
	for _, e := range c.got()[before:] {
		if e.GetHealth() == nil {
			t.Errorf("%v reached the channel", e)
		}
	}
}
