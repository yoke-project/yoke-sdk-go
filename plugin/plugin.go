// Package plugin is the library a Plugin unit is written with.
//
// One declaration is both the Manifest the library generates and the surface its registration claims,
// so the two cannot be written apart. Starting a unit performs the first acts in their order: read the
// environment, bind the unit's own socket, register, open the Session — and then beats on the terms the
// Core assigned. Everything the Session brings is surfaced, its end included: a Session that ends ends
// the incarnation, and the library never reconnects, never retries an admission, never polls, never
// creates a stream's transport and never chooses a severity.
package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
)

// SDKLine is what this library says it is, at admission.
const SDKLine = "yoke-sdk-go 0.1.0"

// Declaration is what a Plugin says about itself: what is true of the binary wherever it runs.
type Declaration struct {
	ID           string
	Needs        []string // `<class>` or `<class>:<name>`
	Streams      []Stream
	Commands     []string
	Queries      []string
	Occurrences  []string
	Capabilities []Capability
}

// Stream is a stream the Plugin may publish, and what its data tolerates.
type Stream struct {
	ID               string
	ToleratesLoss    bool
	ToleratesReorder bool
}

// Capability is what an operator may grant, governing exactly one object.
type Capability struct {
	Name    string
	Governs Object
}

// Object is the one thing a capability governs: set exactly one field.
type Object struct {
	Stream, Command, Query, Occurrence, Surface string
}

// Manifest is the document the declaration generates.
func (d Declaration) Manifest() []byte {
	type entry struct {
		ID               string `yaml:"id"`
		ToleratesLoss    bool   `yaml:"tolerates_loss,omitempty"`
		ToleratesReorder bool   `yaml:"tolerates_reorder,omitempty"`
	}
	type governs struct {
		Stream     string `yaml:"stream,omitempty"`
		Command    string `yaml:"command,omitempty"`
		Query      string `yaml:"query,omitempty"`
		Occurrence string `yaml:"occurrence,omitempty"`
		Surface    string `yaml:"surface,omitempty"`
	}
	type capability struct {
		Name    string  `yaml:"name"`
		Governs governs `yaml:"governs"`
	}
	ids := func(list []string) []entry {
		var out []entry
		for _, id := range list {
			out = append(out, entry{ID: id})
		}
		return out
	}
	m := struct {
		Manifest     int          `yaml:"manifest"`
		ID           string       `yaml:"id"`
		Protocol     int          `yaml:"protocol"`
		Needs        []string     `yaml:"needs,omitempty"`
		Streams      []entry      `yaml:"streams,omitempty"`
		Commands     []entry      `yaml:"commands,omitempty"`
		Queries      []entry      `yaml:"queries,omitempty"`
		Occurrences  []entry      `yaml:"occurrences,omitempty"`
		Capabilities []capability `yaml:"capabilities,omitempty"`
	}{Manifest: 1, ID: d.ID, Protocol: base.PluginContract, Needs: d.Needs,
		Commands: ids(d.Commands), Queries: ids(d.Queries), Occurrences: ids(d.Occurrences)}
	for _, s := range d.Streams {
		m.Streams = append(m.Streams, entry{ID: s.ID, ToleratesLoss: s.ToleratesLoss, ToleratesReorder: s.ToleratesReorder})
	}
	for _, c := range d.Capabilities {
		m.Capabilities = append(m.Capabilities, capability{Name: c.Name, Governs: governs(c.Governs)})
	}
	b, _ := yaml.Marshal(m)
	return b
}

// surface is what the registration claims: the declaration again, from the same value.
func (d Declaration) surface() *pluginv1.Surface {
	s := &pluginv1.Surface{Commands: d.Commands, Queries: d.Queries}
	for _, c := range d.Capabilities {
		s.Capabilities = append(s.Capabilities, c.Name)
	}
	for _, st := range d.Streams {
		s.Streams = append(s.Streams, st.ID)
	}
	return s
}

// Scope is four lists: what was granted, or what was withheld.
type Scope struct {
	Capabilities, Streams, Commands, Queries []string
}

func scopeOf(s *pluginv1.Surface) Scope {
	return Scope{s.GetCapabilities(), s.GetStreams(), s.GetCommands(), s.GetQueries()}
}

// Admission is what the Core answered.
type Admission struct {
	Restricted bool
	Granted    Scope
	Withheld   Scope // item by item
}

// Events the library surfaces, in the order it received them.
type (
	// Command is an instruction the Core sent, to be acknowledged.
	Command struct {
		ID      string
		Type    string
		Payload []byte
	}
	// Question is a query the Core asked, to be answered.
	Question struct {
		ID      string
		Type    string
		Payload []byte
	}
	// Activated says a stream may now be emitted on, and where.
	Activated struct {
		Stream    string
		Transport string
		Address   string
	}
	// Stopped says a stream may no longer be emitted on.
	Stopped struct{ Stream string }
	// Refused is an error the Core answered a message with.
	Refused struct {
		Correlation string
		Err         error
	}
	// Ended is the end of the Session: closed by this unit, or revoked by the Core. Nothing follows it.
	Ended struct {
		Closed bool
		Cause  string // for a revocation: liveness lost, plugin disabled, scope exceeded, protocol failure
		Line   string
	}
)

// Unit is a started unit and its Session.
type Unit struct {
	admission Admission
	conn      interface{ Close() error }
	listener  net.Listener
	stream    pluginv1.Session_OpenClient
	envelopes *base.Envelopes
	events    chan any
	done      chan struct{}
	cancel    context.CancelFunc

	mu        sync.Mutex
	active    map[string]Activated
	ended     bool
	closing   bool
	endedOnce sync.Once
}

// Start starts a unit from its environment.
func Start(ctx context.Context, d Declaration) (*Unit, error) { return StartWith(ctx, d, os.Getenv) }

// StartWith starts a unit from the environment getenv reads: bind, register, open the Session. A refusal
// is returned with its stage and code, and nothing is tried again.
func StartWith(ctx context.Context, d Declaration, getenv func(string) string) (*Unit, error) {
	env, err := base.Environment(getenv)
	if err != nil {
		return nil, err
	}
	// Bind before registering: registered and unreachable is the one order that is wrong.
	os.Remove(env.Bind)
	listener, err := net.Listen("unix", env.Bind)
	if err != nil {
		return nil, fmt.Errorf("the unit's socket %s cannot be bound: %w", env.Bind, err)
	}
	conn, err := base.Dial(env.Socket)
	if err != nil {
		listener.Close()
		return nil, err
	}
	resp, err := pluginv1.NewRegisterClient(conn).Register(ctx, &pluginv1.RegisterRequest{
		Plugin: env.Plugin, Unit: env.Unit, Token: env.Token, Protocol: uint32(base.PluginContract),
		Language: "go", SdkLine: SDKLine, Declared: d.surface(),
	})
	if err != nil {
		conn.Close()
		listener.Close()
		return nil, err
	}
	if resp.Outcome == pluginv1.RegisterResponse_OUTCOME_REFUSED {
		conn.Close()
		listener.Close()
		return nil, &base.Refusal{Code: resp.Code, Message: resp.Message, Stage: stageName(resp.Stage)}
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	stream, err := pluginv1.NewSessionClient(conn).Open(sessionCtx)
	if err != nil {
		cancel()
		conn.Close()
		listener.Close()
		return nil, err
	}
	u := &Unit{
		admission: Admission{Restricted: resp.Outcome == pluginv1.RegisterResponse_OUTCOME_ACCEPTED_WITH_RESTRICTIONS, Granted: scopeOf(resp.Granted), Withheld: scopeOf(resp.Withheld)},
		conn:      conn, listener: listener, stream: stream, envelopes: base.NewEnvelopes(resp.SessionId),
		events: make(chan any, 64), done: make(chan struct{}), cancel: cancel, active: map[string]Activated{},
	}
	if err := u.send(&pluginv1.Envelope{Payload: &pluginv1.Envelope_Session{Session: &pluginv1.SessionMessage{Kind: &pluginv1.SessionMessage_Open_{Open: &pluginv1.SessionMessage_Open{}}}}}); err != nil {
		u.finish(Ended{Cause: "protocol failure", Line: err.Error()})
		return nil, err
	}
	go u.receive()
	go u.beat(resp.Heartbeat.GetInterval().AsDuration())
	return u, nil
}

func stageName(s pluginv1.Stage) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s.String(), "STAGE_")), "_", " ")
}

// Admission is what the Core answered the registration with.
func (u *Unit) Admission() Admission { return u.admission }

// Events are what the Session brings, in order; the last is an Ended.
func (u *Unit) Events() <-chan any { return u.events }

// Done is closed when the Session has ended. The incarnation is over: the process should finish.
func (u *Unit) Done() <-chan struct{} { return u.done }

func (u *Unit) send(e *pluginv1.Envelope) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.ended {
		return &base.Refusal{Code: "session.revoked", Message: "the Session has ended"}
	}
	return u.stream.Send(u.envelopes.Seal(e))
}

func (u *Unit) answer(to string, e *pluginv1.Envelope) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.ended {
		return &base.Refusal{Code: "session.revoked", Message: "the Session has ended"}
	}
	return u.stream.Send(u.envelopes.Answer(to, e))
}

// beat sends a heartbeat at the interval the Core assigned, until the Session ends.
func (u *Unit) beat(interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-u.done:
			return
		case <-ticker.C:
			u.send(&pluginv1.Envelope{Payload: &pluginv1.Envelope_Health{Health: &pluginv1.Health{Grade: 99}}})
		}
	}
}

func (u *Unit) receive() {
	for {
		e, err := u.stream.Recv()
		if err != nil {
			u.mu.Lock()
			closing := u.closing
			u.mu.Unlock()
			if closing {
				u.finish(Ended{Closed: true})
			} else {
				u.finish(Ended{Cause: "liveness lost", Line: "the Session's stream ended: " + err.Error()})
			}
			return
		}
		switch {
		case e.GetSession().GetRevoked() != nil:
			r := e.GetSession().GetRevoked()
			cause := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(r.Cause.String(), "CAUSE_")), "_", " ")
			u.finish(Ended{Cause: cause, Line: r.Line})
			return
		case e.GetControl().GetCommand() != nil:
			c := e.GetControl().GetCommand()
			u.events <- Command{ID: e.MessageId, Type: c.Type, Payload: c.Payload}
		case e.GetControl().GetActivate() != nil:
			a := e.GetControl().GetActivate()
			act := Activated{Stream: a.Stream, Transport: a.Transport.String(), Address: a.Address}
			u.mu.Lock()
			u.active[a.Stream] = act
			u.mu.Unlock()
			u.events <- act
		case e.GetControl().GetStop() != nil:
			stream := e.GetControl().GetStop().Stream
			u.mu.Lock()
			delete(u.active, stream)
			u.mu.Unlock()
			u.events <- Stopped{Stream: stream}
		case e.GetQuery().GetQuestion() != nil:
			q := e.GetQuery().GetQuestion()
			u.events <- Question{ID: e.MessageId, Type: q.Type, Payload: q.Payload}
		case e.GetError() != nil:
			u.events <- Refused{Correlation: e.CorrelationId, Err: base.RefusalOf(e.GetError())}
		}
	}
}

// finish ends the Session once: the end is surfaced and nothing follows it.
func (u *Unit) finish(end Ended) {
	u.endedOnce.Do(func() {
		u.mu.Lock()
		u.ended = true
		u.mu.Unlock()
		u.events <- end
		close(u.events)
		close(u.done)
		u.cancel()
		u.conn.Close()
		u.listener.Close()
	})
}

// Close ends the Session in order: a CLOSE, the unit's own departure.
func (u *Unit) Close() error {
	u.mu.Lock()
	if u.ended {
		u.mu.Unlock()
		return nil
	}
	u.closing = true
	u.mu.Unlock()
	err := u.send(&pluginv1.Envelope{Payload: &pluginv1.Envelope_Session{Session: &pluginv1.SessionMessage{Kind: &pluginv1.SessionMessage_Close_{Close: &pluginv1.SessionMessage_Close{}}}}})
	u.stream.CloseSend()
	go func() {
		// The Core ends the stream on a CLOSE; if it does not, the departure still is one.
		select {
		case <-u.done:
		case <-time.After(2 * time.Second):
			u.finish(Ended{Closed: true})
		}
	}()
	return err
}

// Outcome is what became of a command.
type Outcome int

const (
	Accepted Outcome = iota + 1
	Done
	Failed
)

// Ack says what became of a command, correlated to it.
func (u *Unit) Ack(c Command, outcome Outcome, line string) error {
	return u.answer(c.ID, &pluginv1.Envelope{Payload: &pluginv1.Envelope_Ack{Ack: &pluginv1.Ack{Outcome: pluginv1.Ack_Outcome(outcome), Line: line}}})
}

// Answer answers a question, correlated to it.
func (u *Unit) Answer(q Question, payload []byte) error {
	return u.answer(q.ID, &pluginv1.Envelope{Payload: &pluginv1.Envelope_Query{Query: &pluginv1.Query{Kind: &pluginv1.Query_Answer_{Answer: &pluginv1.Query_Answer{Payload: payload}}}}})
}

// Severity is how routine or alarming an occurrence is, from 0 to 99: the author's statement and nobody
// else's. The zero value states nothing, and an occurrence reported with it is refused.
type Severity struct {
	value uint32
	set   bool
}

// SeverityOf is a severity the author states.
func SeverityOf(n uint8) Severity { return Severity{value: uint32(n), set: true} }

// Report reports an occurrence of a declared class, with the author's severity.
func (u *Unit) Report(occurrence string, severity Severity, line string, detail []byte) error {
	if !severity.set {
		return errors.New("an occurrence is reported with the author's severity, and none was stated")
	}
	if severity.value > 99 {
		return fmt.Errorf("a severity runs from 0 to 99, and %d is not one", severity.value)
	}
	return u.send(&pluginv1.Envelope{Payload: &pluginv1.Envelope_Event{Event: &pluginv1.Event{Occurrence: occurrence, Severity: severity.value, Line: line, Detail: detail}}})
}

// Health reports how well the unit is: a grade from 0 to 99, and a line.
func (u *Unit) Health(grade uint8, line string) error {
	if grade > 99 {
		return fmt.Errorf("a grade runs from 0 to 99, and %d is not one", grade)
	}
	return u.send(&pluginv1.Envelope{Payload: &pluginv1.Envelope_Health{Health: &pluginv1.Health{Grade: uint32(grade), Line: line}}})
}

// Emit sends data on a stream. Only the Core creates a stream's transport, so a stream it has not
// activated has nowhere to be written, and the library refuses rather than make one.
func (u *Unit) Emit(stream string, payload []byte) error {
	u.mu.Lock()
	_, active := u.active[stream]
	u.mu.Unlock()
	if !active {
		return &base.Refusal{Code: "stream.inactive", Message: fmt.Sprintf("the stream %s has not been activated", stream)}
	}
	return &base.Refusal{Code: "stream.inactive", Message: fmt.Sprintf("the transport of %s is not one this library reaches yet", stream)}
}
