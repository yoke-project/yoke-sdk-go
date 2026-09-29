// Package admin is the library tooling and a person's session are written with: the administrative
// contract, on either of its two projections, over the base.
//
// Every operation of the contract's union is one method, the same on both projections. A change answers
// what it replaced, when it takes effect and what it did to each unit; a refusal is the base's error,
// carrying its code and its detail. Nothing is retried, and nothing reconnects: a subscription that ends
// has ended, and its caller decides what happens next.
package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	administrativev1 "github.com/yoke-project/yoke/proto/yoke/administrative/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
)

// Contract is the version of the administrative contract this library covers, stated on every request.
const Contract = int(administrativev1.Contract_CONTRACT_VERSION)

// Addresses are an instance's two administrative sockets.
type Addresses struct {
	Operator string
	Shell    string
}

func under(root string) Addresses {
	return Addresses{Operator: filepath.Join(root, "operator.sock"), Shell: filepath.Join(root, "shell.sock")}
}

// Service is the service form's addresses, under its runtime directory.
func Service(runtimeDir string) Addresses { return under(runtimeDir) }

// Application is an application instance's addresses, derived from its name under the account's runtime
// directory. A name that could choose where the addresses are is refused.
func Application(name string, getenv func(string) string) (Addresses, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return Addresses{}, fmt.Errorf("the instance name %q cannot name an instance", name)
	}
	runtime := getenv("XDG_RUNTIME_DIR")
	if runtime == "" {
		return Addresses{}, errors.New("XDG_RUNTIME_DIR is not set, and an application instance's addresses derive from it")
	}
	return under(filepath.Join(runtime, "yoke", name)), nil
}

// Effective is when a change takes effect.
type Effective int

const (
	Immediately Effective = iota + 1
	AtNextAdmission
)

// Consequence is what a change did to one unit.
type Consequence struct {
	Unit        string
	Incarnation uint64
	What        string
}

// Changed is what every change answers: what it replaced, when it takes effect, and one consequence per
// unit affected.
type Changed struct {
	Previously   *administrativev1.Previously
	Effective    Effective
	Consequences []Consequence
}

func changedOf(c *administrativev1.Changed) Changed {
	out := Changed{Previously: c.GetPreviously()}
	switch c.GetEffective() {
	case administrativev1.Changed_EFFECTIVE_IMMEDIATELY:
		out.Effective = Immediately
	case administrativev1.Changed_EFFECTIVE_AT_NEXT_ADMISSION:
		out.Effective = AtNextAdmission
	}
	for _, q := range c.GetConsequences() {
		out.Consequences = append(out.Consequences, Consequence{Unit: q.GetUnit(), Incarnation: q.GetIncarnation(), What: q.GetWhat()})
	}
	return out
}

// Retention is a retention override; each limit left out is unconstrained.
type Retention struct {
	Age     time.Duration
	Bytes   *uint64
	Entries *uint64
}

// refusalOf is a refusal as the base's error, with its detail.
func refusalOf(r *administrativev1.Refusal) error {
	out := &base.Refusal{Code: r.GetCode(), Message: r.GetMessage(), Item: r.GetItem()}
	if s := r.GetSubject(); s != nil {
		out.Subject = base.Subject{Kind: s.GetKind(), Identity: s.GetIdentity(), Incarnation: s.GetIncarnation()}
	}
	return out
}

// fromStatus is the refusal a transport's status carries, or the transport's own error where it carries
// none.
func fromStatus(err error) error {
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*administrativev1.Refusal); ok {
			return refusalOf(r)
		}
	}
	return err
}

// caller carries a request: answered once, or answered by a stream the caller reads until it ends.
type caller interface {
	call(ctx context.Context, r *administrativev1.Request) (*administrativev1.Response, error)
	watch(ctx context.Context, r *administrativev1.Request) (func() (*administrativev1.Response, error), error)
}

// Operations are the contract's operations, one method each, on whichever projection carries them.
type Operations struct{ c caller }

func (o Operations) do(ctx context.Context, r *administrativev1.Request) (*administrativev1.Response, error) {
	r.Version = uint32(Contract)
	return o.c.call(ctx, r)
}

func (o Operations) change(ctx context.Context, r *administrativev1.Request, pick func(*administrativev1.Response) *administrativev1.Changed) (Changed, error) {
	resp, err := o.do(ctx, r)
	if err != nil {
		return Changed{}, err
	}
	return changedOf(pick(resp)), nil
}

func (o Operations) EnablePlugin(ctx context.Context, plugin string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_PluginEnable{PluginEnable: &administrativev1.PluginPolicy{Plugin: plugin}}},
		(*administrativev1.Response).GetPluginEnable)
}

func (o Operations) DisablePlugin(ctx context.Context, plugin string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_PluginDisable{PluginDisable: &administrativev1.PluginPolicy{Plugin: plugin}}},
		(*administrativev1.Response).GetPluginDisable)
}

func (o Operations) Grant(ctx context.Context, plugin, capability string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_PluginGrant{PluginGrant: &administrativev1.PluginGrant{Plugin: plugin, Capability: capability}}},
		(*administrativev1.Response).GetPluginGrant)
}

func (o Operations) Withdraw(ctx context.Context, plugin, capability string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_PluginWithdraw{PluginWithdraw: &administrativev1.PluginGrant{Plugin: plugin, Capability: capability}}},
		(*administrativev1.Response).GetPluginWithdraw)
}

func (o Operations) StartUnit(ctx context.Context, unit string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_UnitStart{UnitStart: &administrativev1.UnitAct{Unit: unit}}},
		(*administrativev1.Response).GetUnitStart)
}

func (o Operations) StopUnit(ctx context.Context, unit string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_UnitStop{UnitStop: &administrativev1.UnitAct{Unit: unit}}},
		(*administrativev1.Response).GetUnitStop)
}

func (o Operations) RestartUnit(ctx context.Context, unit string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_UnitRestart{UnitRestart: &administrativev1.UnitAct{Unit: unit}}},
		(*administrativev1.Response).GetUnitRestart)
}

func (o Operations) StartStream(ctx context.Context, unit, stream string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_UnitStreamStart{UnitStreamStart: &administrativev1.UnitStream{Unit: unit, Stream: stream}}},
		(*administrativev1.Response).GetUnitStreamStart)
}

func (o Operations) StopStream(ctx context.Context, unit, stream string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_UnitStreamStop{UnitStreamStop: &administrativev1.UnitStream{Unit: unit, Stream: stream}}},
		(*administrativev1.Response).GetUnitStreamStop)
}

func (o Operations) SetRetention(ctx context.Context, unit string, r Retention) (Changed, error) {
	set := &administrativev1.UnitRetention{Unit: unit, Bytes: r.Bytes, Entries: r.Entries}
	if r.Age > 0 {
		set.Age = durationpb.New(r.Age)
	}
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_UnitRetentionSet{UnitRetentionSet: set}},
		(*administrativev1.Response).GetUnitRetentionSet)
}

func (o Operations) ClearRetention(ctx context.Context, unit string) (Changed, error) {
	return o.change(ctx, &administrativev1.Request{Operation: &administrativev1.Request_UnitRetentionClear{UnitRetentionClear: &administrativev1.UnitAct{Unit: unit}}},
		(*administrativev1.Response).GetUnitRetentionClear)
}

// Ask carries a question to a unit and returns its answer. Neither is read by anybody on the way.
func (o Operations) Ask(ctx context.Context, unit, typ string, question []byte) ([]byte, error) {
	resp, err := o.do(ctx, &administrativev1.Request{Operation: &administrativev1.Request_UnitAsk{UnitAsk: &administrativev1.UnitAsk{Unit: unit, Type: typ, Question: question}}})
	if err != nil {
		return nil, err
	}
	return resp.GetUnitAsk().GetAnswer(), nil
}

// Read reads a subject kind, or the one subject named.
func (o Operations) Read(ctx context.Context, kind, identity string) ([]*administrativev1.Record, error) {
	resp, err := o.do(ctx, &administrativev1.Request{Operation: &administrativev1.Request_Read{Read: &administrativev1.Read{Kind: kind, Identity: identity}}})
	if err != nil {
		return nil, err
	}
	return resp.GetRead().GetRecords(), nil
}

// QueryLog answers a page of the log store, and the cursor to continue from.
func (o Operations) QueryLog(ctx context.Context, q *administrativev1.LogQuery) (*administrativev1.LogPage, error) {
	resp, err := o.do(ctx, &administrativev1.Request{Operation: &administrativev1.Request_LogQuery{LogQuery: q}})
	if err != nil {
		return nil, err
	}
	return resp.GetLogQuery(), nil
}

// FollowLog follows the log store until ctx ends or the Core ends the follow.
func (o Operations) FollowLog(ctx context.Context, f *administrativev1.LogFollow) (*Follow, error) {
	next, err := o.c.watch(ctx, &administrativev1.Request{Version: uint32(Contract), Operation: &administrativev1.Request_LogFollow{LogFollow: f}})
	if err != nil {
		return nil, err
	}
	return &Follow{next: next}, nil
}

// Subscribe subscribes with a filter, until ctx ends or the Core ends the subscription.
func (o Operations) Subscribe(ctx context.Context, f *administrativev1.Filter) (*Subscription, error) {
	next, err := o.c.watch(ctx, &administrativev1.Request{Version: uint32(Contract), Operation: &administrativev1.Request_Subscribe{Subscribe: &administrativev1.Subscribe{Filter: f}}})
	if err != nil {
		return nil, err
	}
	return &Subscription{next: next}, nil
}

// Follow is a follow of the log, delivered in order.
type Follow struct {
	next func() (*administrativev1.Response, error)
}

// Next is the next entry, or where the follow fell behind; an error once it ends, io.EOF where it ended
// in order.
func (f *Follow) Next() (*administrativev1.Followed, error) {
	resp, err := f.next()
	if err != nil {
		return nil, err
	}
	return resp.GetLogFollow(), nil
}

// Subscription is a subscription, delivered in order: its snapshot, then events and overflows.
type Subscription struct {
	next func() (*administrativev1.Response, error)
}

// Next is the next delivery; an error once the subscription ends, io.EOF where it ended in order.
func (s *Subscription) Next() (*administrativev1.Subscribed, error) {
	resp, err := s.next()
	if err != nil {
		return nil, err
	}
	return resp.GetSubscribe(), nil
}

// Operator is the operator projection: a call and its answer, or a call answered by a stream.
type Operator struct {
	Operations
	conn *grpc.ClientConn
}

// DialOperator reaches the operator projection at path.
func DialOperator(path string) (*Operator, error) {
	conn, err := base.Dial(path)
	if err != nil {
		return nil, err
	}
	o := &Operator{conn: conn}
	o.Operations = Operations{c: operatorCaller{administrativev1.NewOperatorClient(conn)}}
	return o, nil
}

// Close lets the connection go.
func (o *Operator) Close() error { return o.conn.Close() }

type operatorCaller struct {
	client administrativev1.OperatorClient
}

func (c operatorCaller) call(ctx context.Context, r *administrativev1.Request) (*administrativev1.Response, error) {
	resp, err := c.client.Call(ctx, r)
	if err != nil {
		return nil, fromStatus(err)
	}
	return resp, nil
}

func (c operatorCaller) watch(ctx context.Context, r *administrativev1.Request) (func() (*administrativev1.Response, error), error) {
	stream, err := c.client.Watch(ctx, r)
	if err != nil {
		return nil, fromStatus(err)
	}
	return func() (*administrativev1.Response, error) {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if err != nil {
			return nil, fromStatus(err)
		}
		return resp, nil
	}, nil
}

// ErrEnded is what a call on a shell connection answers once the connection has ended.
var ErrEnded = errors.New("the shell connection ended")

// Shell is a connection held open on the shell projection. Its calls run concurrently, each answered by
// the frames carrying the identity it was given.
type Shell struct {
	Operations
	// Connection is the identity the Core issued, and Person the actor the channel established.
	Connection string
	Person     string
	// Standing is the subscription the connection holds from the moment it opened.
	Standing *Subscription

	conn    *grpc.ClientConn
	stream  administrativev1.Shell_ConnectClient
	end     context.CancelFunc
	sending sync.Mutex
	mu      sync.Mutex
	count   int
	calls   map[string]chan *administrativev1.CoreFrame
	ended   bool
}

// Connect opens a connection on the shell projection at path, and waits for its opening until ctx ends.
func Connect(ctx context.Context, path string) (*Shell, error) {
	conn, err := base.Dial(path)
	if err != nil {
		return nil, err
	}
	held, end := context.WithCancel(context.Background())
	fail := func(err error) (*Shell, error) {
		end()
		conn.Close()
		return nil, err
	}
	stream, err := administrativev1.NewShellClient(conn).Connect(held)
	if err != nil {
		return fail(err)
	}
	opening := make(chan *administrativev1.CoreFrame, 1)
	failed := make(chan error, 1)
	go func() {
		f, err := stream.Recv()
		if err != nil {
			failed <- err
			return
		}
		opening <- f
	}()
	var first *administrativev1.CoreFrame
	select {
	case first = <-opening:
	case err := <-failed:
		return fail(err)
	case <-ctx.Done():
		return fail(ctx.Err())
	}
	o := first.GetOpening()
	if o == nil {
		return fail(fmt.Errorf("the connection opened with %v, and not an opening", first))
	}
	s := &Shell{Connection: o.GetConnection(), Person: o.GetActor().GetPerson(), conn: conn, stream: stream, end: end,
		calls: map[string]chan *administrativev1.CoreFrame{}}
	s.Operations = Operations{c: s}
	if standing := o.GetSubscription(); standing != "" {
		frames := s.register(standing)
		s.Standing = &Subscription{next: func() (*administrativev1.Response, error) { return s.streamed(frames) }}
	}
	go s.receive()
	return s, nil
}

// Close ends the connection.
func (s *Shell) Close() error {
	s.end()
	return s.conn.Close()
}

// receive hands every frame to the call it belongs to, until the connection ends.
func (s *Shell) receive() {
	for {
		f, err := s.stream.Recv()
		if err != nil {
			s.mu.Lock()
			s.ended = true
			for call, frames := range s.calls {
				close(frames)
				delete(s.calls, call)
			}
			s.mu.Unlock()
			return
		}
		s.mu.Lock()
		frames, ok := s.calls[f.GetCall()]
		s.mu.Unlock()
		if ok {
			frames <- f
		}
	}
}

func (s *Shell) register(call string) chan *administrativev1.CoreFrame {
	frames := make(chan *administrativev1.CoreFrame, 256)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		close(frames)
		return frames
	}
	s.calls[call] = frames
	return frames
}

func (s *Shell) forget(call string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.calls, call)
}

func (s *Shell) send(f *administrativev1.ClientFrame) error {
	s.sending.Lock()
	defer s.sending.Unlock()
	return s.stream.Send(f)
}

// issue sends a request under a new call identity, and returns the frames that answer it.
func (s *Shell) issue(r *administrativev1.Request) (string, chan *administrativev1.CoreFrame, error) {
	s.mu.Lock()
	s.count++
	call := fmt.Sprintf("q-%d", s.count)
	s.mu.Unlock()
	frames := s.register(call)
	if err := s.send(&administrativev1.ClientFrame{Call: call, Carries: &administrativev1.ClientFrame_Request{Request: r}}); err != nil {
		s.forget(call)
		return "", nil, err
	}
	return call, frames, nil
}

func (s *Shell) cancel(call string) {
	s.send(&administrativev1.ClientFrame{Call: call, Carries: &administrativev1.ClientFrame_Cancel{Cancel: &administrativev1.Cancel{}}})
}

func (s *Shell) call(ctx context.Context, r *administrativev1.Request) (*administrativev1.Response, error) {
	call, frames, err := s.issue(r)
	if err != nil {
		return nil, err
	}
	defer s.forget(call)
	select {
	case f, ok := <-frames:
		switch {
		case !ok:
			return nil, ErrEnded
		case f.GetRefusal() != nil:
			return nil, refusalOf(f.GetRefusal())
		default:
			return f.GetAnswer(), nil
		}
	case <-ctx.Done():
		s.cancel(call)
		return nil, ctx.Err()
	}
}

// streamed is the next answer a streaming call's frames carry: an event is a subscription's delivery,
// and the completion is the end.
func (s *Shell) streamed(frames chan *administrativev1.CoreFrame) (*administrativev1.Response, error) {
	f, ok := <-frames
	switch {
	case !ok:
		return nil, ErrEnded
	case f.GetRefusal() != nil:
		return nil, refusalOf(f.GetRefusal())
	case f.GetCompletion() != nil:
		return nil, io.EOF
	case f.GetEvent() != nil:
		return &administrativev1.Response{Answer: &administrativev1.Response_Subscribe{Subscribe: &administrativev1.Subscribed{
			Carries: &administrativev1.Subscribed_Event{Event: f.GetEvent()}}}}, nil
	default:
		return f.GetAnswer(), nil
	}
}

// watch issues a streaming call; ending ctx cancels it, and the Core's completion ends it.
func (s *Shell) watch(ctx context.Context, r *administrativev1.Request) (func() (*administrativev1.Response, error), error) {
	call, frames, err := s.issue(r)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		select {
		case <-ctx.Done():
			s.cancel(call)
		case <-done:
		}
	}()
	return func() (*administrativev1.Response, error) {
		resp, err := s.streamed(frames)
		if err != nil {
			once.Do(func() { close(done); s.forget(call) })
		}
		return resp, err
	}, nil
}
