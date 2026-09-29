package admin_test

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	administrativev1 "github.com/yoke-project/yoke/proto/yoke/administrative/v1"

	"github.com/yoke-project/yoke-sdk-go/admin"
	"github.com/yoke-project/yoke-sdk-go/base"
)

// member is the name of the union member a message carries.
func member(m protoreflect.ProtoMessage, oneof protoreflect.Name) string {
	r := m.ProtoReflect()
	f := r.WhichOneof(r.Descriptor().Oneofs().ByName(oneof))
	if f == nil {
		return ""
	}
	return string(f.Name())
}

// answerTo is an empty answer of the member the request names.
func answerTo(r *administrativev1.Request) *administrativev1.Response {
	resp := &administrativev1.Response{}
	m := resp.ProtoReflect()
	f := m.Descriptor().Fields().ByName(protoreflect.Name(member(r, "operation")))
	m.Set(f, m.NewField(f))
	return resp
}

func subscribed(s *administrativev1.Subscribed) *administrativev1.Response {
	return &administrativev1.Response{Answer: &administrativev1.Response_Subscribe{Subscribe: s}}
}

func snapshotAt(at uint64) *administrativev1.Subscribed {
	return &administrativev1.Subscribed{Carries: &administrativev1.Subscribed_Snapshot{Snapshot: &administrativev1.Snapshot{At: at}}}
}

func eventAt(seq uint64) *administrativev1.Subscribed {
	return &administrativev1.Subscribed{Carries: &administrativev1.Subscribed_Event{Event: &administrativev1.Event{Seq: seq}}}
}

// core is a Core that records every request, and answers as it is told to.
type core struct {
	administrativev1.UnimplementedOperatorServer
	administrativev1.UnimplementedShellServer

	mu       sync.Mutex
	sent     map[string][]string // by projection: the members requested, in order
	versions []uint32
	watched  []string

	answer func(r *administrativev1.Request) (*administrativev1.Response, *administrativev1.Refusal)
	stream func(r *administrativev1.Request, send func(*administrativev1.Response) error)
	// standing is what the shell's standing subscription delivers.
	standing []*administrativev1.Subscribed
}

func newCore() *core {
	return &core{
		sent: map[string][]string{},
		answer: func(r *administrativev1.Request) (*administrativev1.Response, *administrativev1.Refusal) {
			return answerTo(r), nil
		},
		stream: func(r *administrativev1.Request, send func(*administrativev1.Response) error) {
			send(answerTo(r))
		},
	}
}

func (c *core) record(projection string, r *administrativev1.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent[projection] = append(c.sent[projection], member(r, "operation"))
	c.versions = append(c.versions, r.GetVersion())
}

func refused(r *administrativev1.Refusal) error {
	st, _ := status.New(codes.FailedPrecondition, r.GetMessage()).WithDetails(r)
	return st.Err()
}

func (c *core) Call(_ context.Context, r *administrativev1.Request) (*administrativev1.Response, error) {
	c.record("operator", r)
	resp, ref := c.answer(r)
	if ref != nil {
		return nil, refused(ref)
	}
	return resp, nil
}

func (c *core) Watch(r *administrativev1.Request, stream administrativev1.Operator_WatchServer) error {
	c.record("operator", r)
	c.mu.Lock()
	c.watched = append(c.watched, member(r, "operation"))
	c.mu.Unlock()
	c.stream(r, stream.Send)
	return nil
}

func streams(name string) bool { return name == "subscribe" || name == "log_follow" }

func (c *core) Connect(stream administrativev1.Shell_ConnectServer) error {
	var sending sync.Mutex
	send := func(f *administrativev1.CoreFrame) error {
		sending.Lock()
		defer sending.Unlock()
		return stream.Send(f)
	}
	send(&administrativev1.CoreFrame{Carries: &administrativev1.CoreFrame_Opening{Opening: &administrativev1.Opening{
		Connection: "c-7", Actor: &administrativev1.Actor{Class: "operator", Person: "ada"}, Subscription: "standing", Version: 1}}})
	for _, s := range c.standing {
		if s.GetEvent() != nil {
			send(&administrativev1.CoreFrame{Call: "standing", Carries: &administrativev1.CoreFrame_Event{Event: s.GetEvent()}})
		} else {
			send(&administrativev1.CoreFrame{Call: "standing", Carries: &administrativev1.CoreFrame_Answer{Answer: subscribed(s)}})
		}
	}
	for {
		f, err := stream.Recv()
		if err != nil {
			return nil
		}
		r, call := f.GetRequest(), f.GetCall()
		if r == nil {
			continue
		}
		c.record("shell", r)
		go func() {
			if streams(member(r, "operation")) {
				c.stream(r, func(resp *administrativev1.Response) error {
					if e := resp.GetSubscribe().GetEvent(); e != nil {
						return send(&administrativev1.CoreFrame{Call: call, Carries: &administrativev1.CoreFrame_Event{Event: e}})
					}
					return send(&administrativev1.CoreFrame{Call: call, Carries: &administrativev1.CoreFrame_Answer{Answer: resp}})
				})
				send(&administrativev1.CoreFrame{Call: call, Carries: &administrativev1.CoreFrame_Completion{Completion: &administrativev1.Completion{}}})
				return
			}
			resp, ref := c.answer(r)
			if ref != nil {
				send(&administrativev1.CoreFrame{Call: call, Carries: &administrativev1.CoreFrame_Refusal{Refusal: ref}})
				return
			}
			send(&administrativev1.CoreFrame{Call: call, Carries: &administrativev1.CoreFrame_Answer{Answer: resp}})
		}()
	}
}

// served serves c on the two sockets of a directory of the test's, and returns their addresses.
func served(t *testing.T, c *core) admin.Addresses {
	t.Helper()
	dir, _ := os.MkdirTemp("", "yga")
	t.Cleanup(func() { os.RemoveAll(dir) })
	a := admin.Service(dir)
	for path, register := range map[string]func(*grpc.Server){
		a.Operator: func(s *grpc.Server) { administrativev1.RegisterOperatorServer(s, c) },
		a.Shell:    func(s *grpc.Server) { administrativev1.RegisterShellServer(s, c) },
	} {
		l, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		s := grpc.NewServer()
		register(s)
		go s.Serve(l)
		t.Cleanup(s.Stop)
	}
	return a
}

// both are the operations through each projection.
func both(t *testing.T, a admin.Addresses) map[string]admin.Operations {
	t.Helper()
	o, err := admin.DialOperator(a.Operator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	s, err := admin.Connect(ctx, a.Shell)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return map[string]admin.Operations{"operator": o.Operations, "shell": s.Operations}
}

func timeout(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// std: yoke-sdk-go:the-administrative-library.01
func TestBothAddressesAreComputedFromTheIdentity(t *testing.T) {
	if a := admin.Service("/run/yoke"); a.Operator != "/run/yoke/operator.sock" || a.Shell != "/run/yoke/shell.sock" {
		t.Errorf("the service form's addresses are %+v", a)
	}
	env := func(k string) string { return map[string]string{"XDG_RUNTIME_DIR": "/run/user/1000"}[k] }
	a, err := admin.Application("bench-a", env)
	if err != nil || a.Operator != "/run/user/1000/yoke/bench-a/operator.sock" || a.Shell != "/run/user/1000/yoke/bench-a/shell.sock" {
		t.Errorf("bench-a's addresses are %+v (%v)", a, err)
	}
	if _, err := admin.Application("bench/a", env); err == nil || !strings.Contains(err.Error(), "bench/a") {
		t.Errorf("a name holding a separator was answered %v", err)
	}
}

// std: yoke-sdk-go:the-administrative-library.02
func TestOneMethodPerOperationOnEitherProjection(t *testing.T) {
	c := newCore()
	for projection, o := range both(t, served(t, c)) {
		ctx := timeout(t)
		for name, do := range map[string]func() error{
			"plugin_enable":        func() error { _, err := o.EnablePlugin(ctx, "p"); return err },
			"plugin_disable":       func() error { _, err := o.DisablePlugin(ctx, "p"); return err },
			"plugin_grant":         func() error { _, err := o.Grant(ctx, "p", "k"); return err },
			"plugin_withdraw":      func() error { _, err := o.Withdraw(ctx, "p", "k"); return err },
			"unit_start":           func() error { _, err := o.StartUnit(ctx, "u"); return err },
			"unit_stop":            func() error { _, err := o.StopUnit(ctx, "u"); return err },
			"unit_restart":         func() error { _, err := o.RestartUnit(ctx, "u"); return err },
			"unit_stream_start":    func() error { _, err := o.StartStream(ctx, "u", "s"); return err },
			"unit_stream_stop":     func() error { _, err := o.StopStream(ctx, "u", "s"); return err },
			"unit_retention_set":   func() error { _, err := o.SetRetention(ctx, "u", admin.Retention{Age: time.Hour}); return err },
			"unit_retention_clear": func() error { _, err := o.ClearRetention(ctx, "u"); return err },
			"unit_ask":             func() error { _, err := o.Ask(ctx, "u", "status", []byte("q")); return err },
			"read":                 func() error { _, err := o.Read(ctx, "unit", ""); return err },
			"log_query":            func() error { _, err := o.QueryLog(ctx, &administrativev1.LogQuery{}); return err },
			"log_follow": func() error {
				f, err := o.FollowLog(ctx, &administrativev1.LogFollow{})
				if err == nil {
					_, err = f.Next()
				}
				return err
			},
			"subscribe": func() error {
				s, err := o.Subscribe(ctx, &administrativev1.Filter{})
				if err == nil {
					_, err = s.Next()
				}
				return err
			},
		} {
			if err := do(); err != nil {
				t.Errorf("%s through the %s projection failed: %v", name, projection, err)
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, projection := range []string{"operator", "shell"} {
		got := slices.Sorted(slices.Values(c.sent[projection]))
		if len(got) != 16 || len(slices.Compact(got)) != 16 {
			t.Errorf("the %s projection was sent %v, want the sixteen once each", projection, c.sent[projection])
		}
	}
	for _, v := range c.versions {
		if v != 1 {
			t.Errorf("a request stated the version %d", v)
		}
	}
	if w := slices.Sorted(slices.Values(c.watched)); !slices.Equal(w, []string{"log_follow", "subscribe"}) {
		t.Errorf("Watch carried %v, want the two answered by a stream", w)
	}
}

// std: yoke-sdk-go:the-administrative-library.03
func TestAChangeAnswersWhatItReplacedAndWhatItDid(t *testing.T) {
	c := newCore()
	c.answer = func(r *administrativev1.Request) (*administrativev1.Response, *administrativev1.Refusal) {
		return &administrativev1.Response{Answer: &administrativev1.Response_PluginDisable{PluginDisable: &administrativev1.Changed{
			Previously: &administrativev1.Previously{Value: &administrativev1.Previously_Enabled{Enabled: true}},
			Effective:  administrativev1.Changed_EFFECTIVE_IMMEDIATELY,
			Consequences: []*administrativev1.Consequence{
				{Unit: "acquire", Incarnation: 3, What: "its Session was revoked"}, {Unit: "archive", Incarnation: 5, What: "its Session was revoked"}},
		}}}, nil
	}
	o := both(t, served(t, c))["operator"]
	got, err := o.DisablePlugin(timeout(t), "com.example.station")
	if err != nil {
		t.Fatal(err)
	}
	want := []admin.Consequence{{Unit: "acquire", Incarnation: 3, What: "its Session was revoked"}, {Unit: "archive", Incarnation: 5, What: "its Session was revoked"}}
	if !got.Previously.GetEnabled() || got.Effective != admin.Immediately || !slices.Equal(got.Consequences, want) {
		t.Errorf("the disable answered %+v", got)
	}
}

// std: yoke-sdk-go:the-administrative-library.04
func TestARefusalIsTheBasesErrorWithItsDetail(t *testing.T) {
	c := newCore()
	c.answer = func(r *administrativev1.Request) (*administrativev1.Response, *administrativev1.Refusal) {
		if r.GetUnitStop() != nil {
			return nil, &administrativev1.Refusal{Code: "subject.unknown", Message: "no unit nobody is declared",
				Detail: &administrativev1.Refusal_Subject{Subject: &administrativev1.Subject{Kind: "unit", Identity: "nobody"}}}
		}
		return nil, &administrativev1.Refusal{Code: "capability.undeclared", Message: "undeclared", Detail: &administrativev1.Refusal_Item{Item: "head.move"}}
	}
	for projection, o := range both(t, served(t, c)) {
		_, err := o.StopUnit(timeout(t), "nobody")
		var r *base.Refusal
		if !errors.As(err, &r) || r.Code != "subject.unknown" || r.Subject != (base.Subject{Kind: "unit", Identity: "nobody"}) {
			t.Errorf("through the %s projection the stop was answered %#v", projection, err)
		}
		_, err = o.Grant(timeout(t), "com.example.station", "head.move")
		if !errors.As(err, &r) || r.Code != "capability.undeclared" || r.Item != "head.move" {
			t.Errorf("through the %s projection the grant was answered %#v", projection, err)
		}
	}
}

// std: yoke-sdk-go:the-administrative-library.05
func TestAShellSurfacesWhoItIsAndItsCallsRunConcurrently(t *testing.T) {
	c := newCore()
	release := make(chan struct{})
	c.answer = func(r *administrativev1.Request) (*administrativev1.Response, *administrativev1.Refusal) {
		if r.GetRead().GetIdentity() == "slow" {
			<-release
		}
		return &administrativev1.Response{Answer: &administrativev1.Response_Read{Read: &administrativev1.Records{Records: []*administrativev1.Record{
			{Subject: &administrativev1.Record_Document{Document: &administrativev1.DocumentRecord{Path: r.GetRead().GetIdentity()}}}}}}}, nil
	}
	s, err := admin.Connect(timeout(t), served(t, c).Shell)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Connection != "c-7" || s.Person != "ada" || s.Standing == nil {
		t.Errorf("the connection surfaced %q, %q and %v", s.Connection, s.Person, s.Standing)
	}
	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	for _, id := range []string{"slow", "fast"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			records, err := s.Read(timeout(t), "document", id)
			if err != nil || len(records) != 1 || records[0].GetDocument().GetPath() != id {
				t.Errorf("reading %s answered %v, %v", id, records, err)
			}
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			if id == "fast" {
				close(release)
			}
		}()
		time.Sleep(50 * time.Millisecond)
	}
	wg.Wait()
	if !slices.Equal(order, []string{"fast", "slow"}) {
		t.Errorf("the reads returned in the order %v", order)
	}
}

// std: yoke-sdk-go:the-administrative-library.06
func TestASubscriptionIsDeliveredInOrderAndNothingReconnects(t *testing.T) {
	c := newCore()
	c.stream = func(r *administrativev1.Request, send func(*administrativev1.Response) error) {
		send(subscribed(snapshotAt(4)))
		send(subscribed(eventAt(5)))
		send(subscribed(eventAt(6)))
		send(subscribed(&administrativev1.Subscribed{Carries: &administrativev1.Subscribed_Overflow{Overflow: &administrativev1.Snapshot{At: 90}}}))
	}
	c.standing = []*administrativev1.Subscribed{snapshotAt(2), eventAt(3)}
	a := served(t, c)
	o, err := admin.DialOperator(a.Operator)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	sub, err := o.Subscribe(timeout(t), &administrativev1.Filter{SubjectKind: "unit"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for {
		d, err := sub.Next()
		if err != nil {
			break
		}
		switch {
		case d.GetSnapshot() != nil:
			got = append(got, "snapshot")
		case d.GetEvent() != nil:
			got = append(got, "event")
		case d.GetOverflow() != nil:
			got = append(got, "overflow")
		}
	}
	if !slices.Equal(got, []string{"snapshot", "event", "event", "overflow"}) {
		t.Errorf("the subscription delivered %v", got)
	}
	time.Sleep(200 * time.Millisecond)
	c.mu.Lock()
	if len(c.watched) != 1 {
		t.Errorf("the Core saw %d subscriptions, want 1", len(c.watched))
	}
	c.mu.Unlock()

	s, err := admin.Connect(timeout(t), a.Shell)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first, err := s.Standing.Next()
	if err != nil || first.GetSnapshot().GetAt() != 2 {
		t.Fatalf("the standing subscription first delivered %v, %v", first, err)
	}
	second, err := s.Standing.Next()
	if err != nil || second.GetEvent().GetSeq() != 3 {
		t.Errorf("the standing subscription then delivered %v, %v", second, err)
	}
}

// std: yoke-sdk-go:the-administrative-library.07
func TestAFollowDeliversEntriesAndWhereItFellBehind(t *testing.T) {
	c := newCore()
	c.stream = func(r *administrativev1.Request, send func(*administrativev1.Response) error) {
		for _, f := range []*administrativev1.Followed{
			{Carries: &administrativev1.Followed_Entry{Entry: &administrativev1.LogEntry{Seq: 7}}},
			{Carries: &administrativev1.Followed_Entry{Entry: &administrativev1.LogEntry{Seq: 8}}},
			{Carries: &administrativev1.Followed_BehindAt{BehindAt: 41}},
		} {
			send(&administrativev1.Response{Answer: &administrativev1.Response_LogFollow{LogFollow: f}})
		}
	}
	o := both(t, served(t, c))["operator"]
	f, err := o.FollowLog(timeout(t), &administrativev1.LogFollow{Unit: "acquire"})
	if err != nil {
		t.Fatal(err)
	}
	var got []uint64
	for range 3 {
		d, err := f.Next()
		if err != nil {
			t.Fatal(err)
		}
		if e := d.GetEntry(); e != nil {
			got = append(got, e.GetSeq())
		} else {
			got = append(got, d.GetBehindAt())
		}
	}
	if !slices.Equal(got, []uint64{7, 8, 41}) {
		t.Errorf("the follow delivered %v", got)
	}
}
