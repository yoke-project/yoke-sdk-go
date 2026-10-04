package iface_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	interfacev1 "github.com/yoke-project/yoke/proto/yoke/interface/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
	"github.com/yoke-project/yoke-sdk-go/iface"
)

// core is a Core's interface surface that records what it is sent and answers as each case scripts it.
type core struct {
	interfacev1.UnimplementedInterfaceServer
	opening  *interfacev1.Opening
	refuse   *interfacev1.Refusal
	answer   func(call string, r *interfacev1.Request, send func(*interfacev1.CoreFrame))
	standing []*interfacev1.CoreFrame // sent after the opening
	endAfter int                      // ends the attachment after this many requests, when not zero

	mu          sync.Mutex
	attachments int
	requests    []*interfacev1.Request
	cancelled   []string
}

func (c *core) Attach(stream interfacev1.Interface_AttachServer) error {
	c.mu.Lock()
	c.attachments++
	c.mu.Unlock()
	if c.refuse != nil {
		st, _ := status.New(codes.FailedPrecondition, c.refuse.GetMessage()).WithDetails(c.refuse)
		return st.Err()
	}
	var sending sync.Mutex
	send := func(f *interfacev1.CoreFrame) {
		sending.Lock()
		defer sending.Unlock()
		stream.Send(f)
	}
	send(&interfacev1.CoreFrame{Carries: &interfacev1.CoreFrame_Opening{Opening: c.opening}})
	for _, f := range c.standing {
		send(f)
	}
	for {
		f, err := stream.Recv()
		if err != nil {
			return nil
		}
		if f.GetCancel() != nil {
			c.mu.Lock()
			c.cancelled = append(c.cancelled, f.GetCall())
			c.mu.Unlock()
			continue
		}
		c.mu.Lock()
		c.requests = append(c.requests, f.GetRequest())
		n := len(c.requests)
		c.mu.Unlock()
		if c.answer != nil {
			go c.answer(f.GetCall(), f.GetRequest(), send)
		}
		if c.endAfter != 0 && n >= c.endAfter {
			time.Sleep(50 * time.Millisecond)
			return nil
		}
	}
}

func (c *core) seen() []*interfacev1.Request {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*interfacev1.Request(nil), c.requests...)
}

// serve binds c on a local socket in a directory of the test's, and returns the socket's path.
func serve(t *testing.T, c *core) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "iface-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "panel.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	interfacev1.RegisterInterfaceServer(s, c)
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return path
}

func opening(version uint32) *interfacev1.Opening {
	return &interfacev1.Opening{Version: version, Subscription: "standing", Picture: &interfacev1.Snapshot{At: 4,
		Records: []*interfacev1.Record{{Subject: &interfacev1.Record_Unit{Unit: &interfacev1.UnitRecord{
			Declared: &interfacev1.UnitRecord_Declared{Identity: "acquire"}}}}}}}
}

func attach(t *testing.T, c *core) *iface.Attachment {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, err := iface.Attach(ctx, serve(t, c))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}

func answered(call string, resp *interfacev1.Response) *interfacev1.CoreFrame {
	return &interfacev1.CoreFrame{Call: call, Carries: &interfacev1.CoreFrame_Answer{Answer: resp}}
}

func eventAt(sequence uint64) *interfacev1.CoreFrame {
	return &interfacev1.CoreFrame{Call: "standing", Carries: &interfacev1.CoreFrame_Event{Event: &interfacev1.Event{Seq: sequence}}}
}

// std: yoke-sdk-go:the-interface-library.01
func TestAChannelsAddressIsComputedOrHanded(t *testing.T) {
	if got, err := iface.Service("/run/yoke", "panel"); err != nil || got != "/run/yoke/interfaces/panel.sock" {
		t.Errorf("the service form's address is %q %v", got, err)
	}
	env := map[string]string{"XDG_RUNTIME_DIR": "/run/user/1000", "YOKE_SOCKET": "/run/yoke/interfaces/kiosk.sock"}
	getenv := func(k string) string { return env[k] }
	if got, err := iface.Application("bench-a", "panel", getenv); err != nil || got != "/run/user/1000/yoke/bench-a/interfaces/panel.sock" {
		t.Errorf("an application instance's address is %q %v", got, err)
	}
	if got, err := iface.Managed(getenv); err != nil || got != "/run/yoke/interfaces/kiosk.sock" {
		t.Errorf("a managed interface's address is %q %v", got, err)
	}
	if _, err := iface.Managed(func(string) string { return "" }); err == nil {
		t.Error("a managed interface handed no socket was given an address")
	} else if !contains(err.Error(), "YOKE_SOCKET") {
		t.Errorf("the refusal does not name YOKE_SOCKET: %v", err)
	}
	if _, err := iface.Service("/run/yoke", "a/b"); err == nil {
		t.Error("a channel name holding a / was given an address")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// std: yoke-sdk-go:the-interface-library.02
func TestAttachingSurfacesTheOpening(t *testing.T) {
	a := attach(t, &core{opening: opening(1)})
	if a.Version != 1 || a.Standing == nil || a.Standing.ID != "standing" || len(a.Picture.GetRecords()) != 1 ||
		a.Picture.GetRecords()[0].GetUnit().GetDeclared().GetIdentity() != "acquire" {
		t.Errorf("the opening surfaced version %d, standing %v, picture %v", a.Version, a.Standing, a.Picture)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := iface.Attach(ctx, serve(t, &core{opening: opening(2)})); !base.IsRefusal(err, "compat.unsupported") ||
		!contains(err.Error(), "1") || !contains(err.Error(), "2") {
		t.Errorf("an opening at version 2 was answered %v", err)
	}
	refused := &core{refuse: &interfacev1.Refusal{Code: "channel.in_use", Message: "the channel takes one client"}}
	if _, err := iface.Attach(ctx, serve(t, refused)); !base.IsRefusal(err, "channel.in_use") {
		t.Errorf("a refused attachment was answered %v", err)
	}
}

// answerAll answers every operation with its kind of answer, the question about slow only after the
// command to fast.
func answerAll(fast chan struct{}) func(string, *interfacev1.Request, func(*interfacev1.CoreFrame)) {
	return func(call string, r *interfacev1.Request, send func(*interfacev1.CoreFrame)) {
		var resp *interfacev1.Response
		switch op := r.GetOperation().(type) {
		case *interfacev1.Request_Authenticate:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_Authenticate{Authenticate: &interfacev1.Authenticated{Account: "ada"}}}
		case *interfacev1.Request_Read:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_Read{Read: &interfacev1.Records{}}}
		case *interfacev1.Request_Subscribe:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_Subscribe{Subscribe: &interfacev1.Subscribed{
				Carries: &interfacev1.Subscribed_Snapshot{Snapshot: &interfacev1.Snapshot{}}}}}
		case *interfacev1.Request_Confirm:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_Confirm{Confirm: &interfacev1.Confirmed{}}}
		case *interfacev1.Request_Command:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_Command{Command: &interfacev1.Acknowledged{Outcome: interfacev1.Acknowledged_OUTCOME_DONE}}}
			if op.Command.GetUnit() == "fast" && fast != nil {
				defer close(fast)
			}
		case *interfacev1.Request_Query:
			if op.Query.GetUnit() == "slow" && fast != nil {
				<-fast
			}
			resp = &interfacev1.Response{Answer: &interfacev1.Response_Query{Query: &interfacev1.Answered{Payload: []byte(op.Query.GetUnit())}}}
		case *interfacev1.Request_StreamStart:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_StreamStart{StreamStart: &interfacev1.Acknowledged{Outcome: interfacev1.Acknowledged_OUTCOME_DONE}}}
		case *interfacev1.Request_StreamStop:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_StreamStop{StreamStop: &interfacev1.Acknowledged{Outcome: interfacev1.Acknowledged_OUTCOME_DONE}}}
		case *interfacev1.Request_StreamSubscribe:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_StreamSubscribe{StreamSubscribe: &interfacev1.Delivering{Delivery: "d-1",
				Arrives: &interfacev1.Delivering_Connection{Connection: true}}}}
		case *interfacev1.Request_StreamUnsubscribe:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_StreamUnsubscribe{StreamUnsubscribe: &interfacev1.Released{}}}
		case *interfacev1.Request_Reclaim:
			resp = &interfacev1.Response{Answer: &interfacev1.Response_Reclaim{Reclaim: &interfacev1.Reclaimed{Changed: true}}}
		}
		send(answered(call, resp))
	}
}

// std: yoke-sdk-go:the-interface-library.03
func TestOneMethodPerOperationOnOneConnection(t *testing.T) {
	c := &core{opening: opening(1), answer: answerAll(nil)}
	a := attach(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := a.Authenticate(ctx, "ada", "secret")
	must(err)
	_, err = a.Read(ctx, "unit", "")
	must(err)
	sub, err := a.Subscribe(ctx, &interfacev1.Filter{})
	must(err)
	sub.Close()
	must(a.Confirm(ctx, "standing", 3))
	_, err = a.Command(ctx, "acquire", "calibrate", nil)
	must(err)
	_, err = a.Ask(ctx, "acquire", "status", nil)
	must(err)
	_, err = a.StartStream(ctx, "acquire", "station.spectra")
	must(err)
	_, err = a.StopStream(ctx, "acquire", "station.spectra")
	must(err)
	d, err := a.SubscribeStream(ctx, "acquire", "station.spectra")
	must(err)
	must(d.Release(ctx))
	_, err = a.Reclaim(ctx)
	must(err)

	sent := map[string]int{}
	for _, r := range c.seen() {
		if r.GetVersion() != 1 {
			t.Errorf("a request states the version %d", r.GetVersion())
		}
		sent[iface.Name(r)]++
	}
	for _, name := range []string{"authenticate", "read", "subscribe", "confirm", "command", "query", "stream.start",
		"stream.stop", "stream.subscribe", "stream.unsubscribe", "reclaim"} {
		if sent[name] != 1 {
			t.Errorf("%s was sent %d times", name, sent[name])
		}
	}
	if c.attachments != 1 {
		t.Errorf("the Core saw %d attachments", c.attachments)
	}

	// The question about slow is answered only once the command to fast has been: the command returns
	// while the question is still in flight, and the question then returns its own answer.
	fast := make(chan struct{})
	c.answer = answerAll(fast)
	asked := make(chan string, 1)
	go func() {
		payload, err := a.Ask(ctx, "slow", "status", nil)
		if err != nil {
			payload = []byte(err.Error())
		}
		asked <- string(payload)
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := a.Command(ctx, "fast", "calibrate", nil); err != nil {
		t.Fatalf("the command waited behind the question, and was answered %v", err)
	}
	if got := <-asked; got != "slow" {
		t.Errorf("the question was answered %q", got)
	}
}

// std: yoke-sdk-go:the-interface-library.04
func TestAnInterfaceRefusalIsTheBasesErrorWithItsSuspension(t *testing.T) {
	c := &core{opening: opening(1), answer: func(call string, r *interfacev1.Request, send func(*interfacev1.CoreFrame)) {
		ref := &interfacev1.Refusal{Code: "subject.unknown", Message: "no unit nobody",
			Detail: &interfacev1.Refusal_Subject{Subject: &interfacev1.Subject{Kind: "unit", Identity: "nobody"}}}
		if r.GetCommand() != nil {
			ref = &interfacev1.Refusal{Code: "channel.suspended", Message: "suspended",
				Detail: &interfacev1.Refusal_Suspension{Suspension: &interfacev1.Suspension{Grade: "read-only", By: "service"}}}
		}
		send(&interfacev1.CoreFrame{Call: call, Carries: &interfacev1.CoreFrame_Refusal{Refusal: ref}})
	}}
	a := attach(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := a.Read(ctx, "unit", "nobody")
	var r *base.Refusal
	if !errors.As(err, &r) || r.Code != "subject.unknown" || r.Subject.Kind != "unit" || r.Subject.Identity != "nobody" {
		t.Errorf("a read of nobody was answered %v", err)
	}
	_, err = a.Command(ctx, "acquire", "calibrate", nil)
	if !base.IsRefusal(err, "channel.suspended") {
		t.Errorf("a command on a suspended channel was answered %v", err)
	}
	if grade, by, ok := iface.SuspensionOf(err); !ok || grade != "read-only" || by != "service" {
		t.Errorf("the suspension surfaced %q in favour of %q (%v)", grade, by, ok)
	}
}

// std: yoke-sdk-go:the-interface-library.05
func TestASubscriptionIsDeliveredInOrderUntilItsCallerEndsIt(t *testing.T) {
	overflow := &interfacev1.CoreFrame{Call: "standing", Carries: &interfacev1.CoreFrame_Answer{Answer: &interfacev1.Response{
		Answer: &interfacev1.Response_Subscribe{Subscribe: &interfacev1.Subscribed{Carries: &interfacev1.Subscribed_Overflow{
			Overflow: &interfacev1.Snapshot{At: 40}}}}}}}
	c := &core{opening: opening(1), standing: []*interfacev1.CoreFrame{eventAt(5), eventAt(6), eventAt(7), overflow}, endAfter: 3}
	c.answer = func(call string, r *interfacev1.Request, send func(*interfacev1.CoreFrame)) {
		if r.GetSubscribe() == nil {
			send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_Read{Read: &interfacev1.Records{}}}))
			return
		}
		send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_Subscribe{Subscribe: &interfacev1.Subscribed{
			Carries: &interfacev1.Subscribed_Snapshot{Snapshot: &interfacev1.Snapshot{At: 8}}}}}))
		for _, n := range []uint64{9, 10} {
			send(&interfacev1.CoreFrame{Call: call, Carries: &interfacev1.CoreFrame_Event{Event: &interfacev1.Event{Seq: n}}})
		}
	}
	a := attach(t, c)
	for _, want := range []uint64{5, 6, 7} {
		got, err := a.Standing.Next()
		if err != nil || got.GetEvent().GetSeq() != want {
			t.Fatalf("the standing subscription delivered %v %v, want the event %d", got, err, want)
		}
	}
	if got, err := a.Standing.Next(); err != nil || got.GetOverflow().GetAt() != 40 {
		t.Fatalf("the standing subscription delivered %v %v, want the overflow", got, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sub, err := a.Subscribe(ctx, &interfacev1.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := sub.Next(); err != nil || got.GetSnapshot().GetAt() != 8 {
		t.Fatalf("the subscription opened with %v %v", got, err)
	}
	for _, want := range []uint64{9, 10} {
		if got, err := sub.Next(); err != nil || got.GetEvent().GetSeq() != want {
			t.Fatalf("the subscription delivered %v %v, want the event %d", got, err, want)
		}
	}
	sub.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.cancelled)
		c.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			if n != 1 {
				t.Errorf("ending the subscription cancelled %d calls", n)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.Read(ctx, "unit", "")
	a.Read(ctx, "unit", "")
	deadline = time.Now().Add(2 * time.Second)
	for {
		_, err := a.Read(ctx, "unit", "")
		if errors.Is(err, iface.ErrEnded) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the attachment ended a read was answered %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := a.Standing.Next(); !errors.Is(err, iface.ErrEnded) {
		t.Errorf("the standing subscription went on with %v", err)
	}
	if c.attachments != 1 {
		t.Errorf("the Core saw %d attachments: something reconnected", c.attachments)
	}
}

// std: yoke-sdk-go:the-interface-library.06
func TestConfirmationIsSentOnlyWhenTheAuthorConfirms(t *testing.T) {
	c := &core{opening: opening(1), answer: answerAll(nil)}
	a := attach(t, c)
	time.Sleep(200 * time.Millisecond)
	if sent := c.seen(); len(sent) != 0 {
		t.Fatalf("the library sent %v with nothing asked", sent)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.Confirm(ctx, a.Standing.ID, 7); err != nil {
		t.Fatal(err)
	}
	sent := c.seen()
	if len(sent) != 1 || sent[0].GetConfirm().GetSubscription() != "standing" || sent[0].GetConfirm().GetSequence() != 7 {
		t.Errorf("the library sent %v", sent)
	}
}

// header is a frame on a per-subscriber socket: the sequence and the clock, little-endian, then the payload.
func header(sequence, at uint64, payload string) []byte {
	b := make([]byte, 16+len(payload))
	binary.LittleEndian.PutUint64(b[0:8], sequence)
	binary.LittleEndian.PutUint64(b[8:16], at)
	copy(b[16:], payload)
	return b
}

// std: yoke-sdk-go:the-interface-library.07
func TestAStreamsDeliveryIsReadWhereItsAnswerSaysItArrives(t *testing.T) {
	dir, err := os.MkdirTemp("", "iface-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "00000001.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		conn.Write(header(1, 1001, "one"))
		conn.Write(header(2, 1002, "two"))
		conn.Close()
	}()
	subscriptions := 0
	var mu sync.Mutex
	c := &core{opening: opening(1)}
	c.answer = func(call string, r *interfacev1.Request, send func(*interfacev1.CoreFrame)) {
		if r.GetStreamUnsubscribe() != nil {
			send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_StreamUnsubscribe{StreamUnsubscribe: &interfacev1.Released{}}}))
			return
		}
		mu.Lock()
		subscriptions++
		n := subscriptions
		mu.Unlock()
		switch n {
		case 1:
			send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_StreamSubscribe{StreamSubscribe: &interfacev1.Delivering{
				Delivery: "00000001", Flowing: true, Arrives: &interfacev1.Delivering_Socket{Socket: socket}}}}))
		case 2:
			send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_StreamSubscribe{StreamSubscribe: &interfacev1.Delivering{
				Delivery: "00000002", Flowing: true, Arrives: &interfacev1.Delivering_Connection{Connection: true}}}}))
			time.Sleep(50 * time.Millisecond)
			for i, p := range []string{"one", "two"} {
				send(&interfacev1.CoreFrame{Call: "00000002", Carries: &interfacev1.CoreFrame_Delivery{Delivery: &interfacev1.StreamDelivery{
					Delivery: "00000002", Sequence: uint64(i + 1), SentAt: uint64(1001 + i), Payload: []byte(p)}}})
			}
			send(&interfacev1.CoreFrame{Call: "00000002", Carries: &interfacev1.CoreFrame_Completion{Completion: &interfacev1.Completion{By: interfacev1.Completion_BY_CORE}}})
		default:
			send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_StreamSubscribe{StreamSubscribe: &interfacev1.Delivering{
				Delivery: "00000003", Arrives: &interfacev1.Delivering_Connection{Connection: true}}}}))
		}
	}
	a := attach(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, want := range []string{"00000001", "00000002"} {
		d, err := a.SubscribeStream(ctx, "acquire", "station.spectra")
		if err != nil {
			t.Fatal(err)
		}
		if d.ID != want || !d.Flowing {
			t.Errorf("the delivery is %q, flowing %v", d.ID, d.Flowing)
		}
		for i, payload := range []string{"one", "two"} {
			f, err := d.Next()
			if err != nil || f.Sequence != uint64(i+1) || f.SentAt != uint64(1001+i) || string(f.Payload) != payload {
				t.Errorf("%s's frame %d is %+v %v", want, i, f, err)
			}
		}
		if _, err := d.Next(); !errors.Is(err, io.EOF) {
			t.Errorf("%s did not end: %v", want, err)
		}
	}
	d, err := a.SubscribeStream(ctx, "acquire", "station.spectra")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Release(ctx); err != nil {
		t.Fatal(err)
	}
	sent := c.seen()
	if last := sent[len(sent)-1]; last.GetStreamUnsubscribe().GetDelivery() != "00000003" {
		t.Errorf("releasing sent %v", last)
	}
}
