// Package iface is the library an interface client is written with: the interface contract, on its
// typed projection, over the base.
//
// A client attaches to a channel and is given its opening: what the channel may address, the standing
// subscription, and the contract's version. Every operation of the contract's union is one method, all
// on the one connection, each answered on its own; a refusal is the base's error, carrying its code and
// its detail. Nothing is retried and nothing reconnects, and nothing is sent the author did not ask for:
// a confirmation is the author's statement of what it has processed, so the library never makes one.
//
// A channel at a routable address is reached over TLS 1.3 against the roots the caller supplies, and
// establishes its caller before anything else: the credential is the attachment's first frame, and the
// session token its answer carries is the caller's to present to a later attachment.
package iface

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/yoke-project/yoke-sdk-go/internal/ring"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	interfacev1 "github.com/yoke-project/yoke/proto/yoke/interface/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
)

// Contract is the version of the interface contract this library covers, stated on every request.
const Contract = int(interfacev1.Contract_CONTRACT_VERSION)

// ErrEnded is what a call answers once the attachment has ended.
var ErrEnded = errors.New("the attachment ended")

// channelSocket is a channel's local socket under an instance's root; a name that could choose where the
// socket is is refused.
func channelSocket(root, channel string) (string, error) {
	if channel == "" || channel == "." || channel == ".." || strings.ContainsAny(channel, "/\x00") {
		return "", fmt.Errorf("the channel name %q cannot name a channel", channel)
	}
	return filepath.Join(root, "interfaces", channel+".sock"), nil
}

// Service is a channel's address in the service form, under its runtime directory.
func Service(runtimeDir, channel string) (string, error) { return channelSocket(runtimeDir, channel) }

// Application is a channel's address in an application instance, derived from the instance's name under
// the account's runtime directory.
func Application(name, channel string, getenv func(string) string) (string, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return "", fmt.Errorf("the instance name %q cannot name an instance", name)
	}
	runtime := getenv("XDG_RUNTIME_DIR")
	if runtime == "" {
		return "", errors.New("XDG_RUNTIME_DIR is not set, and an application instance's addresses derive from it")
	}
	return channelSocket(filepath.Join(runtime, "yoke", name), channel)
}

// Managed is the address of the channel a managed interface was launched for, which the Core hands it.
func Managed(getenv func(string) string) (string, error) {
	if socket := getenv("YOKE_SOCKET"); socket != "" {
		return socket, nil
	}
	return "", errors.New("YOKE_SOCKET is not set: a managed interface is handed its channel's address, and this process was not")
}

// Name is the operation a request carries, as the contract names it.
func Name(r *interfacev1.Request) string {
	switch r.GetOperation().(type) {
	case *interfacev1.Request_Authenticate:
		return "authenticate"
	case *interfacev1.Request_Read:
		return "read"
	case *interfacev1.Request_Subscribe:
		return "subscribe"
	case *interfacev1.Request_Confirm:
		return "confirm"
	case *interfacev1.Request_Command:
		return "command"
	case *interfacev1.Request_Query:
		return "query"
	case *interfacev1.Request_StreamStart:
		return "stream.start"
	case *interfacev1.Request_StreamStop:
		return "stream.stop"
	case *interfacev1.Request_StreamSubscribe:
		return "stream.subscribe"
	case *interfacev1.Request_StreamUnsubscribe:
		return "stream.unsubscribe"
	case *interfacev1.Request_Reclaim:
		return "reclaim"
	}
	return ""
}

// Suspended is a refusal of a suspended channel: the base's refusal, with the grade the channel is
// suspended in and the channel it is suspended in favour of.
type Suspended struct {
	*base.Refusal
	Grade string
	By    string
}

func (s *Suspended) Unwrap() error { return s.Refusal }

// SuspensionOf is the grade and the favoured channel a refusal carries, and whether it carries them.
func SuspensionOf(err error) (grade, by string, ok bool) {
	var s *Suspended
	if errors.As(err, &s) {
		return s.Grade, s.By, true
	}
	return "", "", false
}

// refusalOf is a refusal as the base's error, with its detail.
func refusalOf(r *interfacev1.Refusal) error {
	out := &base.Refusal{Code: r.GetCode(), Message: r.GetMessage(), Item: r.GetItem()}
	if s := r.GetSubject(); s != nil {
		out.Subject = base.Subject{Kind: s.GetKind(), Identity: s.GetIdentity(), Incarnation: s.GetIncarnation()}
	}
	if s := r.GetSuspension(); s != nil {
		return &Suspended{Refusal: out, Grade: s.GetGrade(), By: s.GetBy()}
	}
	return out
}

// fromStatus is the refusal a transport's status carries, or the transport's own error where it carries
// none.
func fromStatus(err error) error {
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*interfacev1.Refusal); ok {
			return refusalOf(r)
		}
	}
	return err
}

// queue holds the frames of one call in the order they arrived, however many, so a call nobody is
// reading never holds up the others.
type queue struct {
	mu     sync.Mutex
	frames []*interfacev1.CoreFrame
	ready  chan struct{}
	closed bool
}

func newQueue() *queue { return &queue{ready: make(chan struct{}, 1)} }

func (q *queue) push(f *interfacev1.CoreFrame) {
	q.mu.Lock()
	q.frames = append(q.frames, f)
	q.mu.Unlock()
	q.wake()
}

func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.wake()
}

func (q *queue) wake() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// pop is the next frame, or false once the queue is closed and empty; it waits until ctx ends.
func (q *queue) pop(ctx context.Context) (*interfacev1.CoreFrame, bool, error) {
	for {
		q.mu.Lock()
		if len(q.frames) > 0 {
			f := q.frames[0]
			q.frames = q.frames[1:]
			q.mu.Unlock()
			return f, true, nil
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return nil, false, nil
		}
		select {
		case <-q.ready:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}

// Attachment is a client attached to a channel. Picture is what the channel was given when it attached,
// Standing the subscription it holds from then on, and Version the contract's version the Core speaks.
// Established is the answer to the credential an attachment presented — the account, and the session
// token with its expiry — and nil where it presented none.
type Attachment struct {
	Picture     *interfacev1.Snapshot
	Standing    *Subscription
	Version     int
	Established *interfacev1.Authenticated

	conn    *grpc.ClientConn
	stream  interfacev1.Interface_AttachClient
	end     context.CancelFunc
	sending sync.Mutex
	mu      sync.Mutex
	count   int
	calls   map[string]*queue
	ended   bool
	// received is closed once nothing more arrives from the Core.
	received chan struct{}
}

// Option is how an attachment reaches its channel, and what it presents there.
type Option func(*options)

type options struct {
	tls        bool
	roots      *x509.CertPool
	serverName string
	credential *interfacev1.Authenticate
}

// TLS reaches the channel over TLS 1.3 and nothing below it, verifying the Core's chain against roots and
// no others; name is the name the certificate must carry, the address's host where it is empty. No
// client certificate is offered: the caller is established by a credential, above the transport.
func TLS(roots *x509.CertPool, name string) Option {
	return func(o *options) { o.tls, o.roots, o.serverName = true, roots, name }
}

// Password presents an account and its secret, for the host to judge, as the attachment's first frame.
func Password(account, secret string) Option {
	return func(o *options) {
		o.credential = &interfacev1.Authenticate{Credential: &interfacev1.Authenticate_Password_{
			Password: &interfacev1.Authenticate_Password{Account: account, Secret: secret}}}
	}
}

// Token presents a session token an earlier authentication on the same channel issued, as the
// attachment's first frame.
func Token(token string) Option {
	return func(o *options) {
		o.credential = &interfacev1.Authenticate{Credential: &interfacev1.Authenticate_Token{Token: token}}
	}
}

// dial reaches a channel: a path is a local socket, anything else an address, over TLS where asked.
func dial(address string, o options) (*grpc.ClientConn, error) {
	if strings.HasPrefix(address, "/") {
		if o.tls {
			return nil, fmt.Errorf("%s is a local socket, and a local socket is not reached over TLS", address)
		}
		return base.Dial(address)
	}
	if !o.tls {
		return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if o.roots == nil {
		return nil, errors.New("TLS was asked with no roots, and the Core's chain is verified only against the roots its caller supplies")
	}
	return grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, RootCAs: o.roots, ServerName: o.serverName})))
}

// authenticating is the call identity of the credential an attachment presents first.
const authenticating = "c-1"

// Attach attaches to the channel at address, and waits for its opening until ctx ends. A Core that
// speaks another version of the contract is refused before any operation, and the connection let go.
// A credential among opts is sent before anything else, and Attach waits for its answer too.
func Attach(ctx context.Context, address string, opts ...Option) (*Attachment, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	conn, err := dial(address, o)
	if err != nil {
		return nil, err
	}
	held, end := context.WithCancel(context.Background())
	fail := func(err error) (*Attachment, error) {
		end()
		conn.Close()
		return nil, err
	}
	stream, err := interfacev1.NewInterfaceClient(conn).Attach(held)
	if err != nil {
		return fail(fromStatus(err))
	}
	if o.credential != nil {
		r := &interfacev1.Request{Version: uint32(Contract), Operation: &interfacev1.Request_Authenticate{Authenticate: o.credential}}
		if err := stream.Send(&interfacev1.ClientFrame{Call: authenticating, Carries: &interfacev1.ClientFrame_Request{Request: r}}); err != nil {
			return fail(fromStatus(err))
		}
	}
	first := make(chan *interfacev1.CoreFrame, 1)
	failed := make(chan error, 1)
	go func() {
		f, err := stream.Recv()
		if err != nil {
			failed <- err
			return
		}
		first <- f
	}()
	var f *interfacev1.CoreFrame
	select {
	case f = <-first:
	case err := <-failed:
		return fail(fromStatus(err))
	case <-ctx.Done():
		return fail(ctx.Err())
	}
	op := f.GetOpening()
	if op == nil {
		return fail(fmt.Errorf("the attachment opened with %v, and not an opening", f))
	}
	if int(op.GetVersion()) != Contract {
		return fail(&base.Refusal{Code: "compat.unsupported", Message: fmt.Sprintf(
			"this library speaks the interface contract's version %d, and the Core %d", Contract, op.GetVersion())})
	}
	a := &Attachment{Picture: op.GetPicture(), Version: int(op.GetVersion()), conn: conn, stream: stream, end: end,
		calls: map[string]*queue{}, received: make(chan struct{})}
	if standing := op.GetSubscription(); standing != "" {
		a.Standing = &Subscription{ID: standing, a: a, frames: a.register(standing), standing: true}
	}
	if o.credential == nil {
		go a.receive()
		return a, nil
	}
	a.count = 1
	q := a.register(authenticating)
	go a.receive()
	resp, err := a.first(ctx, authenticating, q)
	a.forget(authenticating)
	if err != nil {
		a.Close()
		return nil, err
	}
	a.Established = resp.GetAuthenticate()
	return a, nil
}

// closing is how long Close waits for the Core to end an attachment told it is closing.
const closing = time.Second

// Close ends the attachment in order: it tells the Core the client is done, so the Core reads a client
// that closed rather than one that went away, and lets the connection go once the Core has ended it.
func (a *Attachment) Close() error {
	a.sending.Lock()
	a.stream.CloseSend()
	a.sending.Unlock()
	select {
	case <-a.received:
	case <-time.After(closing):
	}
	a.end()
	return a.conn.Close()
}

// receive hands every frame to the call it belongs to, until the attachment ends. A delivery on the
// connection is registered as its answer arrives, so none of its frames can arrive before it is.
func (a *Attachment) receive() {
	for {
		f, err := a.stream.Recv()
		if err != nil {
			a.mu.Lock()
			a.ended = true
			for call, q := range a.calls {
				q.close()
				delete(a.calls, call)
			}
			a.mu.Unlock()
			close(a.received)
			return
		}
		if d := f.GetAnswer().GetStreamSubscribe(); d != nil && d.GetConnection() {
			a.register(d.GetDelivery())
		}
		a.mu.Lock()
		q, ok := a.calls[f.GetCall()]
		a.mu.Unlock()
		if ok {
			q.push(f)
		}
	}
}

func (a *Attachment) register(call string) *queue {
	q := newQueue()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ended {
		q.close()
		return q
	}
	if held, ok := a.calls[call]; ok {
		return held
	}
	a.calls[call] = q
	return q
}

func (a *Attachment) forget(call string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.calls, call)
}

func (a *Attachment) send(f *interfacev1.ClientFrame) error {
	a.sending.Lock()
	defer a.sending.Unlock()
	return a.stream.Send(f)
}

// issue sends a request under a new call identity, stating the contract's version, and returns the
// frames that answer it.
func (a *Attachment) issue(r *interfacev1.Request) (string, *queue, error) {
	r.Version = uint32(Contract)
	a.mu.Lock()
	a.count++
	call := fmt.Sprintf("c-%d", a.count)
	ended := a.ended
	a.mu.Unlock()
	if ended {
		return "", nil, ErrEnded
	}
	q := a.register(call)
	if err := a.send(&interfacev1.ClientFrame{Call: call, Carries: &interfacev1.ClientFrame_Request{Request: r}}); err != nil {
		a.forget(call)
		return "", nil, ErrEnded
	}
	return call, q, nil
}

func (a *Attachment) cancel(call string) {
	a.send(&interfacev1.ClientFrame{Call: call, Carries: &interfacev1.ClientFrame_Cancel{Cancel: &interfacev1.Cancel{}}})
}

// first is a call's first frame as its answer: a refusal is the base's error, and an attachment that
// ended answers that it ended.
func (a *Attachment) first(ctx context.Context, call string, q *queue) (*interfacev1.Response, error) {
	f, ok, err := q.pop(ctx)
	switch {
	case err != nil:
		a.cancel(call)
		return nil, err
	case !ok:
		return nil, ErrEnded
	case f.GetRefusal() != nil:
		return nil, refusalOf(f.GetRefusal())
	}
	return f.GetAnswer(), nil
}

// call sends a request answered once.
func (a *Attachment) call(ctx context.Context, r *interfacev1.Request) (*interfacev1.Response, error) {
	call, q, err := a.issue(r)
	if err != nil {
		return nil, err
	}
	defer a.forget(call)
	return a.first(ctx, call, q)
}

// Authenticate presents an account and its secret, on a channel whose class takes a credential; on one
// whose class establishes the client, it answers who was established.
func (a *Attachment) Authenticate(ctx context.Context, account, secret string) (*interfacev1.Authenticated, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_Authenticate{Authenticate: &interfacev1.Authenticate{
		Credential: &interfacev1.Authenticate_Password_{Password: &interfacev1.Authenticate_Password{Account: account, Secret: secret}}}}})
	return resp.GetAuthenticate(), err
}

// AuthenticateToken presents a token an earlier authentication issued.
func (a *Attachment) AuthenticateToken(ctx context.Context, token string) (*interfacev1.Authenticated, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_Authenticate{Authenticate: &interfacev1.Authenticate{
		Credential: &interfacev1.Authenticate_Token{Token: token}}}})
	return resp.GetAuthenticate(), err
}

// Read is the records of a kind the channel sees, or of the one subject an identity names.
func (a *Attachment) Read(ctx context.Context, kind, identity string) ([]*interfacev1.Record, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_Read{Read: &interfacev1.Read{Kind: kind, Identity: identity}}})
	return resp.GetRead().GetRecords(), err
}

// Confirm states that the author has processed a subscription up to a sequence. Only the author knows
// that, so the library never confirms on its own.
func (a *Attachment) Confirm(ctx context.Context, subscription string, sequence uint64) error {
	_, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_Confirm{Confirm: &interfacev1.Confirm{
		Subscription: subscription, Sequence: sequence}}})
	return err
}

// Command sends a command to a unit, its payload opaque, and returns how the unit acknowledged it.
func (a *Attachment) Command(ctx context.Context, unit, typ string, payload []byte) (*interfacev1.Acknowledged, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_Command{Command: &interfacev1.Command{
		Unit: unit, Type: typ, Payload: payload}}})
	return resp.GetCommand(), err
}

// Ask asks a unit a question, its payload opaque, and returns the answer's.
func (a *Attachment) Ask(ctx context.Context, unit, typ string, payload []byte) ([]byte, error) {
	return a.AskCaused(ctx, "", unit, typ, payload)
}

// AskCaused asks a unit a question on a need's channel while answering another: cause is the identifier
// of the question the asking unit was delivered and is answering, and the Core gives this question that
// one's chain and its deadline less a margin. Only a need's channel takes a cause; any other refuses it
// `use.cause.unknown`.
func (a *Attachment) AskCaused(ctx context.Context, cause, unit, typ string, payload []byte) ([]byte, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_Query{Query: &interfacev1.Question{
		Unit: unit, Type: typ, Payload: payload, Cause: cause}}})
	return resp.GetQuery().GetPayload(), err
}

// StartStream asks for a unit's stream to flow, for everyone.
func (a *Attachment) StartStream(ctx context.Context, unit, stream string) (*interfacev1.Acknowledged, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_StreamStart{StreamStart: &interfacev1.UnitStream{
		Unit: unit, Stream: stream}}})
	return resp.GetStreamStart(), err
}

// StopStream asks for a unit's stream to stop flowing.
func (a *Attachment) StopStream(ctx context.Context, unit, stream string) (*interfacev1.Acknowledged, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_StreamStop{StreamStop: &interfacev1.UnitStream{
		Unit: unit, Stream: stream}}})
	return resp.GetStreamStop(), err
}

// Reclaim takes control back for a channel bound as a local socket.
func (a *Attachment) Reclaim(ctx context.Context) (*interfacev1.Reclaimed, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_Reclaim{Reclaim: &interfacev1.Reclaim{}}})
	return resp.GetReclaim(), err
}

// Subscribe opens a subscription narrowed by a filter, and returns once its snapshot or its refusal
// arrives.
func (a *Attachment) Subscribe(ctx context.Context, f *interfacev1.Filter) (*Subscription, error) {
	call, q, err := a.issue(&interfacev1.Request{Operation: &interfacev1.Request_Subscribe{Subscribe: &interfacev1.Subscribe{Filter: f}}})
	if err != nil {
		return nil, err
	}
	resp, err := a.first(ctx, call, q)
	if err != nil {
		a.forget(call)
		return nil, err
	}
	return &Subscription{ID: call, a: a, frames: q, pending: resp.GetSubscribe()}, nil
}

// Subscription is a subscription, delivered in order: its snapshot, then events and overflows, until its
// caller ends it, the Core completes it or the attachment ends. Nothing reconnects it.
type Subscription struct {
	// ID is what a confirmation names.
	ID string

	a        *Attachment
	frames   *queue
	pending  *interfacev1.Subscribed
	standing bool
	once     sync.Once
}

// Next is the next delivery; io.EOF once the subscription completed, ErrEnded once the attachment ended.
func (s *Subscription) Next() (*interfacev1.Subscribed, error) {
	if p := s.pending; p != nil {
		s.pending = nil
		return p, nil
	}
	f, ok, _ := s.frames.pop(context.Background())
	switch {
	case !ok:
		return nil, ErrEnded
	case f.GetRefusal() != nil:
		return nil, refusalOf(f.GetRefusal())
	case f.GetCompletion() != nil:
		return nil, io.EOF
	case f.GetEvent() != nil:
		return &interfacev1.Subscribed{Carries: &interfacev1.Subscribed_Event{Event: f.GetEvent()}}, nil
	}
	return f.GetAnswer().GetSubscribe(), nil
}

// Close ends the subscription on the wire. The standing subscription is the attachment's, and ends with
// it.
func (s *Subscription) Close() {
	s.once.Do(func() {
		if s.standing {
			return
		}
		s.a.cancel(s.ID)
		s.a.forget(s.ID)
		s.frames.close()
	})
}

// Frame is one message of a stream as a delivery carries it: its sequence, the producer's clock, and the
// payload, opaque.
type Frame struct {
	Sequence, SentAt uint64
	Payload          []byte
}

// header is the size of a frame's header on a per-subscriber socket: the sequence and the clock.
const header = 16

// packet is the largest frame a per-subscriber socket carries: a payload of 1 MiB and its header.
const packet = header + 1<<20

// Delivery is a copy of a stream delivered to this client, read where the answer said it arrives.
// Flowing is whether the stream was flowing when the subscription was answered.
type Delivery struct {
	ID      string
	Flowing bool
	// Arrives is where the Core said the delivery arrives: "socket", "stream_socket", "shared" or
	// "connection".
	Arrives string

	a      *Attachment
	socket net.Conn
	frames *queue
	buf    []byte
	// On a stream socket, each frame after its length.
	lengthed bool
	// On the shared ring: the lifeline, what it said, and the ring of the activation being read.
	lifeline *net.UnixConn
	said     chan lifelineMessage
	ring     *ring.Reader
}

// Reads is what this library can read a delivery from, beyond the per-subscriber packet socket.
type Reads = interfacev1.UnitStream_Reads

// The deliveries this library reads.
const (
	ReadsStreamSocket = interfacev1.UnitStream_READS_STREAM_SOCKET
	ReadsSharedObject = interfacev1.UnitStream_READS_SHARED_OBJECT
)

// lifelineMessage is one message of a shared delivery's lifeline: a ring, a stop, or its end.
type lifelineMessage struct {
	ring *os.File
	stop bool
	end  bool
}

// SubscribeStream asks for a copy of a unit's stream, naming every delivery this library reads, and opens
// it where the answer says it arrives: a per-subscriber socket the Core listens on, a stream socket, the
// stream's ring itself, or the attachment's own connection.
func (a *Attachment) SubscribeStream(ctx context.Context, unit, stream string) (*Delivery, error) {
	return a.SubscribeStreamReading(ctx, unit, stream, ReadsStreamSocket, ReadsSharedObject)
}

// SubscribeStreamReading is SubscribeStream naming only the deliveries given; with none, the packet socket
// every library reads.
func (a *Attachment) SubscribeStreamReading(ctx context.Context, unit, stream string, reads ...Reads) (*Delivery, error) {
	resp, err := a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_StreamSubscribe{StreamSubscribe: &interfacev1.UnitStream{
		Unit: unit, Stream: stream, Reads: reads}}})
	if err != nil {
		return nil, err
	}
	d := resp.GetStreamSubscribe()
	out := &Delivery{ID: d.GetDelivery(), Flowing: d.GetFlowing(), a: a}
	switch arrives := d.GetArrives().(type) {
	case *interfacev1.Delivering_Socket:
		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "unixpacket", arrives.Socket)
		if err != nil {
			return nil, fmt.Errorf("the delivery's socket %s cannot be reached: %w", arrives.Socket, err)
		}
		out.socket, out.buf, out.Arrives = conn, make([]byte, packet), "socket"
	case *interfacev1.Delivering_StreamSocket:
		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "unix", arrives.StreamSocket)
		if err != nil {
			return nil, fmt.Errorf("the delivery's socket %s cannot be reached: %w", arrives.StreamSocket, err)
		}
		out.socket, out.lengthed, out.Arrives = conn, true, "stream_socket"
	case *interfacev1.Delivering_Shared:
		conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: arrives.Shared, Net: "unix"})
		if err != nil {
			return nil, fmt.Errorf("the delivery's lifeline %s cannot be reached: %w", arrives.Shared, err)
		}
		out.lifeline, out.said, out.Arrives = conn, make(chan lifelineMessage, 4), "shared"
		go listen(conn, out.said)
	case *interfacev1.Delivering_Connection:
		out.frames, out.Arrives = a.register(out.ID), "connection"
	default:
		return nil, fmt.Errorf("the delivery %s arrives where this projection cannot read it: %v", out.ID, d.GetArrives())
	}
	return out, nil
}

// listen reads a shared delivery's lifeline: each message after its length, one byte carrying a ring's
// descriptor or nothing for a stop; its closing is the delivery's end.
func listen(conn *net.UnixConn, said chan<- lifelineMessage) {
	defer close(said)
	for {
		head, oob := make([]byte, 4), make([]byte, unix.CmsgSpace(4))
		n, oobn, _, _, err := conn.ReadMsgUnix(head, oob)
		if err != nil || n == 0 {
			said <- lifelineMessage{end: true}
			return
		}
		for n < 4 {
			m, err := conn.Read(head[n:])
			if err != nil {
				said <- lifelineMessage{end: true}
				return
			}
			n += m
		}
		length := binary.LittleEndian.Uint32(head)
		if length == 0 {
			said <- lifelineMessage{stop: true}
			continue
		}
		body := make([]byte, length)
		m, more, _, _, err := conn.ReadMsgUnix(body, oob[oobn:])
		if err != nil || m == 0 {
			said <- lifelineMessage{end: true}
			return
		}
		oobn += more
		var f *os.File
		if messages, err := unix.ParseSocketControlMessage(oob[:oobn]); err == nil {
			for _, msg := range messages {
				if fds, err := unix.ParseUnixRights(&msg); err == nil && len(fds) == 1 {
					f = os.NewFile(uintptr(fds[0]), "ring")
				}
			}
		}
		if f != nil {
			said <- lifelineMessage{ring: f}
		}
	}
}

// nextOnTheRing reads the ring of the activation being read, taking each ring and each stop the lifeline
// brings; io.EOF once the lifeline closed. It waits on the ring's word with no bound, and on the lifeline
// when the word is woken with nothing published — the Core of 0.16.0 does so after every stop and every end
// the lifeline carries, the message already written — so neither is read at an interval.
func (d *Delivery) nextOnTheRing() (Frame, error) {
	// take applies one message of the lifeline, and says whether the delivery ended.
	take := func(m lifelineMessage, open bool) (bool, error) {
		if d.ring != nil {
			d.ring.Close()
			d.ring = nil
		}
		if !open || m.end {
			return true, nil
		}
		if m.ring != nil {
			r, err := ring.OpenReader(m.ring)
			m.ring.Close()
			if err != nil {
				return false, err
			}
			d.ring = r
		}
		return false, nil
	}
	for {
		var m lifelineMessage
		open, got := false, false
		if d.ring == nil {
			m, open = <-d.said
			got = true
		} else {
			select {
			case m, open = <-d.said:
				got = true
			default:
			}
		}
		if !got {
			f, _, err := d.ring.NextWoken()
			if err == nil {
				return Frame{Sequence: f.Sequence, SentAt: f.Clock, Payload: f.Payload}, nil
			}
			if !errors.Is(err, ring.ErrWoken) {
				return Frame{}, err
			}
			// The word moved with nothing published: the lifeline's message is on its way.
			m, open = <-d.said
		}
		ended, err := take(m, open)
		if err != nil {
			return Frame{}, err
		}
		if ended {
			return Frame{}, io.EOF
		}
	}
}

// Next is the next frame, in order; io.EOF once the delivery ended, ErrEnded once the attachment did. On
// the shared ring, a frame the reader fell behind for is a gap in the sequence.
func (d *Delivery) Next() (Frame, error) {
	if d.lifeline != nil {
		return d.nextOnTheRing()
	}
	if d.socket != nil && d.lengthed {
		var head [4]byte
		if _, err := io.ReadFull(d.socket, head[:]); err != nil {
			return Frame{}, io.EOF
		}
		body := make([]byte, binary.LittleEndian.Uint32(head[:]))
		if _, err := io.ReadFull(d.socket, body); err != nil {
			return Frame{}, io.EOF
		}
		if len(body) < header {
			return Frame{}, fmt.Errorf("a frame of %d bytes is shorter than its header", len(body))
		}
		return Frame{Sequence: binary.LittleEndian.Uint64(body[0:8]), SentAt: binary.LittleEndian.Uint64(body[8:16]), Payload: body[header:]}, nil
	}
	if d.socket != nil {
		n, err := d.socket.Read(d.buf)
		if err != nil || n == 0 {
			return Frame{}, io.EOF
		}
		if n < header {
			return Frame{}, fmt.Errorf("a frame of %d bytes is shorter than its header", n)
		}
		payload := make([]byte, n-header)
		copy(payload, d.buf[header:n])
		return Frame{Sequence: binary.LittleEndian.Uint64(d.buf[0:8]), SentAt: binary.LittleEndian.Uint64(d.buf[8:16]), Payload: payload}, nil
	}
	f, ok, _ := d.frames.pop(context.Background())
	switch {
	case !ok:
		return Frame{}, ErrEnded
	case f.GetCompletion() != nil:
		return Frame{}, io.EOF
	case f.GetRefusal() != nil:
		return Frame{}, refusalOf(f.GetRefusal())
	}
	s := f.GetDelivery()
	return Frame{Sequence: s.GetSequence(), SentAt: s.GetSentAt(), Payload: s.GetPayload()}, nil
}

// Release ends the delivery: the Core is told, and what was opened for it is let go.
func (d *Delivery) Release(ctx context.Context) error {
	_, err := d.a.call(ctx, &interfacev1.Request{Operation: &interfacev1.Request_StreamUnsubscribe{StreamUnsubscribe: &interfacev1.StreamRelease{
		Delivery: d.ID}}})
	if d.socket != nil {
		d.socket.Close()
	}
	if d.lifeline != nil {
		d.lifeline.Close()
	}
	if d.frames != nil {
		d.a.forget(d.ID)
	}
	return err
}

// Follow is a unit's stream followed: started and subscribed, and started and subscribed again each time its
// delivery ends because the unit's Session did — a restart of the unit, on a need's channel or any other.
// Frames come in order within a delivery; the next delivery's numbering is its own.
type Follow struct {
	a            *Attachment
	unit, stream string
	reads        []Reads
	ctx          context.Context
	cancel       context.CancelFunc
	current      *Delivery
	retry        time.Duration
	once         sync.Once
	mu           sync.Mutex
}

// FollowRetry is how long a follow waits between two attempts while the unit is not there to subscribe to.
const FollowRetry = 250 * time.Millisecond

// returning is whether a refusal says the unit is not there yet — not running, or running with no Session
// — which a follow waits out.
func returning(err error) bool {
	var r *base.Refusal
	return errors.As(err, &r) && (r.Code == "unit.not_running" || r.Code == "unit.no_session")
}

// Follow follows a unit's stream, naming every delivery this library reads, or those given. The first
// subscription is made before it returns; a refusal of it other than the unit's absence is returned.
func (a *Attachment) Follow(ctx context.Context, unit, stream string, reads ...Reads) (*Follow, error) {
	if len(reads) == 0 {
		reads = []Reads{ReadsStreamSocket, ReadsSharedObject}
	}
	inner, cancel := context.WithCancel(context.Background())
	f := &Follow{a: a, unit: unit, stream: stream, reads: reads, ctx: inner, cancel: cancel, retry: FollowRetry}
	if err := f.subscribe(ctx); err != nil {
		cancel()
		return nil, err
	}
	return f, nil
}

// subscribe starts the stream, where it may, and subscribes to it, until it succeeds, the unit's absence
// aside.
func (f *Follow) subscribe(ctx context.Context) error {
	for {
		_, err := f.a.StartStream(ctx, f.unit, f.stream)
		if err == nil || !returning(err) {
			// A start refused for any other reason leaves a stream that may flow anyway: the subscription says.
			d, err := f.a.SubscribeStreamReading(ctx, f.unit, f.stream, f.reads...)
			if err == nil {
				f.mu.Lock()
				f.current = d
				f.mu.Unlock()
				return nil
			}
			if !returning(err) {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.ctx.Done():
			return ErrEnded
		case <-time.After(f.retry):
		}
	}
}

// Delivery is the identifier of the delivery the last frame came on.
func (f *Follow) Delivery() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current == nil {
		return ""
	}
	return f.current.ID
}

// Next is the next frame of the stream, across the unit's restarts; ErrEnded once the follow was closed or
// the attachment ended.
func (f *Follow) Next() (Frame, error) {
	for {
		f.mu.Lock()
		d := f.current
		f.mu.Unlock()
		if d == nil {
			return Frame{}, ErrEnded
		}
		frame, err := d.Next()
		if err == nil {
			return frame, nil
		}
		if f.ctx.Err() != nil || errors.Is(err, ErrEnded) {
			return Frame{}, ErrEnded
		}
		if !errors.Is(err, io.EOF) && !returning(err) {
			return Frame{}, err
		}
		// The delivery ended with the unit's Session: what was opened for it is let go, and the stream
		// followed again.
		d.Release(f.ctx)
		if err := f.subscribe(f.ctx); err != nil {
			return Frame{}, ErrEnded
		}
	}
}

// Close ends the follow and releases its delivery.
func (f *Follow) Close(ctx context.Context) error {
	var err error
	f.once.Do(func() {
		f.cancel()
		f.mu.Lock()
		d := f.current
		f.current = nil
		f.mu.Unlock()
		if d != nil {
			err = d.Release(ctx)
		}
	})
	return err
}
