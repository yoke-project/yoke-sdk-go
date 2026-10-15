package iface_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	interfacev1 "github.com/yoke-project/yoke/proto/yoke/interface/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
	"github.com/yoke-project/yoke-sdk-go/iface"
)

// root is a certificate a client may trust, and the chain it issued for 127.0.0.1.
type root struct {
	pool  *x509.CertPool
	chain tls.Certificate
}

func newRoot(t *testing.T, name string) root {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return root{pool: pool, chain: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}}
}

// routable is a Core's interface surface on a routable address: it reads the first frame before it opens,
// and records what each connection negotiated.
type routable struct {
	interfacev1.UnimplementedInterfaceServer
	token string

	mu     sync.Mutex
	states []tls.ConnectionState
	firsts []*interfacev1.ClientFrame
}

func (c *routable) Attach(stream interfacev1.Interface_AttachServer) error {
	if p, ok := peer.FromContext(stream.Context()); ok {
		if info, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			c.mu.Lock()
			c.states = append(c.states, info.State)
			c.mu.Unlock()
		}
	}
	if c.token == "" {
		// A Core that establishes nothing opens at once.
		stream.Send(&interfacev1.CoreFrame{Carries: &interfacev1.CoreFrame_Opening{Opening: opening(1)}})
		stream.Recv()
		return nil
	}
	f, err := stream.Recv()
	if err != nil {
		return nil
	}
	c.mu.Lock()
	c.firsts = append(c.firsts, f)
	c.mu.Unlock()
	auth := f.GetRequest().GetAuthenticate()
	p := auth.GetPassword()
	if !(p.GetAccount() == "alice" && p.GetSecret() == "hunter2x") && auth.GetToken() != c.token {
		ref := &interfacev1.Refusal{Code: "auth.invalid", Message: "the credential establishes nobody"}
		st, _ := status.New(codes.Unauthenticated, ref.GetMessage()).WithDetails(ref)
		return st.Err()
	}
	stream.Send(&interfacev1.CoreFrame{Carries: &interfacev1.CoreFrame_Opening{Opening: opening(1)}})
	stream.Send(eventAt(5))
	stream.Send(answered(f.GetCall(), &interfacev1.Response{Answer: &interfacev1.Response_Authenticate{Authenticate: &interfacev1.Authenticated{
		Account: "alice", Token: c.token, Expires: timestamppb.New(time.Unix(1_800_000_000, 0))}}}))
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

func (c *routable) negotiated() []tls.ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]tls.ConnectionState(nil), c.states...)
}

// serveTLS binds c on loopback over TLS, presenting the chain r issued, and returns its address.
func serveTLS(t *testing.T, c *routable, r root, max uint16) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{r.chain}, MaxVersion: max})))
	interfacev1.RegisterInterfaceServer(s, c)
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return l.Addr().String()
}

// std: yoke-sdk-go:across-a-routable-address.01
func TestTheLibraryDialsTLS13AgainstTheCallersRoots(t *testing.T) {
	a, b := newRoot(t, "a"), newRoot(t, "b")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	good := &routable{}
	att, err := iface.Attach(ctx, serveTLS(t, good, a, 0), iface.TLS(a.pool, ""))
	if err != nil {
		t.Fatalf("attaching over TLS with the right roots: %v", err)
	}
	att.Close()
	if st := good.negotiated(); len(st) != 1 || st[0].Version != tls.VersionTLS13 || st[0].NegotiatedProtocol != "h2" || len(st[0].PeerCertificates) != 0 {
		t.Errorf("the Core read %+v", st)
	}

	old := &routable{}
	if att, err := iface.Attach(ctx, serveTLS(t, old, a, tls.VersionTLS12), iface.TLS(a.pool, "")); err == nil {
		att.Close()
		t.Error("a Core capped at TLS 1.2 was attached")
	}
	other := &routable{}
	if att, err := iface.Attach(ctx, serveTLS(t, other, b, 0), iface.TLS(a.pool, "")); err == nil {
		att.Close()
		t.Error("a Core presenting a chain the roots do not hold was attached")
	}
	if len(old.negotiated())+len(other.negotiated()) != 0 {
		t.Error("a refused handshake reached the Core's surface")
	}

	if _, err := iface.Attach(ctx, "127.0.0.1:1", iface.TLS(nil, "")); err == nil || !contains(err.Error(), "roots") {
		t.Errorf("no roots was answered %v", err)
	}
	if _, err := iface.Attach(ctx, filepath.Join(t.TempDir(), "panel.sock"), iface.TLS(a.pool, "")); err == nil || !contains(err.Error(), "local socket") {
		t.Errorf("TLS on a local socket was answered %v", err)
	}
}

// std: yoke-sdk-go:across-a-routable-address.02
func TestTheCredentialIsTheFirstFrameAndItsAnswerSurfaced(t *testing.T) {
	r := newRoot(t, "a")
	c := &routable{token: "dG9rZW4tZm9yLWFsaWNl"}
	address := serveTLS(t, c, r, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i, presented := range []iface.Option{iface.Password("alice", "hunter2x"), iface.Token(c.token)} {
		a, err := iface.Attach(ctx, address, iface.TLS(r.pool, ""), presented)
		if err != nil {
			t.Fatalf("attachment %d: %v", i, err)
		}
		if a.Version != 1 || a.Standing == nil || len(a.Picture.GetRecords()) != 1 {
			t.Errorf("attachment %d surfaced version %d, standing %v, picture %v", i, a.Version, a.Standing, a.Picture)
		}
		e := a.Established
		if e.GetAccount() != "alice" || e.GetToken() != c.token || e.GetExpires().AsTime().Unix() != 1_800_000_000 {
			t.Errorf("attachment %d established %v", i, e)
		}
		if d, err := a.Standing.Next(); err != nil || d.GetEvent().GetSeq() != 5 {
			t.Errorf("attachment %d's standing subscription delivered %v %v", i, d, err)
		}
		a.Close()
	}
	c.mu.Lock()
	firsts := append([]*interfacev1.ClientFrame(nil), c.firsts...)
	c.mu.Unlock()
	if len(firsts) != 2 {
		t.Fatalf("the Core read %d first frames", len(firsts))
	}
	for i, f := range firsts {
		if iface.Name(f.GetRequest()) != "authenticate" || f.GetRequest().GetVersion() != uint32(iface.Contract) {
			t.Errorf("attachment %d's first frame is %v", i, f)
		}
	}
	if p := firsts[0].GetRequest().GetAuthenticate().GetPassword(); p.GetAccount() != "alice" || p.GetSecret() != "hunter2x" {
		t.Errorf("the first attachment presented %v", firsts[0])
	}
	if firsts[1].GetRequest().GetAuthenticate().GetToken() != c.token {
		t.Errorf("the second attachment presented %v", firsts[1])
	}

	if a, err := iface.Attach(ctx, address, iface.TLS(r.pool, ""), iface.Password("alice", "wrong")); !base.IsRefusal(err, "auth.invalid") {
		if a != nil {
			a.Close()
		}
		t.Errorf("a secret the host refuses was answered %v", err)
	}
}
