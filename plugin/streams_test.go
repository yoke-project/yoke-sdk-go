package plugin_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"

	"github.com/yoke-project/yoke-sdk-go/base"
	"github.com/yoke-project/yoke-sdk-go/plugin"
)

// transports are a packet socket and a datagram socket the test listens on, as the Core would for two
// activated streams, and an address nothing listens on.
type transports struct {
	ordered       *net.UnixListener
	framed        *net.UnixConn
	orderedPath   string
	framedPath    string
	nobody        string
	orderedReader chan []byte
}

func listening(t *testing.T) *transports {
	t.Helper()
	dir, _ := os.MkdirTemp("", "ye")
	t.Cleanup(func() { os.RemoveAll(dir) })
	tr := &transports{orderedPath: filepath.Join(dir, "spectra.sock"), framedPath: filepath.Join(dir, "preview.sock"),
		nobody: filepath.Join(dir, "nobody.sock"), orderedReader: make(chan []byte, 16)}
	var err error
	if tr.ordered, err = net.ListenUnix("unixpacket", &net.UnixAddr{Name: tr.orderedPath, Net: "unixpacket"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.ordered.Close() })
	if tr.framed, err = net.ListenUnixgram("unixgram", &net.UnixAddr{Name: tr.framedPath, Net: "unixgram"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.framed.Close() })
	go func() {
		conn, err := tr.ordered.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1<<16)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				close(tr.orderedReader)
				return
			}
			tr.orderedReader <- bytes.Clone(buf[:n])
		}
	}()
	return tr
}

func activate(id, stream string, transport pluginv1.Control_Activate_Transport, address string) *pluginv1.Envelope {
	return &pluginv1.Envelope{MessageId: id, SessionId: "sid-1", Payload: &pluginv1.Envelope_Control{Control: &pluginv1.Control{
		Kind: &pluginv1.Control_Activate_{Activate: &pluginv1.Control_Activate{Stream: stream, Transport: transport, Address: address}}}}}
}

// acked is the acknowledgement the channel received for the message identified, waiting for it.
func acked(t *testing.T, c *channel, id string) *pluginv1.Ack {
	t.Helper()
	var found *pluginv1.Ack
	until(t, "the acknowledgement of "+id, func() bool {
		for _, e := range c.got() {
			if e.CorrelationId == id && e.GetAck() != nil {
				found = e.GetAck()
				return true
			}
		}
		return false
	})
	return found
}

// handed is the next event of the kind the author is handed, skipping others.
func handed[T any](t *testing.T, u *plugin.Unit) T {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-u.Events():
			if v, ok := e.(T); ok {
				return v
			}
		case <-deadline:
			var zero T
			t.Fatalf("the author was handed no %T", zero)
			return zero
		}
	}
}

// std: yoke-sdk-go:the-plugin-library.15
func TestAnActivationConnectsTheLibraryAndIsAcknowledged(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	tr := listening(t)
	c.send(activate("c-1", "station.spectra", pluginv1.Control_Activate_TRANSPORT_ORDERED, tr.orderedPath))
	if a := acked(t, c, "c-1"); a.GetOutcome() != pluginv1.Ack_OUTCOME_DONE {
		t.Errorf("the ordered activation was acknowledged %v", a)
	}
	if a := handed[plugin.Activated](t, u); a.Stream != "station.spectra" {
		t.Errorf("the author was handed %+v", a)
	}
	c.send(activate("c-2", "station.preview", pluginv1.Control_Activate_TRANSPORT_FRAMED, tr.framedPath))
	if a := acked(t, c, "c-2"); a.GetOutcome() != pluginv1.Ack_OUTCOME_DONE {
		t.Errorf("the framed activation was acknowledged %v", a)
	}
	if a := handed[plugin.Activated](t, u); a.Stream != "station.preview" {
		t.Errorf("the author was handed %+v", a)
	}
	c.send(activate("c-3", "station.diagnostics", pluginv1.Control_Activate_TRANSPORT_ORDERED, tr.nobody))
	if a := acked(t, c, "c-3"); a.GetOutcome() != pluginv1.Ack_OUTCOME_FAILED || a.GetLine() == "" {
		t.Errorf("the activation at an address nothing listens on was acknowledged %v", a)
	}
	var refusal *base.Refusal
	if err := u.Emit("station.diagnostics", []byte("x")); !errors.As(err, &refusal) || refusal.Code != "stream.inactive" {
		t.Errorf("emitting on the stream that failed gave %v", err)
	}
}

// std: yoke-sdk-go:the-plugin-library.16
func TestEmitWritesOneEnvelopePerPacketOrOneFramePerDatagram(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	tr := listening(t)
	c.send(activate("c-1", "station.spectra", pluginv1.Control_Activate_TRANSPORT_ORDERED, tr.orderedPath))
	c.send(activate("c-2", "station.preview", pluginv1.Control_Activate_TRANSPORT_FRAMED, tr.framedPath))
	acked(t, c, "c-1")
	acked(t, c, "c-2")
	before := len(c.got())
	for i := byte(1); i <= 3; i++ {
		if err := u.Emit("station.spectra", []byte{i, 0xff}); err != nil {
			t.Fatalf("emitting on the ordered stream: %v", err)
		}
		if err := u.Emit("station.preview", []byte{i, 0xee}); err != nil {
			t.Fatalf("emitting on the framed stream: %v", err)
		}
	}
	for i := uint64(1); i <= 3; i++ {
		select {
		case raw := <-tr.orderedReader:
			e := &pluginv1.Envelope{}
			if err := proto.Unmarshal(raw, e); err != nil || e.GetData().GetSequence() != i || !bytes.Equal(e.GetData().GetPayload(), []byte{byte(i), 0xff}) ||
				e.MessageId == "" || e.SessionId != "sid-1" || e.SentAtUnixNano == 0 {
				t.Errorf("packet %d is %v %v", i, e, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("the ordered socket read nothing for packet %d", i)
		}
		tr.framed.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 64)
		n, _, err := tr.framed.ReadFromUnix(buf)
		if err != nil || n != 18 || binary.LittleEndian.Uint64(buf[0:8]) != i || binary.LittleEndian.Uint64(buf[8:16]) == 0 ||
			!bytes.Equal(buf[16:n], []byte{byte(i), 0xee}) {
			t.Errorf("datagram %d is %x %v", i, buf[:n], err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	for _, e := range c.got()[before:] {
		if e.GetData() != nil {
			t.Errorf("a data message reached the Session: %v", e)
		}
	}
}

// std: yoke-sdk-go:the-plugin-library.17
func TestAStopClosesTheTransportAndEmitIsRefusedAfter(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	tr := listening(t)
	c.send(activate("c-1", "station.spectra", pluginv1.Control_Activate_TRANSPORT_ORDERED, tr.orderedPath))
	acked(t, c, "c-1")
	handed[plugin.Activated](t, u)
	c.send(&pluginv1.Envelope{MessageId: "c-2", SessionId: "sid-1", Payload: &pluginv1.Envelope_Control{Control: &pluginv1.Control{
		Kind: &pluginv1.Control_Stop_{Stop: &pluginv1.Control_Stop{Stream: "station.spectra"}}}}})
	if a := acked(t, c, "c-2"); a.GetOutcome() != pluginv1.Ack_OUTCOME_DONE {
		t.Errorf("the stop was acknowledged %v", a)
	}
	if s := handed[plugin.Stopped](t, u); s.Stream != "station.spectra" {
		t.Errorf("the author was handed %+v", s)
	}
	select {
	case raw, open := <-tr.orderedReader:
		if open {
			t.Errorf("after the stop the socket read %x", raw)
		}
	case <-time.After(2 * time.Second):
		t.Error("the library's connection is still open")
	}
	var refusal *base.Refusal
	if err := u.Emit("station.spectra", []byte("late")); !errors.As(err, &refusal) || refusal.Code != "stream.inactive" || !strings.Contains(refusal.Message, "station.spectra") {
		t.Errorf("emitting after the stop gave %v", err)
	}
}
