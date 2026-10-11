package plugin_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	pluginv1 "github.com/yoke-project/yoke/proto/yoke/plugin/v1"
)

// coreRing is a ring of layout 1 behind a packet socket, as the Core creates one: the first connection is
// handed the descriptor that writes, on one byte.
type coreRing struct {
	path     string
	mem      []byte
	slots    int
	capacity int
}

func newCoreRing(t *testing.T, slots, capacity int) *coreRing {
	t.Helper()
	fd, err := unix.MemfdCreate("yoke-test", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	size := 64 + slots*(32+capacity)
	unix.Ftruncate(fd, int64(size))
	mem, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Munmap(mem) })
	copy(mem, "YOKERING")
	binary.LittleEndian.PutUint32(mem[8:], 1)
	binary.LittleEndian.PutUint32(mem[12:], uint32(slots))
	binary.LittleEndian.PutUint32(mem[16:], uint32(capacity))
	dir, _ := os.MkdirTemp("", "yr")
	t.Cleanup(func() { os.RemoveAll(dir) })
	r := &coreRing{path: filepath.Join(dir, "preview.sock"), mem: mem, slots: slots, capacity: capacity}
	l, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: r.path, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close(); unix.Close(fd) })
	go func() {
		c, err := l.AcceptUnix()
		if err != nil {
			return
		}
		c.WriteMsgUnix([]byte{0}, unix.UnixRights(fd), nil)
	}()
	return r
}

func (r *coreRing) published() uint64 { return binary.LittleEndian.Uint64(r.mem[24:]) }

// slot is frame n's sequence, clock and payload as its slot holds them.
func (r *coreRing) slot(n uint64) (uint64, uint64, []byte) {
	at := 64 + int((n-1)%uint64(r.slots))*(32+r.capacity)
	le := binary.LittleEndian
	return le.Uint64(r.mem[at:]), le.Uint64(r.mem[at+8:]), r.mem[at+32 : at+32+int(le.Uint32(r.mem[at+16:]))]
}

// std: yoke-sdk-go:the-plugin-library.18
func TestActivatedOnTheRingEmitPublishesIntoIt(t *testing.T) {
	c := &channel{}
	u := start(t, c)
	until(t, "the Session opening", func() bool { return len(c.got()) > 0 })
	r := newCoreRing(t, 4, 64)
	c.send(activate("c-1", "station.preview", pluginv1.Control_Activate_TRANSPORT_SHARED_OBJECT, r.path))
	if a := acked(t, c, "c-1"); a.GetOutcome() != pluginv1.Ack_OUTCOME_DONE {
		t.Fatalf("the activation on the ring was acknowledged %v", a)
	}
	for i := byte(1); i <= 3; i++ {
		if err := u.Emit("station.preview", []byte{i, 0xdd}); err != nil {
			t.Fatalf("emitting on the ring: %v", err)
		}
	}
	if r.published() != 3 {
		t.Fatalf("%d frames published, want 3", r.published())
	}
	for n := uint64(1); n <= 3; n++ {
		sequence, clock, payload := r.slot(n)
		if sequence != n || clock == 0 || !bytes.Equal(payload, []byte{byte(n), 0xdd}) {
			t.Errorf("frame %d is %d %d %x", n, sequence, clock, payload)
		}
	}
	err := u.Emit("station.preview", make([]byte, 65))
	if err == nil || !strings.Contains(err.Error(), "64") {
		t.Errorf("a payload larger than a slot: %v", err)
	}
	if r.published() != 3 {
		t.Errorf("%d frames published after the refusal", r.published())
	}
}
