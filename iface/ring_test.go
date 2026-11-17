package iface_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	interfacev1 "github.com/yoke-project/yoke/proto/yoke/interface/v1"

	"github.com/yoke-project/yoke-sdk-go/iface"
)

// wakeups is how many times this process's threads were scheduled so far.
func wakeups() int {
	total := 0
	tasks, _ := filepath.Glob("/proc/self/task/*/status")
	for _, task := range tasks {
		b, _ := os.ReadFile(task)
		for _, line := range strings.Split(string(b), "\n") {
			if k, v, ok := strings.Cut(line, ":"); ok && strings.HasSuffix(k, "ctxt_switches") {
				n, _ := strconv.Atoi(strings.TrimSpace(v))
				total += n
			}
		}
	}
	return total
}

// std: yoke-sdk-go:the-interface-library.10
func TestAReaderOfTheRingWaitsOnItsWakeWord(t *testing.T) {
	// The ring, as the Core makes it: four slots of 64 bytes, handed on the lifeline at each activation.
	const slots, capacity = 4, 64
	size := 64 + slots*(32+capacity)
	fd, err := unix.MemfdCreate("yoke-test", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	memory := os.NewFile(uintptr(fd), "ring")
	defer memory.Close()
	unix.Ftruncate(fd, int64(size))
	mem, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(mem)
	copy(mem, "YOKERING")
	le := binary.LittleEndian
	le.PutUint32(mem[8:], 1)
	le.PutUint32(mem[12:], slots)
	le.PutUint32(mem[16:], capacity)
	word := (*uint32)(unsafe.Pointer(&mem[32]))
	// wake moves the word and wakes every waiter, as a producer does at a frame and the Core after a
	// message on the lifeline.
	wake := func() {
		atomic.AddUint32(word, 1)
		unix.Syscall6(unix.SYS_FUTEX, uintptr(unsafe.Pointer(word)), 1, 1<<30, 0, 0, 0)
	}

	dir, err := os.MkdirTemp("", "iface-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "00000001.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan *net.UnixConn, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		conn.WriteMsgUnix([]byte{1, 0, 0, 0, 0}, unix.UnixRights(fd), nil)
		accepted <- conn
	}()

	c := &core{opening: opening(1)}
	c.answer = func(call string, r *interfacev1.Request, send func(*interfacev1.CoreFrame)) {
		send(answered(call, &interfacev1.Response{Answer: &interfacev1.Response_StreamSubscribe{StreamSubscribe: &interfacev1.Delivering{
			Delivery: "00000001", Flowing: true, Arrives: &interfacev1.Delivering_Shared{Shared: path}}}}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, err := iface.Attach(ctx, serve(t, c))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	d, err := a.SubscribeStream(ctx, "acquire", "spectra")
	if err != nil {
		t.Fatal(err)
	}
	conn := <-accepted

	type read struct {
		f   iface.Frame
		err error
	}
	reads := make(chan read, 4)
	next := func() {
		f, err := d.Next()
		reads <- read{f, err}
	}
	go next()
	time.Sleep(300 * time.Millisecond)
	before := wakeups()
	time.Sleep(time.Second)
	idle := wakeups() - before

	// A frame published wakes the reader at once.
	le.PutUint64(mem[64+8:], 1001)
	le.PutUint32(mem[64+16:], 3)
	copy(mem[64+32:], "one")
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&mem[64])), 1)
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&mem[24])), 1)
	published := time.Now()
	wake()
	var got read
	select {
	case got = <-reads:
	case <-time.After(2 * time.Second):
		t.Fatal("the frame published was not read")
	}
	took := time.Since(published)

	// The lifeline closes and the ring is woken, as the Core does at a delivery's end.
	go next()
	time.Sleep(100 * time.Millisecond)
	conn.Close()
	wake()
	var ended read
	select {
	case ended = <-reads:
	case <-time.After(2 * time.Second):
		t.Fatal("the delivery did not end once its lifeline closed and the ring was woken")
	}

	if idle > 30 {
		t.Errorf("waiting a second on an empty ring, the process was scheduled %d times", idle)
	}
	if got.err != nil || got.f.Sequence != 1 || got.f.SentAt != 1001 || string(got.f.Payload) != "one" {
		t.Errorf("the frame was read as %+v, %v", got.f, got.err)
	}
	if took > 100*time.Millisecond {
		t.Errorf("the frame was read %v after it was published", took)
	}
	if ended.err != io.EOF {
		t.Errorf("the delivery ended with %v", ended.err)
	}
}
