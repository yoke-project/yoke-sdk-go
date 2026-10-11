// Package ring is the producer's side of the shared ring, layout 1: a frame is published into the slot
// its sequence selects, and every reader waiting on the shared word is woken.
package ring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	headerSize   = 64
	slotHeader   = 32
	magic        = "YOKERING"
	version      = 1
	offSlots     = 12
	offCapacity  = 16
	offPublished = 24
	offWake      = 32
	futexWakeOp  = 1
)

// Writer publishes into a ring the producer was handed.
type Writer struct {
	mem       []byte
	slots     uint32
	capacity  uint32
	published uint64
}

// OpenWriter maps the ring a descriptor names, for writing, and reads its geometry from its header.
func OpenWriter(f *os.File) (*Writer, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < headerSize {
		return nil, fmt.Errorf("the ring is %d bytes, shorter than its header", info.Size())
	}
	mem, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	le := binary.LittleEndian
	w := &Writer{mem: mem, slots: le.Uint32(mem[offSlots:]), capacity: le.Uint32(mem[offCapacity:])}
	switch {
	case string(mem[:8]) != magic:
		err = errors.New("the memory handed is not a ring")
	case le.Uint32(mem[8:]) != version:
		err = fmt.Errorf("the ring is of layout %d; this library writes layout %d", le.Uint32(mem[8:]), version)
	case w.slots == 0 || headerSize+int(w.slots)*(slotHeader+int(w.capacity)) > len(mem):
		err = fmt.Errorf("the ring's geometry, %d slots of %d bytes, does not fit its %d bytes", w.slots, w.capacity, len(mem))
	}
	if err != nil {
		unix.Munmap(mem)
		return nil, err
	}
	w.published = atomic.LoadUint64((*uint64)(unsafe.Pointer(&mem[offPublished])))
	return w, nil
}

// Publish writes one frame and wakes every waiter. A frame larger than a slot is refused.
func (w *Writer) Publish(clock uint64, payload []byte) error {
	if len(payload) > int(w.capacity) {
		return fmt.Errorf("the frame is %d bytes, and a slot of this ring holds %d", len(payload), w.capacity)
	}
	n := w.published + 1
	at := headerSize + int((n-1)%uint64(w.slots))*(slotHeader+int(w.capacity))
	sequence := (*uint64)(unsafe.Pointer(&w.mem[at]))
	atomic.StoreUint64(sequence, 0)
	binary.LittleEndian.PutUint64(w.mem[at+8:], clock)
	binary.LittleEndian.PutUint32(w.mem[at+16:], uint32(len(payload)))
	copy(w.mem[at+slotHeader:], payload)
	atomic.StoreUint64(sequence, n)
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&w.mem[offPublished])), n)
	wake := (*uint32)(unsafe.Pointer(&w.mem[offWake]))
	atomic.AddUint32(wake, 1)
	unix.Syscall6(unix.SYS_FUTEX, uintptr(unsafe.Pointer(wake)), futexWakeOp, uintptr(math.MaxInt32), 0, 0, 0)
	w.published = n
	return nil
}

// Close lets the mapping go.
func (w *Writer) Close() error { return unix.Munmap(w.mem) }
