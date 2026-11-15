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
	"time"
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
	futexWaitOp  = 0
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

// Frame is one frame a reader read from a ring.
type Frame struct {
	Sequence, Clock uint64
	Payload         []byte
}

// ErrNoFrame is what Next answers when nothing was published within the interval.
var ErrNoFrame = errors.New("no frame was published within the interval")

// Reader is one reader of a ring, at its own pace: a client the Core handed the ring for reading alone.
type Reader struct {
	mem      []byte
	slots    uint32
	capacity uint32
	next     uint64
}

// OpenReader maps the ring a descriptor names, for reading alone, and reads its geometry from its header.
// It starts from the oldest frame the ring still holds.
func OpenReader(f *os.File) (*Reader, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < headerSize {
		return nil, fmt.Errorf("the ring is %d bytes, shorter than its header", info.Size())
	}
	mem, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	le := binary.LittleEndian
	r := &Reader{mem: mem, slots: le.Uint32(mem[offSlots:]), capacity: le.Uint32(mem[offCapacity:]), next: 1}
	switch {
	case string(mem[:8]) != magic:
		err = errors.New("the memory handed is not a ring")
	case le.Uint32(mem[8:]) != version:
		err = fmt.Errorf("the ring is of layout %d; this library reads layout %d", le.Uint32(mem[8:]), version)
	case r.slots == 0 || headerSize+int(r.slots)*(slotHeader+int(r.capacity)) > len(mem):
		err = fmt.Errorf("the ring's geometry, %d slots of %d bytes, does not fit its %d bytes", r.slots, r.capacity, len(mem))
	}
	if err != nil {
		unix.Munmap(mem)
		return nil, err
	}
	return r, nil
}

// Next returns the next frame the reader can still read, and how many before it were lost: a lap by the
// producer, or a slot overwritten while it was copied. It waits at most the interval for a frame.
func (r *Reader) Next(interval time.Duration) (Frame, uint64, error) {
	deadline := time.Now().Add(interval)
	var gap uint64
	wakeAt := (*uint32)(unsafe.Pointer(&r.mem[offWake]))
	publishedAt := (*uint64)(unsafe.Pointer(&r.mem[offPublished]))
	for {
		wake := atomic.LoadUint32(wakeAt)
		published := atomic.LoadUint64(publishedAt)
		if published < r.next {
			left := time.Until(deadline)
			if left <= 0 {
				return Frame{}, gap, ErrNoFrame
			}
			ts := unix.NsecToTimespec(left.Nanoseconds())
			unix.Syscall6(unix.SYS_FUTEX, uintptr(unsafe.Pointer(wakeAt)), futexWaitOp, uintptr(wake), uintptr(unsafe.Pointer(&ts)), 0, 0)
			continue
		}
		if published >= uint64(r.slots) {
			if oldest := published - uint64(r.slots) + 1; r.next < oldest {
				gap += oldest - r.next
				r.next = oldest
			}
		}
		n := r.next
		r.next++
		at := headerSize + int((n-1)%uint64(r.slots))*(slotHeader+int(r.capacity))
		sequence := (*uint64)(unsafe.Pointer(&r.mem[at]))
		before := atomic.LoadUint64(sequence)
		le := binary.LittleEndian
		clock := le.Uint64(r.mem[at+8:])
		length := min(le.Uint32(r.mem[at+16:]), r.capacity)
		payload := append([]byte(nil), r.mem[at+slotHeader:at+slotHeader+int(length)]...)
		if after := atomic.LoadUint64(sequence); before != n || after != n {
			gap++
			continue
		}
		return Frame{Sequence: n, Clock: clock, Payload: payload}, gap, nil
	}
}

// Close lets the reader's mapping go.
func (r *Reader) Close() error { return unix.Munmap(r.mem) }
