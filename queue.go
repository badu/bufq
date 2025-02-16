package bufq

import (
	"fmt"
	"math/bits"
	"path/filepath"
	"runtime"
	"sync"
)

type (
	Queue struct {
		mu   sync.Mutex
		cond sync.Cond

		q      []slot
		qr, qw int64

		b    int64
		r, w int64

		closed bool
	}

	slot struct {
		start int64
		size  int
	}

	Message struct {
		Msg        int
		Start, End int
	}

	ErrorCode int
)

// Special msg values returned by Allocate and Consume meaning errors.
const (
	Closed = -1 - iota
	WouldBlock
)

// Special size values.
const (
	Cancel = -1

	// slot states
	sizeWriting = Cancel - iota
	// sizeCommitted = positive size or Cancel
	sizeReading
	sizeFree
)

var (
	ErrClosed     = ErrorCode(Closed)
	ErrWouldBlock = ErrorCode(WouldBlock)
)

func New(n, buf int) *Queue {
	if n&0x3 != 0 {
		panic(n)
	}
	if buf&0xf != 0 {
		panic(buf)
	}

	q := &Queue{}
	q.Reset(n, buf)

	return q
}

func (q *Queue) ResetSame() {
	q.Reset(len(q.q), int(q.b))
}

func (q *Queue) Reset(n, buf int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	q.cond.L = &q.mu

	if n > cap(q.q) {
		q.q = make([]slot, n)
	} else {
		q.q = q.q[:n]
	}

	q.qr, q.qw = 0, 0

	q.b = int64(buf)
	q.r, q.w = 0, 0

	q.closed = false
}

func (q *Queue) Allocate(size, align int, blocking bool) (msg, st, end int) {
	if align > 0 {
		align = 1 << bits.Len(uint(align)-1)
	}

	defer q.mu.Unlock()
	q.mu.Lock()

	return q.allocate(size, align, blocking)
}

func (q *Queue) AllocateN(size, align int, blocking bool, buf []Message) (n int) {
	if align > 0 {
		align = 1 << bits.Len(uint(align)-1)
	}

	defer q.mu.Unlock()
	q.mu.Lock()

	for n < len(buf) {
		msg, st, end := q.allocate(size, align, blocking && n == 0)
		if msg < 0 && n > 0 {
			return n
		}
		if msg < 0 {
			return msg
		}

		buf[n] = Message{
			Msg:   msg,
			Start: st,
			End:   end,
		}

		n++
	}

	return n
}

func (q *Queue) allocate(size, align int, blocking bool) (msg, st, end int) {
	//	defer func() {
	//		log.Printf("allocate %5v -> %3x  from %v %v %v", blocking, msg, caller(1), caller(2), caller(3))
	//	}()

	for {
		if q.closed {
			return Closed, 0, 0
		}

		if a := int64(align); a != 0 && q.w&(a-1) != 0 {
			q.w = q.w - q.w&(a-1) + a
		}

		if q.b != 0 && q.w&(q.b-1)+int64(size) > q.b {
			q.w = q.w - q.w&(q.b-1) + q.b
		}

		if q.qw+1 > q.qr+int64(len(q.q)) || q.w+int64(size) > q.r+q.b {
			if !blocking {
				return WouldBlock, 0, 0
			}

			q.cond.Wait()

			continue
		}

		qmask := len(q.q) - 1
		mask := int(q.b) - 1

		msg := int(q.qw) & qmask
		q.qw++

		q.q[msg] = slot{start: q.w, size: sizeWriting}

		st := int(q.w) & mask
		end := st + size

		q.w += int64(size)

		return msg, st, end
	}
}

func (q *Queue) Commit(msg, size int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	q.commit(msg, size)
}

func (q *Queue) CommitN(ms []Message) {
	defer q.mu.Unlock()
	q.mu.Lock()

	for _, m := range ms {
		q.commit(m.Msg, m.End-m.Start)
	}
}

func (q *Queue) commit(msg, size int) {
	if size < 0 {
		size = Cancel
	}

	if q.q[msg].size != sizeWriting {
		return
	}

	q.q[msg].size = size

	if size == Cancel {
		q.done()
	} else {
		q.cond.Broadcast()
	}
}

func (q *Queue) Consume(blocking bool) (msg, st, end int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	return q.consume(blocking)
}

func (q *Queue) ConsumeN(blocking bool, buf []Message) (n int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	for n < len(buf) {
		msg, st, end := q.consume(blocking && n == 0)
		if msg < 0 && n > 0 {
			return n
		}
		if msg < 0 {
			return msg
		}

		buf[n] = Message{
			Msg:   msg,
			Start: st,
			End:   end,
		}

		n++
	}

	return n
}

func (q *Queue) consume(blocking bool) (msg, st, end int) {
	qmask := len(q.q) - 1
	mask := int(q.b - 1)

	for {
		for msg := q.qr; msg < q.qw; msg++ {
			m := int(msg) & qmask

			s := q.q[m]
			if s.size < 0 {
				continue
			}

			st := int(s.start) & mask
			end := st + s.size

			q.q[m].size = sizeReading

			return m, st, end
		}

		if q.qr == q.qw && q.closed {
			return Closed, 0, 0
		}

		if !blocking {
			return WouldBlock, 0, 0
		}

		q.cond.Wait()
	}
}

func (q *Queue) Done(msg int) {
	defer q.mu.Unlock()
	q.mu.Lock()

	q.q[msg].size = sizeFree

	q.done()
}

func (q *Queue) DoneN(ms []Message) {
	defer q.mu.Unlock()
	q.mu.Lock()

	for _, m := range ms {
		q.q[m.Msg].size = sizeFree
	}

	q.done()
}

func (q *Queue) done() {
	qmask := len(q.q) - 1

	for q.qr < q.qw {
		msg := int(q.qr) & qmask
		size := q.q[msg].size

		if size == Cancel || size == sizeFree {
			q.qr++
			continue
		}

		break
	}

	if q.qr == q.qw {
		q.r = q.w
	} else {
		msg := int(q.qr) & qmask
		q.r = q.q[msg].start
	}

	q.cond.Broadcast()
}

func (q *Queue) Close() error {
	defer q.mu.Unlock()
	q.mu.Lock()

	q.closed = true

	q.cond.Broadcast()

	return nil
}

func (q *Queue) len() int {
	defer q.mu.Unlock()
	q.mu.Lock()

	qmask := len(q.q) - 1

	n := 0

	for msg := q.qr; msg < q.qw; msg++ {
		m := int(msg) & qmask
		s := q.q[m]

		if s.size >= 0 {
			n++
		}
	}

	return n
}

func (q *Queue) Stats() (qr, qw, r, w int64) {
	defer q.mu.Unlock()
	q.mu.Lock()

	return q.qr, q.qw, q.r, q.w
}

func (q *Queue) state() (x uint64) {
	qmask := len(q.q) - 1

	for msg := q.qr; msg < q.qw; msg++ {
		m := int(msg) & qmask
		s := q.q[m]

		sh := 4 * int(msg-q.qr)
		if sh >= 64 {
			break
		}

		switch {
		case s.size == sizeFree:
			// x |= 0 << sh
		case s.size == sizeWriting:
			x |= 1 << sh
		case s.size >= 0:
			x |= 2 << sh
		case s.size == sizeReading:
			x |= 3 << sh
		case s.size == Cancel:
			x |= 4 << sh
		default:
			panic(s.size)
		}
	}

	return x
}

func (q *Queue) pState() string {
	return fmt.Sprintf("q %3x-%3x  b %4x-%4x  s %x", q.qr, q.qw, q.r, q.w, q.state())
}

func ToError(msg int64) error {
	if msg >= 0 {
		return nil
	}

	return ErrorCode(msg)
}

func (e ErrorCode) Error() string {
	switch e {
	case Closed:
		return "closed"
	case WouldBlock:
		return "would block"
	default:
		return fmt.Sprintf("unknown error: %d", int(e))
	}
}

func caller(d int) string {
	_, file, line, _ := runtime.Caller(1 + d)

	return fmt.Sprintf("%v:%v", filepath.Base(file), line)
}
