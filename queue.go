package bufq

import (
	"fmt"
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
	if align&(align-1) != 0 {
		panic(align)
	}

	defer q.mu.Unlock()
	q.mu.Lock()

	for {
		if q.closed {
			return Closed, 0, 0
		}

		if a := int64(align); a != 0 && q.w&a != 0 {
			q.w = q.w&^(a-1) + a
		}

		if q.b != 0 && q.w&q.b+int64(size) > q.b {
			q.w = q.w - (q.w & q.b) + q.b
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

	if size < 0 {
		size = Cancel
	}

	if q.q[msg].size != sizeWriting {
		return
	}

	q.q[msg].size = size

	q.cond.Broadcast()
}

func (q *Queue) Consume(blocking bool) (msg, st, end int) {
	defer q.mu.Unlock()
	q.mu.Lock()

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

	qmask := len(q.q) - 1

	q.q[msg].size = sizeFree

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
