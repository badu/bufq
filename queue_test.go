package bufq_test

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"nikand.dev/go/bufq"
)

func TestQueue(tb *testing.T) {
	const Q, N, T = 8, 10, 1024

	var wg, wwg sync.WaitGroup
	var counter atomic.Int32

	read := make([]byte, T)

	b := make([]byte, 16*Q)
	q := bufq.New(Q, len(b))

	wg.Add(N)
	wwg.Add(N)

	for i := range N {
		go func() {
			defer wg.Done()
			defer wwg.Done()

			for {
				x := counter.Add(1) - 1
				if x >= T {
					break
				}

				msg, st, _ := q.Allocate(15, 16, true)
				if msg < 0 {
					tb.Errorf("allocate: %x", msg)
					break
				}

				res := fmt.Appendf(b[:st], "%04x s_%02x_%03x", x, i, msg)
				size := len(res) - st

				runtime.Gosched()

				q.Commit(msg, size)

				runtime.Gosched()
			}
		}()
	}

	wg.Add(N)
	wwg.Add(N)

	for i := range N {
		go func() {
			defer wg.Done()
			defer wwg.Done()

			ms := make([]bufq.Message, 3)

			for {
				m := q.AllocateN(15, 16, true, ms)
				if m == bufq.Closed {
					break
				}
				if m < 0 {
					tb.Errorf("allocate: %x", m)
					break
				}

				first := int(counter.Add(int32(m))) - m
				x := first

				for ; x < T && x < first+m; x++ {
					msg := &ms[x-first]

					res := fmt.Appendf(b[:msg.Start], "%04x N_%02x_%03x", x, i, msg.Msg)

					msg.End = len(res)
				}
				for j := x; j < first+m; j++ {
					ms[j-first].End = bufq.Cancel
				}

				runtime.Gosched()

				q.CommitN(ms[:m])

				runtime.Gosched()

				if first+m >= T {
					break
				}
			}
		}()
	}

	wg.Add(N)

	for i := range N {
		go func() {
			defer wg.Done()

			for {
				msg, st, end := q.Consume(true)
				if msg == bufq.Closed {
					break
				}
				if msg < 0 {
					tb.Errorf("msg: %x", msg)
					break
				}

				tb.Logf("job s_%x, msg %03x, bs %3x-%3x: %s", i, msg, st, end, b[st:end])

				x, err := strconv.ParseUint(string(b[st:st+4]), 16, 32)
				if err != nil {
					tb.Errorf("parse x: %v", err)
				}

				read[x]++

				runtime.Gosched()

				q.Done(msg)

				runtime.Gosched()
			}
		}()
	}

	wg.Add(N)

	for i := range N {
		go func() {
			defer wg.Done()

			ms := make([]bufq.Message, 3)

			for {
				m := q.ConsumeN(true, ms)
				if m == bufq.Closed {
					break
				}
				if m < 0 {
					tb.Errorf("msg: %x", m)
					break
				}

				for _, mm := range ms[:m] {
					msg, st, end := mm.Msg, mm.Start, mm.End

					tb.Logf("job %d_%x, msg %03x, bs %3x-%3x: %s", m, i, msg, st, end, b[st:end])

					x, err := strconv.ParseUint(string(b[st:st+4]), 16, 32)
					if err != nil {
						tb.Errorf("parse x: %v", err)
					}

					read[x]++
				}

				runtime.Gosched()

				q.DoneN(ms[:m])

				runtime.Gosched()
			}
		}()
	}

	wwg.Wait()
	q.Close()

	wg.Wait()

	for i := range T {
		if read[i] != 1 {
			tb.Errorf("x %x = %x", i, read[i])
		}
	}
}
