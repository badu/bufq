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
	const Q, N, T = 8, 10, 128

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

	wwg.Wait()
	q.Close()

	wg.Wait()

	for i := range T {
		if read[i] != 1 {
			tb.Errorf("x %x = %x", i, read[i])
		}
	}
}
