package bufq_test

import (
	"fmt"
	"io"
	"net/netip"
	"runtime"
	"sync"

	"nikand.dev/go/bufq"
)

type fakeNetPacketListener struct {
	n int
}

func Example() {
	// lets create a packet reader and processors

	var wg sync.WaitGroup

	type Meta struct {
		Addr netip.AddrPort
	}

	const MaxPacketSize, Workers = 0x100, 4

	meta := make([]Meta, 0x1000)               // meta info buffer
	b := make([]byte, len(meta)*MaxPacketSize) // data buffer

	q := bufq.New(len(meta), len(b))

	var p interface {
		ReadFromUDPAddrPort([]byte) (int, netip.AddrPort, error)
	} = newFakeNetPacketListener() // generates few packets and returns io.EOF

	wg.Add(1)

	go func() (err error) {
		defer wg.Done()
		defer q.Close()

		defer func() {
			fmt.Printf("reader finished: %v\n", err)
		}()

		for {
			msg, st, end := q.Allocate(MaxPacketSize, 16, true)
			if msg < 0 {
				return bufq.ToError(msg)
			}

			// meta[msg] and b[st:end] can be safely modified between Allocate and Commit calls.

			n, addr, err := p.ReadFromUDPAddrPort(b[st:end])
			if err != nil {
				q.Commit(msg, bufq.Cancel) // unlock message buffer
				return err
			}

			meta[msg].Addr = addr

			q.Commit(msg, n)

			runtime.Gosched()
		}
	}()

	wg.Add(Workers)

	for worker := 0; worker < Workers; worker++ {
		go func() (err error) {
			defer wg.Done()

			defer func() {
				fmt.Printf("worker %d finished: %v\n", worker, err)
			}()

			for {
				msg, st, end := q.Consume(true)
				if msg < 0 {
					return bufq.ToError(msg)
				}

				// meta[msg] and b[st:end] can be safely read between Consume and Done calls.

				fmt.Printf("worker %d: message from %v: %s\n", worker, meta[msg].Addr, b[st:end])

				q.Done(msg)

				runtime.Gosched()
			}
		}()
	}

	wg.Wait()

	// Output
}

func newFakeNetPacketListener() *fakeNetPacketListener { return &fakeNetPacketListener{} }

func (p *fakeNetPacketListener) ReadFromUDPAddrPort(b []byte) (n int, addr netip.AddrPort, err error) {
	if p.n == 10 {
		return 0, addr, io.EOF
	}

	res := fmt.Appendf(b[:0], "hello %4x", p.n)
	p.n++

	return len(res), netip.MustParseAddrPort("1.2.3.4:10000"), nil
}
