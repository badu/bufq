package bufq_test

import (
	"fmt"
	"io"
	"log"
	"net/netip"
	"runtime"
	"sync"

	"nikand.dev/go/bufq"
)

type fakeBatchReader struct {
	n int
}

func Example_n() {
	var wg sync.WaitGroup

	type Meta struct {
		Addr netip.AddrPort
	}

	const MaxPacketSize, Workers = 0x100, 4

	// msgs := make([]ipv6.Message, 1024) // []golang.org/x/net/ipv6.Message

	meta := make([]Meta, 0x1000)               // meta info buffer
	b := make([]byte, len(meta)*MaxPacketSize) // data buffer

	q := bufq.New(len(meta), len(b))

	p := newFakeBatchReader() // golang.org/x/net/ipv6.NewPacketConn(...)

	wg.Add(1)

	go func() (err error) {
		defer wg.Done()
		defer q.Close()

		defer func() {
			log.Printf("reader finished: %v\n", err)
		}()

		batch := make([]bufq.Message, 1024) // len(msgs)

		for {
			err := func() error {
				m := q.AllocateN(MaxPacketSize, 16, true, batch)
				if m < 0 {
					return bufq.Error(m)
				}

				defer q.CommitN(batch[:m])

				for i := range m {
					_ = i // msgs[i].Buffers[0] = s.Buf[b.Start:b.End]
				}

				used, err := p.ReadBatch(b, batch[:m]) // ipv6.PacketConn.ReadBatch(msgs[:m], flags)
				if err == nil {
					for i := range used {
						// meta[msg].Addr = msgs[i].Addr
						// ms[i].End = ms[i].Start + msgs[i].N
						_ = i
					}
				}
				for i := used; i < m; i++ {
					batch[i].End = bufq.Cancel
				}

				// q.ComminN is called with defer

				runtime.Gosched()

				return err
			}()
			if err != nil {
				return err
			}
		}
	}()

	wg.Add(Workers)

	for worker := range Workers {
		go func() (err error) {
			defer wg.Done()

			defer func() {
				log.Printf("worker %d finished: %v\n", worker, err)
			}()

			batch := make([]bufq.Message, 1024)

			for {
				m := q.ConsumeN(true, batch)
				if m < 0 {
					return bufq.Error(m)
				}

				for i := range m {
					s := batch[i]

					log.Printf("worker %d: message: %s\n", worker, b[s.Start:s.End])
				}

				q.DoneN(batch[:m])

				runtime.Gosched()
			}
		}()
	}

	wg.Wait()

	// Output:
}

func newFakeBatchReader() *fakeBatchReader { return &fakeBatchReader{} }

func (p *fakeBatchReader) ReadBatch(b []byte, ms []bufq.Message) (int, error) {
	const N = 3

	for i := 0; i < N; i++ {
		if p.n == 10 {
			return i, io.EOF
		}

		res := fmt.Appendf(b[:ms[i].Start], "hello %4d, batch %d/%d", p.n, i, len(ms))
		ms[i].End = len(res)

		p.n++
	}

	return N, nil
}
