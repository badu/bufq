[![Documentation](https://pkg.go.dev/badge/nikand.dev/go/bufq)](https://pkg.go.dev/nikand.dev/go/bufq?tab=doc)
[![Go workflow](https://github.com/nikandfor/bufq/actions/workflows/go.yml/badge.svg)](https://github.com/nikandfor/bufq/actions/workflows/go.yml)
[![CircleCI](https://circleci.com/gh/nikandfor/bufq.svg?style=svg)](https://circleci.com/gh/nikandfor/bufq)
[![codecov](https://codecov.io/gh/nikandfor/bufq/tags/latest/graph/badge.svg)](https://codecov.io/gh/nikandfor/bufq)
[![Go Report Card](https://goreportcard.com/badge/nikand.dev/go/bufq)](https://goreportcard.com/report/nikand.dev/go/bufq)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/nikandfor/bufq?sort=semver)

# bufq

`bufq` is a queue for passing bytes buffers along with metadata in an efficient way.
Initial task was to read and process 1GBit/s+ of small udp packets.

## Usage

Queue operates only with indexes, it's unopinionated about buffer and metadata types and where they stored.
Buffer can be a slice or a mmapped file.

Common pattarn is: there are one or more producers and one or more consumers.
Each producer and consumer can produce/consume one or multiple messages at a time.

Basic use case: one udp reader reads packets as fast as it can into shared buffer and few workers process packets.

```
	type Meta struct {
		Addr netip.AddrPort
	}

	const MaxPacketSize, Workers = 0x100, 4

	meta := make([]Meta, 0x1000)               // meta info buffer
	b := make([]byte, len(meta)*MaxPacketSize) // data buffer

	q := bufq.New(len(meta), len(b))

    var p *net.UDPConn // = ...

	go func() (err error) {
		defer q.Close()

		for {
			msg, st, end := q.Allocate(MaxPacketSize, 16, true)
			if msg < 0 {
				return bufq.ToError(msg)
			}

			// meta[msg] and b[st:end] can be safely used between Allocate and Commit calls.

			n, addr, err := p.ReadFromUDPAddrPort(b[st:end])
			if err != nil {
				q.Commit(msg, bufq.Cancel) // unlock message buffer
				return err
			}

			meta[msg].Addr = addr

			q.Commit(msg, n)
		}
	}()

	for worker := 0; worker < Workers; worker++ {
		go func() (err error) {
			for {
				msg, st, end := q.Consume(true)
				if msg < 0 {
					return bufq.ToError(msg)
				}

				// meta[msg] and b[st:end] can be safely used between Consume and Done calls.

				fmt.Printf("worker %d: message from %v: %s\n", worker, meta[msg].Addr, b[st:end])

				q.Done(msg)
			}
		}()
	}
```
