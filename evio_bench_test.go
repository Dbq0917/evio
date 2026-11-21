package evio

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkRoundRobinAccept measures the accept/dispatch hot path under a
// multi-loop RoundRobin server, guarding the fix against a throughput
// regression. The counter change is a single lock-free atomic add, so
// accepted-connection throughput should match a plain atomic operation.
func BenchmarkRoundRobinAccept(b *testing.B) {
	for _, numLoops := range []int{4, 8} {
		b.Run("", func(b *testing.B) {
			var accepted int64
			var closed int64
			events := Events{
				NumLoops:    numLoops,
				LoadBalance: RoundRobin,
				Opened: func(c Conn) (out []byte, opts Options, action Action) {
					atomic.AddInt64(&accepted, 1)
					return
				},
				Closed: func(c Conn, err error) (action Action) {
					atomic.AddInt64(&closed, 1)
					return
				},
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			socket := ln.Addr().String()
			ln.Close()

			go Serve(events, "tcp://"+socket)
			time.Sleep(100 * time.Millisecond)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c, err := net.Dial("tcp", socket)
					if err != nil {
						b.Fatal(err)
					}
					c.Close()
				}
			})
			b.StopTimer()

			deadline := time.Now().Add(3 * time.Second)
			for atomic.LoadInt64(&closed) < atomic.LoadInt64(&accepted) &&
				time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			b.ReportMetric(float64(atomic.LoadInt64(&accepted)), "accepted")
		})
	}
}

// BenchmarkMultiLoopEcho measures sustained multi-loop echo throughput with
// persistent connections, exercising data dispatch across all loops.
func BenchmarkMultiLoopEcho(b *testing.B) {
	const numLoops = 8
	events := Events{
		NumLoops:    numLoops,
		LoadBalance: RoundRobin,
		Data: func(c Conn, in []byte) (out []byte, action Action) {
			return in, None
		},
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	socket := ln.Addr().String()
	ln.Close()

	go Serve(events, "tcp://"+socket)
	time.Sleep(100 * time.Millisecond)

	const conns = 64
	clients := make([]net.Conn, conns)
	for i := range clients {
		c, err := net.Dial("tcp", socket)
		if err != nil {
			b.Fatal(err)
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	payload := make([]byte, 512)
	echo := make([]byte, 512)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c := clients[i%conns]
			i++
			if _, err := c.Write(payload); err != nil {
				b.Fatal(err)
			}
			if _, err := io.ReadFull(c, echo); err != nil {
				b.Fatal(err)
			}
		}
	})
}
