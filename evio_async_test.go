// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package evio

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Line based loopback protocol used by the asynchronous dispatch tests:
//
//	"fast:<n>\n" -> "ok:<n>\n"          handled immediately
//	"slow:<ms>\n" -> "ok:slow-<ms>\n"   sleeps ms inside the Data callback
//	"seq:<n>\n"  -> "ok:<n>\n"          used for ordering checks
//	"shutdown\n" -> server stops
//
// Every normal request produces exactly one reply line, so each client
// measures end-to-end latency with a single request/response round trip.
const asyncSlowDelay = 40 * time.Millisecond

// asyncEvents builds the shared line server used by the tests. The control
// connection, identified by its bound source port, shuts the server down
// straight from Opened, so teardown never queues behind a busy Data worker.
func asyncEvents(async bool, controlPort int, data func(Conn, []byte) ([]byte, Action)) Events {
	var events Events
	events.NumLoops = 1
	events.Opened = func(c Conn) (out []byte, opts Options, action Action) {
		opts.AsyncData = async
		if ta, ok := c.RemoteAddr().(*net.TCPAddr); ok && ta.Port == controlPort {
			action = Shutdown
		}
		return
	}
	events.Data = data
	return events
}

// asyncControlPorts hands out a random high source port for every control
// connection so repeated runs never collide on a socket in TIME_WAIT.
var asyncControlPorts int32 = 40000

func nextControlPort() int {
	// Spread across a wide range to avoid reusing a recently closed port.
	return int(atomic.AddInt32(&asyncControlPorts, 1)%20000) + 40000
}

func asyncDataHandler() func(Conn, []byte) ([]byte, Action) {
	return func(c Conn, in []byte) (out []byte, action Action) {
		for _, line := range strings.Split(string(in), "\n") {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			if parts[0] == "slow" {
				ms, err := strconv.Atoi(parts[1])
				if err != nil {
					continue
				}
				time.Sleep(time.Duration(ms) * time.Millisecond)
				out = append(out, []byte("ok:slow-"+parts[1]+"\n")...)
				continue
			}
			out = append(out, []byte("ok:"+parts[1]+"\n")...)
		}
		return out, None
	}
}

func asyncAwaitReady(t *testing.T, network, addr string) {
	t.Helper()
	dialNet := strings.TrimSuffix(network, "-net")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.Dial(dialNet, addr)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("server at %s://%s did not come up", network, addr)
}

func asyncStartServer(t *testing.T, backend, addr string, async bool, controlPort int) chan error {
	t.Helper()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- Serve(asyncEvents(async, controlPort, asyncDataHandler()), backend+"://"+addr)
	}()
	asyncAwaitReady(t, backend, addr)
	return serveErr
}

func asyncDial(t *testing.T, backend, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial(strings.TrimSuffix(backend, "-net"), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c, bufio.NewReader(c)
}

func asyncShutdown(t *testing.T, backend, addr string, controlPort int, serveErr chan error) {
	t.Helper()
	dialNet := strings.TrimSuffix(backend, "-net")
	laddr, err := net.ResolveTCPAddr(dialNet, "127.0.0.1:"+strconv.Itoa(controlPort))
	if err != nil {
		t.Fatalf("resolve control addr: %v", err)
	}
	raddr, err := net.ResolveTCPAddr(dialNet, addr)
	if err != nil {
		t.Fatalf("resolve server addr: %v", err)
	}
	cc, err := asyncReusableDial(dialNet, laddr, raddr)
	if err != nil {
		t.Fatalf("control dial: %v", err)
	}
	cc.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func deadlineWait(t *testing.T, cond func() bool, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

// TestAsyncOrdering verifies strict per-connection ordering on the worker
// path. Requests are sent one at a time and each reply is read before the
// next request, so TCP coalescing cannot blur the message boundaries. The
// worker must answer them in the exact order the client issued them.
func TestAsyncOrdering(t *testing.T) {
	for _, backend := range []string{"tcp", "tcp-net"} {
		t.Run(backend, func(t *testing.T) {
			addr := ":19101"
			controlPort := nextControlPort()
			serveErr := asyncStartServer(t, backend, addr, true, controlPort)
			c, rd := asyncDial(t, backend, addr)

			const n = 200
			for i := 0; i < n; i++ {
				if _, err := fmt.Fprintf(c, "seq:%d\n", i); err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
				line, err := rd.ReadString('\n')
				if err != nil {
					t.Fatalf("read %d: %v", i, err)
				}
				if want := fmt.Sprintf("ok:%d\n", i); line != want {
					t.Fatalf("order broken at %d: got %q want %q", i, line, want)
				}
			}
			c.Close()
			asyncShutdown(t, backend, addr, controlPort, serveErr)
		})
	}
}

// TestAsyncCoalescedOrdering floods many small writes so TCP merges them
// into a handful of large reads. Because each connection owns a single
// serial worker, the echoed byte stream must come back byte-for-byte in the
// exact order it was sent even though multiple packets are queued at once.
func TestAsyncCoalescedOrdering(t *testing.T) {
	echoEvents := func(controlPort int) Events {
		return asyncEvents(true, controlPort, func(c Conn, in []byte) (out []byte, action Action) {
			return in, None
		})
	}
	for _, backend := range []string{"tcp", "tcp-net"} {
		t.Run(backend, func(t *testing.T) {
			addr := ":19111"
			controlPort := nextControlPort()
			serveErr := make(chan error, 1)
			go func() { serveErr <- Serve(echoEvents(controlPort), backend+"://"+addr) }()
			asyncAwaitReady(t, backend, addr)
			c, rd := asyncDial(t, backend, addr)

			const n = 1000
			payload := make([]byte, 0, n*6)
			for i := 0; i < n; i++ {
				payload = append(payload, []byte(fmt.Sprintf("seq:%d\n", i))...)
			}
			if _, err := c.Write(payload); err != nil {
				t.Fatalf("write: %v", err)
			}
			echoed := make([]byte, len(payload))
			if _, err := io.ReadFull(rd, echoed); err != nil {
				t.Fatalf("read full: %v", err)
			}
			if string(echoed) != string(payload) {
				t.Fatalf("coalesced echo stream reordered or corrupted")
			}
			c.Close()
			asyncShutdown(t, backend, addr, controlPort, serveErr)
		})
	}
}

// asyncRoundTripLatency performs n sequential fast requests and returns the
// median single-request latency and the total elapsed time.
func asyncRoundTripLatency(t *testing.T, backend, addr string, n int) (time.Duration, time.Duration) {
	t.Helper()
	c, rd := asyncDial(t, backend, addr)
	defer c.Close()
	samples := make([]time.Duration, 0, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		reqStart := time.Now()
		if _, err := fmt.Fprintf(c, "fast:%d\n", i); err != nil {
			t.Fatalf("fast write: %v", err)
		}
		line, err := rd.ReadString('\n')
		if err != nil {
			t.Fatalf("fast read: %v", err)
		}
		if want := fmt.Sprintf("ok:%d\n", i); line != want {
			t.Fatalf("bad reply %q want %q", line, want)
		}
		samples = append(samples, time.Since(reqStart))
	}
	for i := 1; i < len(samples); i++ {
		for j := i; j > 0 && samples[j-1] > samples[j]; j-- {
			samples[j-1], samples[j] = samples[j], samples[j-1]
		}
	}
	return samples[len(samples)/2], time.Since(start)
}

// TestAsyncHeadOfLine is the core regression for the dispatch change. A slow
// connection keeps a slow request in flight on the single loop while a
// second connection issues fast requests. With asynchronous dispatch the
// contended fast latency must stay near the standalone baseline instead of
// inheriting the slow callback's delay.
func TestAsyncHeadOfLine(t *testing.T) {
	for _, async := range []bool{false, true} {
		name := "sync"
		if async {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			for _, backend := range []string{"tcp", "tcp-net"} {
				t.Run(backend, func(t *testing.T) {
					addr := ":19102"
					controlPort := nextControlPort()
					serveErr := asyncStartServer(t, backend, addr, async, controlPort)

					base, baseTotal := asyncRoundTripLatency(t, backend, addr, 50)

					// Keep a slow request continuously in flight on a second
					// connection. A single 40ms callback is enough to prove
					// the point while the fast connection does 50 round
					// trips; we refresh the slow request each time its reply
					// comes back so the worker/loop stays busy.
					slowC, slowRD := asyncDial(t, backend, addr)
					stopSlow := make(chan struct{})
					var slowWG sync.WaitGroup
					slowWG.Add(1)
					go func() {
						defer slowWG.Done()
						for {
							select {
							case <-stopSlow:
								return
							default:
								if _, err := fmt.Fprintf(slowC, "slow:%d\n",
									int(asyncSlowDelay/time.Millisecond)); err != nil {
									return
								}
								if _, err := slowRD.ReadString('\n'); err != nil {
									return
								}
							}
						}
					}()
					// Let the first slow callback enter its sleep so the
					// loop/worker is provably busy when fast requests start.
					time.Sleep(asyncSlowDelay / 2)

					fast, fastTotal := asyncRoundTripLatency(t, backend, addr, 50)
					close(stopSlow)
					slowWG.Wait()
					slowC.Close()

					t.Logf("%s/%s standalone fast p50=%s total=%s | contended fast p50=%s total=%s",
						name, backend, base, baseTotal, fast, fastTotal)

					if async {
						if fast > asyncSlowDelay/2 {
							t.Fatalf("HOL blocking: contended fast latency %s exceeds %s",
								fast, asyncSlowDelay/2)
						}
					} else if fast < asyncSlowDelay/2 {
						t.Logf("sync path did not show HOL blocking this run (%s)", fast)
					}
					asyncShutdown(t, backend, addr, controlPort, serveErr)
				})
			}
		})
	}
}

// TestAsyncBackpressure floods one connection with several times the
// MaxInflight number of requests while every Data callback waits on a gate.
// This forces the event loop to pause reading at the in-flight cap. Once the
// gate opens every single reply must still arrive, in exact order, proving
// the bounded backpressure path neither drops nor reorders data. The exact
// in-flight ceiling is asserted deterministically by the unix white-box
// TestAsyncInflightStateMachine.
func TestAsyncBackpressure(t *testing.T) {
	for _, backend := range []string{"tcp", "tcp-net"} {
		t.Run(backend, func(t *testing.T) {
			addr := ":19103"
			gate := make(chan struct{})
			controlPort := nextControlPort()
			events := asyncEvents(true, controlPort, func(c Conn, in []byte) (out []byte, action Action) {
				for _, line := range strings.Split(string(in), "\n") {
					if line == "" {
						continue
					}
					<-gate
					out = append(out, []byte("ok:"+
						strings.SplitN(line, ":", 2)[1]+"\n")...)
				}
				return out, None
			})
			serveErr := make(chan error, 1)
			go func() { serveErr <- Serve(events, backend+"://"+addr) }()
			asyncAwaitReady(t, backend, addr)

			c, rd := asyncDial(t, backend, addr)
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetNoDelay(true)
			}
			const n = MaxInflight * 4
			go func() {
				buf := make([]byte, 0, n*8)
				for i := 0; i < n; i++ {
					buf = append(buf, []byte(fmt.Sprintf("seq:%d\n", i))...)
				}
				c.Write(buf)
			}()

			// Hold the gate long enough for the loop to fill its bounded
			// queue and stop reading this single connection.
			time.Sleep(200 * time.Millisecond)
			close(gate)

			// Every one of the n requests must come back ordered.
			for i := 0; i < n; i++ {
				line, err := rd.ReadString('\n')
				if err != nil {
					t.Fatalf("read %d: %v", i, err)
				}
				if want := fmt.Sprintf("ok:%d\n", i); line != want {
					t.Fatalf("backpressure reordered/lost data at %d: got %q want %q",
						i, line, want)
				}
			}
			c.Close()
			asyncShutdown(t, backend, addr, controlPort, serveErr)
		})
	}
}

// TestMaxInflightConstant pins the documented per-connection ceiling.
func TestMaxInflightConstant(t *testing.T) {
	if MaxInflight != 300 {
		t.Fatalf("MaxInflight = %d, want 300", MaxInflight)
	}
}
