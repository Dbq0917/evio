// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build darwin netbsd freebsd openbsd dragonfly linux

package evio

import (
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	reuseport "github.com/kavu/go_reuseport"
	"github.com/tidwall/evio/internal"
)

type conn struct {
	fd         int              // file descriptor
	lnidx      int              // listener index in the server lns list
	out        []byte           // write buffer
	sa         syscall.Sockaddr // remote socket address
	reuse      bool             // should reuse input buffer
	opened     bool             // connection opened event fired
	action     Action           // next user action
	ctx        interface{}      // user-defined context
	addrIndex  int              // index of listening address
	localAddr  net.Addr         // local addre
	remoteAddr net.Addr         // remote addr
	loop       *loop            // connected loop

	// AsyncData dispatch state. All fields below are owned by the event
	// loop except inside the worker goroutine, which only touches the
	// channels it received when it was started.
	async    bool             // Data runs on a dedicated worker
	jobs     chan []byte      // event loop -> worker packets (bounded)
	results  chan asyncResult // worker -> event loop results (bounded)
	wake     chan struct{}    // coalesced synthetic wake notifications
	inflight int              // packets handed to the worker, not done
	paused   bool             // reading paused because the job queue is full
	closing  bool             // event loop asked the worker to exit
	closed   bool             // worker has exited and reaped its result
	done     chan struct{}    // closed when the worker goroutine exits
}

// asyncResult is the outcome of a single Data callback executed on a
// connection worker. It carries references to the exact slices returned by
// the callback; the dispatch path never copies the packet or the reply.
type asyncResult struct {
	c      *conn
	out    []byte
	action Action
	wake   bool // synthetic wake event, in is nil
}

func (c *conn) Context() interface{}       { return c.ctx }
func (c *conn) SetContext(ctx interface{}) { c.ctx = ctx }
func (c *conn) AddrIndex() int             { return c.addrIndex }
func (c *conn) LocalAddr() net.Addr        { return c.localAddr }
func (c *conn) RemoteAddr() net.Addr       { return c.remoteAddr }
func (c *conn) Wake() {
	if c.loop != nil {
		c.loop.poll.Trigger(c)
	}
}

type server struct {
	events   Events             // user events
	loops    []*loop            // all the loops
	lns      []*listener        // all the listeners
	wg       sync.WaitGroup     // loop close waitgroup
	cond     *sync.Cond         // shutdown signaler
	balance  LoadBalance        // load balancing method
	accepted uintptr            // accept counter
	tch      chan time.Duration // ticker channel

	//ticktm   time.Time      // next tick time
}

type loop struct {
	idx     int            // loop index in the server loops list
	poll    *internal.Poll // epoll or kqueue
	packet  []byte         // read packet buffer
	fdconns map[int]*conn  // loop connections fd -> conn
	count   int32          // connection count
}

// asyncWake is the note delivered to a loop when one of its connection
// workers has posted at least one result that needs draining.
type asyncWake struct {
	c *conn
}

// waitForShutdown waits for a signal to shutdown
func (s *server) waitForShutdown() {
	s.cond.L.Lock()
	s.cond.Wait()
	s.cond.L.Unlock()
}

// signalShutdown signals a shutdown an begins server closing
func (s *server) signalShutdown() {
	s.cond.L.Lock()
	s.cond.Signal()
	s.cond.L.Unlock()
}

func serve(events Events, listeners []*listener) error {
	// figure out the correct number of loops/goroutines to use.
	numLoops := events.NumLoops
	if numLoops <= 0 {
		if numLoops == 0 {
			numLoops = 1
		} else {
			numLoops = runtime.NumCPU()
		}
	}

	s := &server{}
	s.events = events
	s.lns = listeners
	s.cond = sync.NewCond(&sync.Mutex{})
	s.balance = events.LoadBalance
	s.tch = make(chan time.Duration)

	//println("-- server starting")
	if s.events.Serving != nil {
		var svr Server
		svr.NumLoops = numLoops
		svr.Addrs = make([]net.Addr, len(listeners))
		for i, ln := range listeners {
			svr.Addrs[i] = ln.lnaddr
		}
		action := s.events.Serving(svr)
		switch action {
		case None:
		case Shutdown:
			return nil
		}
	}

	defer func() {
		// wait on a signal for shutdown
		s.waitForShutdown()

		// notify all loops to close by closing all listeners
		for _, l := range s.loops {
			l.poll.Trigger(errClosing)
		}

		// wait on all loops to complete reading events
		s.wg.Wait()

		// close loops and all outstanding connections
		for _, l := range s.loops {
			for _, c := range l.fdconns {
				loopCloseConn(s, l, c, nil)
			}
			l.poll.Close()
		}
		//println("-- server stopped")
	}()

	// create loops locally and bind the listeners.
	for i := 0; i < numLoops; i++ {
		l := &loop{
			idx:     i,
			poll:    internal.OpenPoll(),
			packet:  make([]byte, 0xFFFF),
			fdconns: make(map[int]*conn),
		}
		for _, ln := range listeners {
			l.poll.AddRead(ln.fd)
		}
		s.loops = append(s.loops, l)
	}
	// start loops in background
	s.wg.Add(len(s.loops))
	for _, l := range s.loops {
		go loopRun(s, l)
	}
	return nil
}

func loopCloseConn(s *server, l *loop, c *conn, err error) error {
	if c.async {
		// Stop the worker and reclaim every result it still owed us. The
		// worker cannot exit while a result is pending on the bounded
		// channel, so this loop must drain here; otherwise a slow callback
		// on a closing connection would leak a goroutine forever.
		loopAsyncReap(s, l, c)
	}
	atomic.AddInt32(&l.count, -1)
	delete(l.fdconns, c.fd)
	syscall.Close(c.fd)
	if s.events.Closed != nil {
		switch s.events.Closed(c, err) {
		case None:
		case Shutdown:
			return errClosing
		}
	}
	return nil
}

func loopDetachConn(s *server, l *loop, c *conn, err error) error {
	if s.events.Detached == nil {
		return loopCloseConn(s, l, c, err)
	}
	if c.async {
		loopAsyncReap(s, l, c)
	}
	l.poll.ModDetach(c.fd)

	atomic.AddInt32(&l.count, -1)
	delete(l.fdconns, c.fd)
	if err := syscall.SetNonblock(c.fd, false); err != nil {
		return err
	}
	switch s.events.Detached(c, &detachedConn{fd: c.fd}) {
	case None:
	case Shutdown:
		return errClosing
	}
	return nil
}

func loopNote(s *server, l *loop, note interface{}) error {
	var err error
	switch v := note.(type) {
	case time.Duration:
		delay, action := s.events.Tick()
		switch action {
		case None:
		case Shutdown:
			err = errClosing
		}
		s.tch <- delay
	case error: // shutdown
		err = v
	case *conn:
		// Wake called for connection
		if l.fdconns[v.fd] != v {
			return nil // ignore stale wakes
		}
		if v.async {
			return loopAsyncWake(s, l, v)
		}
		return loopWake(s, l, v)
	case asyncWake:
		return loopDrainConn(s, l, v.c)
	}
	return err
}

func loopRun(s *server, l *loop) {
	defer func() {
		//fmt.Println("-- loop stopped --", l.idx)
		s.signalShutdown()
		s.wg.Done()
	}()

	if l.idx == 0 && s.events.Tick != nil {
		go loopTicker(s, l)
	}

	//fmt.Println("-- loop started --", l.idx)
	l.poll.Wait(func(fd int, note interface{}) error {
		if fd == 0 {
			return loopNote(s, l, note)
		}
		c := l.fdconns[fd]
		switch {
		case c == nil:
			return loopAccept(s, l, fd)
		case !c.opened:
			return loopOpened(s, l, c)
		case len(c.out) > 0:
			return loopWrite(s, l, c)
		case c.action != None:
			return loopAction(s, l, c)
		default:
			return loopRead(s, l, c)
		}
	})
}

func loopTicker(s *server, l *loop) {
	for {
		if err := l.poll.Trigger(time.Duration(0)); err != nil {
			break
		}
		time.Sleep(<-s.tch)
	}
}

func loopAccept(s *server, l *loop, fd int) error {
	for i, ln := range s.lns {
		if ln.fd == fd {
			if len(s.loops) > 1 {
				switch s.balance {
				case LeastConnections:
					n := atomic.LoadInt32(&l.count)
					for _, lp := range s.loops {
						if lp.idx != l.idx {
							if atomic.LoadInt32(&lp.count) < n {
								return nil // do not accept
							}
						}
					}
				case RoundRobin:
					idx := int(atomic.LoadUintptr(&s.accepted)) % len(s.loops)
					if idx != l.idx {
						return nil // do not accept
					}
					atomic.AddUintptr(&s.accepted, 1)
				}
			}
			if ln.pconn != nil {
				return loopUDPRead(s, l, i, fd)
			}
			nfd, sa, err := syscall.Accept(fd)
			if err != nil {
				if err == syscall.EAGAIN {
					return nil
				}
				return err
			}
			if err := syscall.SetNonblock(nfd, true); err != nil {
				return err
			}
			c := &conn{fd: nfd, sa: sa, lnidx: i, loop: l}
			c.out = nil
			l.fdconns[c.fd] = c
			l.poll.AddReadWrite(c.fd)
			atomic.AddInt32(&l.count, 1)
			break
		}
	}
	return nil
}

func loopUDPRead(s *server, l *loop, lnidx, fd int) error {
	n, sa, err := syscall.Recvfrom(fd, l.packet, 0)
	if err != nil || n == 0 {
		return nil
	}
	if s.events.Data != nil {
		var sa6 syscall.SockaddrInet6
		switch sa := sa.(type) {
		case *syscall.SockaddrInet4:
			sa6.ZoneId = 0
			sa6.Port = sa.Port
			for i := 0; i < 12; i++ {
				sa6.Addr[i] = 0
			}
			sa6.Addr[12] = sa.Addr[0]
			sa6.Addr[13] = sa.Addr[1]
			sa6.Addr[14] = sa.Addr[2]
			sa6.Addr[15] = sa.Addr[3]
		case *syscall.SockaddrInet6:
			sa6 = *sa
		}
		c := &conn{}
		c.addrIndex = lnidx
		c.localAddr = s.lns[lnidx].lnaddr
		c.remoteAddr = internal.SockaddrToAddr(&sa6)
		in := append([]byte{}, l.packet[:n]...)
		out, action := s.events.Data(c, in)
		if len(out) > 0 {
			if s.events.PreWrite != nil {
				s.events.PreWrite()
			}
			syscall.Sendto(fd, out, 0, sa)
		}
		switch action {
		case Shutdown:
			return errClosing
		}
	}
	return nil
}

func loopOpened(s *server, l *loop, c *conn) error {
	c.opened = true
	c.addrIndex = c.lnidx
	c.localAddr = s.lns[c.lnidx].lnaddr
	c.remoteAddr = internal.SockaddrToAddr(c.sa)
	if s.events.Opened != nil {
		out, opts, action := s.events.Opened(c)
		if len(out) > 0 {
			c.out = append([]byte{}, out...)
		}
		c.action = action
		c.reuse = opts.ReuseInputBuffer
		if opts.AsyncData {
			if s.events.Data == nil {
				// Nothing to dispatch; keep the synchronous path.
			} else {
				c.async = true
				// ReuseInputBuffer hands the shared read packet straight to
				// the callback. A worker would read that packet's contents
				// after the next socket read overwrote it, so asynchronous
				// dispatch always needs the per-packet copy (which is already
				// the default) and never adds a copy of its own.
				c.reuse = false
				c.jobs = make(chan []byte, MaxInflight)
				c.results = make(chan asyncResult, MaxInflight)
				c.wake = make(chan struct{}, 1)
				c.done = make(chan struct{})
				go loopAsyncWorker(s, l, c)
			}
		}
		if opts.TCPKeepAlive > 0 {
			if _, ok := s.lns[c.lnidx].ln.(*net.TCPListener); ok {
				internal.SetKeepAlive(c.fd, int(opts.TCPKeepAlive/time.Second))
			}
		}
	}
	if len(c.out) == 0 && c.action == None {
		l.poll.ModRead(c.fd)
	}
	return nil
}

func loopWrite(s *server, l *loop, c *conn) error {
	if s.events.PreWrite != nil {
		s.events.PreWrite()
	}
	n, err := syscall.Write(c.fd, c.out)
	if err != nil {
		if err == syscall.EAGAIN {
			return nil
		}
		return loopCloseConn(s, l, c, err)
	}
	if n == len(c.out) {
		// release the connection output page if it goes over page size,
		// otherwise keep reusing existing page.
		if cap(c.out) > 4096 {
			c.out = nil
		} else {
			c.out = c.out[:0]
		}
	} else {
		c.out = c.out[n:]
	}
	if len(c.out) == 0 && c.action == None {
		loopArmRead(l, c)
	}
	return nil
}

func loopAction(s *server, l *loop, c *conn) error {
	switch c.action {
	default:
		c.action = None
	case Close:
		return loopCloseConn(s, l, c, nil)
	case Shutdown:
		return errClosing
	case Detach:
		return loopDetachConn(s, l, c, nil)
	}
	if len(c.out) == 0 && c.action == None {
		loopArmRead(l, c)
	}
	return nil
}

// loopArmRead re-arms a connection for reading unless backpressure has the
// connection paused, in which case the worker's resume event re-arms it.
func loopArmRead(l *loop, c *conn) {
	if c.paused {
		l.poll.ModSuspendRead(c.fd, len(c.out) > 0 || c.action != None)
		return
	}
	l.poll.ModRead(c.fd)
}

func loopWake(s *server, l *loop, c *conn) error {
	if s.events.Data == nil {
		return nil
	}
	out, action := s.events.Data(c, nil)
	c.action = action
	if len(out) > 0 {
		c.out = append([]byte{}, out...)
	}
	if len(c.out) != 0 || c.action != None {
		l.poll.ModReadWrite(c.fd)
	}
	return nil
}

func loopRead(s *server, l *loop, c *conn) error {
	if c.async {
		return loopReadAsync(s, l, c)
	}
	var in []byte
	n, err := syscall.Read(c.fd, l.packet)
	if n == 0 || err != nil {
		if err == syscall.EAGAIN {
			return nil
		}
		return loopCloseConn(s, l, c, err)
	}
	in = l.packet[:n]
	if !c.reuse {
		in = append([]byte{}, in...)
	}
	if s.events.Data != nil {
		out, action := s.events.Data(c, in)
		c.action = action
		if len(out) > 0 {
			c.out = append(c.out[:0], out...)
		}
	}
	if len(c.out) != 0 || c.action != None {
		l.poll.ModReadWrite(c.fd)
	}
	return nil
}

// loopAsyncWorker is the per-connection Data callback goroutine. It drains
// the bounded job queue in arrival order, so the ordering guarantee of a
// single TCP connection is preserved even while other connections keep being
// served by the event loop. The worker never touches the socket or any
// connection write state; every result is handed back to the owning loop.
func loopAsyncWorker(s *server, l *loop, c *conn) {
	defer close(c.done)
	for {
		select {
		case in, ok := <-c.jobs:
			if !ok {
				return
			}
			out, action := s.events.Data(c, in)
			c.results <- asyncResult{c: c, out: out, action: action}
			l.poll.Trigger(asyncWake{c})
		case <-c.wake:
			out, action := s.events.Data(c, nil)
			c.results <- asyncResult{c: c, out: out, action: action, wake: true}
			l.poll.Trigger(asyncWake{c})
		}
	}
}

// loopReadAsync enqueues a freshly read packet to the connection worker.
// At most MaxInflight packets per connection are in flight; once the limit
// is hit reading on that fd is suspended while every other fd keeps being
// dispatched. The byte slice produced by the normal copy-on-read path is
// handed over as-is, so the dispatch adds no second copy.
func loopReadAsync(s *server, l *loop, c *conn) error {
	if c.inflight >= MaxInflight {
		c.paused = true
		l.poll.ModSuspendRead(c.fd, len(c.out) > 0 || c.action != None)
		return nil
	}
	n, err := syscall.Read(c.fd, l.packet)
	if n == 0 || err != nil {
		if err == syscall.EAGAIN {
			return nil
		}
		return loopCloseConn(s, l, c, err)
	}
	in := append([]byte{}, l.packet[:n]...)
	c.inflight++
	// len(jobs) <= inflight < MaxInflight == cap(jobs), so this send has a
	// free slot and never stalls the event loop.
	c.jobs <- in
	return nil
}

// loopAsyncWake schedules a synthetic nil Data event on the worker. Wake
// notifications are coalesced and do not consume an in-flight slot.
func loopAsyncWake(s *server, l *loop, c *conn) error {
	if s.events.Data == nil || c.closing {
		return nil
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}

// loopDrainConn applies every result a worker has posted for c, in the
// order the callbacks completed. It runs on the owning event loop, which is
// the only goroutine allowed to mutate the connection and its write buffer.
func loopDrainConn(s *server, l *loop, c *conn) error {
	if l.fdconns[c.fd] != c || c.closed {
		return nil // stale wake after close/reap
	}
	for {
		select {
		case res := <-c.results:
			if err := loopApplyAsyncResult(s, l, c, res); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

func loopApplyAsyncResult(s *server, l *loop, c *conn, res asyncResult) error {
	if c.closing {
		return nil
	}
	if !res.wake {
		c.inflight--
	}
	if len(res.out) > 0 {
		c.out = append(c.out[:0], res.out...)
	}
	c.action = res.action
	switch res.action {
	case Close:
		return loopCloseConn(s, l, c, nil)
	case Shutdown:
		return errClosing
	case Detach:
		return loopDetachConn(s, l, c, nil)
	}
	if c.paused && c.inflight < MaxInflight {
		// A slot freed up; re-arm reading on this single connection.
		c.paused = false
		loopArmRead(l, c)
	} else if len(c.out) != 0 || c.action != None {
		l.poll.ModReadWrite(c.fd)
	}
	return nil
}

// loopAsyncReap stops the worker and drains every result it still owed.
// The worker cannot exit while a result is pending on the bounded channel,
// so a closing/detaching connection must drain here or leak the goroutine.
func loopAsyncReap(s *server, l *loop, c *conn) {
	if c.closed {
		return
	}
	c.closing = true
	close(c.jobs)
	for {
		select {
		case res := <-c.results:
			if !res.wake && c.inflight > 0 {
				c.inflight--
			}
		default:
			<-c.done
			c.closed = true
			return
		}
	}
}

type detachedConn struct {
	fd int
}

func (c *detachedConn) Close() error {
	err := syscall.Close(c.fd)
	if err != nil {
		return err
	}
	c.fd = -1
	return nil
}

func (c *detachedConn) Read(p []byte) (n int, err error) {
	n, err = syscall.Read(c.fd, p)
	if err != nil {
		return n, err
	}
	if n == 0 {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	return n, nil
}

func (c *detachedConn) Write(p []byte) (n int, err error) {
	n = len(p)
	for len(p) > 0 {
		nn, err := syscall.Write(c.fd, p)
		if err != nil {
			return n, err
		}
		p = p[nn:]
	}
	return n, nil
}

func (ln *listener) close() {
	if ln.fd != 0 {
		syscall.Close(ln.fd)
	}
	if ln.f != nil {
		ln.f.Close()
	}
	if ln.ln != nil {
		ln.ln.Close()
	}
	if ln.pconn != nil {
		ln.pconn.Close()
	}
	if ln.network == "unix" {
		os.RemoveAll(ln.addr)
	}
}

// system takes the net listener and detaches it from it's parent
// event loop, grabs the file descriptor, and makes it non-blocking.
func (ln *listener) system() error {
	var err error
	switch netln := ln.ln.(type) {
	case nil:
		switch pconn := ln.pconn.(type) {
		case *net.UDPConn:
			ln.f, err = pconn.File()
		}
	case *net.TCPListener:
		ln.f, err = netln.File()
	case *net.UnixListener:
		ln.f, err = netln.File()
	}
	if err != nil {
		ln.close()
		return err
	}
	ln.fd = int(ln.f.Fd())
	return syscall.SetNonblock(ln.fd, true)
}

func reuseportListenPacket(proto, addr string) (l net.PacketConn, err error) {
	return reuseport.ListenPacket(proto, addr)
}

func reuseportListen(proto, addr string) (l net.Listener, err error) {
	return reuseport.Listen(proto, addr)
}
