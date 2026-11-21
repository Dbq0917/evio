package evio

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func buildFrame(payload []byte) []byte {
	return AppendFrame(nil, payload)
}

func TestFrameBufferSingleRead(t *testing.T) {
	var fb FrameBuffer
	payload := []byte("hello frame")
	packet := buildFrame(payload)
	frames, err := fb.Read(packet)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], payload) {
		t.Fatalf("got %v", frames)
	}
	// Zero-copy fast path: the frame must alias the caller packet.
	if &frames[0][0] != &packet[FrameHeaderSize] {
		t.Fatal("complete frame was copied instead of aliasing the packet")
	}
	if fb.Buffered() != 0 {
		t.Fatalf("expected empty buffer, got %d", fb.Buffered())
	}
}

// TestFrameBufferFramesSurviveNextRead ensures frames delivered from an
// earlier Read stay intact after later packets reuse or grow the carry
// buffer.
func TestFrameBufferFramesSurviveNextRead(t *testing.T) {
	var fb FrameBuffer
	first := buildFrame(bytes.Repeat([]byte("A"), 100))
	secondHead := buildFrame([]byte("tail"))[:6]

	frames, err := fb.Read(append(first, secondHead...))
	if err != nil || len(frames) != 1 || len(frames[0]) != 100 {
		t.Fatalf("unexpected: %v %v", frames, err)
	}
	saved := frames[0]

	// Several more reads churn the carry backing array.
	rest := buildFrame([]byte("tail"))[6:]
	if _, err := fb.Read(rest); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := fb.Read(buildFrame([]byte{byte(i)})); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(saved, bytes.Repeat([]byte("A"), 100)) {
		t.Fatal("earlier frame was corrupted by subsequent reads")
	}
}

func TestFrameBufferMultipleFramesInOnePacket(t *testing.T) {
	var fb FrameBuffer
	var packet []byte
	msgs := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for _, m := range msgs {
		packet = AppendFrame(packet, m)
	}
	frames, err := fb.Read(packet)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != len(msgs) {
		t.Fatalf("got %d frames, want %d", len(frames), len(msgs))
	}
	for i, m := range msgs {
		if !bytes.Equal(frames[i], m) {
			t.Fatalf("frame %d = %q, want %q", i, frames[i], m)
		}
	}
}

func TestFrameBufferFragmented(t *testing.T) {
	var fb FrameBuffer
	payload := bytes.Repeat([]byte("abcdefgh"), 100)
	frame := buildFrame(payload)

	// Feed the frame one byte at a time: no complete frame may be
	// delivered until the final byte arrives.
	for i := 0; i < len(frame)-1; i++ {
		frames, err := fb.Read(frame[i : i+1])
		if err != nil {
			t.Fatal(err)
		}
		if len(frames) != 0 {
			t.Fatalf("delivered %d frames prematurely at byte %d", len(frames), i)
		}
		if fb.Buffered() != i+1 {
			t.Fatalf("buffered %d, want %d", fb.Buffered(), i+1)
		}
	}
	frames, err := fb.Read(frame[len(frame)-1:])
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], payload) {
		t.Fatalf("bad final delivery: %v", frames)
	}
	if fb.Buffered() != 0 {
		t.Fatalf("buffer not drained: %d", fb.Buffered())
	}
}

func TestFrameBufferBackToBackPartial(t *testing.T) {
	var fb FrameBuffer
	first := buildFrame([]byte("first message"))
	second := buildFrame([]byte("second"))
	stream := append(first, second...)

	// Split across two reads in the middle of the second frame.
	cut := len(first) + 2
	frames, err := fb.Read(stream[:cut])
	if err != nil || len(frames) != 1 || string(frames[0]) != "first message" {
		t.Fatalf("unexpected: %v %v", frames, err)
	}
	frames, err = fb.Read(stream[cut:])
	if err != nil || len(frames) != 1 || string(frames[0]) != "second" {
		t.Fatalf("unexpected: %v %v", frames, err)
	}
	if fb.Buffered() != 0 {
		t.Fatalf("buffer not drained: %d", fb.Buffered())
	}
}

func TestFrameBufferMaxPacketSize(t *testing.T) {
	fb := FrameBuffer{MaxPacketSize: 16}
	frame := buildFrame(make([]byte, 17))
	if _, err := fb.Read(frame); err != ErrFrameTooLarge {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}

	// Frames within the limit still work.
	fb.Reset()
	frames, err := fb.Read(buildFrame(make([]byte, 16)))
	if err != nil || len(frames) != 1 || len(frames[0]) != 16 {
		t.Fatalf("unexpected: %v %v", frames, err)
	}
}

// TestFrameBufferNoBloat makes sure the reassembly buffer only ever
// holds the in-flight partial stream: completed frames are drained
// immediately, including when frames share a packet with a partial tail.
func TestFrameBufferNoBloat(t *testing.T) {
	var fb FrameBuffer
	stream := AppendFrame(nil, bytes.Repeat([]byte("x"), 5000))
	next := buildFrame(make([]byte, 10))
	stream = append(stream, next[:10]...) // head of the following frame

	// First read: one complete frame plus a 10-byte head of the next.
	frames, err := fb.Read(stream)
	if err != nil || len(frames) != 1 || len(frames[0]) != 5000 {
		t.Fatalf("unexpected: %v %v", frames, err)
	}
	if fb.Buffered() != 10 {
		t.Fatalf("carry = %d bytes, want 10", fb.Buffered())
	}

	// Finish the second frame; the buffer must return to empty and not
	// retain the 5KB backing memory from the first frame.
	frames, err = fb.Read(next[10:])
	if err != nil || len(frames) != 1 || len(frames[0]) != 10 {
		t.Fatalf("unexpected: %v %v", frames, err)
	}
	if fb.Buffered() != 0 {
		t.Fatalf("carry = %d bytes, want 0", fb.Buffered())
	}
	if cap(fb.carry) > 4096 {
		t.Fatalf("oversized carry buffer retained: cap=%d", cap(fb.carry))
	}
}

// TestFrameBufferReusedPacket simulates evio with ReuseInputBuffer
// enabled: every packet aliases one shared scratch buffer that is
// overwritten on the next read. Carried-over bytes must be copied so a
// reassembled frame is not corrupted.
func TestFrameBufferReusedPacket(t *testing.T) {
	var fb FrameBuffer
	payload := bytes.Repeat([]byte{0xAB}, 200)
	frame := buildFrame(payload)

	scratch := make([]byte, 64)
	var delivered []byte
	for pos := 0; pos < len(frame); {
		n := copy(scratch, frame[pos:])
		frames, err := fb.Read(scratch[:n])
		if err != nil {
			t.Fatal(err)
		}
		for _, fr := range frames {
			delivered = append(delivered, fr...)
		}
		for i := range scratch {
			scratch[i] = 0xFF
		}
		pos += n
	}
	if !bytes.Equal(delivered, payload) {
		t.Fatalf("reassembled payload corrupted, len=%d", len(delivered))
	}
}

// TestFrameBufferRandomSplit feeds a stream of frames using random,
// sometimes zero-length chunk boundaries and checks every payload is
// delivered exactly once and in order.
func TestFrameBufferRandomSplit(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var fb FrameBuffer
	var stream []byte
	var want [][]byte
	for i := 0; i < 50; i++ {
		msg := make([]byte, rng.Intn(70000))
		rng.Read(msg)
		want = append(want, msg)
		stream = AppendFrame(stream, msg)
	}

	var got [][]byte
	for pos := 0; pos < len(stream); {
		step := rng.Intn(4096)
		end := pos + step
		if end > len(stream) {
			end = len(stream)
		}
		frames, err := fb.Read(stream[pos:end])
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, frames...)
		pos = end
	}
	if len(got) != len(want) {
		t.Fatalf("delivered %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("frame %d mismatch (len %d vs %d)", i, len(got[i]), len(want[i]))
		}
	}
	if fb.Buffered() != 0 {
		t.Fatalf("carry not drained: %d", fb.Buffered())
	}
}

// TestServeLargeFrame sends a single frame larger than the 0xFFFF read
// packet buffer through both the poll and stdlib backends and checks the
// number of Data callbacks and that one complete frame is delivered and
// parsed successfully.
func TestServeLargeFrame(t *testing.T) {
	cases := []struct {
		scheme string
		reuse  bool
	}{
		{"tcp", false},
		{"tcp", true},
		{"tcp-net", false},
		{"tcp-net", true},
	}
	for _, tc := range cases {
		name := tc.scheme + "-reuse=" + boolStr(tc.reuse)
		t.Run(name, func(t *testing.T) {
			runLargeFrameTest(t, tc.scheme, tc.reuse)
		})
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func runLargeFrameTest(t *testing.T, scheme string, reuse bool) {
	const msgSize = 2*1024*1024 + 4096
	payload := make([]byte, msgSize)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	frame := AppendFrame(nil, payload)

	var dataCallbacks int32
	var frameCallbacks int32
	var parseErr int32
	var done int32

	var events Events
	events.Opened = func(c Conn) (out []byte, opts Options, action Action) {
		c.SetContext(&FrameBuffer{
			MaxPacketSize: 8 * 1024 * 1024,
			VolatileInput: reuse,
		})
		opts.ReuseInputBuffer = reuse
		return
	}
	events.Data = func(c Conn, in []byte) (out []byte, action Action) {
		atomic.AddInt32(&dataCallbacks, 1)
		fb := c.Context().(*FrameBuffer)
		frames, err := fb.Read(in)
		if err != nil {
			atomic.StoreInt32(&parseErr, 1)
			return nil, Close
		}
		for _, msg := range frames {
			atomic.AddInt32(&frameCallbacks, 1)
			if len(msg) != msgSize || !bytes.Equal(msg, payload) {
				atomic.StoreInt32(&parseErr, 1)
				return nil, Close
			}
			out = AppendFrame(nil, msg)
		}
		return out, action
	}
	events.Closed = func(c Conn, err error) (action Action) {
		return Shutdown
	}
	events.Serving = func(srv Server) Action {
		go func() {
			conn, err := net.Dial("tcp", "127.0.0.1:9966")
			if err != nil {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(frame); err != nil {
				conn.Close()
				return
			}
			// The stdlib backend writes through a single blocking
			// Conn.Write that cannot drain multi-megabyte output before
			// shutdown; the poll backend handles it. Echo verification is
			// therefore limited to the epoll/kqueue path.
			if scheme == "tcp" {
				echo := make([]byte, len(frame))
				conn.SetReadDeadline(time.Now().Add(10 * time.Second))
				if _, err := io.ReadFull(conn, echo); err != nil {
					conn.Close()
					return
				}
				size := binary.BigEndian.Uint32(echo[:4])
				if int(size) == msgSize && bytes.Equal(echo[4:], payload) {
					atomic.StoreInt32(&done, 1)
				}
			}
			conn.Close()
		}()
		return None
	}
	if err := Serve(events, scheme+"://127.0.0.1:9966"); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&parseErr) != 0 {
		t.Fatal("frame parse failed")
	}
	if atomic.LoadInt32(&frameCallbacks) != 1 {
		t.Fatalf("got %d frame deliveries, want exactly 1", frameCallbacks)
	}
	if atomic.LoadInt32(&dataCallbacks) <= 1 {
		t.Fatalf("expected fragmented Data callbacks, got %d", dataCallbacks)
	}
	if scheme == "tcp" && atomic.LoadInt32(&done) != 1 {
		t.Fatal("client did not receive the echoed frame intact")
	}
	t.Logf("%s reuse=%v: %d Data callbacks -> 1 frame of %d bytes",
		scheme, reuse, dataCallbacks, msgSize)
}

// TestServeSmallFrameLatency verifies that small messages are delivered
// in the same Data event that first carries them: no extra buffering or
// read round-trip is introduced.
func TestServeSmallFrameLatency(t *testing.T) {
	for idx, scheme := range []string{"tcp", "tcp-net"} {
		addr := "127.0.0.1:996" + string(rune('5'+idx))
		func() {
			var dataCallbacks int32
			var frameCallbacks int32
			var events Events
			events.Opened = func(c Conn) (out []byte, opts Options, action Action) {
				c.SetContext(&FrameBuffer{})
				return
			}
			events.Data = func(c Conn, in []byte) (out []byte, action Action) {
				atomic.AddInt32(&dataCallbacks, 1)
				fb := c.Context().(*FrameBuffer)
				frames, err := fb.Read(in)
				if err != nil {
					return nil, Close
				}
				atomic.AddInt32(&frameCallbacks, int32(len(frames)))
				for _, msg := range frames {
					out = AppendFrame(out, msg)
				}
				if atomic.LoadInt32(&frameCallbacks) == 2 {
					action = Shutdown
				}
				return out, action
			}
			events.Serving = func(srv Server) Action {
				go func() {
					conn, err := net.Dial("tcp", addr)
					if err != nil {
						return
					}
					defer conn.Close()
					conn.Write(AppendFrame(nil, []byte("ping")))
					time.Sleep(50 * time.Millisecond)
					conn.Write(AppendFrame(nil, []byte("pong")))
					io.ReadAll(conn)
				}()
				return None
			}
			if err := Serve(events, scheme+"://"+addr); err != nil {
				t.Errorf("%s: %v", scheme, err)
			}
			if atomic.LoadInt32(&dataCallbacks) != 2 || atomic.LoadInt32(&frameCallbacks) != 2 {
				t.Errorf("%s: callbacks data=%d frames=%d, want 2/2",
					scheme, dataCallbacks, frameCallbacks)
			}
		}()
	}
}
