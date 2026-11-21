package evio

import (
	"encoding/binary"
	"errors"
)

// FrameHeaderSize is the number of bytes used by the length-prefixed
// frame header: a single big-endian uint32 payload length.
const FrameHeaderSize = 4

// ErrFrameTooLarge is returned by FrameBuffer.Read when an incoming
// frame declares a payload larger than the configured MaxPacketSize.
// The connection should be closed at this point because the buffered
// stream can no longer be interpreted safely.
var ErrFrameTooLarge = errors.New("evio: frame payload exceeds MaxPacketSize")

// AppendFrame prepends a big-endian uint32 length header to data and
// appends the resulting frame to b. It is the client-side counterpart
// of FrameBuffer.
func AppendFrame(b, data []byte) []byte {
	var hdr [FrameHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	b = append(b, hdr[:]...)
	b = append(b, data...)
	return b
}

// FrameBuffer reassembles length-prefixed frames out of the possibly
// fragmented packets delivered by Data events.
//
// A wire frame is a 4-byte big-endian uint32 payload length followed by
// that many payload bytes. evio hands over whatever bytes are currently
// available in the socket read buffer, so any single frame may be spread
// over several Data events (and one event may carry several frames).
// FrameBuffer holds the partial bytes until whole frames can be handed
// to the application.
//
// One FrameBuffer must be created per connection, typically inside the
// Opened event, and it is not safe for concurrent use because a
// connection is driven by a single loop.
type FrameBuffer struct {
	// MaxPacketSize caps how many payload bytes a single frame may carry.
	// A value of zero means no limit. When a frame exceeds the cap
	// Read returns ErrFrameTooLarge so the caller can close the
	// connection instead of letting the reassembly buffer grow without
	// bound.
	MaxPacketSize int

	// VolatileInput must be set to true when the connection uses
	// Options.ReuseInputBuffer. In that mode the byte slice passed to
	// Data is a shared scratch buffer whose contents are overwritten on
	// every read, so carried-over bytes are copied into owned storage.
	// When false (the default) a fully contained frame is returned as a
	// slice of the Data packet itself, with no copy.
	VolatileInput bool

	carry []byte // owned partial-stream bytes carried between reads
}

// Reset drops all buffered state. It is safe to reuse the FrameBuffer
// afterwards.
func (f *FrameBuffer) Reset() {
	f.carry = f.carry[:0]
}

// Buffered returns the number of bytes held while waiting for a frame to
// complete.
func (f *FrameBuffer) Buffered() int {
	return len(f.carry)
}

// Read consumes packet, the exact byte slice passed to a Data event, and
// returns the payloads of every complete frame found in the accumulated
// stream. A nil result means more packets are needed before a frame can
// be delivered.
//
// Returned payload slices are only valid until the next Read call.
// Partial frames stay buffered, and the buffer never grows past the
// bytes genuinely in flight: once enough bytes arrive the complete
// frames are drained immediately.
func (f *FrameBuffer) Read(packet []byte) (frames [][]byte, err error) {
	data := packet
	if len(f.carry) > 0 {
		// A partial stream is pending; concatenate it with the new
		// packet into owned storage, then reuse the same backing array
		// for future carries.
		f.carry = append(f.carry, packet...)
		data = f.carry
	} else if f.VolatileInput && len(packet) > 0 {
		// The packet memory is reused by the event loop; take a copy so
		// that any tail left after parsing survives the next read.
		f.carry = append(f.carry[:0], packet...)
		data = f.carry
	}
	// Frames parsed out of owned carry storage must be copied: that
	// backing array is reused (and therefore overwritten) by the next
	// Read call. When data directly aliases the caller's packet the
	// frame slice is returned as-is on the zero-copy fast path.
	fromCarry := len(f.carry) > 0 && &data[0] == &f.carry[0]

	consumed := 0
	for len(data)-consumed >= FrameHeaderSize {
		size := int(binary.BigEndian.Uint32(data[consumed : consumed+FrameHeaderSize]))
		if f.MaxPacketSize > 0 && size > f.MaxPacketSize {
			return frames, ErrFrameTooLarge
		}
		end := FrameHeaderSize + size
		frameEnd := consumed + end
		if frameEnd > len(data) {
			break
		}
		frame := data[consumed+FrameHeaderSize : frameEnd : frameEnd]
		if fromCarry {
			frame = append([]byte(nil), frame...)
		}
		frames = append(frames, frame)
		consumed += end
	}
	data = data[consumed:]

	if len(data) == 0 {
		// Everything was consumed. Drop oversized backing arrays so one
		// big frame cannot pin its memory on the connection forever.
		if cap(f.carry) > 4096 {
			f.carry = nil
		} else {
			f.carry = f.carry[:0]
		}
		return frames, nil
	}
	// Keep the unparsed tail. Move it to the front of the owned carry
	// buffer when possible to reuse its capacity; when data aliases the
	// caller packet the tail is copied into new owned storage.
	if cap(f.carry) >= len(data) && len(f.carry) > 0 {
		n := copy(f.carry, data)
		f.carry = f.carry[:n]
	} else {
		f.carry = append(f.carry[:0], data...)
	}
	return frames, nil
}
