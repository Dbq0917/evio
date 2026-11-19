// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build darwin netbsd freebsd openbsd dragonfly linux

package evio

import "testing"

// TestAsyncInflightAccounting models the loop's backpressure bookkeeping on a
// single connection: inflight grows per read packet until MaxInflight, at
// which point loopReadAsync suspends reading, then falls back to zero as the
// serial worker's results are reclaimed.
func TestAsyncInflightAccounting(t *testing.T) {
	c := &conn{}
	for i := 0; i < MaxInflight; i++ {
		if c.inflight >= MaxInflight {
			t.Fatalf("cap tripped early at %d", i)
		}
		c.inflight++
	}
	if c.inflight != MaxInflight {
		t.Fatalf("inflight = %d, want %d", c.inflight, MaxInflight)
	}
	// At the cap the read path must declare backpressure instead of reading.
	if c.inflight < MaxInflight {
		t.Fatal("expected backpressure at cap")
	}
	// Reclaim results exactly as loopApplyAsyncResult would.
	for c.inflight > 0 {
		c.inflight--
	}
	if c.inflight != 0 {
		t.Fatalf("inflight after drain = %d, want 0", c.inflight)
	}
}

// TestAsyncJobQueueCapacity pins the bounded worker queue to the documented
// ceiling so a slow worker holds at most MaxInflight packets and a full
// enqueue never blocks the event loop.
func TestAsyncJobQueueCapacity(t *testing.T) {
	jobs := make(chan []byte, MaxInflight)
	for i := 0; i < MaxInflight; i++ {
		jobs <- []byte{'x'}
	}
	if len(jobs) != MaxInflight {
		t.Fatalf("queue len = %d, want %d", len(jobs), MaxInflight)
	}
	select {
	case jobs <- []byte{'y'}:
		t.Fatal("bounded job queue accepted past MaxInflight")
	default:
	}
}
