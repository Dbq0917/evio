// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build !darwin,!netbsd,!freebsd,!openbsd,!dragonfly,!linux

package evio

import "net"

// asyncReusableDial falls back to an ordinary fixed-source-port dial on
// platforms where SO_REUSEADDR is not wired up in these tests.
func asyncReusableDial(network string, laddr, raddr *net.TCPAddr) (net.Conn, error) {
	d := net.Dialer{LocalAddr: laddr}
	return d.Dial(network, raddr.String())
}
