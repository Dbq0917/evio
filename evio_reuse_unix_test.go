// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build darwin netbsd freebsd openbsd dragonfly linux

package evio

import (
	"net"
	"syscall"
)

// asyncReusableDial dials raddr from a fixed local port with SO_REUSEADDR so
// a control source port left in TIME_WAIT can be rebound by the next test.
func asyncReusableDial(network string, laddr, raddr *net.TCPAddr) (net.Conn, error) {
	d := net.Dialer{
		LocalAddr: laddr,
		Control: func(network, address string, rc syscall.RawConn) error {
			var serr error
			if err := rc.Control(func(fd uintptr) {
				serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			}); err != nil {
				return err
			}
			return serr
		},
	}
	return d.Dial(network, raddr.String())
}
