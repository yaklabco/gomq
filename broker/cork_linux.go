//go:build linux

package broker

import (
	"net"
	"syscall"
)

// setCork enables or disables TCP_CORK on a TCP connection. When corked, the
// kernel buffers small writes into a single TCP segment, reducing the number of
// packets sent for multi-frame deliveries (method + header + body).
func setCork(conn net.Conn, cork bool) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return
	}
	val := 0
	if cork {
		val = 1
	}
	_ = raw.Control(func(fd uintptr) { //nolint:errcheck // best-effort optimization
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_CORK, val) //nolint:errcheck // best-effort optimization
	})
}
