//go:build !linux

package broker

import "net"

// setCork is a no-op on non-Linux platforms. TCP_CORK is Linux-specific;
// other platforms rely on the bufio.Writer batching in the frame writer.
func setCork(_ net.Conn, _ bool) {}
