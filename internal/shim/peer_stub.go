//go:build !linux

package shim

import (
	"net"

	"mydocker/internal/isolation"
)

// peerProcessID fails closed because Linux SO_PEERCRED process identity is unavailable.
func peerProcessID(*net.UnixConn) (int, error) { return 0, isolation.ErrUnsupportedPlatform }
