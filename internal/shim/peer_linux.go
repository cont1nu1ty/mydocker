//go:build linux

package shim

import (
	"errors"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// peerProcessID reads SO_PEERCRED from the connected Unix socket and requires
// the same effective UID before returning the positive wrapper PID.
func peerProcessID(connection *net.UnixConn) (int, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return 0, err
	}
	var credential *unix.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if controlErr != nil {
		return 0, controlErr
	}
	if credential == nil || credential.Pid <= 0 || credential.Uid != uint32(os.Geteuid()) {
		return 0, errors.New("control peer credentials do not match the daemon identity")
	}
	return int(credential.Pid), nil
}
