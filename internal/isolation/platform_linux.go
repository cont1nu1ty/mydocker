//go:build linux

package isolation

// platformSupported reports that the Linux syscall implementation is available.
func platformSupported() bool { return true }
