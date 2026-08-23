//go:build !linux

package isolation

// platformSupported reports that host isolation must fail closed on this operating system.
func platformSupported() bool { return false }
