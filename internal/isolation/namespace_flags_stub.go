//go:build !linux

package isolation

// namespaceCloneFlag fails closed because setns flags are Linux-specific.
func namespaceCloneFlag(NamespaceType) (int, error) { return 0, ErrUnsupportedPlatform }

// mountNamespaceFlag returns a harmless sentinel used only before the non-Linux Ops rejects unshare.
func mountNamespaceFlag() int { return 0 }

// filesystemContextFlag returns a harmless sentinel before the non-Linux Ops rejects unshare.
func filesystemContextFlag() int { return 0 }

// privateRecursiveFlags returns a harmless sentinel on unsupported platforms.
func privateRecursiveFlags() uintptr { return 0 }

// selfBindRecursiveFlags returns a harmless sentinel on unsupported platforms.
func selfBindRecursiveFlags() uintptr { return 0 }

// fileBindFlags has no non-Linux implementation and is reachable only through failing stub Ops.
func fileBindFlags() uintptr { return 0 }

// safeProcFlags returns a harmless sentinel on unsupported platforms.
func safeProcFlags() uintptr { return 0 }

// safeDevFlags returns a harmless sentinel on unsupported platforms.
func safeDevFlags() uintptr { return 0 }

// detachUnmountFlag returns a harmless sentinel on unsupported platforms.
func detachUnmountFlag() int { return 0 }
