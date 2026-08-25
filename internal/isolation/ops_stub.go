//go:build !linux

package isolation

// systemOps is a fail-closed non-Linux implementation used only to keep callers portable.
type systemOps struct{}

// NewSystemOps returns a boundary whose every host operation reports ErrUnsupportedPlatform.
func NewSystemOps() Ops { return systemOps{} }

// EffectiveUID returns a non-root sentinel on unsupported platforms.
func (systemOps) EffectiveUID() int { return -1 }

// ProcessID returns a zero sentinel on unsupported platforms.
func (systemOps) ProcessID() int { return 0 }

// ThreadID returns a zero sentinel on unsupported platforms.
func (systemOps) ThreadID() int { return 0 }

// ReadFile fails closed because procfs evidence is unavailable.
func (systemOps) ReadFile(string) ([]byte, error) { return nil, ErrUnsupportedPlatform }

// Readlink fails closed because procfs evidence is unavailable.
func (systemOps) Readlink(string) (string, error) { return "", ErrUnsupportedPlatform }

// Lstat fails closed because Linux path identity is unavailable.
func (systemOps) Lstat(string) (FileInfo, error) { return FileInfo{}, ErrUnsupportedPlatform }

// StatFS fails closed because Linux filesystem magic is unavailable.
func (systemOps) StatFS(string) (FileSystemInfo, error) {
	return FileSystemInfo{}, ErrUnsupportedPlatform
}

// OpenNamespace fails closed because namespace descriptors are Linux-only.
func (systemOps) OpenNamespace(string) (int, error) { return -1, ErrUnsupportedPlatform }

// OpenDirectoryNoSymlink fails closed because openat2 ownership resolution is Linux-only.
func (systemOps) OpenDirectoryNoSymlink(string) (int, error) { return -1, ErrUnsupportedPlatform }

// OpenDirectoryBeneath fails closed because openat2 ownership resolution is Linux-only.
func (systemOps) OpenDirectoryBeneath(string, string) (int, error) { return -1, ErrUnsupportedPlatform }

// OpenFileBeneath fails closed because openat2 ownership resolution is Linux-only.
func (systemOps) OpenFileBeneath(string, string) (int, error) { return -1, ErrUnsupportedPlatform }

// OpenDirectoryAt fails closed because fd-relative openat2 resolution is Linux-only.
func (systemOps) OpenDirectoryAt(int, string) (int, error) { return -1, ErrUnsupportedPlatform }

// OpenFileAt fails closed because fd-relative openat2 resolution is Linux-only.
func (systemOps) OpenFileAt(int, string) (int, error) { return -1, ErrUnsupportedPlatform }

// Fstat fails closed because Linux descriptor evidence is unavailable.
func (systemOps) Fstat(int) (FileInfo, error) { return FileInfo{}, ErrUnsupportedPlatform }

// FstatFS fails closed because Linux filesystem identity is unavailable.
func (systemOps) FstatFS(int) (FileSystemInfo, error) {
	return FileSystemInfo{}, ErrUnsupportedPlatform
}

// ReadlinkFD fails closed because procfs descriptor links are unavailable.
func (systemOps) ReadlinkFD(int) (string, error) { return "", ErrUnsupportedPlatform }

// Close fails closed because no Linux descriptor could have been opened.
func (systemOps) Close(int) error { return ErrUnsupportedPlatform }

// Dup fails closed because namespace descriptors are Linux-only.
func (systemOps) Dup(int) (int, error) { return -1, ErrUnsupportedPlatform }

// PidfdOpen fails closed because pidfds are Linux-only.
func (systemOps) PidfdOpen(int) (int, error) { return -1, ErrUnsupportedPlatform }

// PidfdSendSignal fails closed because pidfds are Linux-only.
func (systemOps) PidfdSendSignal(int, int) error { return ErrUnsupportedPlatform }

// setns fails closed because namespaces are Linux-only.
func (systemOps) setns(int, int) error { return ErrUnsupportedPlatform }

// unshare fails closed because namespaces are Linux-only.
func (systemOps) unshare(int) error { return ErrUnsupportedPlatform }

// hostname fails closed because UTS nodename inspection is Linux-only.
func (systemOps) hostname() (string, error) { return "", ErrUnsupportedPlatform }

// setHostname fails closed because UTS nodename mutation is Linux-only.
func (systemOps) setHostname(string) error { return ErrUnsupportedPlatform }

// loopbackUp fails closed because network-interface inspection is Linux-only.
func (systemOps) loopbackUp() (bool, error) { return false, ErrUnsupportedPlatform }

// setLoopbackUp fails closed because network-interface mutation is Linux-only.
func (systemOps) setLoopbackUp(bool) error { return ErrUnsupportedPlatform }

// mount fails closed because the M2 mount contract is Linux-only.
func (systemOps) mount(string, string, string, uintptr, string) error { return ErrUnsupportedPlatform }

// unmount fails closed because the M2 mount contract is Linux-only.
func (systemOps) unmount(string, int) error { return ErrUnsupportedPlatform }

// pivotRoot fails closed because pivot_root is Linux-only.
func (systemOps) pivotRoot(string, string) error { return ErrUnsupportedPlatform }

// mkdir fails closed because rootfs preparation is Linux-only.
func (systemOps) mkdir(string, uint32) error { return ErrUnsupportedPlatform }

// mknod fails closed because device-node creation is Linux-only.
func (systemOps) mknod(string, uint32, int) error { return ErrUnsupportedPlatform }

// chmod fails closed because rootfs device permission changes are Linux-only.
func (systemOps) chmod(string, uint32) error { return ErrUnsupportedPlatform }

// remove fails closed because rootfs preparation is Linux-only.
func (systemOps) remove(string) error { return ErrUnsupportedPlatform }

// chdir fails closed because rootfs preparation is Linux-only.
func (systemOps) chdir(string) error { return ErrUnsupportedPlatform }
