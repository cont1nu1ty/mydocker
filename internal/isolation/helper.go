package isolation

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"
)

const defaultNamespaceCleanupTimeout = 5 * time.Second

// LockedHelper is a short-lived capability for namespace and rootfs mutations
// on the dedicated OS thread owned by RunLockedHelper.
//
// A helper becomes tainted after a successful unshare. Its goroutine then exits
// without unlocking the OS thread, which makes the Go runtime discard that
// thread instead of returning namespace-modified state to the daemon pool.
type LockedHelper struct {
	ops            helperOps
	threadID       int
	active         bool
	tainted        bool
	session        *namespaceSession
	created        map[NamespaceType]uint64
	fsPrivate      bool
	pid1Child      bool
	rootfsStarted  bool
	rootfsPrepared bool
	hostnameSet    *string
	loopbackSet    *bool
}

// osThreadLocker abstracts thread pinning so pure tests can prove lock and
// release behavior without invoking runtime thread operations.
type osThreadLocker interface {
	lock()
	unlock()
}

// runtimeThreadLocker binds production helper goroutines to one OS thread.
type runtimeThreadLocker struct{}

// lock pins the current production helper goroutine to its OS thread.
func (runtimeThreadLocker) lock() { runtime.LockOSThread() }

// unlock releases an untainted production helper goroutine from its OS thread.
func (runtimeThreadLocker) unlock() { runtime.UnlockOSThread() }

// RunLockedHelper runs action on a fresh goroutine pinned to one OS thread.
// If action successfully unshares, the goroutine exits while still pinned so
// the runtime discards the changed thread; otherwise the original thread state
// is released normally. A returned nil means action completed before disposal.
func RunLockedHelper(ctx context.Context, ops Ops, action func(*LockedHelper) error) error {
	return runLockedHelper(ctx, ops, runtimeThreadLocker{}, action)
}

// runLockedHelper supplies injectable thread pinning for pure fault tests.
func runLockedHelper(ctx context.Context, ops Ops, locker osThreadLocker, action func(*LockedHelper) error) error {
	return runLockedHelperWithCleanup(ctx, ops, locker, defaultNamespaceCleanupTimeout, action)
}

// runLockedHelperWithCleanup runs one helper with an independently bounded namespace cleanup window.
func runLockedHelperWithCleanup(ctx context.Context, ops Ops, locker osThreadLocker, cleanupTimeout time.Duration, action func(*LockedHelper) error) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := requireOps(ops); err != nil {
		return err
	}
	mutations, ok := ops.(helperOps)
	if !ok {
		return fmt.Errorf("isolation Ops does not provide private helper mutations")
	}
	if locker == nil {
		return fmt.Errorf("OS-thread locker must not be nil")
	}
	if action == nil {
		return fmt.Errorf("locked helper action must not be nil")
	}
	if cleanupTimeout <= 0 {
		return fmt.Errorf("namespace cleanup timeout must be positive")
	}
	result := make(chan error, 1)
	go func() {
		locker.lock()
		helper := &LockedHelper{
			ops:      mutations,
			threadID: ops.ThreadID(),
			active:   true,
			created:  make(map[NamespaceType]uint64),
		}
		actionErr := runHelperAction(helper, action)
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), cleanupTimeout)
		clean, cleanupErr := helper.cleanupNamespaceSession(cleanupCtx)
		cancelCleanup()
		if !clean {
			helper.tainted = true
		}
		helper.active = false
		if !helper.tainted {
			locker.unlock()
		}
		result <- errors.Join(actionErr, cleanupErr)
	}()
	return <-result
}

// runHelperAction converts a callback panic into a fail-closed error while the
// runner retains responsibility for disposing a possibly tainted OS thread.
func runHelperAction(helper *LockedHelper, action func(*LockedHelper) error) (actionErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			actionErr = fmt.Errorf("locked isolation helper panicked: %v", recovered)
		}
	}()
	return action(helper)
}

// checkThread rejects zero, expired, or thread-migrated helper capabilities.
func (h *LockedHelper) checkThread() error {
	if h == nil || h.ops == nil || !h.active {
		return fmt.Errorf("%w: locked helper is not active", ErrClosed)
	}
	if h.ops.ThreadID() != h.threadID {
		return ErrWrongThread
	}
	return nil
}

// unshare verifies the helper thread, performs one namespace creation, and
// seals the OS thread for disposal immediately after the syscall succeeds.
func (h *LockedHelper) unshare(flags int) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	if err := h.ops.unshare(flags); err != nil {
		return err
	}
	h.tainted = true
	return nil
}

// setns verifies the helper thread, joins one namespace, and immediately seals the thread for disposal.
func (h *LockedHelper) setns(fd, namespaceFlag int) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	if err := h.ops.setns(fd, namespaceFlag); err != nil {
		return err
	}
	h.tainted = true
	return nil
}

// ensurePrivateFS unshares CLONE_FS before a mount-namespace setns so filesystem state is not shared with daemon threads.
func (h *LockedHelper) ensurePrivateFS() error {
	if err := h.checkThread(); err != nil {
		return err
	}
	if h.fsPrivate {
		return nil
	}
	if err := h.unshare(filesystemContextFlag()); err != nil {
		return fmt.Errorf("unshare filesystem context: %w", err)
	}
	h.fsPrivate = true
	return nil
}

// mount verifies the dedicated helper thread before one mount mutation.
func (h *LockedHelper) mount(source, target, filesystem string, flags uintptr, data string) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	return h.ops.mount(source, target, filesystem, flags, data)
}

// unmount verifies the dedicated helper thread before one detach mutation.
func (h *LockedHelper) unmount(target string, flags int) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	return h.ops.unmount(target, flags)
}

// pivotRoot verifies the dedicated helper thread before switching roots.
func (h *LockedHelper) pivotRoot(newRoot, putOld string) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	return h.ops.pivotRoot(newRoot, putOld)
}

// mkdir verifies the dedicated helper thread before creating a rootfs path.
func (h *LockedHelper) mkdir(path string, mode uint32) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	return h.ops.mkdir(path, mode)
}

// remove verifies the dedicated helper thread before removing a rootfs path.
func (h *LockedHelper) remove(path string) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	return h.ops.remove(path)
}

// chdir verifies the dedicated helper thread before changing its working directory.
func (h *LockedHelper) chdir(path string) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	return h.ops.chdir(path)
}
