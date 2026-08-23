package isolation

import (
	"context"
	"fmt"
	"time"
)

const (
	// PID1BootstrapSchemaVersion is the serialized parent-to-wrapper bootstrap contract.
	PID1BootstrapSchemaVersion = 1
)

// PID1Bootstrap carries the new PID/mount namespace receipts and rootfs intent from the parent helper to PID 1.
type PID1Bootstrap struct {
	SchemaVersion int                 `json:"schema_version"`
	Namespaces    CreatedNamespaceSet `json:"namespaces"`
	Rootfs        RootfsConfig        `json:"rootfs"`
}

// Clone returns a bootstrap without sharing its mutable namespace inode map.
func (b PID1Bootstrap) Clone() PID1Bootstrap {
	b.Namespaces = b.Namespaces.Clone()
	return b
}

// Validate rejects bootstrap data without exactly one PID and one mount namespace receipt.
func (b PID1Bootstrap) Validate() error {
	if b.SchemaVersion != PID1BootstrapSchemaVersion {
		return fmt.Errorf("unsupported PID 1 bootstrap schema version %d", b.SchemaVersion)
	}
	if err := b.Namespaces.Validate(); err != nil {
		return err
	}
	if len(b.Namespaces.Inodes) != 2 || b.Namespaces.Inodes[NamespacePID] == 0 || b.Namespaces.Inodes[NamespaceMount] == 0 {
		return fmt.Errorf("%w: PID 1 bootstrap requires exactly PID and mount namespace receipts", ErrUnsafeIdentity)
	}
	return b.Rootfs.Validate()
}

// AttemptIsolationReceipt binds the final PID and mount namespaces to the long-lived PID 1 wrapper owner.
type AttemptIsolationReceipt struct {
	Owner ProcessEvidence   `json:"owner"`
	PID   NamespaceEvidence `json:"pid_namespace"`
	Mount NamespaceEvidence `json:"mount_namespace"`
}

// Validate rejects a receipt that is not strongly bound to one final wrapper owner.
func (r AttemptIsolationReceipt) Validate() error {
	if err := r.Owner.Validate(); err != nil {
		return err
	}
	if err := r.PID.Validate(); err != nil {
		return err
	}
	if err := r.Mount.Validate(); err != nil {
		return err
	}
	if r.PID.Type != NamespacePID || r.Mount.Type != NamespaceMount || r.PID.Owner != r.Owner || r.Mount.Owner != r.Owner {
		return fmt.Errorf("%w: Attempt namespace receipt is not bound to its final PID 1 owner", ErrUnsafeIdentity)
	}
	return nil
}

// PID1Launcher forks and execs the long-lived PID 1 wrapper from the calling LockedHelper thread.
// It returns only after the child has completed RunPID1Child identity readiness, and
// transfers an action-time verified ProcessHandle for the final wrapper owner.
type PID1Launcher interface {
	// ForkPID1 starts the wrapper from the calling helper thread and waits for verified child readiness.
	ForkPID1(ctx context.Context, bootstrap PID1Bootstrap) (*ProcessHandle, error)
}

// NewPID1Bootstrap verifies the parent helper's pid_for_children and active mount receipts before fork.
func (h *LockedHelper) NewPID1Bootstrap(ctx context.Context, rootfs RootfsConfig) (PID1Bootstrap, error) {
	if err := validateContext(ctx); err != nil {
		return PID1Bootstrap{}, err
	}
	if err := h.checkThread(); err != nil {
		return PID1Bootstrap{}, err
	}
	if h.pid1Child {
		return PID1Bootstrap{}, fmt.Errorf("PID 1 child cannot create another Attempt bootstrap")
	}
	if err := rootfs.Validate(); err != nil {
		return PID1Bootstrap{}, err
	}
	created := CreatedNamespaceSet{Inodes: map[NamespaceType]uint64{
		NamespacePID:   h.created[NamespacePID],
		NamespaceMount: h.created[NamespaceMount],
	}}
	bootstrap := PID1Bootstrap{SchemaVersion: PID1BootstrapSchemaVersion, Namespaces: created, Rootfs: rootfs}
	if err := bootstrap.Validate(); err != nil {
		return PID1Bootstrap{}, err
	}
	if err := verifyBootstrapNamespaces(h.ops, bootstrap); err != nil {
		return PID1Bootstrap{}, err
	}
	return bootstrap, nil
}

// ForkPID1 launches the wrapper on the helper thread and derives namespace receipts only from that final owner.
// On success the caller owns the returned ProcessHandle and must close it.
func (h *LockedHelper) ForkPID1(ctx context.Context, launcher PID1Launcher, bootstrap PID1Bootstrap) (*ProcessHandle, AttemptIsolationReceipt, error) {
	if err := validateContext(ctx); err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	if err := h.checkThread(); err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	if launcher == nil {
		return nil, AttemptIsolationReceipt{}, fmt.Errorf("PID 1 launcher must not be nil")
	}
	if err := bootstrap.Validate(); err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	if bootstrap.Namespaces.Inodes[NamespacePID] != h.created[NamespacePID] || bootstrap.Namespaces.Inodes[NamespaceMount] != h.created[NamespaceMount] {
		return nil, AttemptIsolationReceipt{}, fmt.Errorf("%w: PID 1 bootstrap does not match helper namespace receipts", ErrUnsafeIdentity)
	}
	if err := verifyBootstrapNamespaces(h.ops, bootstrap); err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	process, err := launcher.ForkPID1(ctx, bootstrap.Clone())
	if err != nil {
		return nil, AttemptIsolationReceipt{}, fmt.Errorf("fork PID 1 wrapper: %w", err)
	}
	if process == nil {
		return nil, AttemptIsolationReceipt{}, fmt.Errorf("%w: PID 1 launcher returned a nil process handle", ErrUnsafeIdentity)
	}
	success := false
	defer func() {
		if !success {
			_ = process.Close()
		}
	}()
	if err := process.Verify(ctx); err != nil {
		return nil, AttemptIsolationReceipt{}, fmt.Errorf("verify final PID 1 wrapper: %w", err)
	}
	owner, err := process.Evidence()
	if err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	pidHandle, err := OpenNamespaceHandle(ctx, process, NamespacePID)
	if err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	defer pidHandle.Close()
	mountHandle, err := OpenNamespaceHandle(ctx, process, NamespaceMount)
	if err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	defer mountHandle.Close()
	pidEvidence, err := pidHandle.Evidence()
	if err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	mountEvidence, err := mountHandle.Evidence()
	if err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	receipt := AttemptIsolationReceipt{Owner: owner, PID: pidEvidence, Mount: mountEvidence}
	if err := receipt.Validate(); err != nil {
		return nil, AttemptIsolationReceipt{}, err
	}
	if receipt.PID.Inode != bootstrap.Namespaces.Inodes[NamespacePID] || receipt.Mount.Inode != bootstrap.Namespaces.Inodes[NamespaceMount] {
		return nil, AttemptIsolationReceipt{}, fmt.Errorf("%w: final PID 1 namespace evidence differs from bootstrap", ErrUnsafeIdentity)
	}
	success = true
	return process, receipt, nil
}

// RunPID1Child verifies active child namespace identity and then runs the long-lived wrapper action.
// The action may durably checkpoint the final owner receipts before explicitly
// invoking helper.PrepareRoot; this function performs no automatic mounts.
func RunPID1Child(ctx context.Context, ops Ops, bootstrap PID1Bootstrap, action func(context.Context, *LockedHelper) error) error {
	return runPID1Child(ctx, ops, runtimeThreadLocker{}, defaultNamespaceCleanupTimeout, bootstrap, action)
}

// runPID1Child supplies fake thread pinning to pure child-order and identity tests.
func runPID1Child(ctx context.Context, ops Ops, locker osThreadLocker, cleanupTimeout time.Duration, bootstrap PID1Bootstrap, action func(context.Context, *LockedHelper) error) error {
	if err := bootstrap.Validate(); err != nil {
		return err
	}
	if action == nil {
		return fmt.Errorf("PID 1 wrapper action must not be nil")
	}
	return runLockedHelperWithCleanup(ctx, ops, locker, cleanupTimeout, func(helper *LockedHelper) error {
		if ops.ProcessID() != 1 {
			return fmt.Errorf("%w: PID 1 child entry requires namespace PID 1", ErrUnsafeIdentity)
		}
		pidInode, err := activeNamespaceInode(ops, NamespacePID)
		if err != nil {
			return fmt.Errorf("verify active PID namespace: %w", err)
		}
		mountInode, err := activeNamespaceInode(ops, NamespaceMount)
		if err != nil {
			return fmt.Errorf("verify active mount namespace: %w", err)
		}
		if pidInode != bootstrap.Namespaces.Inodes[NamespacePID] || mountInode != bootstrap.Namespaces.Inodes[NamespaceMount] {
			return fmt.Errorf("%w: PID 1 child namespace evidence differs from bootstrap", ErrUnsafeIdentity)
		}
		helper.created = bootstrap.Namespaces.Clone().Inodes
		helper.fsPrivate = true
		helper.pid1Child = true
		helper.tainted = true
		return action(ctx, helper)
	})
}

// verifyBootstrapNamespaces rechecks the parent's pid_for_children and active mount namespace immediately before fork.
func verifyBootstrapNamespaces(ops Ops, bootstrap PID1Bootstrap) error {
	pidInode, err := currentNamespaceInode(ops, NamespacePID)
	if err != nil {
		return fmt.Errorf("verify pid_for_children before fork: %w", err)
	}
	mountInode, err := currentNamespaceInode(ops, NamespaceMount)
	if err != nil {
		return fmt.Errorf("verify mount namespace before fork: %w", err)
	}
	if pidInode != bootstrap.Namespaces.Inodes[NamespacePID] || mountInode != bootstrap.Namespaces.Inodes[NamespaceMount] {
		return fmt.Errorf("%w: parent helper namespace evidence changed before fork", ErrUnsafeIdentity)
	}
	return nil
}

// activeNamespaceInode opens the caller's active namespace entry rather than PID pid_for_children.
func activeNamespaceInode(ops Ops, namespaceType NamespaceType) (uint64, error) {
	path := fmt.Sprintf("/proc/self/ns/%s", namespaceType.procName())
	fd, err := ops.OpenNamespace(path)
	if err != nil {
		return 0, err
	}
	stat, statErr := ops.Fstat(fd)
	if statErr == nil {
		statErr = validateNamespaceFD(ops, fd, namespaceType, stat.Ino)
	}
	closeErr := ops.Close(fd)
	if err := closeError(statErr, closeErr); err != nil {
		return 0, err
	}
	return stat.Ino, nil
}
