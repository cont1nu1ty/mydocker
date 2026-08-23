package isolation

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
)

const maximumUTSHostnameBytes = 64

// ConfigureHostname applies and verifies the immutable Sandbox nodename only
// while this helper owns a newly created or strong-handle-joined UTS namespace.
// Repeating the same value is safe; a different value fails before mutation.
func (h *LockedHelper) ConfigureHostname(ctx context.Context, hostname string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateUTSHostname(hostname); err != nil {
		return err
	}
	if err := h.verifyOwnedNamespace(NamespaceUTS); err != nil {
		return err
	}
	if h.hostnameSet != nil && *h.hostnameSet != hostname {
		return fmt.Errorf("%w: UTS hostname was already configured with another value", ErrUnsafeIdentity)
	}
	current, err := h.ops.hostname()
	if err != nil {
		return fmt.Errorf("read UTS hostname: %w", err)
	}
	if current != hostname {
		if err := validateContext(ctx); err != nil {
			return err
		}
		if err := h.ops.setHostname(hostname); err != nil {
			return fmt.Errorf("set UTS hostname: %w", err)
		}
		current, err = h.ops.hostname()
		if err != nil {
			return fmt.Errorf("read back UTS hostname: %w", err)
		}
	}
	if current != hostname {
		return fmt.Errorf("%w: UTS hostname readback differs", ErrUnsafeIdentity)
	}
	configured := hostname
	h.hostnameSet = &configured
	return nil
}

// VerifyHostname reads the exact joined or newly created UTS namespace without
// mutating it, and rejects a live nodename that differs from the retained
// Sandbox configuration.
func (h *LockedHelper) VerifyHostname(ctx context.Context, hostname string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateUTSHostname(hostname); err != nil {
		return err
	}
	if err := h.verifyOwnedNamespace(NamespaceUTS); err != nil {
		return err
	}
	current, err := h.ops.hostname()
	if err != nil {
		return fmt.Errorf("read UTS hostname: %w", err)
	}
	if current != hostname {
		return fmt.Errorf("%w: UTS hostname readback differs", ErrUnsafeIdentity)
	}
	return nil
}

// ConfigureLoopback applies and verifies the complete M3 none/loopback policy
// only inside a newly created or strong-handle-joined network namespace. The
// boolean means loopback up; false is the isolated network=none contract.
func (h *LockedHelper) ConfigureLoopback(ctx context.Context, up bool) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := h.verifyOwnedNamespace(NamespaceNetwork); err != nil {
		return err
	}
	if h.loopbackSet != nil && *h.loopbackSet != up {
		return fmt.Errorf("%w: loopback was already configured with another policy", ErrUnsafeIdentity)
	}
	current, err := h.ops.loopbackUp()
	if err != nil {
		return fmt.Errorf("read loopback state: %w", err)
	}
	if current != up {
		if err := validateContext(ctx); err != nil {
			return err
		}
		if err := h.ops.setLoopbackUp(up); err != nil {
			return fmt.Errorf("set loopback state: %w", err)
		}
		current, err = h.ops.loopbackUp()
		if err != nil {
			return fmt.Errorf("read back loopback state: %w", err)
		}
	}
	if current != up {
		return fmt.Errorf("%w: loopback readback differs", ErrUnsafeIdentity)
	}
	configured := up
	h.loopbackSet = &configured
	return nil
}

// VerifyLoopback reads the exact joined or newly created network namespace
// without changing it, and rejects a loopback state that differs from the
// retained M3 none/loopback policy.
func (h *LockedHelper) VerifyLoopback(ctx context.Context, up bool) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := h.verifyOwnedNamespace(NamespaceNetwork); err != nil {
		return err
	}
	current, err := h.ops.loopbackUp()
	if err != nil {
		return fmt.Errorf("read loopback state: %w", err)
	}
	if current != up {
		return fmt.Errorf("%w: loopback readback differs", ErrUnsafeIdentity)
	}
	return nil
}

// verifyOwnedNamespace ensures a Sandbox configuration syscall can affect only
// the exact namespace unshared by this helper or joined through a verified
// NamespaceHandle, never the daemon's inherited namespace.
func (h *LockedHelper) verifyOwnedNamespace(namespaceType NamespaceType) error {
	if err := h.checkThread(); err != nil {
		return err
	}
	expected, found := h.created[namespaceType]
	if !found && h.session != nil {
		h.session.mu.Lock()
		expected, found = h.session.targets[namespaceType]
		h.session.mu.Unlock()
	}
	if !found || expected == 0 {
		return fmt.Errorf("%w: %s namespace is neither created nor verified-joined by this helper", ErrUnsafeIdentity, namespaceType)
	}
	current, err := currentNamespaceInode(h.ops, namespaceType)
	if err != nil {
		return fmt.Errorf("verify active %s namespace: %w", namespaceType, err)
	}
	if current != expected {
		return fmt.Errorf("%w: active %s namespace differs from the helper receipt", ErrUnsafeIdentity, namespaceType)
	}
	return nil
}

// validateUTSHostname keeps the syscall boundary aligned with the persisted
// Sandbox contract while permitting the explicit empty Linux nodename default.
func validateUTSHostname(hostname string) error {
	if len(hostname) > maximumUTSHostnameBytes || !utf8.ValidString(hostname) || strings.ContainsRune(hostname, '\x00') {
		return fmt.Errorf("UTS hostname must be valid UTF-8 without NUL and no longer than %d bytes", maximumUTSHostnameBytes)
	}
	return nil
}
