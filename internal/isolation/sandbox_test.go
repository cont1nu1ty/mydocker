package isolation

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestSandboxConfigurationRequiresCreatedNamespaces verifies hostname and
// loopback mutations cannot target the daemon's inherited namespaces.
func TestSandboxConfigurationRequiresCreatedNamespaces(t *testing.T) {
	ops := newFakeOps()
	_, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		hostnameErr := helper.ConfigureHostname(context.Background(), "sandbox-one")
		loopbackErr := helper.ConfigureLoopback(context.Background(), true)
		if !errors.Is(hostnameErr, ErrUnsafeIdentity) || !errors.Is(loopbackErr, ErrUnsafeIdentity) {
			return errors.New("configuration without created namespaces did not fail closed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range ops.mutations {
		if strings.HasPrefix(mutation, "sethostname:") || strings.HasPrefix(mutation, "set-loopback:") {
			t.Fatalf("inherited namespace mutation = %q", mutation)
		}
	}
}

// TestSandboxConfigurationAppliesExactReadback verifies one helper can set a
// created UTS nodename and loopback mode, replay each value, and reject drift.
func TestSandboxConfigurationAppliesExactReadback(t *testing.T) {
	ops := newFakeOps()
	_, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		if _, err := helper.UnshareNamespaces(context.Background(), NamespaceUTS, NamespaceNetwork); err != nil {
			return err
		}
		if err := helper.ConfigureHostname(context.Background(), "sandbox-one"); err != nil {
			return err
		}
		if err := helper.ConfigureLoopback(context.Background(), true); err != nil {
			return err
		}
		if err := helper.VerifyHostname(context.Background(), "sandbox-one"); err != nil {
			return err
		}
		if err := helper.VerifyLoopback(context.Background(), true); err != nil {
			return err
		}
		mutations := len(ops.mutations)
		if err := helper.ConfigureHostname(context.Background(), "sandbox-one"); err != nil {
			return err
		}
		if err := helper.ConfigureLoopback(context.Background(), true); err != nil {
			return err
		}
		if len(ops.mutations) != mutations {
			return errors.New("same-value replay repeated a Sandbox configuration mutation")
		}
		if err := helper.ConfigureHostname(context.Background(), "sandbox-two"); !errors.Is(err, ErrUnsafeIdentity) {
			return errors.New("different hostname replay did not fail closed")
		}
		if err := helper.ConfigureLoopback(context.Background(), false); !errors.Is(err, ErrUnsafeIdentity) {
			return errors.New("different loopback replay did not fail closed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if ops.hostnameValue != "sandbox-one" || !ops.loopbackValue {
		t.Fatalf("configured hostname=%q loopback=%v", ops.hostnameValue, ops.loopbackValue)
	}
}

// TestSandboxConfigurationVerificationRejectsDrift verifies read-only
// inspection never repairs a changed namespace configuration before reporting
// the retained receipt as unsafe.
func TestSandboxConfigurationVerificationRejectsDrift(t *testing.T) {
	ops := newFakeOps()
	_, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		if _, err := helper.UnshareNamespaces(context.Background(), NamespaceUTS, NamespaceNetwork); err != nil {
			return err
		}
		if err := helper.ConfigureHostname(context.Background(), "sandbox-one"); err != nil {
			return err
		}
		if err := helper.ConfigureLoopback(context.Background(), true); err != nil {
			return err
		}
		ops.hostnameValue = "changed"
		if err := helper.VerifyHostname(context.Background(), "sandbox-one"); !errors.Is(err, ErrUnsafeIdentity) {
			return errors.New("hostname drift was not rejected by read-only verification")
		}
		ops.hostnameValue = "sandbox-one"
		ops.loopbackValue = false
		if err := helper.VerifyLoopback(context.Background(), true); !errors.Is(err, ErrUnsafeIdentity) {
			return errors.New("loopback drift was not rejected by read-only verification")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestSandboxConfigurationAllowsVerifiedJoinedNamespaces verifies a daemon
// retry may configure a live keeper only after joining strong namespace
// handles and cannot carry that authority outside the bounded session.
func TestSandboxConfigurationAllowsVerifiedJoinedNamespaces(t *testing.T) {
	ops := newFakeOps()
	process := captureTestProcess(t, ops)
	uts, err := OpenNamespaceHandle(context.Background(), process, NamespaceUTS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = uts.Close() })
	network, err := OpenNamespaceHandle(context.Background(), process, NamespaceNetwork)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = network.Close() })
	_, err = runFakeNamespaceSession(context.Background(), ops, []*NamespaceHandle{uts, network}, func(ctx context.Context, helper *LockedHelper) error {
		if err := helper.ConfigureHostname(ctx, "joined-sandbox"); err != nil {
			return err
		}
		return helper.ConfigureLoopback(ctx, true)
	})
	if err != nil {
		t.Fatal(err)
	}
	if ops.hostnameValue != "joined-sandbox" || !ops.loopbackValue {
		t.Fatalf("joined configuration hostname=%q loopback=%v", ops.hostnameValue, ops.loopbackValue)
	}
	_, err = runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		if err := helper.ConfigureHostname(context.Background(), "outside-session"); !errors.Is(err, ErrUnsafeIdentity) {
			return errors.New("expired namespace session authority was accepted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestSandboxNetworkNoneForcesLoopbackDown verifies network=none is an exact
// down-state policy rather than an omitted loopback setup step.
func TestSandboxNetworkNoneForcesLoopbackDown(t *testing.T) {
	ops := newFakeOps()
	ops.loopbackValue = true
	_, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		if _, err := helper.UnshareNamespaces(context.Background(), NamespaceNetwork); err != nil {
			return err
		}
		return helper.ConfigureLoopback(context.Background(), false)
	})
	if err != nil {
		t.Fatal(err)
	}
	if ops.loopbackValue {
		t.Fatal("network=none left loopback administratively up")
	}
}

// TestSandboxConfigurationRejectsInvalidOrCancelledInput verifies validation
// and cancellation complete before either privileged configuration syscall.
func TestSandboxConfigurationRejectsInvalidOrCancelledInput(t *testing.T) {
	ops := newFakeOps()
	_, err := runFakeLockedHelper(ops, func(helper *LockedHelper) error {
		if _, err := helper.UnshareNamespaces(context.Background(), NamespaceUTS, NamespaceNetwork); err != nil {
			return err
		}
		if err := helper.ConfigureHostname(context.Background(), strings.Repeat("h", maximumUTSHostnameBytes+1)); err == nil {
			return errors.New("overlong hostname was accepted")
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := helper.ConfigureLoopback(cancelled, true); !errors.Is(err, context.Canceled) {
			return errors.New("cancelled loopback configuration did not return context cancellation")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range ops.mutations[1:] {
		if strings.HasPrefix(mutation, "sethostname:") || strings.HasPrefix(mutation, "set-loopback:") {
			t.Fatalf("invalid or cancelled input caused mutation %q", mutation)
		}
	}
}
