package shim

import (
	"errors"
	"os"
	"testing"
)

// TestWaitLaunchReleaseAcceptsOneByteThenEOF verifies the child proceeds only
// after the parent writes the protocol byte and relinquishes its pipe endpoint.
func TestWaitLaunchReleaseAcceptsOneByteThenEOF(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	guard := &fakeLaunchParentGuard{}
	done := make(chan error, 1)
	go func() { done <- waitLaunchRelease(int(reader.Fd()), guard) }()
	if _, err := writer.Write([]byte{LaunchReleaseByte}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if guard.clears != 1 {
		t.Fatalf("death-signal clears=%d, want one", guard.clears)
	}
}

// TestWaitLaunchReleaseRejectsParentDeathAndExtraData verifies EOF without
// authorization and ambiguous multi-byte writes both fail closed.
func TestWaitLaunchReleaseRejectsParentDeathAndExtraData(t *testing.T) {
	for _, payload := range [][]byte{nil, {LaunchReleaseByte, LaunchReleaseByte}} {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		if len(payload) > 0 {
			if _, err := writer.Write(payload); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		guard := &fakeLaunchParentGuard{}
		if err := waitLaunchRelease(int(reader.Fd()), guard); err == nil {
			t.Fatalf("payload %v was accepted", payload)
		}
		_ = reader.Close()
		if guard.clears != 0 {
			t.Fatalf("payload %v cleared parent death signal", payload)
		}
	}
}

// TestWaitLaunchReleasePropagatesDeathSignalClearFailure verifies exact
// authorization still fails before config or namespace work when prctl cannot clear.
func TestWaitLaunchReleasePropagatesDeathSignalClearFailure(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte{LaunchReleaseByte}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected clear failure")
	guard := &fakeLaunchParentGuard{err: injected}
	if err := waitLaunchRelease(int(reader.Fd()), guard); !errors.Is(err, injected) {
		t.Fatalf("wait error=%v, want clear failure", err)
	}
	_ = reader.Close()
	if guard.clears != 1 {
		t.Fatalf("death-signal clears=%d, want one attempted clear", guard.clears)
	}
}

// fakeLaunchParentGuard records death-signal clearing without calling Linux prctl.
type fakeLaunchParentGuard struct {
	clears int
	err    error
}

// ClearParentDeathSignal records one injected clear result for release protocol tests.
func (guard *fakeLaunchParentGuard) ClearParentDeathSignal() error {
	guard.clears++
	return guard.err
}
