package shim

import (
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
	done := make(chan error, 1)
	go func() { done <- WaitLaunchRelease(int(reader.Fd())) }()
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
		if err := WaitLaunchRelease(int(reader.Fd())); err == nil {
			t.Fatalf("payload %v was accepted", payload)
		}
		_ = reader.Close()
	}
}
