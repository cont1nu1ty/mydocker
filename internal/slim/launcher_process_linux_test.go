//go:build linux

package slim

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// TestClassifyClone3CgroupPIDFDProbe verifies preflight reports capability only
// for the no-child EBADF proof and rejects missing, blocked, or unsupported flags.
func TestClassifyClone3CgroupPIDFDProbe(t *testing.T) {
	for _, test := range []struct {
		name  string
		errno syscall.Errno
		ok    bool
	}{
		{name: "features parsed", errno: unix.EBADF, ok: true},
		{name: "syscall missing", errno: unix.ENOSYS},
		{name: "seccomp blocked", errno: unix.EPERM},
		{name: "flag unsupported", errno: unix.EINVAL},
		{name: "unexpected success", errno: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := classifyClone3CgroupPIDFDProbe(test.errno)
			if (err == nil) != test.ok {
				t.Fatalf("classify errno %v error=%v, want ok=%v", test.errno, err, test.ok)
			}
		})
	}
}

// TestOSProcessFactoryClosesTransferredFDOnPreStartFailure verifies canceled
// validation paths consume caller-owned ExtraFDs without reaching exec.
func TestOSProcessFactoryClosesTransferredFDOnPreStartFailure(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (OSProcessFactory{}).Start(ctx, ProcessLaunchSpec{ExtraFDs: []int{int(reader.Fd())}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error=%v, want canceled", err)
	}
	if _, err := reader.Stat(); err == nil {
		t.Fatal("canceled Start retained a transferred descriptor")
	}
	// Mark the os.File wrapper closed after the raw transferred descriptor was
	// consumed so its finalizer cannot later close an unrelated reused number.
	_ = reader.Close()
}

// TestStartedProcessAbortIsConcurrentAndIdempotent verifies one exact signal,
// one reap result, and cached completion serve concurrent and repeated aborts.
func TestStartedProcessAbortIsConcurrentAndIdempotent(t *testing.T) {
	process, reader, signals, closes := newTestStartedProcess(t)
	defer reader.Close()
	results := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	for index := 0; index < 2; index++ {
		go func() {
			defer wait.Done()
			results <- process.Abort(context.Background())
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if signals.Load() != 1 || closes.Load() != 1 {
		t.Fatalf("signals=%d closes=%d, want one each", signals.Load(), closes.Load())
	}
	if err := process.Abort(context.Background()); err != nil || signals.Load() != 1 {
		t.Fatalf("repeated Abort=(%v), signals=%d", err, signals.Load())
	}
	if err := process.Commit(); err == nil {
		t.Fatal("Commit succeeded after Abort")
	}
}

// TestStartedProcessCommitAndReleaseFailureTransitions verifies committed
// children reject launch abort and failed release remains abortable but not committable.
func TestStartedProcessCommitAndReleaseFailureTransitions(t *testing.T) {
	t.Run("commit rejects abort", func(t *testing.T) {
		process, reader, signals, _ := newTestStartedProcess(t)
		defer reader.Close()
		process.pidfd = 70
		if _, err := process.TakePIDFD(); err != nil {
			t.Fatal(err)
		}
		if err := process.Release(); err != nil {
			t.Fatal(err)
		}
		if err := process.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := process.Abort(context.Background()); err == nil || signals.Load() != 0 {
			t.Fatalf("Abort after Commit=(%v), signals=%d", err, signals.Load())
		}
	})
	t.Run("release failure requires abort", func(t *testing.T) {
		process, reader, signals, _ := newTestStartedProcess(t)
		defer reader.Close()
		if err := process.release.Close(); err != nil {
			t.Fatal(err)
		}
		if err := process.Release(); err == nil {
			t.Fatal("closed release pipe reported success")
		}
		if err := process.Commit(); err == nil {
			t.Fatal("Commit succeeded after release failure")
		}
		if err := process.Abort(context.Background()); err != nil || signals.Load() != 1 {
			t.Fatalf("Abort after release failure=(%v), signals=%d", err, signals.Load())
		}
	})
}

// newTestStartedProcess creates a syscall-free state-machine fixture with a real pipe gate and fake pidfd operations.
func newTestStartedProcess(t *testing.T) (*osStartedProcess, *os.File, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	done <- nil
	var signals atomic.Int32
	var closes atomic.Int32
	process := &osStartedProcess{
		pid: 123, pidfd: -1, cleanupFD: 71, release: writer, done: done,
		state: startedProcessGated, abortDone: make(chan struct{}),
		signalPIDFD: func(int) error { signals.Add(1); return nil },
		closeFD:     func(int) error { closes.Add(1); return nil },
	}
	return process, reader, &signals, &closes
}
