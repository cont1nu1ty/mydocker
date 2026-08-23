package engine

import (
	"runtime"
	"strconv"
	"testing"

	"mydocker/internal/operation"
)

// TestTargetLocksSerializeWaitersAndReleaseRegistry verifies one key never has
// overlapping holders and its entry disappears after the final waiter exits.
func TestTargetLocksSerializeWaitersAndReleaseRegistry(t *testing.T) {
	locks := newTargetLocks()
	target := operation.Target{Kind: operation.TargetContainer, ID: "container-lock-test"}
	releaseFirst := locks.lock(target)
	started := make(chan struct{})
	acquired := make(chan func(), 1)
	go func() {
		close(started)
		acquired <- locks.lock(target)
	}()
	<-started
	for {
		locks.mu.Lock()
		entry := locks.locks[string(target.Kind)+"\x00"+target.ID]
		var refs uint64
		if entry != nil {
			refs = entry.refs
		}
		locks.mu.Unlock()
		if refs == 2 {
			break
		}
		runtime.Gosched()
	}
	select {
	case release := <-acquired:
		release()
		t.Fatal("second target lock acquired while the first holder was active")
	default:
	}
	releaseFirst()
	releaseSecond := <-acquired
	releaseSecond()
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if len(locks.locks) != 0 {
		t.Fatalf("target lock registry size = %d, want 0", len(locks.locks))
	}
}

// TestTargetLocksDiscardHighChurnKeys verifies deleted and failed resource IDs
// do not accumulate process-local mutexes for the daemon lifetime.
func TestTargetLocksDiscardHighChurnKeys(t *testing.T) {
	locks := newTargetLocks()
	for index := 0; index < 10_000; index++ {
		target := operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-" + strconv.Itoa(index)}
		locks.lock(target)()
	}
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if len(locks.locks) != 0 {
		t.Fatalf("target lock registry size after churn = %d, want 0", len(locks.locks))
	}
}
