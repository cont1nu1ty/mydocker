package slim

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"

	"mydocker/internal/isolation"
	"mydocker/internal/ownership"
	"mydocker/internal/provider"
)

// TestLinuxShimLauncherSerializesStaleRemoveWithRelaunch uses a barrier inside
// P2 launch to verify concurrent stale Remove(P1) waits for Ensure, then cannot
// unlink P2's control socket or act on P2 after the journal has rebound.
func TestLinuxShimLauncherSerializesStaleRemoveWithRelaunch(t *testing.T) {
	fixture := newKeeperLauncherFixture(t)
	shortRoot, err := os.MkdirTemp("/tmp", "mdr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortRoot) })
	if err := os.Chmod(shortRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	artifacts, err := newArtifactStore(shortRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := artifacts.ensureOwnerDirectory(fixture.request.Owner); err != nil {
		t.Fatal(err)
	}
	paths, err := deriveArtifactPaths(shortRoot, fixture.request.Owner)
	if err != nil {
		t.Fatal(err)
	}
	fixture.launcher.runtimeRoot = shortRoot
	fixture.request.Paths = paths
	fixture.control.fixture = &fixture
	config, intent := ensureKeeperIntent(t, fixture)
	oldEvidence := fixture.evidence
	authorized, err := intent.withProcess(launchPhaseAuthorized, oldEvidence)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := authorized.withProcess(launchPhaseReady, oldEvidence)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newLaunchStore(fixture.request.Paths, fixture.request.Owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(intent, authorized); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(authorized, ready); err != nil {
		t.Fatal(err)
	}

	newEvidence := isolation.ProcessEvidence{
		PID: oldEvidence.PID + 1, BootID: oldEvidence.BootID, StartTime: oldEvidence.StartTime + 1,
		CgroupPath: oldEvidence.CgroupPath, Executable: oldEvidence.Executable,
	}
	fixture.runtime.present = false
	fixture.runtime.presenceByEvidence = map[isolation.ProcessEvidence]bool{oldEvidence: false, newEvidence: false}
	ensureAtStart := make(chan struct{})
	releaseEnsure := make(chan struct{})
	var endpoint net.Listener
	fixture.factory.onStart = func(ProcessLaunchSpec) error {
		fixture.runtime.evidence = newEvidence
		fixture.started.pid = newEvidence.PID
		fixture.manager.setMembers(newEvidence.PID)
		fixture.control.peerPID = newEvidence.PID
		var listenErr error
		endpoint, listenErr = net.Listen("unix", fixture.request.Paths.ControlSocket)
		close(ensureAtStart)
		if listenErr != nil {
			return listenErr
		}
		<-releaseEnsure
		return nil
	}
	oldDigest, err := ownership.EvidenceDigest(oldEvidence)
	if err != nil {
		t.Fatal(err)
	}
	reference := ResourceReference{
		Owner: fixture.request.Owner, Kind: ownership.KindKeeperProcess, LocalID: localIDFor(ownership.KindKeeperProcess),
		ReceiptEvidenceSHA256: oldDigest, LauncherEvidenceSHA256: oldDigest, WrapperEvidenceSHA256: config.WrapperEvidence,
		ProcessEvidence: oldEvidence, SandboxID: fixture.request.SandboxID, Paths: fixture.request.Paths,
	}
	ensureDone := make(chan struct {
		process LaunchedProcess
		err     error
	}, 1)
	go func() {
		process, ensureErr := fixture.launcher.EnsureKeeper(context.Background(), fixture.request)
		ensureDone <- struct {
			process LaunchedProcess
			err     error
		}{process: process, err: ensureErr}
	}()
	select {
	case <-ensureAtStart:
	case result := <-ensureDone:
		t.Fatalf("EnsureKeeper() returned before P2 launch barrier: %v", result.err)
	}
	if endpoint == nil {
		t.Fatal("P2 launch did not publish its test control socket")
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	removeContext := &removeValidationBarrierContext{Context: context.Background(), entered: make(chan struct{})}
	removeDone := make(chan struct {
		observation provider.CleanupObservation
		err         error
	}, 1)
	go func() {
		observation, removeErr := fixture.launcher.Remove(removeContext, reference)
		removeDone <- struct {
			observation provider.CleanupObservation
			err         error
		}{observation: observation, err: removeErr}
	}()
	select {
	case <-removeContext.entered:
	case result := <-removeDone:
		close(releaseEnsure)
		<-ensureDone
		t.Fatalf("Remove(P1) returned before reaching its owner-lock boundary: %v", result.err)
	}
	close(releaseEnsure)
	ensureResult := <-ensureDone
	if ensureResult.err != nil {
		t.Fatal(ensureResult.err)
	}
	if ensureResult.process.ProcessEvidence != newEvidence {
		t.Fatalf("EnsureKeeper() evidence = %+v, want P2 %+v", ensureResult.process.ProcessEvidence, newEvidence)
	}
	removeResult := <-removeDone
	if removeResult.err != nil {
		t.Fatal(removeResult.err)
	}
	observation := removeResult.observation
	if observation.Disposition != provider.CleanupAlreadyAbsent {
		t.Fatalf("Remove(P1) disposition = %q, want %q", observation.Disposition, provider.CleanupAlreadyAbsent)
	}
	info, err := os.Lstat(fixture.request.Paths.ControlSocket)
	if err != nil {
		t.Fatalf("P2 control socket was removed: %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("P2 control endpoint mode = %v, want Unix socket", info.Mode())
	}
	present, err := fixture.runtime.Present(context.Background(), newEvidence)
	if err != nil || !present {
		t.Fatalf("P2 presence = %t, error = %v", present, err)
	}
	journal := readKeeperLaunchJournal(t, fixture)
	if journal.ProcessEvidence == nil || *journal.ProcessEvidence != newEvidence || journal.Phase != launchPhaseReady {
		t.Fatalf("journal after stale Remove = %+v, want ready P2", journal)
	}
}

// removeValidationBarrierContext announces that Remove completed validation
// immediately before it attempts to acquire the owner operation lock.
type removeValidationBarrierContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

// Err preserves the wrapped context result while publishing the deterministic
// test barrier used to overlap stale Remove with an in-progress Ensure.
func (ctx *removeValidationBarrierContext) Err() error {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Err()
}
