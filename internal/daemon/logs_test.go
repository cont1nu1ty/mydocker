package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/domain"
	"mydocker/internal/lifecycle"
	"mydocker/internal/logstore"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/shim"
)

type successfulCreateMutator struct {
	*recordingMutator
	result lifecycle.ContainerResult
}

type successfulDeleteMutator struct {
	*recordingMutator
	result lifecycle.ContainerResult
	calls  int
}

// CreateContainer returns one already validated pure lifecycle result so the adapter registration path can be exercised without host effects.
func (mutator *successfulCreateMutator) CreateContainer(_ context.Context, request lifecycle.ContainerCreateRequest) (lifecycle.ContainerResult, error) {
	mutator.createContainerCalls++
	mutator.lastCreateContainer = request
	result := mutator.result.Clone()
	result.Operation.ID = request.OperationID
	if result.Resolution == "" {
		result.Resolution = operation.ResolutionNew
	}
	return result, nil
}

// DeleteContainer returns one replayable terminal Delete result without touching host resources.
func (mutator *successfulDeleteMutator) DeleteContainer(context.Context, lifecycle.ContainerActionRequest) (lifecycle.ContainerResult, error) {
	mutator.calls++
	result := mutator.result.Clone()
	if mutator.calls == 1 {
		result.Resolution = operation.ResolutionResume
	} else {
		result.Resolution = operation.ResolutionReplay
	}
	return result, nil
}

// TestRuntimeLogRegistryReadsWhileShimWriterOwnsLock verifies owner-only registration resolves a live file without accepting a path or acquiring the writer lock.
func TestRuntimeLogRegistryReadsWhileShimWriterOwnsLock(t *testing.T) {
	root, owner, identity, logPath := newRuntimeLogLocation(t, "op-log-live", "container-live", "attempt-live")
	writer, err := logstore.Open(logPath, identity)
	if err != nil {
		t.Fatalf("open shim writer: %v", err)
	}
	defer writer.Close()
	if _, err := writer.Append(logstore.StreamStdout, []byte("live output")); err != nil {
		t.Fatalf("append live output: %v", err)
	}
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	if err := registry.RegisterAttempt(identity, owner); err != nil {
		t.Fatalf("RegisterAttempt() error = %v", err)
	}
	source, err := registry.Locate(context.Background(), identity)
	if err != nil {
		t.Fatalf("Locate() while writer lock held = %v", err)
	}
	frames, err := source.Read(0, 10)
	if err != nil || len(frames) != 1 || string(frames[0].Payload) != "live output" {
		t.Fatalf("live frames = (%+v, %v)", frames, err)
	}
}

// TestRuntimeLogRegistrationChecksExactOwner verifies transport replay is idempotent while a different create owner cannot replace the identity binding.
func TestRuntimeLogRegistrationChecksExactOwner(t *testing.T) {
	root, owner, identity, _ := newRuntimeLogLocation(t, "op-log-owner", "container-owner", "attempt-owner")
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	if err := registry.RegisterAttempt(identity, owner); err != nil {
		t.Fatalf("first RegisterAttempt() error = %v", err)
	}
	if err := registry.RegisterAttempt(identity, owner); err != nil {
		t.Fatalf("replayed RegisterAttempt() error = %v", err)
	}
	other, err := ownership.NewOwnerKey("op-log-owner-other", operation.Target{Kind: operation.TargetContainer, ID: string(identity.ContainerID)}, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("construct other owner: %v", err)
	}
	if err := registry.RegisterAttempt(identity, other); !errors.Is(err, ErrLogAlreadyRegistered) {
		t.Fatalf("replacement RegisterAttempt() error = %v, want ErrLogAlreadyRegistered", err)
	}
	wrongTarget, err := ownership.NewOwnerKey("op-log-wrong", operation.Target{Kind: operation.TargetContainer, ID: "container-other"}, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("construct wrong-target owner: %v", err)
	}
	if err := registry.RegisterAttempt(identity, wrongTarget); !errors.Is(err, ErrLogRegistrationUnsafe) {
		t.Fatalf("wrong-target RegisterAttempt() error = %v, want ErrLogRegistrationUnsafe", err)
	}
	if err := registry.UnregisterAttempt(identity, other); err != nil {
		t.Fatalf("UnregisterAttempt(old owner) error = %v", err)
	}
	registry.mu.RLock()
	retained := registry.owners[identity]
	registry.mu.RUnlock()
	if retained != owner {
		t.Fatalf("owner-mismatched unregister retained = %#v, want %#v", retained, owner)
	}
	if err := registry.UnregisterAttempt(identity, owner); err != nil {
		t.Fatalf("UnregisterAttempt(exact owner) error = %v", err)
	}
	registry.mu.RLock()
	_, retainedFound := registry.owners[identity]
	registry.mu.RUnlock()
	if retainedFound {
		t.Fatal("exact owner remained registered after UnregisterAttempt")
	}
}

// TestRuntimeLogRegistryRediscoversStrictShimConfig verifies daemon restart can rebuild an identity binding without trusting the config's path fields.
func TestRuntimeLogRegistryRediscoversStrictShimConfig(t *testing.T) {
	root, owner, identity, logPath := newRuntimeLogLocation(t, "op-log-restart", "container-restart", "attempt-restart")
	writeRuntimeLogConfig(t, root, owner, identity, logPath)
	writer, err := logstore.Open(logPath, identity)
	if err != nil {
		t.Fatalf("open shim writer: %v", err)
	}
	defer writer.Close()
	if _, err := writer.Append(logstore.StreamStderr, []byte("after daemon restart")); err != nil {
		t.Fatalf("append restart output: %v", err)
	}
	restarted, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	source, err := restarted.Locate(context.Background(), identity)
	if err != nil {
		t.Fatalf("Locate() after registry restart = %v", err)
	}
	frames, err := source.Read(0, 10)
	if err != nil || len(frames) != 1 || string(frames[0].Payload) != "after daemon restart" {
		t.Fatalf("restart frames = (%+v, %v)", frames, err)
	}
}

// TestRuntimeLogRegistryRejectsConfigPathAndLogLink verifies neither persisted config text nor a link can redirect the identity outside its derived owner location.
func TestRuntimeLogRegistryRejectsConfigPathAndLogLink(t *testing.T) {
	t.Run("config path", func(t *testing.T) {
		root, owner, identity, logPath := newRuntimeLogLocation(t, "op-log-path", "container-path", "attempt-path")
		writeRuntimeLogConfig(t, root, owner, identity, filepath.Join(t.TempDir(), "redirected.log"))
		registry, err := NewRuntimeLogRegistry(root)
		if err != nil {
			t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
		}
		if _, err := registry.Locate(context.Background(), identity); !errors.Is(err, ErrLogRegistrationUnsafe) {
			t.Fatalf("Locate() config path error = %v, want ErrLogRegistrationUnsafe", err)
		}
		if _, err := os.Lstat(logPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("derived log unexpectedly created during lookup: %v", err)
		}
	})

	t.Run("log symlink", func(t *testing.T) {
		root, owner, identity, logPath := newRuntimeLogLocation(t, "op-log-link", "container-link", "attempt-link")
		target := filepath.Join(t.TempDir(), "foreign.log")
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatalf("create foreign log: %v", err)
		}
		if err := os.Symlink(target, logPath); err != nil {
			t.Fatalf("create log symlink: %v", err)
		}
		registry, err := NewRuntimeLogRegistry(root)
		if err != nil {
			t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
		}
		if err := registry.RegisterAttempt(identity, owner); err != nil {
			t.Fatalf("RegisterAttempt() error = %v", err)
		}
		if _, err := registry.Locate(context.Background(), identity); !errors.Is(err, logstore.ErrUnsafePath) {
			t.Fatalf("Locate() symlink error = %v, want ErrUnsafePath", err)
		}
	})
}

// TestLogAPIResponseHidesLocatorAndMapsCorruption verifies successful and failed responses expose only bounded frame data, never path or descriptor details.
func TestLogAPIResponseHidesLocatorAndMapsCorruption(t *testing.T) {
	coordinator, pair := seededCoordinator(t)
	root, owner, identity, logPath := newRuntimeLogLocation(t, "op-container-create", string(pair.Container.ID), string(pair.Attempt.ID))
	writer, err := logstore.Open(logPath, identity)
	if err != nil {
		t.Fatalf("open shim writer: %v", err)
	}
	if _, err := writer.Append(logstore.StreamStdout, []byte("public payload")); err != nil {
		t.Fatalf("append public payload: %v", err)
	}
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	if err := registry.RegisterAttempt(identity, owner); err != nil {
		t.Fatalf("RegisterAttempt() error = %v", err)
	}
	service, err := newService(&recordingMutator{}, coordinator, registry)
	if err != nil {
		t.Fatalf("newService() error = %v", err)
	}
	frames, err := service.LogsAfter(context.Background(), v1.RequestContext{RequestID: "request-log-safe"}, v1.ListLogsRequest{
		ContainerID: string(identity.ContainerID), AttemptID: string(identity.AttemptID), Limit: 10,
	})
	if err != nil {
		t.Fatalf("LogsAfter() error = %v", err)
	}
	payload, err := json.Marshal(frames)
	if err != nil {
		t.Fatalf("marshal public frames: %v", err)
	}
	for _, forbidden := range []string{root, "workload.log", `"path"`, `"fd"`} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("public log response leaked %q: %s", forbidden, payload)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close shim writer: %v", err)
	}
	file, err := os.OpenFile(logPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open log corruption fixture: %v", err)
	}
	first := []byte{0}
	if _, err := file.ReadAt(first, 0); err != nil {
		_ = file.Close()
		t.Fatalf("read log corruption byte: %v", err)
	}
	first[0] ^= 0xff
	if _, err := file.WriteAt(first, 0); err != nil {
		_ = file.Close()
		t.Fatalf("write log corruption byte: %v", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatalf("sync log corruption: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close log corruption fixture: %v", err)
	}
	_, err = service.LogsAfter(context.Background(), v1.RequestContext{RequestID: "request-log-corrupt"}, v1.ListLogsRequest{
		ContainerID: string(identity.ContainerID), AttemptID: string(identity.AttemptID), Limit: 10,
	})
	var apiError *v1.Error
	if !errors.As(err, &apiError) || apiError.Code != v1.CodeInternal || strings.Contains(apiError.Message, root) || strings.Contains(apiError.Message, "workload.log") {
		t.Fatalf("corruption API error = %#v, want bounded internal response", err)
	}
}

// TestCreateContainerRegistersProductionLogOwner verifies the successful adapter response publishes the derived create owner before returning.
func TestCreateContainerRegistersProductionLogOwner(t *testing.T) {
	coordinator, pair := seededCoordinator(t)
	operationValue, err := coordinator.GetOperation(context.Background(), "op-container-create")
	if err != nil {
		t.Fatalf("GetOperation() error = %v", err)
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("chmod runtime root: %v", err)
	}
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	owner, err := ownership.NewOwnerKey("op-container-create", operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)}, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("construct retained create owner: %v", err)
	}
	mutator := &successfulCreateMutator{
		recordingMutator: &recordingMutator{},
		result: lifecycle.ContainerResult{
			Operation: operationValue, ContainerAttempt: &pair,
			HostBinding: &lifecycle.ContainerHostBinding{ContainerID: pair.Container.ID, AttemptID: pair.Attempt.ID, Generation: pair.Container.Status.Generation, Owner: owner},
		},
	}
	service, err := newService(mutator, coordinator, registry)
	if err != nil {
		t.Fatalf("newService() error = %v", err)
	}
	_, err = service.CreateContainer(context.Background(), v1.RequestContext{RequestID: "request-create-log", OperationID: "op-container-create"}, v1.CreateContainerRequest{
		SandboxID: "sandbox-1", ContainerID: string(pair.Container.ID), AttemptID: string(pair.Attempt.ID),
		Process: v1.ProcessSpec{Argv: []string{"/bin/demo"}}, RootFS: "prepared-rootfs-1",
	})
	if err != nil {
		t.Fatalf("CreateContainer() error = %v", err)
	}
	identity := logstore.Identity{ContainerID: pair.Container.ID, AttemptID: pair.Attempt.ID}
	registry.mu.RLock()
	registered, found := registry.owners[identity]
	registry.mu.RUnlock()
	if !found || registered.OperationID != "op-container-create" || registered.Target.ID != string(pair.Container.ID) {
		t.Fatalf("registered owner = %#v, found = %v", registered, found)
	}
}

// TestCreateContainerNoopKeepsOriginalLogOwner verifies a new operation ID
// that reaches an immutable existing Container cannot redirect log discovery
// away from the owner that created the retained host inventory.
func TestCreateContainerNoopKeepsOriginalLogOwner(t *testing.T) {
	coordinator, pair := seededCoordinator(t)
	operationValue, err := coordinator.GetOperation(context.Background(), "op-container-create")
	if err != nil {
		t.Fatal(err)
	}
	root, owner, identity, _ := newRuntimeLogLocation(t, "op-container-create", string(pair.Container.ID), string(pair.Attempt.ID))
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	mutator := &successfulCreateMutator{
		recordingMutator: &recordingMutator{},
		result: lifecycle.ContainerResult{
			Operation: operationValue, ContainerAttempt: &pair,
			HostBinding: &lifecycle.ContainerHostBinding{ContainerID: pair.Container.ID, AttemptID: pair.Attempt.ID, Generation: pair.Container.Status.Generation, Owner: owner},
		},
	}
	service, err := newService(mutator, coordinator, registry)
	if err != nil {
		t.Fatal(err)
	}
	request := v1.CreateContainerRequest{
		SandboxID: "sandbox-1", ContainerID: string(pair.Container.ID), AttemptID: string(pair.Attempt.ID),
		Process: v1.ProcessSpec{Argv: []string{"/bin/demo"}}, RootFS: "prepared-rootfs-1",
	}
	for _, operationID := range []string{"op-container-create", "op-container-create-noop"} {
		if _, err := service.CreateContainer(context.Background(), v1.RequestContext{RequestID: "request-" + operationID, OperationID: operationID}, request); err != nil {
			t.Fatalf("CreateContainer(%s) error = %v", operationID, err)
		}
	}
	registry.mu.RLock()
	registered := registry.owners[identity]
	registry.mu.RUnlock()
	if registered != owner {
		t.Fatalf("registered owner=%#v, want retained create owner %#v", registered, owner)
	}
}

// TestCreateContainerReplayAfterDeletionDoesNotRepublishOldOwner verifies an
// exact create response replay is transport-only and cannot overwrite a later
// incarnation's already registered artifact owner.
func TestCreateContainerReplayAfterDeletionDoesNotRepublishOldOwner(t *testing.T) {
	coordinator, pair := seededCoordinator(t)
	operationValue, err := coordinator.GetOperation(context.Background(), "op-container-create")
	if err != nil {
		t.Fatal(err)
	}
	root, oldOwner, identity, _ := newRuntimeLogLocation(t, "op-container-create", string(pair.Container.ID), string(pair.Attempt.ID))
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	newOwner, err := ownership.NewOwnerKey("op-container-create-replacement", operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)}, domain.InitialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterAttempt(identity, newOwner); err != nil {
		t.Fatal(err)
	}
	mutator := &successfulCreateMutator{
		recordingMutator: &recordingMutator{},
		result: lifecycle.ContainerResult{
			Resolution: operation.ResolutionReplay, Operation: operationValue, ContainerAttempt: &pair,
			HostBinding: &lifecycle.ContainerHostBinding{ContainerID: pair.Container.ID, AttemptID: pair.Attempt.ID, Generation: pair.Container.Status.Generation, Owner: oldOwner},
		},
	}
	service, err := newService(mutator, coordinator, registry)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.CreateContainer(context.Background(), v1.RequestContext{RequestID: "request-replay-old-create", OperationID: string(operationValue.ID)}, v1.CreateContainerRequest{
		SandboxID: "sandbox-1", ContainerID: string(pair.Container.ID), AttemptID: string(pair.Attempt.ID),
		Process: v1.ProcessSpec{Argv: []string{"/bin/demo"}}, RootFS: "prepared-rootfs-1",
	})
	if err != nil {
		t.Fatalf("CreateContainer(replay) error = %v", err)
	}
	registry.mu.RLock()
	retained := registry.owners[identity]
	registry.mu.RUnlock()
	if retained != newOwner {
		t.Fatalf("create replay owner = %#v, want replacement %#v", retained, newOwner)
	}
}

// TestDeleteContainerUnregistersLogsAcrossResponseReplay verifies successful Delete bounds registry memory and permits exact identity reuse even when the response is retried.
func TestDeleteContainerUnregistersLogsAcrossResponseReplay(t *testing.T) {
	coordinator, pair := seededCoordinator(t)
	root, oldOwner, identity, _ := newRuntimeLogLocation(t, "op-container-create", string(pair.Container.ID), string(pair.Attempt.ID))
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	if err := registry.RegisterAttempt(identity, oldOwner); err != nil {
		t.Fatalf("RegisterAttempt(old) error = %v", err)
	}
	deleteOperation := operation.Operation{
		SchemaVersion: operation.SchemaVersion,
		ID:            "op-container-delete-log",
		Type:          operation.TypeDelete,
		Target:        operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)},
		Fingerprint: operation.RequestFingerprint{
			Version: operation.CurrentFingerprintVersion,
			SHA256:  strings.Repeat("d", 64),
		},
		State:    operation.StateSucceeded,
		Stage:    operation.StageComplete,
		Result:   operation.ResultSucceeded,
		Reason:   operation.ReasonNone,
		Response: []byte(`{"removed":true}`),
	}
	if err := deleteOperation.Validate(); err != nil {
		t.Fatalf("delete operation fixture invalid: %v", err)
	}
	mutator := &successfulDeleteMutator{
		recordingMutator: &recordingMutator{},
		result: lifecycle.ContainerResult{
			Operation: deleteOperation, Removed: true,
			HostBinding: &lifecycle.ContainerHostBinding{ContainerID: identity.ContainerID, AttemptID: identity.AttemptID, Generation: pair.Container.Status.Generation, Owner: oldOwner},
		},
	}
	service, err := newService(mutator, coordinator, registry)
	if err != nil {
		t.Fatalf("newService() error = %v", err)
	}
	requestContext := v1.RequestContext{RequestID: "request-delete-log", OperationID: string(deleteOperation.ID)}
	request := v1.DeleteContainerRequest{ContainerID: string(pair.Container.ID)}
	if _, err := service.DeleteContainer(context.Background(), requestContext, request); err != nil {
		t.Fatalf("DeleteContainer(first) error = %v", err)
	}
	registry.mu.RLock()
	ownerCount := len(registry.owners)
	sourceCount := len(registry.sources)
	registry.mu.RUnlock()
	if ownerCount != 0 || sourceCount != 0 || mutator.calls != 1 {
		t.Fatalf("post-delete registry = owners:%d sources:%d calls:%d", ownerCount, sourceCount, mutator.calls)
	}
	newOwner, err := ownership.NewOwnerKey("op-container-create-reused", operation.Target{Kind: operation.TargetContainer, ID: string(pair.Container.ID)}, pair.Container.Status.Generation)
	if err != nil {
		t.Fatalf("NewOwnerKey(reused) error = %v", err)
	}
	if err := registry.RegisterAttempt(identity, newOwner); err != nil {
		t.Fatalf("RegisterAttempt(reused identity) error = %v", err)
	}
	if _, err := service.DeleteContainer(context.Background(), requestContext, request); err != nil {
		t.Fatalf("DeleteContainer(old replay) error = %v", err)
	}
	registry.mu.RLock()
	retained, retainedFound := registry.owners[identity]
	registry.mu.RUnlock()
	if !retainedFound || retained != newOwner {
		t.Fatalf("old delete replay removed replacement owner: %#v, found=%t", retained, retainedFound)
	}
}

// TestLogRegistryDeleteEpochRejectsStaleDiscovery verifies a filesystem scan begun before Delete cannot republish the removed Attempt afterward.
func TestLogRegistryDeleteEpochRejectsStaleDiscovery(t *testing.T) {
	root, oldOwner, identity, logPath := newRuntimeLogLocation(t, "op-container-create-stale", "container-stale", "attempt-stale")
	writeRuntimeLogConfig(t, root, oldOwner, identity, logPath)
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	epoch := registry.discoveryEpoch(identity)
	scanReady := make(chan ownership.OwnerKey, 1)
	releasePublish := make(chan struct{})
	publishResult := make(chan error, 1)
	go func() {
		discovered, discoverErr := registry.discoverOwner(identity)
		if discoverErr != nil {
			publishResult <- discoverErr
			return
		}
		scanReady <- discovered
		<-releasePublish
		publishResult <- registry.registerDiscoveredAttempt(identity, discovered, epoch)
	}()
	discovered := <-scanReady
	if discovered != oldOwner {
		t.Fatalf("discovered owner = %#v, want %#v", discovered, oldOwner)
	}
	registration, found, err := registry.CaptureRegistration(identity)
	if err != nil || found {
		t.Fatalf("CaptureRegistration() = (%#v, %t, %v), want absent binding token", registration, found, err)
	}
	if err := registry.UnregisterRegistration(registration); err != nil {
		t.Fatalf("UnregisterRegistration() error = %v", err)
	}
	close(releasePublish)
	if err := <-publishResult; !errors.Is(err, ErrLogNotFound) {
		t.Fatalf("stale discovery publication error = %v, want ErrLogNotFound", err)
	}
	if _, found, err := registry.CaptureRegistration(identity); err != nil || found {
		t.Fatalf("post-delete CaptureRegistration() = (_, %t, %v), want no binding", found, err)
	}

	newOwner, err := ownership.NewOwnerKey("op-container-create-new", operation.Target{Kind: operation.TargetContainer, ID: string(identity.ContainerID)}, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("NewOwnerKey(new) error = %v", err)
	}
	if err := registry.RegisterAttempt(identity, newOwner); err != nil {
		t.Fatalf("RegisterAttempt(new) error = %v", err)
	}
	newRegistration, found, err := registry.CaptureRegistration(identity)
	if err != nil || !found || newRegistration.revision == 0 {
		t.Fatalf("new CaptureRegistration() = (%#v, %t, %v)", newRegistration, found, err)
	}
}

// TestLogRegistryDeleteEpochPreservesLaterDirectRegistration verifies Delete finalization cannot erase a new create that registered after the capture point.
func TestLogRegistryDeleteEpochPreservesLaterDirectRegistration(t *testing.T) {
	root, _, identity, _ := newRuntimeLogLocation(t, "op-container-create-old", "container-reused", "attempt-reused")
	registry, err := NewRuntimeLogRegistry(root)
	if err != nil {
		t.Fatalf("NewRuntimeLogRegistry() error = %v", err)
	}
	registration, found, err := registry.CaptureRegistration(identity)
	if err != nil || found {
		t.Fatalf("CaptureRegistration() = (%#v, %t, %v), want absent binding token", registration, found, err)
	}
	newOwner, err := ownership.NewOwnerKey("op-container-create-replacement", operation.Target{Kind: operation.TargetContainer, ID: string(identity.ContainerID)}, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("NewOwnerKey(replacement) error = %v", err)
	}
	if err := registry.RegisterAttempt(identity, newOwner); err != nil {
		t.Fatalf("RegisterAttempt(replacement) error = %v", err)
	}
	if err := registry.UnregisterRegistration(registration); err != nil {
		t.Fatalf("UnregisterRegistration(stale capture) error = %v", err)
	}
	registry.mu.RLock()
	retained, retainedFound := registry.owners[identity]
	registry.mu.RUnlock()
	if !retainedFound || retained != newOwner {
		t.Fatalf("replacement owner = %#v, found = %t; want preserved %#v", retained, retainedFound, newOwner)
	}
}

// newRuntimeLogLocation creates the private deterministic owner directory used by pure file-backed daemon tests.
func newRuntimeLogLocation(t *testing.T, operationID, containerID, attemptID string) (string, ownership.OwnerKey, logstore.Identity, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("chmod runtime root: %v", err)
	}
	owner, err := ownership.NewOwnerKey(operation.OperationID(operationID), operation.Target{Kind: operation.TargetContainer, ID: containerID}, domain.InitialGeneration)
	if err != nil {
		t.Fatalf("NewOwnerKey() error = %v", err)
	}
	ownerRoot := filepath.Join(root, "owners", owner.Token)
	if err := os.MkdirAll(ownerRoot, 0o700); err != nil {
		t.Fatalf("create owner directory: %v", err)
	}
	if err := os.Chmod(filepath.Join(root, "owners"), 0o700); err != nil {
		t.Fatalf("chmod owners directory: %v", err)
	}
	if err := os.Chmod(ownerRoot, 0o700); err != nil {
		t.Fatalf("chmod owner directory: %v", err)
	}
	identity := logstore.Identity{ContainerID: domain.ContainerID(containerID), AttemptID: domain.AttemptID(attemptID)}
	return root, owner, identity, filepath.Join(ownerRoot, "workload.log")
}

// writeRuntimeLogConfig writes one strict private init registration, optionally with a deliberately mismatched log path.
func writeRuntimeLogConfig(t *testing.T, root string, owner ownership.OwnerKey, identity logstore.Identity, logPath string) {
	t.Helper()
	ownerRoot := filepath.Join(root, "owners", owner.Token)
	config := shim.RuntimeConfig{
		SchemaVersion:   shim.SchemaVersion,
		Mode:            shim.ModeInit,
		Owner:           owner,
		SandboxID:       "sandbox-log",
		ContainerID:     identity.ContainerID,
		AttemptID:       identity.AttemptID,
		WrapperEvidence: strings.Repeat("a", 64),
		ControlSocket:   filepath.Join(ownerRoot, "control.sock"),
		TerminalPath:    filepath.Join(ownerRoot, "terminal.json"),
		LogPath:         logPath,
		Process:         domain.ProcessSpec{Argv: []string{"/bin/demo"}},
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("runtime log config fixture invalid: %v", err)
	}
	payload, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal runtime log config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ownerRoot, "shim.json"), payload, 0o600); err != nil {
		t.Fatalf("write runtime log config: %v", err)
	}
}
