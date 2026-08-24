package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/pkg/client"
)

type fakeClock struct {
	now time.Time
}

type fakeEvaluationClient struct {
	calls             []string
	events            []v1.Event
	failPhase         string
	failures          map[string]v1.ErrorCode
	omitEventDuration bool
}

type sequentialIDs struct {
	counts map[string]int
}

// Now advances deterministically so duration tests assert clock semantics without millisecond thresholds.
func (clock *fakeClock) Now() time.Time {
	value := clock.now
	clock.now = clock.now.Add(time.Millisecond)
	return value
}

// Next returns a stable path-safe identity for each semantic resource prefix.
func (source *sequentialIDs) Next(prefix string) (string, error) {
	if source.counts == nil {
		source.counts = make(map[string]int)
	}
	source.counts[prefix]++
	return fmt.Sprintf("%s-%d", prefix, source.counts[prefix]), nil
}

// CreateSandbox records a fake public API call and one correlated stage event.
func (fake *fakeEvaluationClient) CreateSandbox(_ context.Context, operationID string, request v1.CreateSandboxRequest) (v1.SandboxResponse, error) {
	err := fake.complete("sandbox.create", operationID)
	return v1.SandboxResponse{Sandbox: v1.Sandbox{ID: request.SandboxID, Status: v1.SandboxStatus{Phase: "ready"}}}, err
}

// StopSandbox records a fake public API call without changing host namespaces or cgroups.
func (fake *fakeEvaluationClient) StopSandbox(_ context.Context, operationID, sandboxID string) (v1.SandboxResponse, error) {
	err := fake.complete("sandbox.stop", operationID)
	return v1.SandboxResponse{Sandbox: v1.Sandbox{ID: sandboxID}}, err
}

// DeleteSandbox records a fake public API call and returns only its operation projection.
func (fake *fakeEvaluationClient) DeleteSandbox(_ context.Context, operationID, _ string) (v1.OperationResponse, error) {
	err := fake.complete("sandbox.delete", operationID)
	return v1.OperationResponse{Operation: v1.Operation{ID: operationID}}, err
}

// CreateContainer records a prepared-rootfs Attempt request without starting a process.
func (fake *fakeEvaluationClient) CreateContainer(_ context.Context, operationID, sandboxID string, request v1.CreateContainerRequest) (v1.ContainerResponse, error) {
	err := fake.complete("container.create", operationID)
	return v1.ContainerResponse{Container: v1.Container{ID: request.ContainerID, SandboxID: sandboxID, AttemptID: request.AttemptID, Status: v1.ContainerStatus{Phase: "created"}}}, err
}

// StartContainer records a fake gate release while spawning no child process.
func (fake *fakeEvaluationClient) StartContainer(_ context.Context, operationID, containerID string) (v1.ContainerResponse, error) {
	err := fake.complete("container.start", operationID)
	return v1.ContainerResponse{Container: v1.Container{ID: containerID, Status: v1.ContainerStatus{Phase: "running"}}}, err
}

// KillContainer records the explicit policy while sending no signal.
func (fake *fakeEvaluationClient) KillContainer(_ context.Context, operationID, containerID string, _ v1.TerminationPolicy) (v1.ContainerResponse, error) {
	err := fake.complete("container.kill", operationID)
	return v1.ContainerResponse{Container: v1.Container{ID: containerID}}, err
}

// DeleteContainer records fake verified teardown without touching mounts or metadata.
func (fake *fakeEvaluationClient) DeleteContainer(_ context.Context, operationID, _ string) (v1.OperationResponse, error) {
	err := fake.complete("container.delete", operationID)
	return v1.OperationResponse{Operation: v1.Operation{ID: operationID}}, err
}

// Events returns the fake daemon stage stream after the supplied opaque sequence as one canonical page.
func (fake *fakeEvaluationClient) Events(_ context.Context, after v1.ResumeToken, _ int) (v1.EventListResponse, error) {
	fake.calls = append(fake.calls, "events")
	sequence, err := v1.ParseResumeToken(after)
	if err != nil {
		return v1.EventListResponse{}, err
	}
	events := make([]v1.Event, 0, len(fake.events))
	for _, event := range fake.events {
		if event.Sequence > sequence {
			events = append(events, event)
		}
	}
	next := v1.ResumeToken("")
	if len(fake.events) > 0 {
		next = v1.NewResumeToken(fake.events[len(fake.events)-1].Sequence)
	}
	return v1.EventListResponse{Events: events, NextResumeToken: next}, nil
}

// complete appends deterministic success/failure evidence for one mutation call.
func (fake *fakeEvaluationClient) complete(phase, operationID string) error {
	fake.calls = append(fake.calls, phase)
	parts := strings.Split(phase, ".")
	operationType := parts[len(parts)-1]
	result := "succeeded"
	var err error
	failureCode := fake.failures[phase]
	if phase == fake.failPhase && failureCode == "" {
		failureCode = v1.CodeFailedPrecondition
	}
	if failureCode != "" {
		result = "failed"
		err = v1.NewError(failureCode, phase, "injected fake lifecycle failure")
	}
	var duration *int64
	if !fake.omitEventDuration {
		measured := int64(time.Millisecond)
		duration = &measured
	}
	fake.events = append(fake.events, v1.Event{
		Sequence:            uint64(len(fake.events) + 1),
		OperationID:         operationID,
		Type:                operationType,
		Target:              v1.ResourceRef{Kind: strings.Split(phase, ".")[0], ID: "fake-target"},
		Stage:               "complete",
		Result:              result,
		Reason:              "fake",
		OccurredAt:          time.Unix(1700000000, 0).UTC(),
		DurationNanoseconds: duration,
	})
	return err
}

// validScenario returns the smallest executable M3 prepared-rootfs+loopback scenario contract.
func validScenario(classification string, samples int) scenario {
	return scenario{
		SchemaVersion:  evaluationSchemaVersion,
		Name:           "prepared-rootfs-loopback-lifecycle",
		Version:        "v1",
		Classification: classification,
		Samples:        samples,
		Concurrency:    1,
		RootFS:         "prepared-rootfs-baseline-v1",
		Sandbox: v1.SandboxSpec{
			Network:   v1.NetworkIntent{Mode: "loopback"},
			Resources: v1.Resources{},
		},
		Process:    v1.ProcessSpec{Argv: []string{"/bin/sleep", "30"}},
		KillPolicy: v1.TerminationPolicy{Signal: "SIGTERM", GracePeriodNanoseconds: int64(time.Second), EscalationSignal: "SIGKILL"},
		Environment: scenarioEnvironment{
			ID:     "fake-linux-host",
			Labels: map[string]string{"purpose": "unit-test", "privilege": "none"},
			Cache: scenarioCacheState{
				Content:         "present-verified",
				UnpackedChain:   "present-verified",
				Snapshot:        preparedRootfsSnapshot,
				PageCache:       "unknown",
				ImmutableLayers: "reused",
			},
			Noise:  "none",
			Warmup: "none",
		},
	}
}

// fakeRecordedEnvironment returns complete deterministic evidence without reading procfs or executing host inspection commands.
func fakeRecordedEnvironment(input scenario, _ string, clock wallClock) recordedEnvironment {
	startedAt := clock.Now()
	return recordedEnvironment{
		ID:                    input.Environment.ID,
		Labels:                cloneLabels(input.Environment.Labels),
		GOOS:                  "linux",
		GOARCH:                "amd64",
		FilesystemProfile:     "prepared-rootfs",
		NetworkMode:           input.Sandbox.Network.Mode,
		ScenarioTag:           formatScenarioLabel(input),
		Commit:                "fake-commit",
		Build:                 buildEnvironment{GoVersion: "go-test", MainModule: "mydocker", MainVersion: "devel", MainChecksum: unknownEvidence, BuildMode: "test", Compiler: "gc", CGOEnabled: "0", BuildTags: unknownEvidence, RaceEnabled: unknownEvidence, Profiling: "disabled", VCS: "git", VCSRevision: "fake-commit", VCSTime: "fake-time", VCSModified: "true"},
		Worktree:              worktreeEnvironment{Root: "/fake/repo", Branch: "test", Status: "dirty", StatusSHA256: "fake-status", TrackedDiffSHA256: "fake-diff", UntrackedContentNote: "fake"},
		Kernel:                kernelEnvironment{Distribution: "fake-linux", Release: "fake-release", Version: "fake-version", BootParameters: "fake-boot"},
		Cgroup:                cgroupEnvironment{Mode: "v2", SelfPath: "/fake", EffectiveRoot: "/sys/fs/cgroup/fake", AvailableControllers: []string{"cpu", "memory"}, EnabledControllers: []string{"cpu", "memory"}},
		CPU:                   cpuEnvironment{Model: "fake-cpu", LogicalCPUs: 2, GOMAXPROCS: 2, Governor: "performance", TopologyNote: "fake"},
		Memory:                memoryEnvironment{HostCapacityBytes: "1024", CgroupLimitBytes: "512", CgroupCurrent: "128"},
		Storage:               storageEnvironment{ObservedPath: "/fake/result", MountPoint: "/fake", Filesystem: "tmpfs", Source: "tmpfs", MountOptions: "rw", DeviceModel: unknownEvidence},
		Cache:                 normalizeCacheState(input.Environment.Cache),
		Concurrency:           input.Concurrency,
		BackgroundNoise:       knownOrUnknown(input.Environment.Noise),
		Warmup:                knownOrUnknown(input.Environment.Warmup),
		ExperimentStartedAt:   startedAt,
		TimeZone:              "UTC",
		TimeZoneOffsetSeconds: 0,
	}
}

// encodeScenario serializes a typed test scenario for the same strict loader used by the command.
func encodeScenario(t *testing.T, input scenario) string {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal scenario: %v", err)
	}
	return string(payload)
}

// decodeRecords parses every raw JSONL line independently to verify stream recoverability.
func decodeRecords(t *testing.T, payload string) []rawRecord {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(payload), "\n")
	records := make([]rawRecord, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		var record rawRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode JSONL record %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

// runFakeScenario executes one scenario against fake transport dependencies and returns raw output plus call order.
func runFakeScenario(t *testing.T, input scenario, fake *fakeEvaluationClient) (int, string, string) {
	t.Helper()
	ids := &sequentialIDs{}
	clock := &fakeClock{now: time.Unix(1700000000, 0).UTC()}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := run(context.Background(), []string{"--scenario", "-", "--experiment-id", "experiment-one"}, strings.NewReader(encodeScenario(t, input)), &stdout, &stderr,
		func(client.Config) (evaluationClient, error) { return fake, nil }, ids.Next, clock, fakeRecordedEnvironment)
	return status, stdout.String(), stderr.String()
}

// TestWarmScenarioReusesSandboxAndEmitsRawJSONL verifies two Attempts share one setup Sandbox and every call/event retains tags.
func TestWarmScenarioReusesSandboxAndEmitsRawJSONL(t *testing.T) {
	fake := &fakeEvaluationClient{}
	status, stdout, stderr := runFakeScenario(t, validScenario("warm", 2), fake)
	if status != 0 {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	wantCalls := []string{
		"events",
		"sandbox.create",
		"container.create", "container.start", "container.kill", "container.delete",
		"container.create", "container.start", "container.kill", "container.delete",
		"sandbox.stop", "sandbox.delete", "events",
	}
	if !reflect.DeepEqual(fake.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", fake.calls, wantCalls)
	}
	records := decodeRecords(t, stdout)
	callerSpans := 0
	e2eSpans := 0
	stageEvents := 0
	for _, record := range records {
		if record.Environment.ScenarioTag != "prepared-rootfs+loopback/warm" || record.Classification != "warm" || record.Environment.Labels["privilege"] != "none" || record.Environment.Commit == "" || record.Environment.Cgroup.Mode == "" || record.Environment.Cache.PageCache == "" || record.Environment.Concurrency != 1 || record.Environment.TimeZone == "" {
			t.Fatalf("missing reproducibility tags: %#v", record)
		}
		switch record.RecordType {
		case "caller_span":
			callerSpans++
			if record.Phase == "warm.create_container_to_running" {
				e2eSpans++
				if len(record.OperationIDs) != 2 || record.DurationNanoseconds != 5*int64(time.Millisecond) {
					t.Fatalf("warm E2E correlation/duration = %#v/%d", record.OperationIDs, record.DurationNanoseconds)
				}
			} else if record.DurationNanoseconds != int64(time.Millisecond) {
				t.Fatalf("caller duration = %d, want deterministic monotonic delta", record.DurationNanoseconds)
			}
		case "stage_event":
			stageEvents++
			if record.Event == nil || record.Event.OperationID != record.OperationID {
				t.Fatalf("uncorrelated stage event: %#v", record)
			}
		}
	}
	if callerSpans != len(wantCalls)+2 || e2eSpans != 2 || stageEvents != len(wantCalls)-2 {
		t.Fatalf("caller/E2E/event counts = %d/%d/%d", callerSpans, e2eSpans, stageEvents)
	}
}

// TestEvaluatorPreservesMissingDaemonDuration verifies stage evidence remains unavailable instead of being normalized to a zero sample.
func TestEvaluatorPreservesMissingDaemonDuration(t *testing.T) {
	fake := &fakeEvaluationClient{omitEventDuration: true}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), fake)
	if status != 0 {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	found := false
	for _, record := range decodeRecords(t, stdout) {
		if record.RecordType != "stage_event" {
			continue
		}
		found = true
		if record.Event == nil || record.Event.DurationNanoseconds != nil {
			t.Fatalf("missing daemon duration became a sample: %#v", record.Event)
		}
	}
	if !found {
		t.Fatal("evaluator emitted no stage event fixture")
	}
}

// TestColdScenarioCreatesOneSandboxPerSample verifies cold samples never silently reuse Sandbox identity.
func TestColdScenarioCreatesOneSandboxPerSample(t *testing.T) {
	fake := &fakeEvaluationClient{}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 2), fake)
	if status != 0 {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	createIndices := make(map[int]bool)
	e2eIndices := make(map[int]bool)
	for _, record := range decodeRecords(t, stdout) {
		if record.RecordType == "caller_span" && record.Phase == "sandbox.create" {
			createIndices[record.SampleIndex] = true
			if record.Environment.ScenarioTag != "prepared-rootfs+loopback/cold" {
				t.Fatalf("cold tag = %q", record.Environment.ScenarioTag)
			}
		}
		if record.RecordType == "caller_span" && record.Phase == "cold.create_sandbox_to_running" {
			e2eIndices[record.SampleIndex] = true
			if len(record.OperationIDs) != 3 || record.DurationNanoseconds != 7*int64(time.Millisecond) {
				t.Fatalf("cold E2E correlation/duration = %#v/%d", record.OperationIDs, record.DurationNanoseconds)
			}
		}
	}
	if !reflect.DeepEqual(createIndices, map[int]bool{0: true, 1: true}) {
		t.Fatalf("cold Sandbox sample indices = %#v", createIndices)
	}
	if !reflect.DeepEqual(e2eIndices, map[int]bool{0: true, 1: true}) {
		t.Fatalf("cold E2E sample indices = %#v", e2eIndices)
	}
}

// TestLifecycleFailureIsRecordedAndKnownResourcesAreCleaned verifies raw failure evidence survives safe cleanup attempts.
func TestLifecycleFailureIsRecordedAndKnownResourcesAreCleaned(t *testing.T) {
	fake := &fakeEvaluationClient{failPhase: "container.create"}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), fake)
	if status != v1.ExitStatus(v1.CodeFailedPrecondition) {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	wantCalls := []string{"events", "sandbox.create", "container.create", "container.delete", "sandbox.stop", "sandbox.delete", "events"}
	if !reflect.DeepEqual(fake.calls, wantCalls) {
		t.Fatalf("calls = %#v, want safe known-resource cleanup %#v", fake.calls, wantCalls)
	}
	foundFailure := false
	for _, record := range decodeRecords(t, stdout) {
		if record.RecordType == "caller_span" && record.Phase == "container.create" {
			foundFailure = record.Error != nil && record.Error.Code == v1.CodeFailedPrecondition && !record.Success
		}
	}
	if !foundFailure {
		t.Fatal("raw JSONL omitted the injected lifecycle failure")
	}
}

// TestSandboxCreateResponseLossStillCleansKnownID verifies an ambiguous create response cannot suppress stop/delete attempts for the preselected Sandbox ID.
func TestSandboxCreateResponseLossStillCleansKnownID(t *testing.T) {
	fake := &fakeEvaluationClient{failures: map[string]v1.ErrorCode{"sandbox.create": v1.CodeUnavailable}}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), fake)
	if status != v1.ExitStatus(v1.CodeUnavailable) {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	wantCalls := []string{"events", "sandbox.create", "sandbox.stop", "sandbox.delete", "events"}
	if !reflect.DeepEqual(fake.calls, wantCalls) {
		t.Fatalf("calls = %#v, want ambiguous Sandbox cleanup %#v", fake.calls, wantCalls)
	}
	foundE2EFailure := false
	for _, record := range decodeRecords(t, stdout) {
		if record.Phase == "cold.create_sandbox_to_running" {
			foundE2EFailure = !record.Success && record.Error != nil && record.Error.Code == v1.CodeUnavailable && len(record.OperationIDs) == 1
		}
	}
	if !foundE2EFailure {
		t.Fatal("ambiguous Sandbox create omitted its failed E2E evidence")
	}
}

// TestAmbiguousContainerCreateRecordsIncompleteCleanup verifies cleanup is still attempted and its own conflict remains explicit when create may be active.
func TestAmbiguousContainerCreateRecordsIncompleteCleanup(t *testing.T) {
	fake := &fakeEvaluationClient{failures: map[string]v1.ErrorCode{
		"container.create": v1.CodeUnavailable,
		"container.delete": v1.CodeFailedPrecondition,
	}}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), fake)
	if status != v1.ExitStatus(v1.CodeUnavailable) {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	wantCalls := []string{"events", "sandbox.create", "container.create", "container.delete", "sandbox.stop", "sandbox.delete", "events"}
	if !reflect.DeepEqual(fake.calls, wantCalls) {
		t.Fatalf("calls = %#v, want ambiguous Container cleanup %#v", fake.calls, wantCalls)
	}
	foundCleanupFailure := false
	for _, record := range decodeRecords(t, stdout) {
		if record.RecordType == "caller_span" && record.Phase == "container.delete" {
			foundCleanupFailure = !record.Success && record.Error != nil && record.Error.Code == v1.CodeFailedPrecondition
		}
	}
	if !foundCleanupFailure {
		t.Fatal("raw evidence did not preserve incomplete cleanup after the ambiguous create")
	}
}

// TestOutputFinalizeReportsSyncAndCloseFailures verifies neither durability failure is swallowed and close still runs after sync fails.
func TestOutputFinalizeReportsSyncAndCloseFailures(t *testing.T) {
	syncFailure := errors.New("injected sync failure")
	closeFailure := errors.New("injected close failure")
	closeCalled := false
	destination := outputDestination{
		writer: io.Discard,
		sync:   func() error { return syncFailure },
		close: func() error {
			closeCalled = true
			return closeFailure
		},
	}
	err := destination.Finalize()
	if !closeCalled || !errors.Is(err, syncFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("Finalize close/errors = %v/%v", closeCalled, err)
	}
}

// TestEvidenceParsersUseExplicitUnknown verifies missing host/cache facts never collapse to empty JSON values.
func TestEvidenceParsersUseExplicitUnknown(t *testing.T) {
	cache := normalizeCacheState(scenarioCacheState{})
	if cache.Content != unknownEvidence || cache.PageCache != unknownEvidence || parseMemoryCapacity("") != unknownEvidence || parseUnifiedCgroupPath("") != unknownEvidence {
		t.Fatalf("unknown normalization = %#v", cache)
	}
	mount := parseMountInfo("", "/result")
	if mount.Filesystem != unknownEvidence || mount.Source != unknownEvidence {
		t.Fatalf("unknown mount evidence = %#v", mount)
	}
}

// TestInvalidScenarioFailsBeforeClientConstruction verifies unsupported network scope cannot touch a daemon transport.
func TestInvalidScenarioFailsBeforeClientConstruction(t *testing.T) {
	input := validScenario("cold", 1)
	input.Sandbox.Network.Mode = "bridge"
	factoryCalled := false
	var stderr bytes.Buffer
	status := run(context.Background(), []string{"--scenario", "-"}, strings.NewReader(encodeScenario(t, input)), io.Discard, &stderr,
		func(client.Config) (evaluationClient, error) {
			factoryCalled = true
			return nil, errors.New("must not be called")
		}, (&sequentialIDs{}).Next, &fakeClock{now: time.Unix(1700000000, 0).UTC()}, fakeRecordedEnvironment)
	if status != 2 || factoryCalled {
		t.Fatalf("status/factory = %d/%v, stderr = %s", status, factoryCalled, stderr.String())
	}
}

// TestScenarioRejectsInventedSnapshotEvidence verifies the M3 evaluator cannot
// sign a per-Attempt snapshot claim while its provider uses one shared prepared rootfs.
func TestScenarioRejectsInventedSnapshotEvidence(t *testing.T) {
	input := validScenario("cold", 1)
	input.Environment.Cache.Snapshot = "new-per-attempt"
	if err := input.Validate(); err == nil {
		t.Fatal("scenario with invented snapshot evidence was accepted")
	}
}

// TestStrictScenarioRejectsUnknownFields verifies future schema fields require an explicit evaluator version update.
func TestStrictScenarioRejectsUnknownFields(t *testing.T) {
	payload := strings.TrimSuffix(encodeScenario(t, validScenario("cold", 1)), "}") + `,"future":true}`
	status := run(context.Background(), []string{"--scenario", "-"}, strings.NewReader(payload), io.Discard, io.Discard,
		func(client.Config) (evaluationClient, error) { return &fakeEvaluationClient{}, nil },
		(&sequentialIDs{}).Next, &fakeClock{now: time.Unix(1700000000, 0).UTC()}, fakeRecordedEnvironment)
	if status != 2 {
		t.Fatalf("status = %d, want invalid scenario schema", status)
	}
}

// TestRandomIdentitySatisfiesPublicContract verifies real evaluation IDs are distinct and path safe.
func TestRandomIdentitySatisfiesPublicContract(t *testing.T) {
	first, err := randomIdentity("sandbox")
	if err != nil {
		t.Fatalf("randomIdentity: %v", err)
	}
	second, err := randomIdentity("sandbox")
	if err != nil {
		t.Fatalf("randomIdentity second: %v", err)
	}
	if first == second {
		t.Fatal("random evaluation identities unexpectedly match")
	}
	if err := v1.ValidateResourceID("sandbox_id", first); err != nil {
		t.Fatalf("generated resource identity: %v", err)
	}
}

// TestCheckedInScenariosValidate verifies both cold and warm examples remain executable inputs for this schema version.
func TestCheckedInScenariosValidate(t *testing.T) {
	paths := []string{
		filepath.Join("..", "..", "scenarios", "prepared-rootfs-loopback-cold.json"),
		filepath.Join("..", "..", "scenarios", "prepared-rootfs-loopback-warm.json"),
	}
	for _, path := range paths {
		loaded, err := loadScenario(path, strings.NewReader(""))
		if err != nil {
			t.Fatalf("load %s: %v", path, err)
		}
		if err := loaded.Validate(); err != nil {
			t.Fatalf("validate %s: %v", path, err)
		}
	}
}
