package main

import (
	"bytes"
	"context"
	"crypto/sha256"
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

type stepClock struct {
	now  time.Time
	step time.Duration
}

type fakeEvaluationClient struct {
	calls              []string
	info               v1.InfoResponse
	infoErr            error
	events             []v1.Event
	failPhase          string
	failures           map[string]v1.ErrorCode
	omitEventDuration  bool
	emitPending        bool
	omitCompletePhase  string
	nonSuccessComplete string
	duplicateComplete  string
}

// Info records the evaluator's mandatory preflight read and returns an immutable fake daemon identity.
func (fake *fakeEvaluationClient) Info(context.Context) (v1.InfoResponse, error) {
	fake.calls = append(fake.calls, "info")
	if fake.infoErr != nil {
		return v1.InfoResponse{}, fake.infoErr
	}
	if fake.info.DaemonBuild.Source != "" {
		return fake.info, nil
	}
	modified := false
	return v1.InfoResponse{DaemonBuild: v1.DaemonBuildIdentity{
		Source: v1.DaemonBuildIdentitySource, GoVersion: "go-test", VCSRevision: "fake-commit", VCSModified: &modified,
	}}, nil
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

// Now returns the current scripted boundary and advances by a caller-selected
// step, including zero or negative steps used to test invalid clock evidence.
func (clock *stepClock) Now() time.Time {
	value := clock.now
	clock.now = clock.now.Add(clock.step)
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
	if fake.nonSuccessComplete == phase {
		result = "failed"
	}
	var duration *int64
	if !fake.omitEventDuration {
		measured := int64(time.Millisecond)
		duration = &measured
	}
	if fake.emitPending {
		fake.events = append(fake.events, v1.Event{
			Sequence:    uint64(len(fake.events) + 1),
			OperationID: operationID,
			Type:        operationType,
			Target:      v1.ResourceRef{Kind: strings.Split(phase, ".")[0], ID: "fake-target"},
			Stage:       "provider.apply",
			Result:      "pending",
			Reason:      "fake",
			OccurredAt:  time.Unix(1700000000, 0).UTC(),
		})
	}
	if fake.omitCompletePhase == phase {
		return err
	}
	completeEvent := v1.Event{
		Sequence:            uint64(len(fake.events) + 1),
		OperationID:         operationID,
		Type:                operationType,
		Target:              v1.ResourceRef{Kind: strings.Split(phase, ".")[0], ID: "fake-target"},
		Stage:               "complete",
		Result:              result,
		Reason:              "fake",
		OccurredAt:          time.Unix(1700000000, 0).UTC(),
		DurationNanoseconds: duration,
	}
	fake.events = append(fake.events, completeEvent)
	if fake.duplicateComplete == phase {
		completeEvent.Sequence = uint64(len(fake.events) + 1)
		fake.events = append(fake.events, completeEvent)
	}
	return err
}

// validScenario returns the smallest executable M3 prepared-rootfs+loopback scenario contract.
func validScenario(classification string, samples int) scenario {
	input := scenario{
		SchemaVersion:  scenarioSchemaVersion,
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
	if classification == "warm" {
		input.WarmupAttempts = 1
	}
	return input
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
		EvaluatorBuildCommit:  "fake-commit",
		EvaluatorBuild:        buildEnvironment{GoVersion: "go-test", MainModule: "mydocker", MainVersion: "devel", MainChecksum: unknownEvidence, BuildMode: "test", Compiler: "gc", CGOEnabled: "0", BuildTags: unknownEvidence, RaceEnabled: unknownEvidence, Profiling: "disabled", VCS: "git", VCSRevision: "fake-commit", VCSTime: "fake-time", VCSModified: "false"},
		OperatorWorktree:      worktreeEnvironment{Root: "/fake/repo", Head: "operator-context-head", Branch: "test", Status: "dirty", StatusSHA256: "fake-status", TrackedDiffSHA256: "fake-diff", UntrackedContentNote: "fake"},
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

// runFakeScenario executes one scenario with a deterministic increasing clock
// and returns the process status plus both output streams.
func runFakeScenario(t *testing.T, input scenario, fake *fakeEvaluationClient) (int, string, string) {
	t.Helper()
	return runFakeScenarioWithClock(t, input, fake, &fakeClock{now: time.Unix(1700000000, 0).UTC()})
}

// runFakeScenarioWithClock injects an exact wall/monotonic boundary sequence
// without accessing a daemon, host process, namespace, cgroup, or mount.
func runFakeScenarioWithClock(t *testing.T, input scenario, fake *fakeEvaluationClient, clock wallClock) (int, string, string) {
	t.Helper()
	ids := &sequentialIDs{}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	status := run(context.Background(), []string{"--scenario", "-", "--experiment-id", "experiment-one"}, strings.NewReader(encodeScenario(t, input)), &stdout, &stderr,
		func(client.Config) (evaluationClient, error) { return fake, nil }, ids.Next, clock, fakeRecordedEnvironment)
	return status, stdout.String(), stderr.String()
}

// TestEvaluatorReadsDaemonInfoBeforeLifecycleSideEffects verifies an unavailable info endpoint stops before event or mutation calls.
func TestEvaluatorReadsDaemonInfoBeforeLifecycleSideEffects(t *testing.T) {
	fake := &fakeEvaluationClient{infoErr: v1.NewError(v1.CodeUnavailable, "daemon_info", "injected info failure")}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), fake)
	if status != v1.ExitStatus(v1.CodeUnavailable) {
		t.Fatalf("status = %d, want unavailable; stderr = %s", status, stderr)
	}
	if stdout != "" {
		t.Fatalf("preflight failure wrote raw evidence without a daemon identity: %q", stdout)
	}
	if !reflect.DeepEqual(fake.calls, []string{"info"}) {
		t.Fatalf("calls = %#v, want only the read-only info preflight", fake.calls)
	}
}

// TestBaselineEligibilityUsesMatchingCleanBinaryRevisions verifies build identity qualification ignores optional sums and tags.
func TestBaselineEligibilityUsesMatchingCleanBinaryRevisions(t *testing.T) {
	input := validScenario("cold", 1)
	input.Environment.Cache.PageCache = "dropped-verified"
	status, stdout, stderr := runFakeScenario(t, input, &fakeEvaluationClient{})
	if status != 0 {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	records := decodeRecords(t, stdout)
	summary := records[len(records)-1].Summary
	if summary == nil || !summary.BaselineEligible || summary.EvidenceQuality != "baseline" || len(summary.IneligibilityReasons) != 0 {
		t.Fatalf("matching clean build summary = %#v", summary)
	}
}

// TestBaselineIneligibilityReasonsBoundBuildConflicts verifies revision and dirty-build failures use stable low-cardinality reasons.
func TestBaselineIneligibilityReasonsBoundBuildConflicts(t *testing.T) {
	input := validScenario("cold", 1)
	input.Environment.Cache.PageCache = "dropped-verified"
	base := fakeRecordedEnvironment(input, "-", &fakeClock{now: time.Unix(1700000000, 0).UTC()})
	clean := false
	dirty := true
	tests := []struct {
		name      string
		daemon    v1.DaemonBuildIdentity
		evaluator buildEnvironment
		want      []string
	}{
		{
			name:      "revision mismatch",
			daemon:    v1.DaemonBuildIdentity{Source: v1.DaemonBuildIdentitySource, VCSRevision: "other-commit", VCSModified: &clean},
			evaluator: base.EvaluatorBuild,
			want:      []string{"daemon_evaluator_revision_mismatch"},
		},
		{
			name:      "daemon modified",
			daemon:    v1.DaemonBuildIdentity{Source: v1.DaemonBuildIdentitySource, VCSRevision: "fake-commit", VCSModified: &dirty},
			evaluator: base.EvaluatorBuild,
			want:      []string{"daemon_build_modified"},
		},
		{
			name:   "evaluator modified",
			daemon: v1.DaemonBuildIdentity{Source: v1.DaemonBuildIdentitySource, VCSRevision: "fake-commit", VCSModified: &clean},
			evaluator: func() buildEnvironment {
				build := base.EvaluatorBuild
				build.VCSModified = "true"
				return build
			}(),
			want: []string{"evaluator_build_modified"},
		},
		{
			name:      "daemon unavailable",
			daemon:    v1.DaemonBuildIdentity{Source: v1.DaemonBuildIdentitySource, Unavailable: true, UnavailableReason: v1.DaemonBuildUnavailableRevision},
			evaluator: base.EvaluatorBuild,
			want:      []string{"daemon_build_identity_unavailable"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := base
			environment.DaemonBuild = test.daemon
			environment.EvaluatorBuild = test.evaluator
			runner := evaluator{scenario: input, environment: environment}
			if reasons := runner.baselineIneligibilityReasons(); !reflect.DeepEqual(reasons, test.want) {
				t.Fatalf("reasons = %#v, want %#v", reasons, test.want)
			}
		})
	}
}

// TestScenarioEnvironmentPlaceholderGateCoversAllDeclaredEvidence verifies non-cache placeholders cannot become baseline eligible.
func TestScenarioEnvironmentPlaceholderGateCoversAllDeclaredEvidence(t *testing.T) {
	base := validScenario("cold", 1)
	base.Environment.Cache.PageCache = "dropped-verified"
	tests := []struct {
		name   string
		mutate func(*scenario)
	}{
		{name: "environment id", mutate: func(input *scenario) { input.Environment.ID = "replace-with-host" }},
		{name: "background noise", mutate: func(input *scenario) { input.Environment.Noise = unknownEvidence }},
		{name: "warmup vocabulary", mutate: func(input *scenario) { input.Environment.Warmup = "replace-before-run" }},
		{name: "label value", mutate: func(input *scenario) { input.Environment.Labels["privilege"] = unknownEvidence }},
		{name: "label key", mutate: func(input *scenario) { input.Environment.Labels["replace-label"] = "known" }},
	}
	if scenarioEnvironmentHasPlaceholder(base) {
		t.Fatal("fully specified cold environment was classified as a placeholder")
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := base
			input.Environment.Labels = cloneLabels(base.Environment.Labels)
			test.mutate(&input)
			if !scenarioEnvironmentHasPlaceholder(input) {
				t.Fatalf("scenario environment placeholder was accepted: %#v", input.Environment)
			}
		})
	}
	warm := validScenario("warm", 1)
	warm.Environment.Cache.PageCache = "warm-verified"
	warm.Environment.Warmup = "one-complete-unmeasured-attempt-before-formal-samples"
	if scenarioEnvironmentHasPlaceholder(warm) {
		t.Fatal("explicit one-attempt warmup vocabulary was classified as a placeholder")
	}
}

// TestBaselineEligibilityHonorsExplicitResultKind verifies a signed scenario
// cannot contradict the machine-readable summary by retaining its debug-only
// declaration after every factual placeholder has been resolved.
func TestBaselineEligibilityHonorsExplicitResultKind(t *testing.T) {
	input := validScenario("cold", 1)
	input.Environment.Cache.PageCache = "dropped-verified"
	input.Environment.Labels["result_kind"] = "raw-debug-evidence"
	clean := false
	runner := evaluator{
		scenario: input,
		environment: recordedEnvironment{
			DaemonBuild: v1.DaemonBuildIdentity{
				Source: v1.DaemonBuildIdentitySource, VCSRevision: "revision-one", VCSModified: &clean,
			},
			EvaluatorBuild: buildEnvironment{VCSRevision: "revision-one", VCSModified: "false"},
		},
	}
	if reasons := runner.baselineIneligibilityReasons(); !reflect.DeepEqual(reasons, []string{"scenario_declares_debug_evidence"}) {
		t.Fatalf("debug result-kind reasons = %#v", reasons)
	}
	input.Environment.Labels["result_kind"] = "raw-baseline-evidence"
	runner.scenario = input
	if reasons := runner.baselineIneligibilityReasons(); len(reasons) != 0 {
		t.Fatalf("baseline result-kind reasons = %#v", reasons)
	}
}

// TestWarmScenarioReusesSandboxAndEmitsRawJSONL verifies one complete warmup
// Attempt precedes the single formal Attempt in the same setup Sandbox.
func TestWarmScenarioReusesSandboxAndEmitsRawJSONL(t *testing.T) {
	fake := &fakeEvaluationClient{}
	status, stdout, stderr := runFakeScenario(t, validScenario("warm", 1), fake)
	if status != 0 {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	wantCalls := []string{
		"info",
		"events",
		"sandbox.create", "events",
		"container.create", "container.start", "container.kill", "container.delete", "events",
		"container.create", "container.start", "container.kill", "container.delete",
		"events", "sandbox.stop", "sandbox.delete", "events", "events",
	}
	if !reflect.DeepEqual(fake.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", fake.calls, wantCalls)
	}
	records := decodeRecords(t, stdout)
	callerSpans := 0
	e2eSpans := 0
	warmupSpans := 0
	stageEvents := 0
	for _, record := range records {
		if record.Environment.ScenarioTag != "prepared-rootfs+loopback/warm" || record.Classification != "warm" && record.Classification != "warmup" || record.Environment.Labels["privilege"] != "none" || record.Environment.DaemonBuild.Source != v1.DaemonBuildIdentitySource || record.Environment.DaemonBuild.VCSRevision != "fake-commit" || record.Environment.EvaluatorBuildCommit == "" || record.Environment.Cgroup.Mode == "" || record.Environment.Cache.PageCache == "" || record.Environment.Concurrency != 1 || record.Environment.TimeZone == "" {
			t.Fatalf("missing reproducibility tags: %#v", record)
		}
		switch record.RecordType {
		case "caller_span":
			callerSpans++
			if record.Phase == "warm.create_container_to_running" {
				e2eSpans++
				if len(record.OperationIDs) != 2 || record.DurationNanoseconds == nil || *record.DurationNanoseconds != 5*int64(time.Millisecond) {
					t.Fatalf("warm E2E correlation/duration = %#v/%v", record.OperationIDs, record.DurationNanoseconds)
				}
			} else if record.Phase == "warmup.create_container_to_running" {
				warmupSpans++
				if record.Classification != "warmup" || len(record.OperationIDs) != 2 || record.DurationNanoseconds == nil || *record.DurationNanoseconds != 5*int64(time.Millisecond) {
					t.Fatalf("warmup E2E evidence = %#v", record)
				}
			} else if record.DurationNanoseconds == nil || *record.DurationNanoseconds != int64(time.Millisecond) {
				t.Fatalf("caller duration = %v, want deterministic monotonic delta", record.DurationNanoseconds)
			}
		case "stage_event":
			stageEvents++
			if record.Event == nil || record.Event.OperationID != record.OperationID {
				t.Fatalf("uncorrelated stage event: %#v", record)
			}
		}
	}
	if callerSpans != len(wantCalls)+1 || e2eSpans != 1 || warmupSpans != 1 || stageEvents != 11 {
		t.Fatalf("caller/formal-E2E/warmup-E2E/event counts = %d/%d/%d/%d", callerSpans, e2eSpans, warmupSpans, stageEvents)
	}
}

// TestEvaluatorClockEvidencePreservesMeasuredZeroAndRejectsInvalidBoundaries
// verifies caller spans distinguish a real zero duration from unusable clocks.
func TestEvaluatorClockEvidencePreservesMeasuredZeroAndRejectsInvalidBoundaries(t *testing.T) {
	base := time.Unix(1700000000, 0).UTC()
	t.Run("equal nonzero boundaries", func(t *testing.T) {
		status, stdout, stderr := runFakeScenarioWithClock(t, validScenario("cold", 1), &fakeEvaluationClient{}, &stepClock{now: base})
		if status != 0 {
			t.Fatalf("status = %d, stderr = %s", status, stderr)
		}
		callerSpans := 0
		for _, record := range decodeRecords(t, stdout) {
			if record.RecordType != "caller_span" {
				continue
			}
			callerSpans++
			if record.DurationNanoseconds == nil || *record.DurationNanoseconds != 0 {
				t.Fatalf("measured zero duration was not preserved: %#v", record)
			}
			if record.Success == nil || !*record.Success {
				t.Fatalf("valid zero-duration caller span was not successful: %#v", record)
			}
		}
		if callerSpans == 0 {
			t.Fatal("evaluator emitted no caller spans for the zero-duration run")
		}
	})

	tests := []struct {
		name  string
		clock wallClock
	}{
		{name: "zero clock boundary", clock: &stepClock{}},
		{name: "regressing clock", clock: &stepClock{now: base, step: -time.Millisecond}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, stdout, stderr := runFakeScenarioWithClock(t, validScenario("cold", 1), &fakeEvaluationClient{}, test.clock)
			if status != v1.ExitStatus(v1.CodeInternal) {
				t.Fatalf("status = %d, want internal clock failure; stderr = %s", status, stderr)
			}
			foundClockFailure := false
			records := decodeRecords(t, stdout)
			for _, record := range records {
				if record.RecordType == "caller_span" && record.Error != nil && record.Error.Code == v1.CodeInternal {
					foundClockFailure = record.DurationNanoseconds == nil && record.Success != nil && !*record.Success
				}
			}
			if !foundClockFailure {
				t.Fatal("invalid clock did not emit an explicit failed caller span without duration")
			}
			if len(records) == 0 || records[len(records)-1].Summary == nil || records[len(records)-1].Summary.LifecycleSuccess || records[len(records)-1].Summary.FormalSamples != 0 {
				t.Fatalf("pre-lifecycle clock failure summary = %#v", records)
			}
		})
	}
}

// TestPendingStageEventOmitsSuccessField verifies an intermediate daemon event
// cannot be decoded by downstream consumers as a completed successful sample.
func TestPendingStageEventOmitsSuccessField(t *testing.T) {
	fake := &fakeEvaluationClient{emitPending: true}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), fake)
	if status != 0 {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	foundPending := false
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatalf("decode raw JSONL line: %v", err)
		}
		var recordType string
		var phase string
		if err := json.Unmarshal(fields["record_type"], &recordType); err != nil {
			t.Fatalf("decode record type: %v", err)
		}
		if err := json.Unmarshal(fields["phase"], &phase); err != nil {
			t.Fatalf("decode phase: %v", err)
		}
		if recordType != "stage_event" || phase != "provider.apply" {
			continue
		}
		foundPending = true
		if _, exists := fields["success"]; exists {
			t.Fatalf("pending stage event wrote a success field: %s", line)
		}
	}
	if !foundPending {
		t.Fatal("fake daemon emitted no pending stage event")
	}
}

// TestMissingCompleteEventFailsEvidenceAndSealsSummary verifies a successful
// API response is insufficient when its owned terminal event never appears.
func TestMissingCompleteEventFailsEvidenceAndSealsSummary(t *testing.T) {
	fake := &fakeEvaluationClient{omitCompletePhase: "container.start"}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), fake)
	if status != v1.ExitStatus(v1.CodeUnavailable) {
		t.Fatalf("status = %d, want unavailable evidence failure; stderr = %s", status, stderr)
	}
	records := decodeRecords(t, stdout)
	if len(records) == 0 || records[len(records)-1].RecordType != "run_summary" {
		t.Fatal("failed evidence run was not sealed by a final summary")
	}
	last := records[len(records)-1]
	if last.Summary == nil || last.Success == nil || *last.Success || !last.Summary.LifecycleSuccess || last.Summary.EventEvidenceComplete {
		t.Fatalf("missing event summary = %#v", last)
	}
	if last.Summary.ExpectedOperations != 7 || last.Summary.CompletedOperations != 6 || last.Summary.FormalSamples != 1 {
		t.Fatalf("missing event counts = %#v", last.Summary)
	}
	if last.Error == nil || last.Error.Code != v1.CodeUnavailable {
		t.Fatalf("missing event summary error = %#v", last.Error)
	}
}

// TestFailedDispatchedOperationStillRequiresTerminalEvidence verifies a sent
// request cannot disappear from completeness accounting merely because its API
// response was an error or could have been lost after durable acceptance.
func TestFailedDispatchedOperationStillRequiresTerminalEvidence(t *testing.T) {
	fake := &fakeEvaluationClient{failPhase: "container.create", omitCompletePhase: "container.create"}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), fake)
	if status != v1.ExitStatus(v1.CodeFailedPrecondition) {
		t.Fatalf("status = %d, want lifecycle failure; stderr = %s", status, stderr)
	}
	records := decodeRecords(t, stdout)
	if len(records) == 0 {
		t.Fatal("failed dispatched operation emitted no summary")
	}
	last := records[len(records)-1]
	if last.Summary == nil || last.Summary.EventEvidenceComplete || last.Summary.LifecycleSuccess {
		t.Fatalf("failed dispatched operation summary = %#v", last)
	}
	if last.Summary.ExpectedOperations != 5 || last.Summary.CompletedOperations != 4 || last.Summary.FormalSamples != 0 {
		t.Fatalf("failed dispatched operation counts = %#v", last.Summary)
	}
}

// TestEventEvidenceRejectsContradictoryOrDuplicateComplete verifies one
// successful API operation accepts exactly one succeeded/noop terminal event.
func TestEventEvidenceRejectsContradictoryOrDuplicateComplete(t *testing.T) {
	tests := []struct {
		name string
		fake *fakeEvaluationClient
	}{
		{name: "non-success terminal result", fake: &fakeEvaluationClient{nonSuccessComplete: "container.start"}},
		{name: "duplicate terminal event after all required operations", fake: &fakeEvaluationClient{duplicateComplete: "sandbox.delete"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, stdout, stderr := runFakeScenario(t, validScenario("cold", 1), test.fake)
			if status != v1.ExitStatus(v1.CodeInternal) {
				t.Fatalf("status = %d, want internal evidence conflict; stderr = %s", status, stderr)
			}
			records := decodeRecords(t, stdout)
			if len(records) == 0 {
				t.Fatal("event evidence conflict emitted no records")
			}
			last := records[len(records)-1]
			if last.RecordType != "run_summary" || last.Success == nil || *last.Success || last.Summary == nil || !last.Summary.LifecycleSuccess || last.Summary.EventEvidenceComplete {
				t.Fatalf("event evidence conflict summary = %#v", last)
			}
			if last.Error == nil || last.Error.Code != v1.CodeInternal {
				t.Fatalf("event evidence conflict error = %#v", last.Error)
			}
		})
	}
}

// TestEarlyEventFailureMarksUnexecutedFormalSamplesIncomplete verifies event
// corruption cannot leave a partially executed multi-sample run lifecycle-ok.
func TestEarlyEventFailureMarksUnexecutedFormalSamplesIncomplete(t *testing.T) {
	fake := &fakeEvaluationClient{nonSuccessComplete: "container.start"}
	status, stdout, stderr := runFakeScenario(t, validScenario("cold", 2), fake)
	if status != v1.ExitStatus(v1.CodeInternal) {
		t.Fatalf("status = %d, want internal event failure; stderr = %s", status, stderr)
	}
	records := decodeRecords(t, stdout)
	if len(records) == 0 {
		t.Fatal("partial multi-sample run emitted no records")
	}
	last := records[len(records)-1]
	if last.RecordType != "run_summary" || last.Summary == nil {
		t.Fatalf("partial multi-sample final record = %#v", last)
	}
	if last.Summary.LifecycleSuccess || last.Summary.EventEvidenceComplete || last.Summary.FormalSamples != 1 {
		t.Fatalf("partial multi-sample summary = %#v", last.Summary)
	}
}

// TestCanonicalScenarioDigestIsStableAndBindsInput verifies insignificant JSON
// representation changes preserve identity while process argv changes do not.
func TestCanonicalScenarioDigestIsStableAndBindsInput(t *testing.T) {
	canonical := encodeScenario(t, validScenario("cold", 1))
	originalPrefix := `{"schema_version":1,"name":"prepared-rootfs-loopback-lifecycle","version":"v1",`
	reorderedPrefix := `{"version":"v1","name":"prepared-rootfs-loopback-lifecycle","schema_version":1,`
	reordered := strings.Replace(canonical, originalPrefix, reorderedPrefix, 1)
	if reordered == canonical {
		t.Fatal("test fixture did not reorder scenario object fields")
	}
	first, err := loadScenario("-", strings.NewReader(canonical))
	if err != nil {
		t.Fatalf("load canonical scenario: %v", err)
	}
	second, err := loadScenario("-", strings.NewReader("\n  "+reordered+"\n"))
	if err != nil {
		t.Fatalf("load reordered scenario: %v", err)
	}
	firstDigest, err := canonicalScenarioDigest(first)
	if err != nil {
		t.Fatalf("digest canonical scenario: %v", err)
	}
	secondDigest, err := canonicalScenarioDigest(second)
	if err != nil {
		t.Fatalf("digest reordered scenario: %v", err)
	}
	if firstDigest != secondDigest || len(firstDigest) != sha256.Size*2 {
		t.Fatalf("stable digests = %q/%q", firstDigest, secondDigest)
	}
	changed := first
	changed.Process.Argv = append([]string(nil), first.Process.Argv...)
	changed.Process.Argv[1] = "31"
	changedDigest, err := canonicalScenarioDigest(changed)
	if err != nil {
		t.Fatalf("digest changed scenario: %v", err)
	}
	if changedDigest == firstDigest {
		t.Fatal("process argv change did not change scenario digest")
	}
}

// TestSuccessfulRunEndsWithV2Summary verifies the last raw line seals schema,
// event completeness, formal sample count, and current baseline eligibility.
func TestSuccessfulRunEndsWithV2Summary(t *testing.T) {
	input := validScenario("cold", 1)
	status, stdout, stderr := runFakeScenario(t, input, &fakeEvaluationClient{})
	if status != 0 {
		t.Fatalf("status = %d, stderr = %s", status, stderr)
	}
	wantDigest, err := canonicalScenarioDigest(input)
	if err != nil {
		t.Fatalf("digest scenario: %v", err)
	}
	records := decodeRecords(t, stdout)
	summaryCount := 0
	for _, record := range records {
		if record.SchemaVersion != rawRecordSchemaVersion || record.Scenario.DigestSHA256 != wantDigest {
			t.Fatalf("raw v2 provenance = %#v", record)
		}
		if record.RecordType == "run_summary" {
			summaryCount++
		}
	}
	if len(records) == 0 || summaryCount != 1 || records[len(records)-1].RecordType != "run_summary" {
		t.Fatalf("summary count/final record = %d/%#v", summaryCount, records)
	}
	last := records[len(records)-1]
	if last.Success == nil || !*last.Success || last.Summary == nil {
		t.Fatalf("successful final summary = %#v", last)
	}
	summary := last.Summary
	if !summary.Completed || !summary.LifecycleSuccess || !summary.EventEvidenceComplete || summary.ExpectedOperations != 7 || summary.CompletedOperations != 7 || summary.FormalSamples != 1 {
		t.Fatalf("summary completeness = %#v", summary)
	}
	if summary.BaselineEligible || summary.EvidenceQuality != "debug" || !reflect.DeepEqual(summary.IneligibilityReasons, []string{"scenario_environment_placeholder"}) {
		t.Fatalf("summary baseline classification = %#v", summary)
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
			if len(record.OperationIDs) != 3 || record.DurationNanoseconds == nil || *record.DurationNanoseconds != 7*int64(time.Millisecond) {
				t.Fatalf("cold E2E correlation/duration = %#v/%v", record.OperationIDs, record.DurationNanoseconds)
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
	wantCalls := []string{"info", "events", "sandbox.create", "container.create", "container.delete", "sandbox.stop", "sandbox.delete", "events", "events"}
	if !reflect.DeepEqual(fake.calls, wantCalls) {
		t.Fatalf("calls = %#v, want safe known-resource cleanup %#v", fake.calls, wantCalls)
	}
	foundFailure := false
	for _, record := range decodeRecords(t, stdout) {
		if record.RecordType == "caller_span" && record.Phase == "container.create" {
			foundFailure = record.Error != nil && record.Error.Code == v1.CodeFailedPrecondition && record.Success != nil && !*record.Success
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
	wantCalls := []string{"info", "events", "sandbox.create", "sandbox.stop", "sandbox.delete", "events", "events"}
	if !reflect.DeepEqual(fake.calls, wantCalls) {
		t.Fatalf("calls = %#v, want ambiguous Sandbox cleanup %#v", fake.calls, wantCalls)
	}
	foundE2EFailure := false
	for _, record := range decodeRecords(t, stdout) {
		if record.Phase == "cold.create_sandbox_to_running" {
			foundE2EFailure = record.Success != nil && !*record.Success && record.Error != nil && record.Error.Code == v1.CodeUnavailable && len(record.OperationIDs) == 1
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
	wantCalls := []string{"info", "events", "sandbox.create", "container.create", "container.delete", "sandbox.stop", "sandbox.delete", "events", "events"}
	if !reflect.DeepEqual(fake.calls, wantCalls) {
		t.Fatalf("calls = %#v, want ambiguous Container cleanup %#v", fake.calls, wantCalls)
	}
	foundCleanupFailure := false
	for _, record := range decodeRecords(t, stdout) {
		if record.RecordType == "caller_span" && record.Phase == "container.delete" {
			foundCleanupFailure = record.Success != nil && !*record.Success && record.Error != nil && record.Error.Code == v1.CodeFailedPrecondition
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

// TestScenarioWarmupAttemptsMatchClassification verifies only warm scenarios
// request at least one bounded unmeasured Attempt before formal samples.
func TestScenarioWarmupAttemptsMatchClassification(t *testing.T) {
	cold := validScenario("cold", 1)
	cold.WarmupAttempts = 1
	if err := cold.Validate(); err == nil {
		t.Fatal("cold scenario accepted warmup_attempts")
	}
	warm := validScenario("warm", 1)
	warm.WarmupAttempts = 0
	if err := warm.Validate(); err == nil {
		t.Fatal("warm scenario accepted zero warmup_attempts")
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

// TestStrictScenarioRejectsAmbiguousDocuments verifies scenario evidence cannot
// be reinterpreted through duplicate keys, case aliases, lossy UTF-8, or a second value.
func TestStrictScenarioRejectsAmbiguousDocuments(t *testing.T) {
	canonical := []byte(encodeScenario(t, validScenario("cold", 1)))
	invalidUTF8 := append([]byte(nil), canonical...)
	nameValue := bytes.Index(invalidUTF8, []byte(`"name":"`))
	if nameValue < 0 {
		t.Fatal("encoded scenario omitted name")
	}
	invalidUTF8[nameValue+len(`"name":"`)] = 0xff
	tests := []struct {
		name    string
		payload []byte
	}{
		{
			name: "duplicate decoded key",
			payload: bytes.Replace(canonical, []byte(`"schema_version":1`),
				[]byte(`"schema_version":1,"\u0073chema_version":1`), 1),
		},
		{
			name:    "case aliased struct field",
			payload: bytes.Replace(canonical, []byte(`"schema_version"`), []byte(`"Schema_Version"`), 1),
		},
		{
			name:    "invalid UTF-8",
			payload: invalidUTF8,
		},
		{
			name:    "second JSON value",
			payload: append(append([]byte(nil), canonical...), []byte("\n{}")...),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := loadScenario("-", bytes.NewReader(test.payload)); err == nil {
				t.Fatal("loadScenario() accepted an ambiguous document")
			}
		})
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
