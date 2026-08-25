package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/strictjson"
	"mydocker/pkg/client"
)

const (
	scenarioSchemaVersion   = uint32(1)
	rawRecordSchemaVersion  = uint32(2)
	maxScenarioBytes        = int64(1 << 20)
	maxEventPages           = 1024
	defaultEvaluationSocket = "/run/mydocker/mydockerd.sock"
	unknownEvidence         = "unknown"
	preparedRootfsSnapshot  = "not-created-prepared-rootfs-shared"
	maxCommandEvidenceBytes = 1 << 20
	maxEvaluationSamples    = 100
)

type evaluationClient interface {
	Info(context.Context) (v1.InfoResponse, error)
	CreateSandbox(context.Context, string, v1.CreateSandboxRequest) (v1.SandboxResponse, error)
	StopSandbox(context.Context, string, string) (v1.SandboxResponse, error)
	DeleteSandbox(context.Context, string, string) (v1.OperationResponse, error)
	CreateContainer(context.Context, string, string, v1.CreateContainerRequest) (v1.ContainerResponse, error)
	StartContainer(context.Context, string, string) (v1.ContainerResponse, error)
	KillContainer(context.Context, string, string, v1.TerminationPolicy) (v1.ContainerResponse, error)
	DeleteContainer(context.Context, string, string) (v1.OperationResponse, error)
	Events(context.Context, v1.ResumeToken, int) (v1.EventListResponse, error)
}

type evaluationClientFactory func(client.Config) (evaluationClient, error)

type environmentCollector func(scenario, string, wallClock) recordedEnvironment

type wallClock interface {
	Now() time.Time
}

type systemClock struct{}

type scenario struct {
	SchemaVersion  uint32               `json:"schema_version"`
	Name           string               `json:"name"`
	Version        string               `json:"version"`
	Classification string               `json:"classification"`
	Samples        int                  `json:"samples"`
	WarmupAttempts int                  `json:"warmup_attempts,omitempty"`
	Concurrency    int                  `json:"concurrency"`
	RootFS         string               `json:"rootfs"`
	Sandbox        v1.SandboxSpec       `json:"sandbox"`
	Process        v1.ProcessSpec       `json:"process"`
	KillPolicy     v1.TerminationPolicy `json:"kill_policy"`
	Environment    scenarioEnvironment  `json:"environment"`
}

type scenarioEnvironment struct {
	ID     string             `json:"id"`
	Labels map[string]string  `json:"labels,omitempty"`
	Cache  scenarioCacheState `json:"cache"`
	Noise  string             `json:"background_noise"`
	Warmup string             `json:"warmup"`
}

type scenarioCacheState struct {
	Content         string `json:"content"`
	UnpackedChain   string `json:"unpacked_chain"`
	Snapshot        string `json:"snapshot"`
	PageCache       string `json:"page_cache"`
	ImmutableLayers string `json:"immutable_layers"`
}

type scenarioIdentity struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	DigestSHA256 string `json:"digest_sha256"`
}

type recordedEnvironment struct {
	ID                    string                 `json:"id"`
	Labels                map[string]string      `json:"labels,omitempty"`
	GOOS                  string                 `json:"goos"`
	GOARCH                string                 `json:"goarch"`
	FilesystemProfile     string                 `json:"filesystem_profile"`
	NetworkMode           string                 `json:"network_mode"`
	ScenarioTag           string                 `json:"scenario_tag"`
	DaemonBuild           v1.DaemonBuildIdentity `json:"daemon_build"`
	EvaluatorBuildCommit  string                 `json:"evaluator_build_commit"`
	EvaluatorBuild        buildEnvironment       `json:"evaluator_build"`
	OperatorWorktree      worktreeEnvironment    `json:"operator_worktree"`
	Kernel                kernelEnvironment      `json:"kernel"`
	Cgroup                cgroupEnvironment      `json:"cgroup"`
	CPU                   cpuEnvironment         `json:"cpu"`
	Memory                memoryEnvironment      `json:"memory"`
	Storage               storageEnvironment     `json:"storage"`
	Cache                 scenarioCacheState     `json:"cache"`
	Concurrency           int                    `json:"concurrency"`
	BackgroundNoise       string                 `json:"background_noise"`
	Warmup                string                 `json:"warmup"`
	ExperimentStartedAt   time.Time              `json:"experiment_started_at"`
	TimeZone              string                 `json:"timezone"`
	TimeZoneOffsetSeconds int                    `json:"timezone_offset_seconds"`
}

type buildEnvironment struct {
	GoVersion    string `json:"go_version"`
	MainModule   string `json:"main_module"`
	MainVersion  string `json:"main_version"`
	MainChecksum string `json:"main_checksum"`
	BuildMode    string `json:"build_mode"`
	Compiler     string `json:"compiler"`
	CGOEnabled   string `json:"cgo_enabled"`
	BuildTags    string `json:"build_tags"`
	RaceEnabled  string `json:"race_enabled"`
	Profiling    string `json:"profiling"`
	VCS          string `json:"vcs"`
	VCSRevision  string `json:"vcs_revision"`
	VCSTime      string `json:"vcs_time"`
	VCSModified  string `json:"vcs_modified"`
}

type worktreeEnvironment struct {
	Root                 string `json:"root"`
	Head                 string `json:"head"`
	Branch               string `json:"branch"`
	Status               string `json:"status"`
	StatusSHA256         string `json:"status_sha256"`
	TrackedDiffSHA256    string `json:"tracked_diff_sha256"`
	UntrackedContentNote string `json:"untracked_content_note"`
}

type kernelEnvironment struct {
	Distribution   string `json:"distribution"`
	Release        string `json:"release"`
	Version        string `json:"version"`
	BootParameters string `json:"boot_parameters"`
}

type cgroupEnvironment struct {
	Mode                 string   `json:"mode"`
	SelfPath             string   `json:"self_path"`
	EffectiveRoot        string   `json:"effective_root"`
	AvailableControllers []string `json:"available_controllers"`
	EnabledControllers   []string `json:"enabled_controllers"`
}

type cpuEnvironment struct {
	Model        string `json:"model"`
	LogicalCPUs  int    `json:"logical_cpus"`
	GOMAXPROCS   int    `json:"gomaxprocs"`
	Governor     string `json:"governor"`
	TopologyNote string `json:"topology_note"`
}

type memoryEnvironment struct {
	HostCapacityBytes string `json:"host_capacity_bytes"`
	CgroupLimitBytes  string `json:"cgroup_limit_bytes"`
	CgroupCurrent     string `json:"cgroup_current_bytes"`
}

type storageEnvironment struct {
	ObservedPath string `json:"observed_path"`
	MountPoint   string `json:"mount_point"`
	Filesystem   string `json:"filesystem"`
	Source       string `json:"source"`
	MountOptions string `json:"mount_options"`
	DeviceModel  string `json:"device_model"`
}

type sampleContext struct {
	Classification string
	Index          int
}

type rawRecord struct {
	SchemaVersion       uint32              `json:"schema_version"`
	RecordType          string              `json:"record_type"`
	ExperimentID        string              `json:"experiment_id"`
	Scenario            scenarioIdentity    `json:"scenario"`
	Environment         recordedEnvironment `json:"environment"`
	Classification      string              `json:"classification"`
	SampleIndex         int                 `json:"sample_index"`
	Phase               string              `json:"phase"`
	OperationID         string              `json:"operation_id,omitempty"`
	OperationIDs        []string            `json:"operation_ids,omitempty"`
	StartedAt           *time.Time          `json:"started_at,omitempty"`
	DurationNanoseconds *int64              `json:"duration_ns,omitempty"`
	Success             *bool               `json:"success,omitempty"`
	Error               *v1.ErrorDetail     `json:"error,omitempty"`
	Event               *v1.Event           `json:"event,omitempty"`
	Summary             *runSummary         `json:"summary,omitempty"`
}

// runSummary seals one semantically complete JSONL stream with machine-readable
// lifecycle, evidence, and baseline-eligibility facts. Finalize separately
// determines whether the enclosing output was durably published.
type runSummary struct {
	Completed             bool     `json:"completed"`
	LifecycleSuccess      bool     `json:"lifecycle_success"`
	EventEvidenceComplete bool     `json:"event_evidence_complete"`
	ExpectedOperations    int      `json:"expected_operations"`
	CompletedOperations   int      `json:"completed_operations"`
	FormalSamples         int      `json:"formal_samples"`
	BaselineEligible      bool     `json:"baseline_eligible"`
	EvidenceQuality       string   `json:"evidence_quality"`
	IneligibilityReasons  []string `json:"ineligibility_reasons,omitempty"`
}

type commandOptions struct {
	socketPath       string
	scenarioPath     string
	outputPath       string
	experimentID     string
	timeout          time.Duration
	transportRetries int
}

type evaluator struct {
	client            evaluationClient
	clock             wallClock
	ids               func(string) (string, error)
	encoder           *json.Encoder
	experimentID      string
	scenario          scenario
	environment       recordedEnvironment
	scenarioDigest    string
	operationContexts map[string]sampleContext
	operationRequired map[string]bool
	operationComplete map[string]bool
	resourceIDs       map[string]struct{}
	eventAfter        v1.ResumeToken
	formalSamples     int
	lifecycleOK       bool
	eventEvidenceOK   bool
}

type callerSpan struct {
	started time.Time
}

type outputDestination struct {
	writer io.Writer
	sync   func() error
	close  func() error
}

type boundedCommandBuffer struct {
	buffer    bytes.Buffer
	remaining int
	truncated bool
}

// main wires the standalone evaluator to the public client and never imports daemon or provider internals.
func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr, newEvaluationClient, randomIdentity, systemClock{}, collectRecordedEnvironment))
}

// newEvaluationClient creates the UDS client without touching kernel isolation or daemon-owned state.
func newEvaluationClient(config client.Config) (evaluationClient, error) {
	return client.New(config)
}

// Now provides wall timestamps with an embedded monotonic component for same-process duration subtraction.
func (systemClock) Now() time.Time {
	return time.Now()
}

// run validates the scenario, executes its public lifecycle calls, and leaves raw JSONL evidence even on failure.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, factory evaluationClientFactory, ids func(string) (string, error), clock wallClock, collectEnvironment environmentCollector) int {
	options, err := parseOptions(args)
	if err != nil {
		return writeRunError(stderr, err)
	}
	loaded, err := loadScenario(options.scenarioPath, stdin)
	if err != nil {
		return writeRunError(stderr, err)
	}
	if err := loaded.Validate(); err != nil {
		return writeRunError(stderr, err)
	}
	scenarioDigest, err := canonicalScenarioDigest(loaded)
	if err != nil {
		return writeRunError(stderr, err)
	}
	if options.experimentID == "" {
		options.experimentID, err = ids("experiment")
		if err != nil {
			return writeRunError(stderr, v1.WrapError(v1.CodeInternal, "experiment_id", "cannot generate experiment identity", false, err))
		}
	}
	if err := v1.ValidateOperationID(options.experimentID); err != nil {
		return writeRunError(stderr, v1.WrapError(v1.CodeInvalidArgument, "experiment-id", "must be a valid bounded identity", false, err))
	}
	environment := collectEnvironment(loaded, options.outputPath, clock)
	apiClient, err := factory(client.Config{
		SocketPath:       options.socketPath,
		Timeout:          options.timeout,
		TransportRetries: options.transportRetries,
	})
	if err != nil {
		return writeRunError(stderr, v1.WrapError(v1.CodeInvalidArgument, "client", err.Error(), false, err))
	}
	daemonInfo, err := apiClient.Info(ctx)
	if err != nil {
		return writeRunError(stderr, err)
	}
	if err := daemonInfo.Validate(); err != nil {
		return writeRunError(stderr, v1.WrapError(v1.CodeInternal, "daemon_info", "mydockerd returned invalid build identity", false, err))
	}
	environment.DaemonBuild = daemonInfo.DaemonBuild
	output, err := openOutput(options.outputPath, stdout)
	if err != nil {
		return writeRunError(stderr, err)
	}
	runner := &evaluator{
		client:            apiClient,
		clock:             clock,
		ids:               ids,
		encoder:           json.NewEncoder(output.writer),
		experimentID:      options.experimentID,
		scenario:          loaded,
		environment:       environment,
		scenarioDigest:    scenarioDigest,
		operationContexts: make(map[string]sampleContext),
		operationRequired: make(map[string]bool),
		operationComplete: make(map[string]bool),
		resourceIDs:       make(map[string]struct{}),
	}
	runErr := runner.Run(ctx)
	runErr = errors.Join(runErr, runner.emitRunSummary(runErr))
	finalizeErr := output.Finalize()
	if err := errors.Join(finalizeErr, runErr); err != nil {
		return writeRunError(stderr, err)
	}
	return 0
}

// canonicalScenarioDigest binds every strict-decoded scenario field while
// remaining stable across insignificant JSON whitespace and object key order.
func canonicalScenarioDigest(input scenario) (string, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return "", v1.WrapError(v1.CodeInternal, "scenario", "cannot canonicalize scenario", false, err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// parseOptions accepts only reproducibility and transport settings, never process argv fragments.
func parseOptions(args []string) (commandOptions, error) {
	options := commandOptions{}
	flags := flag.NewFlagSet("mydocker-eval", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.socketPath, "socket", defaultEvaluationSocket, "absolute mydockerd Unix socket path")
	flags.StringVar(&options.scenarioPath, "scenario", "", "strict JSON scenario path or - for stdin")
	flags.StringVar(&options.outputPath, "output", "-", "new JSONL result path or - for stdout")
	flags.StringVar(&options.experimentID, "experiment-id", "", "stable experiment identity")
	flags.DurationVar(&options.timeout, "timeout", 30*time.Second, "per-request timeout")
	flags.IntVar(&options.transportRetries, "transport-retries", 1, "bounded response-loss retries")
	if err := flags.Parse(args); err != nil {
		return commandOptions{}, invalidArgument("arguments", err.Error())
	}
	if flags.NArg() != 0 {
		return commandOptions{}, invalidArgument("arguments", "positional arguments are not supported")
	}
	if !filepath.IsAbs(options.socketPath) {
		return commandOptions{}, invalidArgument("socket", "must be an absolute path")
	}
	if options.scenarioPath == "" {
		return commandOptions{}, invalidArgument("scenario", "is required")
	}
	if options.outputPath == "" {
		return commandOptions{}, invalidArgument("output", "must be a path or -")
	}
	if options.timeout <= 0 || options.transportRetries < 0 {
		return commandOptions{}, invalidArgument("transport", "timeout must be positive and retries must not be negative")
	}
	return options, nil
}

// Validate enforces the first M3 prepared-rootfs none/loopback lifecycle boundary before any API call.
func (input scenario) Validate() error {
	if input.SchemaVersion != scenarioSchemaVersion {
		return invalidArgument("schema_version", "must be 1")
	}
	if strings.TrimSpace(input.Name) == "" || strings.TrimSpace(input.Version) == "" {
		return invalidArgument("scenario", "name and version are required")
	}
	if input.Classification != "cold" && input.Classification != "warm" {
		return invalidArgument("classification", "must be cold or warm")
	}
	if input.Samples <= 0 || input.Samples > maxEvaluationSamples {
		return invalidArgument("samples", fmt.Sprintf("must be from 1 through %d", maxEvaluationSamples))
	}
	if input.Classification == "cold" && input.WarmupAttempts != 0 {
		return invalidArgument("warmup_attempts", "must be zero for cold scenarios")
	}
	if input.Classification == "warm" && (input.WarmupAttempts <= 0 || input.WarmupAttempts > maxEvaluationSamples) {
		return invalidArgument("warmup_attempts", fmt.Sprintf("must be from 1 through %d for warm scenarios", maxEvaluationSamples))
	}
	if input.Concurrency != 1 {
		return invalidArgument("concurrency", "M3 evaluator currently supports exactly one sequential caller")
	}
	if input.Sandbox.Network.Mode != "none" && input.Sandbox.Network.Mode != "loopback" {
		return invalidArgument("sandbox.network.mode", "M3 evaluation supports only none or loopback")
	}
	if len(input.Sandbox.Network.Attachments) != 0 {
		return invalidArgument("sandbox.network.attachments", "must be empty for the M3 minimal provider")
	}
	probeSandbox := v1.CreateSandboxRequest{SandboxID: "scenario-validation", Spec: input.Sandbox}
	if err := probeSandbox.Validate(); err != nil {
		return err
	}
	probeContainer := v1.CreateContainerRequest{
		SandboxID:   "scenario-validation",
		ContainerID: "scenario-validation",
		AttemptID:   "scenario-validation",
		Process:     input.Process,
		RootFS:      input.RootFS,
	}
	if err := probeContainer.Validate(); err != nil {
		return err
	}
	probeKill := v1.KillContainerRequest{ContainerID: "scenario-validation", Policy: input.KillPolicy}
	if err := probeKill.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(input.Environment.ID) == "" {
		return invalidArgument("environment.id", "is required")
	}
	if strings.ContainsRune(input.Environment.Noise, '\x00') || strings.ContainsRune(input.Environment.Warmup, '\x00') {
		return invalidArgument("environment", "background_noise and warmup must contain no NUL")
	}
	if input.Environment.Cache.Snapshot != preparedRootfsSnapshot {
		return invalidArgument("environment.cache.snapshot", "M3 prepared-rootfs scenarios must declare that no per-Attempt snapshot is created")
	}
	for name, value := range map[string]string{
		"content":          input.Environment.Cache.Content,
		"unpacked_chain":   input.Environment.Cache.UnpackedChain,
		"snapshot":         input.Environment.Cache.Snapshot,
		"page_cache":       input.Environment.Cache.PageCache,
		"immutable_layers": input.Environment.Cache.ImmutableLayers,
	} {
		if strings.ContainsRune(value, '\x00') {
			return invalidArgument("environment.cache."+name, "must contain no NUL")
		}
	}
	for key, value := range input.Environment.Labels {
		if strings.TrimSpace(key) == "" || strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
			return invalidArgument("environment.labels", "keys must be non-empty and labels must contain no NUL")
		}
	}
	return nil
}

// loadScenario reads one bounded strict JSON scenario and rejects unknown fields or trailing values.
func loadScenario(path string, stdin io.Reader) (scenario, error) {
	reader := stdin
	var file *os.File
	if path != "-" {
		opened, err := os.Open(path)
		if err != nil {
			return scenario{}, v1.WrapError(v1.CodeInvalidArgument, "scenario", "cannot open scenario", false, err)
		}
		file = opened
		defer file.Close()
		reader = file
	}
	payload, err := io.ReadAll(io.LimitReader(reader, maxScenarioBytes+1))
	if err != nil {
		return scenario{}, v1.WrapError(v1.CodeInvalidArgument, "scenario", "cannot read scenario", false, err)
	}
	if int64(len(payload)) > maxScenarioBytes {
		return scenario{}, invalidArgument("scenario", "exceeds the 1 MiB limit")
	}
	var loaded scenario
	if err := strictjson.Decode(payload, &loaded); err != nil {
		return scenario{}, v1.WrapError(v1.CodeInvalidArgument, "scenario", "invalid scenario JSON: "+err.Error(), false, err)
	}
	return loaded, nil
}

// openOutput opens a new evidence file plus its parent directory for durable publication, or selects caller-owned stdout for streaming.
func openOutput(path string, stdout io.Writer) (outputDestination, error) {
	if path == "-" {
		return outputDestination{writer: stdout, sync: func() error { return nil }, close: func() error { return nil }}, nil
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return outputDestination{}, v1.WrapError(v1.CodeInvalidArgument, "output", "cannot open the result parent directory", false, err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return outputDestination{}, v1.WrapError(v1.CodeInvalidArgument, "output", "cannot create a new result file", false, errors.Join(err, directory.Close()))
	}
	return outputDestination{
		writer: file,
		sync:   func() error { return errors.Join(file.Sync(), directory.Sync()) },
		close:  func() error { return errors.Join(file.Close(), directory.Close()) },
	}, nil
}

// Finalize forces file-backed JSONL bytes and its directory entry to stable storage and reports every sync/close failure; stdout remains caller-owned.
func (destination outputDestination) Finalize() error {
	syncErr := destination.sync()
	closeErr := destination.close()
	if syncErr == nil && closeErr == nil {
		return nil
	}
	return v1.WrapError(v1.CodeInternal, "output", "cannot durably finalize raw evaluation records", false, errors.Join(syncErr, closeErr))
}

// Write captures bounded read-only command output while reporting truncation instead of allocating without limit.
func (output *boundedCommandBuffer) Write(payload []byte) (int, error) {
	originalLength := len(payload)
	if output.remaining <= 0 {
		output.truncated = true
		return originalLength, nil
	}
	if len(payload) > output.remaining {
		payload = payload[:output.remaining]
		output.truncated = true
	}
	_, _ = output.buffer.Write(payload)
	output.remaining -= len(payload)
	return originalLength, nil
}

// collectRecordedEnvironment snapshots reproducibility evidence once before lifecycle calls so every record from the run has identical context.
func collectRecordedEnvironment(input scenario, outputPath string, clock wallClock) recordedEnvironment {
	startedAt := clock.Now()
	build := collectBuildEnvironment()
	worktree := collectWorktreeEnvironment()
	_, timezoneOffset := startedAt.Zone()
	return recordedEnvironment{
		ID:                    input.Environment.ID,
		Labels:                cloneLabels(input.Environment.Labels),
		GOOS:                  runtime.GOOS,
		GOARCH:                runtime.GOARCH,
		FilesystemProfile:     "prepared-rootfs",
		NetworkMode:           input.Sandbox.Network.Mode,
		ScenarioTag:           formatScenarioLabel(input),
		EvaluatorBuildCommit:  knownOrUnknown(build.VCSRevision),
		EvaluatorBuild:        build,
		OperatorWorktree:      worktree,
		Kernel:                collectKernelEnvironment(),
		Cgroup:                collectCgroupEnvironment(),
		CPU:                   collectCPUEnvironment(),
		Memory:                collectMemoryEnvironment(),
		Storage:               collectStorageEnvironment(outputPath),
		Cache:                 normalizeCacheState(input.Environment.Cache),
		Concurrency:           input.Concurrency,
		BackgroundNoise:       knownOrUnknown(input.Environment.Noise),
		Warmup:                knownOrUnknown(input.Environment.Warmup),
		ExperimentStartedAt:   startedAt,
		TimeZone:              collectTimeZone(startedAt),
		TimeZoneOffsetSeconds: timezoneOffset,
	}
}

// collectBuildEnvironment reads immutable Go build metadata without invoking the compiler or changing the worktree.
func collectBuildEnvironment() buildEnvironment {
	result := buildEnvironment{
		GoVersion:    knownOrUnknown(runtime.Version()),
		MainModule:   unknownEvidence,
		MainVersion:  unknownEvidence,
		MainChecksum: unknownEvidence,
		BuildMode:    unknownEvidence,
		Compiler:     unknownEvidence,
		CGOEnabled:   unknownEvidence,
		BuildTags:    unknownEvidence,
		RaceEnabled:  unknownEvidence,
		Profiling:    "disabled-by-harness",
		VCS:          unknownEvidence,
		VCSRevision:  unknownEvidence,
		VCSTime:      unknownEvidence,
		VCSModified:  unknownEvidence,
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return result
	}
	result.GoVersion = knownOrUnknown(info.GoVersion)
	result.MainModule = knownOrUnknown(info.Main.Path)
	result.MainVersion = knownOrUnknown(info.Main.Version)
	result.MainChecksum = knownOrUnknown(info.Main.Sum)
	settings := make(map[string]string, len(info.Settings))
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	result.BuildMode = knownOrUnknown(settings["-buildmode"])
	result.Compiler = knownOrUnknown(settings["-compiler"])
	result.CGOEnabled = knownOrUnknown(settings["CGO_ENABLED"])
	result.BuildTags = knownOrUnknown(settings["-tags"])
	result.RaceEnabled = knownOrUnknown(settings["-race"])
	result.VCS = knownOrUnknown(settings["vcs"])
	result.VCSRevision = knownOrUnknown(settings["vcs.revision"])
	result.VCSTime = knownOrUnknown(settings["vcs.time"])
	result.VCSModified = knownOrUnknown(settings["vcs.modified"])
	return result
}

// collectWorktreeEnvironment records the operator's current Git context without
// attributing that checkout to either the evaluator binary or the daemon under test.
func collectWorktreeEnvironment() worktreeEnvironment {
	result := worktreeEnvironment{
		Root:                 unknownEvidence,
		Head:                 unknownEvidence,
		Branch:               unknownEvidence,
		Status:               unknownEvidence,
		StatusSHA256:         unknownEvidence,
		TrackedDiffSHA256:    unknownEvidence,
		UntrackedContentNote: "not-hashed; status_sha256 covers untracked paths only",
	}
	root, err := runReadOnlyCommand("git", "rev-parse", "--show-toplevel")
	if err != nil {
		return result
	}
	result.Root = strings.TrimSpace(root)
	if head, commandErr := runReadOnlyCommand("git", "-C", result.Root, "rev-parse", "HEAD"); commandErr == nil {
		result.Head = knownOrUnknown(strings.TrimSpace(head))
	}
	if branch, commandErr := runReadOnlyCommand("git", "-C", result.Root, "branch", "--show-current"); commandErr == nil {
		result.Branch = knownOrUnknown(strings.TrimSpace(branch))
	}
	status, statusErr := runReadOnlyCommand("git", "-C", result.Root, "status", "--porcelain=v1", "--untracked-files=all")
	if statusErr == nil {
		result.StatusSHA256 = sha256Text(status)
		if strings.TrimSpace(status) == "" {
			result.Status = "clean"
		} else {
			result.Status = "dirty"
		}
	}
	diff, diffErr := runReadOnlyCommand("git", "-C", result.Root, "diff", "--binary", "--no-ext-diff", "HEAD", "--")
	if diffErr == nil {
		result.TrackedDiffSHA256 = sha256Text(diff)
	}
	return result
}

// runReadOnlyCommand executes one bounded inspection command with a short deadline and returns unknown-worthy errors on truncation.
func runReadOnlyCommand(name string, arguments ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	output := &boundedCommandBuffer{remaining: maxCommandEvidenceBytes}
	command := exec.CommandContext(ctx, name, arguments...)
	command.Stdout = output
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return "", err
	}
	if output.truncated {
		return "", errors.New("inspection output exceeded the evidence limit")
	}
	return output.buffer.String(), nil
}

// collectKernelEnvironment reads Linux release, distribution, and boot metadata without altering kernel state.
func collectKernelEnvironment() kernelEnvironment {
	return kernelEnvironment{
		Distribution:   parseDistribution(readEvidenceFile("/etc/os-release")),
		Release:        knownOrUnknown(readEvidenceFile("/proc/sys/kernel/osrelease")),
		Version:        knownOrUnknown(readEvidenceFile("/proc/version")),
		BootParameters: knownOrUnknown(readEvidenceFile("/proc/cmdline")),
	}
}

// collectCgroupEnvironment records the current process's read-only unified hierarchy and controller visibility.
func collectCgroupEnvironment() cgroupEnvironment {
	result := cgroupEnvironment{
		Mode:                 unknownEvidence,
		SelfPath:             unknownEvidence,
		EffectiveRoot:        unknownEvidence,
		AvailableControllers: []string{unknownEvidence},
		EnabledControllers:   []string{unknownEvidence},
	}
	controllers := readEvidenceFile("/sys/fs/cgroup/cgroup.controllers")
	if controllers == "" {
		return result
	}
	result.Mode = "v2"
	result.AvailableControllers = normalizedWords(controllers)
	result.SelfPath = parseUnifiedCgroupPath(readEvidenceFile("/proc/self/cgroup"))
	if result.SelfPath == unknownEvidence {
		return result
	}
	effectiveRoot := filepath.Clean(filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(result.SelfPath, "/")))
	if effectiveRoot != "/sys/fs/cgroup" && !strings.HasPrefix(effectiveRoot, "/sys/fs/cgroup/") {
		return result
	}
	result.EffectiveRoot = effectiveRoot
	enabled := readEvidenceFile(filepath.Join(effectiveRoot, "cgroup.subtree_control"))
	if enabled != "" {
		result.EnabledControllers = normalizedWords(strings.ReplaceAll(enabled, "+", ""))
	}
	return result
}

// collectCPUEnvironment records runtime-visible parallelism and stable CPU model/governor evidence.
func collectCPUEnvironment() cpuEnvironment {
	return cpuEnvironment{
		Model:        parseCPUModel(readEvidenceFile("/proc/cpuinfo")),
		LogicalCPUs:  runtime.NumCPU(),
		GOMAXPROCS:   runtime.GOMAXPROCS(0),
		Governor:     knownOrUnknown(readEvidenceFile("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")),
		TopologyNote: "logical counts are runtime-visible; physical topology is not inferred",
	}
}

// collectMemoryEnvironment records host capacity and the current cgroup's observable memory limit/current values.
func collectMemoryEnvironment() memoryEnvironment {
	root := collectCgroupEnvironment().EffectiveRoot
	limit := unknownEvidence
	current := unknownEvidence
	if root != unknownEvidence {
		limit = knownOrUnknown(readEvidenceFile(filepath.Join(root, "memory.max")))
		current = knownOrUnknown(readEvidenceFile(filepath.Join(root, "memory.current")))
	}
	return memoryEnvironment{
		HostCapacityBytes: parseMemoryCapacity(readEvidenceFile("/proc/meminfo")),
		CgroupLimitBytes:  limit,
		CgroupCurrent:     current,
	}
}

// collectStorageEnvironment identifies the filesystem carrying the result path using a read-only mountinfo snapshot.
func collectStorageEnvironment(outputPath string) storageEnvironment {
	observedPath := outputPath
	if outputPath == "-" {
		observedPath, _ = os.Getwd()
	} else {
		observedPath, _ = filepath.Abs(outputPath)
	}
	observedPath = knownOrUnknown(observedPath)
	result := parseMountInfo(readEvidenceFile("/proc/self/mountinfo"), observedPath)
	result.ObservedPath = observedPath
	result.DeviceModel = collectDeviceModel(result.Source)
	return result
}

// parseMountInfo selects the longest mount-point prefix for one observed path and exposes no mutable mount operation.
func parseMountInfo(payload, observedPath string) storageEnvironment {
	result := storageEnvironment{
		ObservedPath: observedPath,
		MountPoint:   unknownEvidence,
		Filesystem:   unknownEvidence,
		Source:       unknownEvidence,
		MountOptions: unknownEvidence,
		DeviceModel:  unknownEvidence,
	}
	bestLength := -1
	scanner := bufio.NewScanner(strings.NewReader(payload))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for index, field := range fields {
			if field == "-" {
				separator = index
				break
			}
		}
		if len(fields) < 6 || separator < 0 || separator+3 >= len(fields) {
			continue
		}
		mountPoint := decodeMountInfoField(fields[4])
		if !pathWithinMount(observedPath, mountPoint) || len(mountPoint) <= bestLength {
			continue
		}
		bestLength = len(mountPoint)
		result.MountPoint = mountPoint
		result.Filesystem = knownOrUnknown(fields[separator+1])
		result.Source = knownOrUnknown(decodeMountInfoField(fields[separator+2]))
		result.MountOptions = knownOrUnknown(fields[5] + "," + fields[separator+3])
	}
	return result
}

// collectDeviceModel resolves a block source to sysfs when possible and otherwise marks the model unknown.
func collectDeviceModel(source string) string {
	if !strings.HasPrefix(source, "/dev/") {
		return unknownEvidence
	}
	resolved, err := filepath.EvalSymlinks(source)
	if err != nil {
		resolved = source
	}
	model := readEvidenceFile(filepath.Join("/sys/class/block", filepath.Base(resolved), "device/model"))
	return knownOrUnknown(model)
}

// readEvidenceFile returns a trimmed bounded snapshot or an empty value so the caller can emit an explicit unknown marker.
func readEvidenceFile(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxCommandEvidenceBytes+1))
	if err != nil || len(payload) > maxCommandEvidenceBytes {
		return ""
	}
	return strings.TrimSpace(string(payload))
}

// parseDistribution extracts the human-readable os-release name while treating malformed or absent data as unknown.
func parseDistribution(payload string) string {
	scanner := bufio.NewScanner(strings.NewReader(payload))
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "=")
		if !found || key != "PRETTY_NAME" {
			continue
		}
		if unquoted, err := strconv.Unquote(value); err == nil {
			return knownOrUnknown(unquoted)
		}
		return knownOrUnknown(value)
	}
	return unknownEvidence
}

// parseUnifiedCgroupPath returns the cgroup v2 membership path from procfs without guessing from legacy controller entries.
func parseUnifiedCgroupPath(payload string) string {
	for _, line := range strings.Split(payload, "\n") {
		if strings.HasPrefix(line, "0::") {
			return knownOrUnknown(strings.TrimPrefix(line, "0::"))
		}
	}
	return unknownEvidence
}

// parseCPUModel reads the first architecture-specific CPU model field and otherwise returns unknown.
func parseCPUModel(payload string) string {
	for _, key := range []string{"model name", "hardware", "processor"} {
		scanner := bufio.NewScanner(strings.NewReader(payload))
		for scanner.Scan() {
			name, value, found := strings.Cut(scanner.Text(), ":")
			if found && strings.EqualFold(strings.TrimSpace(name), key) {
				return knownOrUnknown(value)
			}
		}
	}
	return unknownEvidence
}

// parseMemoryCapacity converts Linux MemTotal kB into a decimal byte string without using zero as an unknown sentinel.
func parseMemoryCapacity(payload string) string {
	scanner := bufio.NewScanner(strings.NewReader(payload))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "MemTotal:" {
			continue
		}
		kilobytes, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || kilobytes > ^uint64(0)/1024 {
			return unknownEvidence
		}
		return strconv.FormatUint(kilobytes*1024, 10)
	}
	return unknownEvidence
}

// normalizedWords preserves kernel controller order while ensuring an empty observation is explicitly unknown.
func normalizedWords(value string) []string {
	words := strings.Fields(value)
	if len(words) == 0 {
		return []string{unknownEvidence}
	}
	return words
}

// decodeMountInfoField restores the documented octal escapes used for spaces and separators in proc mountinfo paths.
func decodeMountInfoField(value string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(value)
}

// pathWithinMount checks component boundaries so a similarly prefixed sibling is never selected as the backing mount.
func pathWithinMount(observedPath, mountPoint string) bool {
	if observedPath == mountPoint || mountPoint == "/" {
		return true
	}
	return strings.HasPrefix(observedPath, strings.TrimSuffix(mountPoint, "/")+"/")
}

// normalizeCacheState carries operator-declared cache facts and replaces every missing dimension with an explicit unknown.
func normalizeCacheState(input scenarioCacheState) scenarioCacheState {
	return scenarioCacheState{
		Content:         knownOrUnknown(input.Content),
		UnpackedChain:   knownOrUnknown(input.UnpackedChain),
		Snapshot:        knownOrUnknown(input.Snapshot),
		PageCache:       knownOrUnknown(input.PageCache),
		ImmutableLayers: knownOrUnknown(input.ImmutableLayers),
	}
}

// collectTimeZone returns an IANA localtime name when discoverable and otherwise records the timestamp location or zone abbreviation.
func collectTimeZone(startedAt time.Time) string {
	location := startedAt.Location().String()
	if location != "" && location != "Local" {
		return location
	}
	if target, err := filepath.EvalSymlinks("/etc/localtime"); err == nil {
		if _, zone, found := strings.Cut(target, "/zoneinfo/"); found {
			return knownOrUnknown(zone)
		}
	}
	zone, _ := startedAt.Zone()
	return knownOrUnknown(zone)
}

// knownOrUnknown normalizes optional evidence so collection failures are visible rather than silently omitted.
func knownOrUnknown(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return unknownEvidence
	}
	return value
}

// sha256Text provides a stable reference to bounded worktree evidence without embedding the full diff in every record.
func sha256Text(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// Run captures the pre-run event tail before side effects, executes the lifecycle boundary, and then collects only later owned events.
func (runner *evaluator) Run(ctx context.Context) error {
	eventTail, err := runner.captureEventTail(ctx)
	if err != nil {
		return err
	}
	runner.eventAfter = eventTail
	var lifecycleErr error
	var eventErr error
	if runner.scenario.Classification == "cold" {
		for index := 0; index < runner.scenario.Samples; index++ {
			sampleErr := runner.runColdSample(ctx, index)
			if sampleErr == nil {
				runner.formalSamples++
			}
			lifecycleErr = errors.Join(lifecycleErr, sampleErr)
			drainErr := runner.collectEvents(ctx)
			eventErr = errors.Join(eventErr, drainErr)
			if sampleErr != nil || drainErr != nil {
				break
			}
		}
	} else {
		lifecycleErr, eventErr = runner.runWarmSamples(ctx)
	}
	eventErr = errors.Join(eventErr, runner.collectEvents(ctx))
	runner.lifecycleOK = lifecycleErr == nil && runner.formalSamples == runner.scenario.Samples
	evidenceErr := runner.validateEventEvidence(eventErr)
	return errors.Join(lifecycleErr, eventErr, evidenceErr)
}

// runColdSample emits one caller-owned CreateSandbox-to-Running span while retaining per-operation spans and best-effort cleanup evidence.
func (runner *evaluator) runColdSample(ctx context.Context, index int) error {
	sample := sampleContext{Classification: "cold", Index: index}
	sandboxID, err := runner.newResourceID("sandbox")
	if err != nil {
		return err
	}
	containerID, err := runner.newResourceID("container")
	if err != nil {
		return err
	}
	attemptID, err := runner.newResourceID("attempt")
	if err != nil {
		return err
	}
	operationID, err := runner.newOperation(sample)
	if err != nil {
		return err
	}
	span := runner.beginCallerSpan()
	operationIDs := []string{operationID}
	sandboxMayExist := true
	containerMayExist := false
	containerMayBeRunning := false
	lifecycleErr := runner.measure(ctx, sample, "sandbox.create", operationID, func() error {
		_, callErr := runner.client.CreateSandbox(ctx, operationID, v1.CreateSandboxRequest{SandboxID: sandboxID, Spec: runner.scenario.Sandbox})
		return callErr
	})
	if lifecycleErr == nil {
		operationID, lifecycleErr = runner.newOperation(sample)
		if lifecycleErr == nil {
			operationIDs = append(operationIDs, operationID)
			containerMayExist = true
			lifecycleErr = runner.measure(ctx, sample, "container.create", operationID, func() error {
				_, callErr := runner.client.CreateContainer(ctx, operationID, sandboxID, v1.CreateContainerRequest{
					ContainerID: containerID,
					AttemptID:   attemptID,
					Process:     runner.scenario.Process,
					RootFS:      runner.scenario.RootFS,
				})
				return callErr
			})
		}
	}
	if lifecycleErr == nil {
		operationID, lifecycleErr = runner.newOperation(sample)
		if lifecycleErr == nil {
			operationIDs = append(operationIDs, operationID)
			containerMayBeRunning = true
			lifecycleErr = runner.measure(ctx, sample, "container.start", operationID, func() error {
				response, callErr := runner.client.StartContainer(ctx, operationID, containerID)
				if callErr == nil {
					runner.operationRequired[operationID] = true
				}
				return errors.Join(callErr, requireRunning(response, callErr))
			})
		}
	}
	lifecycleErr = runner.finishCallerSpan(sample, "cold.create_sandbox_to_running", operationIDs, span, lifecycleErr)
	cleanupErr := runner.cleanupAttempt(ctx, sample, "", sandboxID, containerID, sandboxMayExist, containerMayExist, containerMayBeRunning, true)
	return errors.Join(lifecycleErr, cleanupErr)
}

// runWarmSamples creates one stable Sandbox and returns lifecycle failures
// separately from incremental event-evidence failures.
func (runner *evaluator) runWarmSamples(ctx context.Context) (error, error) {
	setup := sampleContext{Classification: "warm", Index: -1}
	sandboxID, err := runner.newResourceID("sandbox")
	if err != nil {
		return err, nil
	}
	operationID, err := runner.newOperation(setup)
	if err != nil {
		return err, nil
	}
	setupLifecycleErr := runner.measure(ctx, setup, "setup.sandbox.create", operationID, func() error {
		_, callErr := runner.client.CreateSandbox(ctx, operationID, v1.CreateSandboxRequest{SandboxID: sandboxID, Spec: runner.scenario.Sandbox})
		return callErr
	})
	setupEventErr := runner.collectEvents(ctx)
	if setupLifecycleErr != nil || setupEventErr != nil {
		cleanupErr := runner.cleanupSandbox(ctx, setup, "cleanup.", sandboxID)
		return errors.Join(setupLifecycleErr, cleanupErr), errors.Join(setupEventErr, runner.collectEvents(ctx))
	}
	for index := 0; index < runner.scenario.WarmupAttempts; index++ {
		warmupLifecycleErr := runner.runWarmAttempt(ctx, sandboxID, index, "warmup")
		warmupEventErr := runner.collectEvents(ctx)
		if warmupLifecycleErr != nil || warmupEventErr != nil {
			cleanupErr := runner.cleanupSandbox(ctx, setup, "cleanup.", sandboxID)
			return errors.Join(warmupLifecycleErr, cleanupErr), errors.Join(warmupEventErr, runner.collectEvents(ctx))
		}
	}
	var lifecycleErr error
	var eventErr error
	for index := 0; index < runner.scenario.Samples; index++ {
		attemptErr := runner.runWarmAttempt(ctx, sandboxID, index, "warm")
		if attemptErr == nil {
			runner.formalSamples++
		}
		lifecycleErr = errors.Join(lifecycleErr, attemptErr)
		drainErr := runner.collectEvents(ctx)
		eventErr = errors.Join(eventErr, drainErr)
		if attemptErr != nil || drainErr != nil {
			break
		}
	}
	cleanupErr := runner.cleanupSandbox(ctx, setup, "cleanup.", sandboxID)
	eventErr = errors.Join(eventErr, runner.collectEvents(ctx))
	return errors.Join(lifecycleErr, cleanupErr), eventErr
}

// runWarmAttempt emits one caller-owned CreateContainer-to-Running span and cleans every possibly persisted Attempt by its known ID.
func (runner *evaluator) runWarmAttempt(ctx context.Context, sandboxID string, index int, classification string) error {
	sample := sampleContext{Classification: classification, Index: index}
	containerID, err := runner.newResourceID("container")
	if err != nil {
		return err
	}
	attemptID, err := runner.newResourceID("attempt")
	if err != nil {
		return err
	}
	operationID, err := runner.newOperation(sample)
	if err != nil {
		return err
	}
	span := runner.beginCallerSpan()
	operationIDs := []string{operationID}
	containerMayExist := true
	containerMayBeRunning := false
	lifecycleErr := runner.measure(ctx, sample, "container.create", operationID, func() error {
		_, callErr := runner.client.CreateContainer(ctx, operationID, sandboxID, v1.CreateContainerRequest{
			ContainerID: containerID,
			AttemptID:   attemptID,
			Process:     runner.scenario.Process,
			RootFS:      runner.scenario.RootFS,
		})
		return callErr
	})
	if lifecycleErr == nil {
		operationID, lifecycleErr = runner.newOperation(sample)
		if lifecycleErr == nil {
			operationIDs = append(operationIDs, operationID)
			containerMayBeRunning = true
			lifecycleErr = runner.measure(ctx, sample, "container.start", operationID, func() error {
				response, callErr := runner.client.StartContainer(ctx, operationID, containerID)
				if callErr == nil {
					runner.operationRequired[operationID] = true
				}
				return errors.Join(callErr, requireRunning(response, callErr))
			})
		}
	}
	lifecycleErr = runner.finishCallerSpan(sample, classification+".create_container_to_running", operationIDs, span, lifecycleErr)
	cleanupErr := runner.cleanupAttempt(ctx, sample, "", sandboxID, containerID, true, containerMayExist, containerMayBeRunning, false)
	return errors.Join(lifecycleErr, cleanupErr)
}

// cleanupAttempt uses known identities after any sent create/start request, because a failed response cannot prove that the durable side effect is absent.
func (runner *evaluator) cleanupAttempt(ctx context.Context, sample sampleContext, prefix, sandboxID, containerID string, sandboxMayExist, containerMayExist, containerMayBeRunning, removeSandbox bool) error {
	var cleanupErr error
	if containerMayBeRunning {
		operationID, err := runner.newOperation(sample)
		if err == nil {
			err = runner.measure(ctx, sample, prefix+"container.kill", operationID, func() error {
				_, callErr := runner.client.KillContainer(ctx, operationID, containerID, runner.scenario.KillPolicy)
				return callErr
			})
		}
		cleanupErr = errors.Join(cleanupErr, err)
	}
	if containerMayExist {
		operationID, err := runner.newOperation(sample)
		if err == nil {
			err = runner.measure(ctx, sample, prefix+"container.delete", operationID, func() error {
				_, callErr := runner.client.DeleteContainer(ctx, operationID, containerID)
				return callErr
			})
		}
		cleanupErr = errors.Join(cleanupErr, err)
	}
	if removeSandbox && sandboxMayExist {
		cleanupErr = errors.Join(cleanupErr, runner.cleanupSandbox(ctx, sample, prefix, sandboxID))
	}
	return cleanupErr
}

// cleanupSandbox records stop and delete as separate operation-scoped phases so partial cleanup remains visible.
func (runner *evaluator) cleanupSandbox(ctx context.Context, sample sampleContext, prefix, sandboxID string) error {
	operationID, err := runner.newOperation(sample)
	if err != nil {
		return err
	}
	stopErr := runner.measure(ctx, sample, prefix+"sandbox.stop", operationID, func() error {
		_, callErr := runner.client.StopSandbox(ctx, operationID, sandboxID)
		return callErr
	})
	operationID, idErr := runner.newOperation(sample)
	if idErr != nil {
		return errors.Join(stopErr, idErr)
	}
	deleteErr := runner.measure(ctx, sample, prefix+"sandbox.delete", operationID, func() error {
		_, callErr := runner.client.DeleteSandbox(ctx, operationID, sandboxID)
		return callErr
	})
	return errors.Join(stopErr, deleteErr)
}

// beginCallerSpan captures the single-process monotonic start boundary immediately before the first E2E API request.
func (runner *evaluator) beginCallerSpan() callerSpan {
	return callerSpan{started: runner.clock.Now()}
}

// finishCallerSpan emits one aggregate E2E observation correlated with every durable operation used to reach Running.
func (runner *evaluator) finishCallerSpan(sample sampleContext, phase string, operationIDs []string, span callerSpan, callErr error) error {
	finished := runner.clock.Now()
	duration, durationErr := measuredDuration(span.started, finished)
	recordErr := errors.Join(callErr, durationErr)
	success := recordErr == nil
	record := rawRecord{
		SchemaVersion:       rawRecordSchemaVersion,
		RecordType:          "caller_span",
		ExperimentID:        runner.experimentID,
		Scenario:            runner.scenarioIdentity(),
		Environment:         runner.environment,
		Classification:      sample.Classification,
		SampleIndex:         sample.Index,
		Phase:               phase,
		OperationIDs:        append([]string(nil), operationIDs...),
		StartedAt:           &span.started,
		DurationNanoseconds: duration,
		Success:             &success,
	}
	if recordErr != nil {
		detail := stableErrorDetail(recordErr)
		record.Error = &detail
	}
	return errors.Join(recordErr, runner.emit(record))
}

// requireRunning prevents a successful transport response with a non-Running projection from closing an E2E startup sample.
func requireRunning(response v1.ContainerResponse, callErr error) error {
	if callErr != nil {
		return nil
	}
	if response.Container.Status.Phase == "running" {
		return nil
	}
	return v1.NewError(v1.CodeFailedPrecondition, "container.status.phase", "start response did not confirm Running")
}

// measure emits one caller-visible API span using only same-process duration subtraction.
func (runner *evaluator) measure(_ context.Context, sample sampleContext, phase, operationID string, call func() error) error {
	started := runner.clock.Now()
	if operationID != "" {
		runner.operationRequired[operationID] = false
	}
	callErr := call()
	finished := runner.clock.Now()
	duration, durationErr := measuredDuration(started, finished)
	recordErr := errors.Join(callErr, durationErr)
	success := recordErr == nil
	if operationID != "" && callErr == nil {
		runner.operationRequired[operationID] = true
	}
	record := rawRecord{
		SchemaVersion:       rawRecordSchemaVersion,
		RecordType:          "caller_span",
		ExperimentID:        runner.experimentID,
		Scenario:            runner.scenarioIdentity(),
		Environment:         runner.environment,
		Classification:      sample.Classification,
		SampleIndex:         sample.Index,
		Phase:               phase,
		OperationID:         operationID,
		StartedAt:           &started,
		DurationNanoseconds: duration,
		Success:             &success,
	}
	if recordErr != nil {
		detail := stableErrorDetail(recordErr)
		record.Error = &detail
	}
	emitErr := runner.emit(record)
	return errors.Join(recordErr, emitErr)
}

// measuredDuration preserves an explicitly measured zero while rejecting zero
// or regressing clock boundaries as unusable benchmark evidence.
func measuredDuration(started, finished time.Time) (*int64, error) {
	if started.IsZero() || finished.IsZero() {
		return nil, v1.NewError(v1.CodeInternal, "clock", "evaluation clock returned a zero boundary")
	}
	if finished.Before(started) {
		return nil, v1.NewError(v1.CodeInternal, "clock", "evaluation clock regressed during a measured span")
	}
	value := finished.Sub(started).Nanoseconds()
	return &value, nil
}

// captureEventTail reaches a stable pre-run resume token before creating resources, avoiding a scan from sequence zero after the experiment.
func (runner *evaluator) captureEventTail(ctx context.Context) (v1.ResumeToken, error) {
	sample := sampleContext{Classification: runner.scenario.Classification, Index: -1}
	var after v1.ResumeToken
	for page := 0; page < maxEventPages; page++ {
		var response v1.EventListResponse
		if err := runner.measure(ctx, sample, "events.capture_start", "", func() error {
			var callErr error
			response, callErr = runner.client.Events(ctx, after, 500)
			return callErr
		}); err != nil {
			return "", err
		}
		after = response.NextResumeToken
		if !response.HasMore {
			return after, nil
		}
	}
	return "", v1.NewError(v1.CodeUnavailable, "events", "event page limit exceeded while capturing the pre-run tail")
}

// collectEvents pages from the captured pre-run tail, retaining only stage events for operation IDs generated by this run.
func (runner *evaluator) collectEvents(ctx context.Context) error {
	sample := sampleContext{Classification: runner.scenario.Classification, Index: -1}
	after := runner.eventAfter
	for page := 0; page < maxEventPages; page++ {
		var response v1.EventListResponse
		if err := runner.measure(ctx, sample, "events.collect", "", func() error {
			var callErr error
			response, callErr = runner.client.Events(ctx, after, 500)
			return callErr
		}); err != nil {
			return err
		}
		for index := range response.Events {
			event := response.Events[index]
			contextForOperation, owned := runner.operationContexts[event.OperationID]
			if !owned {
				continue
			}
			record := rawRecord{
				SchemaVersion:  rawRecordSchemaVersion,
				RecordType:     "stage_event",
				ExperimentID:   runner.experimentID,
				Scenario:       runner.scenarioIdentity(),
				Environment:    runner.environment,
				Classification: contextForOperation.Classification,
				SampleIndex:    contextForOperation.Index,
				Phase:          event.Stage,
				OperationID:    event.OperationID,
				Event:          &event,
			}
			if event.Stage == "complete" {
				if runner.operationComplete[event.OperationID] {
					return v1.NewError(v1.CodeInternal, "events", "operation emitted more than one terminal complete event")
				}
				if runner.operationRequired[event.OperationID] && event.Result != "succeeded" && event.Result != "noop" {
					return v1.NewError(v1.CodeInternal, "events", "successful API operation has a non-success terminal event")
				}
				runner.operationComplete[event.OperationID] = true
			}
			if err := runner.emit(record); err != nil {
				return err
			}
		}
		if !response.HasMore {
			runner.eventAfter = response.NextResumeToken
			return nil
		}
		after = response.NextResumeToken
		runner.eventAfter = after
	}
	return v1.NewError(v1.CodeUnavailable, "events", "event page limit exceeded before reaching the run boundary")
}

// validateEventEvidence requires every dispatched public lifecycle operation
// to have one owned terminal complete event. The map value separately records
// whether the API returned success for terminal-result consistency checks.
func (runner *evaluator) validateEventEvidence(collectionErr error) error {
	missing := 0
	for operationID := range runner.operationRequired {
		if !runner.operationComplete[operationID] {
			missing++
		}
	}
	runner.eventEvidenceOK = collectionErr == nil && missing == 0
	if missing != 0 {
		return v1.NewError(v1.CodeUnavailable, "events", fmt.Sprintf("%d dispatched operations are missing terminal complete events", missing))
	}
	return nil
}

// newOperation creates and registers the durable operation identity before its first request attempt.
func (runner *evaluator) newOperation(sample sampleContext) (string, error) {
	operationID, err := runner.ids("operation")
	if err != nil {
		return "", v1.WrapError(v1.CodeInternal, "operation_id", "cannot generate identity", false, err)
	}
	if err := v1.ValidateOperationID(operationID); err != nil {
		return "", v1.WrapError(v1.CodeInternal, "operation_id", "generated identity is invalid", false, err)
	}
	if _, exists := runner.operationContexts[operationID]; exists {
		return "", v1.NewError(v1.CodeInternal, "operation_id", "identity generator returned a duplicate operation ID")
	}
	runner.operationContexts[operationID] = sample
	return operationID, nil
}

// newResourceID validates generated Sandbox, Container, and Attempt identities as safe path segments.
func (runner *evaluator) newResourceID(prefix string) (string, error) {
	value, err := runner.ids(prefix)
	if err != nil {
		return "", v1.WrapError(v1.CodeInternal, prefix+"_id", "cannot generate identity", false, err)
	}
	if err := v1.ValidateResourceID(prefix+"_id", value); err != nil {
		return "", v1.WrapError(v1.CodeInternal, prefix+"_id", "generated identity is invalid", false, err)
	}
	if _, exists := runner.resourceIDs[value]; exists {
		return "", v1.NewError(v1.CodeInternal, prefix+"_id", "identity generator returned a duplicate resource ID")
	}
	runner.resourceIDs[value] = struct{}{}
	return value, nil
}

// scenarioIdentity returns the canonical input identity attached to every raw
// line so a same-name scenario edit cannot be confused with prior evidence.
func (runner *evaluator) scenarioIdentity() scenarioIdentity {
	return scenarioIdentity{Name: runner.scenario.Name, Version: runner.scenario.Version, DigestSHA256: runner.scenarioDigest}
}

// emitRunSummary writes the semantic completeness seal even when lifecycle or
// evidence collection failed. Consumers must still require successful outer
// output publication because a stream cannot attest to a later fsync failure.
func (runner *evaluator) emitRunSummary(runErr error) error {
	expected := len(runner.operationRequired)
	completed := 0
	for operationID := range runner.operationRequired {
		if runner.operationComplete[operationID] {
			completed++
		}
	}
	reasons := runner.baselineIneligibilityReasons()
	if runErr != nil {
		reasons = append(reasons, "run_failed")
	}
	if !runner.eventEvidenceOK {
		reasons = append(reasons, "event_evidence_incomplete")
	}
	success := runErr == nil
	summary := runSummary{
		Completed: true, LifecycleSuccess: runner.lifecycleOK, EventEvidenceComplete: runner.eventEvidenceOK,
		ExpectedOperations: expected, CompletedOperations: completed, FormalSamples: runner.formalSamples,
		BaselineEligible: len(reasons) == 0, EvidenceQuality: "debug", IneligibilityReasons: reasons,
	}
	if summary.BaselineEligible {
		summary.EvidenceQuality = "baseline"
	}
	record := rawRecord{
		SchemaVersion: rawRecordSchemaVersion, RecordType: "run_summary", ExperimentID: runner.experimentID,
		Scenario: runner.scenarioIdentity(), Environment: runner.environment,
		Classification: runner.scenario.Classification, SampleIndex: -1, Phase: "run.complete",
		Success: &success, Summary: &summary,
	}
	if runErr != nil {
		detail := stableErrorDetail(runErr)
		record.Error = &detail
	}
	return runner.emit(record)
}

// baselineIneligibilityReasons returns bounded build and scenario reasons that prevent formal baseline use.
func (runner *evaluator) baselineIneligibilityReasons() []string {
	reasons := make([]string, 0, 6)
	daemonBuild := runner.environment.DaemonBuild
	daemonRevisionUsable := v1.UsableVCSRevision(daemonBuild.VCSRevision)
	daemonIdentityUsable := !daemonBuild.Unavailable && daemonRevisionUsable && daemonBuild.VCSModified != nil
	if !daemonIdentityUsable {
		reasons = append(reasons, "daemon_build_identity_unavailable")
	}
	evaluatorBuild := runner.environment.EvaluatorBuild
	evaluatorRevisionUsable := v1.UsableVCSRevision(evaluatorBuild.VCSRevision)
	evaluatorModified, evaluatorModifiedKnown := parseEvaluatorModified(evaluatorBuild.VCSModified)
	if !evaluatorRevisionUsable || !evaluatorModifiedKnown {
		reasons = append(reasons, "evaluator_build_identity_unavailable")
	}
	if daemonRevisionUsable && evaluatorRevisionUsable && daemonBuild.VCSRevision != evaluatorBuild.VCSRevision {
		reasons = append(reasons, "daemon_evaluator_revision_mismatch")
	}
	if daemonBuild.VCSModified != nil && *daemonBuild.VCSModified {
		reasons = append(reasons, "daemon_build_modified")
	}
	if evaluatorModifiedKnown && evaluatorModified {
		reasons = append(reasons, "evaluator_build_modified")
	}
	if scenarioEnvironmentHasPlaceholder(runner.scenario) {
		reasons = append(reasons, "scenario_environment_placeholder")
	}
	if resultKind, declared := runner.scenario.Environment.Labels["result_kind"]; declared && resultKind != "raw-baseline-evidence" {
		reasons = append(reasons, "scenario_declares_debug_evidence")
	}
	return reasons
}

// scenarioEnvironmentHasPlaceholder rejects unresolved environment identity, noise, warmup, label, or cache evidence.
func scenarioEnvironmentHasPlaceholder(input scenario) bool {
	environment := input.Environment
	if placeholderEvidence(environment.ID) || environment.Noise != "none" {
		return true
	}
	validWarmup := input.Classification == "cold" && environment.Warmup == "none" ||
		input.Classification == "warm" && input.WarmupAttempts == 1 && environment.Warmup == "one-complete-unmeasured-attempt-before-formal-samples"
	if !validWarmup {
		return true
	}
	for key, value := range environment.Labels {
		if placeholderEvidence(key) || placeholderEvidence(value) {
			return true
		}
	}
	cache := environment.Cache
	for _, value := range []string{cache.Content, cache.UnpackedChain, cache.Snapshot, cache.PageCache, cache.ImmutableLayers} {
		if placeholderEvidence(value) {
			return true
		}
	}
	return false
}

// placeholderEvidence reports empty, unknown, or replace-prefixed scenario evidence.
func placeholderEvidence(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	return normalized == "" || normalized == unknownEvidence || strings.HasPrefix(normalized, "replace-")
}

// parseEvaluatorModified accepts only the exact boolean spelling emitted by Go build metadata.
func parseEvaluatorModified(value string) (bool, bool) {
	switch value {
	case "false":
		return false, true
	case "true":
		return true, true
	default:
		return false, false
	}
}

// emit writes one complete JSONL record immediately so later failures do not erase prior raw evidence.
func (runner *evaluator) emit(record rawRecord) error {
	if err := runner.encoder.Encode(record); err != nil {
		return v1.WrapError(v1.CodeInternal, "output", "cannot append raw evaluation record", false, err)
	}
	return nil
}

// randomIdentity generates a path-safe 128-bit identifier with a bounded semantic prefix.
func randomIdentity(prefix string) (string, error) {
	value := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, value); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(value), nil
}

// cloneLabels prevents later caller mutation from changing environment evidence already attached to records.
func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

// stableErrorDetail preserves typed API detail and uses bounded messages for transport or internal failures.
func stableErrorDetail(err error) v1.ErrorDetail {
	var remote *client.RemoteError
	if errors.As(err, &remote) {
		return remote.Envelope.Error
	}
	var typed *v1.Error
	if errors.As(err, &typed) {
		return v1.ErrorDetailFrom(typed)
	}
	code := client.CodeOf(err)
	messages := map[v1.ErrorCode]string{
		v1.CodeCanceled:         "evaluation request canceled",
		v1.CodeDeadlineExceeded: "evaluation request deadline exceeded",
		v1.CodeUnavailable:      "mydockerd is unavailable",
		v1.CodeInternal:         "internal evaluation error",
	}
	message := messages[code]
	if message == "" {
		message = string(code)
	}
	return v1.ErrorDetail{Code: code, Message: message, Retryable: code == v1.CodeUnavailable}
}

// invalidArgument constructs a stable evaluation configuration failure for exit status two.
func invalidArgument(field, message string) error {
	return v1.NewError(v1.CodeInvalidArgument, field, message)
}

// writeRunError emits one compact diagnostic to stderr and returns the shared v1 CLI status mapping.
func writeRunError(stderr io.Writer, err error) int {
	detail := stableErrorDetail(err)
	status := v1.ExitStatus(detail.Code)
	_ = json.NewEncoder(stderr).Encode(struct {
		Error      v1.ErrorDetail `json:"error"`
		ExitStatus int            `json:"exit_status"`
	}{Error: detail, ExitStatus: status})
	return status
}

// formatScenarioLabel returns the explicit, non-comparative prepared-rootfs and network tag used by operators.
func formatScenarioLabel(input scenario) string {
	return fmt.Sprintf("prepared-rootfs+%s/%s", input.Sandbox.Network.Mode, input.Classification)
}
