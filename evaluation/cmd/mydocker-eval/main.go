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
	"mydocker/pkg/client"
)

const (
	evaluationSchemaVersion = uint32(1)
	maxScenarioBytes        = int64(1 << 20)
	maxEventPages           = 1024
	defaultEvaluationSocket = "/run/mydocker/mydockerd.sock"
	unknownEvidence         = "unknown"
	maxCommandEvidenceBytes = 1 << 20
)

type evaluationClient interface {
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
	Name    string `json:"name"`
	Version string `json:"version"`
}

type recordedEnvironment struct {
	ID                    string              `json:"id"`
	Labels                map[string]string   `json:"labels,omitempty"`
	GOOS                  string              `json:"goos"`
	GOARCH                string              `json:"goarch"`
	FilesystemProfile     string              `json:"filesystem_profile"`
	NetworkMode           string              `json:"network_mode"`
	ScenarioTag           string              `json:"scenario_tag"`
	Commit                string              `json:"commit"`
	Build                 buildEnvironment    `json:"build"`
	Worktree              worktreeEnvironment `json:"worktree"`
	Kernel                kernelEnvironment   `json:"kernel"`
	Cgroup                cgroupEnvironment   `json:"cgroup"`
	CPU                   cpuEnvironment      `json:"cpu"`
	Memory                memoryEnvironment   `json:"memory"`
	Storage               storageEnvironment  `json:"storage"`
	Cache                 scenarioCacheState  `json:"cache"`
	Concurrency           int                 `json:"concurrency"`
	BackgroundNoise       string              `json:"background_noise"`
	Warmup                string              `json:"warmup"`
	ExperimentStartedAt   time.Time           `json:"experiment_started_at"`
	TimeZone              string              `json:"timezone"`
	TimeZoneOffsetSeconds int                 `json:"timezone_offset_seconds"`
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
	DurationNanoseconds int64               `json:"duration_ns,omitempty"`
	Success             bool                `json:"success"`
	Error               *v1.ErrorDetail     `json:"error,omitempty"`
	Event               *v1.Event           `json:"event,omitempty"`
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
	operationContexts map[string]sampleContext
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
	output, err := openOutput(options.outputPath, stdout)
	if err != nil {
		return writeRunError(stderr, err)
	}
	apiClient, err := factory(client.Config{
		SocketPath:       options.socketPath,
		Timeout:          options.timeout,
		TransportRetries: options.transportRetries,
	})
	if err != nil {
		clientErr := v1.WrapError(v1.CodeInvalidArgument, "client", err.Error(), false, err)
		return writeRunError(stderr, errors.Join(output.Finalize(), clientErr))
	}
	runner := &evaluator{
		client:            apiClient,
		clock:             clock,
		ids:               ids,
		encoder:           json.NewEncoder(output.writer),
		experimentID:      options.experimentID,
		scenario:          loaded,
		environment:       environment,
		operationContexts: make(map[string]sampleContext),
	}
	runErr := runner.Run(ctx)
	finalizeErr := output.Finalize()
	if err := errors.Join(finalizeErr, runErr); err != nil {
		return writeRunError(stderr, err)
	}
	return 0
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
	if input.SchemaVersion != evaluationSchemaVersion {
		return invalidArgument("schema_version", "must be 1")
	}
	if strings.TrimSpace(input.Name) == "" || strings.TrimSpace(input.Version) == "" {
		return invalidArgument("scenario", "name and version are required")
	}
	if input.Classification != "cold" && input.Classification != "warm" {
		return invalidArgument("classification", "must be cold or warm")
	}
	if input.Samples <= 0 || input.Samples > 10000 {
		return invalidArgument("samples", "must be from 1 through 10000")
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
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&loaded); err != nil {
		return scenario{}, v1.WrapError(v1.CodeInvalidArgument, "scenario", "invalid scenario JSON: "+err.Error(), false, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return scenario{}, invalidArgument("scenario", "must contain exactly one JSON value")
		}
		return scenario{}, v1.WrapError(v1.CodeInvalidArgument, "scenario", "invalid trailing JSON data", false, err)
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
	worktree, repositoryCommit := collectWorktreeEnvironment(build)
	commit := knownOrUnknown(build.VCSRevision)
	if commit == unknownEvidence {
		commit = repositoryCommit
	}
	_, timezoneOffset := startedAt.Zone()
	return recordedEnvironment{
		ID:                    input.Environment.ID,
		Labels:                cloneLabels(input.Environment.Labels),
		GOOS:                  runtime.GOOS,
		GOARCH:                runtime.GOARCH,
		FilesystemProfile:     "prepared-rootfs",
		NetworkMode:           input.Sandbox.Network.Mode,
		ScenarioTag:           formatScenarioLabel(input),
		Commit:                knownOrUnknown(commit),
		Build:                 build,
		Worktree:              worktree,
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

// collectWorktreeEnvironment uses bounded read-only Git queries when available and falls back to embedded VCS metadata.
func collectWorktreeEnvironment(build buildEnvironment) (worktreeEnvironment, string) {
	result := worktreeEnvironment{
		Root:                 unknownEvidence,
		Branch:               unknownEvidence,
		Status:               unknownEvidence,
		StatusSHA256:         unknownEvidence,
		TrackedDiffSHA256:    unknownEvidence,
		UntrackedContentNote: "not-hashed; status_sha256 covers untracked paths only",
	}
	commit := knownOrUnknown(build.VCSRevision)
	root, err := runReadOnlyCommand("git", "rev-parse", "--show-toplevel")
	if err != nil {
		if build.VCSModified == "true" {
			result.Status = "dirty"
		} else if build.VCSModified == "false" {
			result.Status = "clean"
		}
		return result, commit
	}
	result.Root = strings.TrimSpace(root)
	if head, commandErr := runReadOnlyCommand("git", "-C", result.Root, "rev-parse", "HEAD"); commandErr == nil {
		commit = knownOrUnknown(strings.TrimSpace(head))
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
	return result, commit
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
	var lifecycleErr error
	if runner.scenario.Classification == "cold" {
		for index := 0; index < runner.scenario.Samples; index++ {
			if err := runner.runColdSample(ctx, index); err != nil {
				lifecycleErr = err
				break
			}
		}
	} else {
		lifecycleErr = runner.runWarmSamples(ctx)
	}
	eventErr := runner.collectEvents(ctx, eventTail)
	return errors.Join(lifecycleErr, eventErr)
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
				return errors.Join(callErr, requireRunning(response, callErr))
			})
		}
	}
	lifecycleErr = runner.finishCallerSpan(sample, "cold.create_sandbox_to_running", operationIDs, span, lifecycleErr)
	cleanupErr := runner.cleanupAttempt(ctx, sample, "", sandboxID, containerID, sandboxMayExist, containerMayExist, containerMayBeRunning, true)
	return errors.Join(lifecycleErr, cleanupErr)
}

// runWarmSamples creates one stable Sandbox, attempts cleanup even after an ambiguous setup response, and measures repeated warm Attempts.
func (runner *evaluator) runWarmSamples(ctx context.Context) error {
	setup := sampleContext{Classification: "warm", Index: -1}
	sandboxID, err := runner.newResourceID("sandbox")
	if err != nil {
		return err
	}
	operationID, err := runner.newOperation(setup)
	if err != nil {
		return err
	}
	setupErr := runner.measure(ctx, setup, "setup.sandbox.create", operationID, func() error {
		_, callErr := runner.client.CreateSandbox(ctx, operationID, v1.CreateSandboxRequest{SandboxID: sandboxID, Spec: runner.scenario.Sandbox})
		return callErr
	})
	if setupErr != nil {
		cleanupErr := runner.cleanupSandbox(ctx, setup, "cleanup.", sandboxID)
		return errors.Join(setupErr, cleanupErr)
	}
	var lifecycleErr error
	for index := 0; index < runner.scenario.Samples; index++ {
		if err := runner.runWarmAttempt(ctx, sandboxID, index); err != nil {
			lifecycleErr = err
			break
		}
	}
	cleanupErr := runner.cleanupSandbox(ctx, setup, "cleanup.", sandboxID)
	return errors.Join(lifecycleErr, cleanupErr)
}

// runWarmAttempt emits one caller-owned CreateContainer-to-Running span and cleans every possibly persisted Attempt by its known ID.
func (runner *evaluator) runWarmAttempt(ctx context.Context, sandboxID string, index int) error {
	sample := sampleContext{Classification: "warm", Index: index}
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
				return errors.Join(callErr, requireRunning(response, callErr))
			})
		}
	}
	lifecycleErr = runner.finishCallerSpan(sample, "warm.create_container_to_running", operationIDs, span, lifecycleErr)
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
	record := rawRecord{
		SchemaVersion:       evaluationSchemaVersion,
		RecordType:          "caller_span",
		ExperimentID:        runner.experimentID,
		Scenario:            scenarioIdentity{Name: runner.scenario.Name, Version: runner.scenario.Version},
		Environment:         runner.environment,
		Classification:      sample.Classification,
		SampleIndex:         sample.Index,
		Phase:               phase,
		OperationIDs:        append([]string(nil), operationIDs...),
		StartedAt:           &span.started,
		DurationNanoseconds: finished.Sub(span.started).Nanoseconds(),
		Success:             callErr == nil,
	}
	if callErr != nil {
		detail := stableErrorDetail(callErr)
		record.Error = &detail
	}
	return errors.Join(callErr, runner.emit(record))
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
	callErr := call()
	finished := runner.clock.Now()
	record := rawRecord{
		SchemaVersion:       evaluationSchemaVersion,
		RecordType:          "caller_span",
		ExperimentID:        runner.experimentID,
		Scenario:            scenarioIdentity{Name: runner.scenario.Name, Version: runner.scenario.Version},
		Environment:         runner.environment,
		Classification:      sample.Classification,
		SampleIndex:         sample.Index,
		Phase:               phase,
		OperationID:         operationID,
		StartedAt:           &started,
		DurationNanoseconds: finished.Sub(started).Nanoseconds(),
		Success:             callErr == nil,
	}
	if callErr != nil {
		detail := stableErrorDetail(callErr)
		record.Error = &detail
	}
	emitErr := runner.emit(record)
	return errors.Join(callErr, emitErr)
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
func (runner *evaluator) collectEvents(ctx context.Context, after v1.ResumeToken) error {
	sample := sampleContext{Classification: runner.scenario.Classification, Index: -1}
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
				SchemaVersion:  evaluationSchemaVersion,
				RecordType:     "stage_event",
				ExperimentID:   runner.experimentID,
				Scenario:       scenarioIdentity{Name: runner.scenario.Name, Version: runner.scenario.Version},
				Environment:    runner.environment,
				Classification: contextForOperation.Classification,
				SampleIndex:    contextForOperation.Index,
				Phase:          event.Stage,
				OperationID:    event.OperationID,
				Success:        event.Result == "succeeded" || event.Result == "noop",
				Event:          &event,
			}
			if err := runner.emit(record); err != nil {
				return err
			}
		}
		if !response.HasMore {
			return nil
		}
		after = response.NextResumeToken
	}
	return v1.NewError(v1.CodeUnavailable, "events", "event page limit exceeded before reaching the run boundary")
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
	return value, nil
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
