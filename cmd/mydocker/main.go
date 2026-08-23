package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/pkg/client"
)

const (
	defaultSocketPath = "/run/mydocker/mydockerd.sock"
	maxInputBytes     = int64(1 << 20)
)

type runtimeClient interface {
	CreateSandbox(context.Context, string, v1.CreateSandboxRequest) (v1.SandboxResponse, error)
	StopSandbox(context.Context, string, string) (v1.SandboxResponse, error)
	DeleteSandbox(context.Context, string, string) (v1.OperationResponse, error)
	GetSandbox(context.Context, string) (v1.SandboxResponse, error)
	ListSandboxes(context.Context) (v1.SandboxListResponse, error)
	CreateContainer(context.Context, string, string, v1.CreateContainerRequest) (v1.ContainerResponse, error)
	StartContainer(context.Context, string, string) (v1.ContainerResponse, error)
	KillContainer(context.Context, string, string, v1.TerminationPolicy) (v1.ContainerResponse, error)
	DeleteContainer(context.Context, string, string) (v1.OperationResponse, error)
	GetContainer(context.Context, string) (v1.ContainerResponse, error)
	ListContainers(context.Context, string) (v1.ContainerListResponse, error)
	GetOperation(context.Context, string) (v1.OperationResponse, error)
	Events(context.Context, v1.ResumeToken, int) (v1.EventListResponse, error)
	Logs(context.Context, string, string, v1.LogCursor, int) (v1.LogListResponse, error)
}

type clientFactory func(client.Config) (runtimeClient, error)

type globalOptions struct {
	socketPath       string
	timeout          time.Duration
	transportRetries int
	operationID      string
}

type commandOutcome struct {
	value       any
	operationID string
	err         error
}

type errorOutput struct {
	Error       v1.ErrorDetail `json:"error"`
	OperationID string         `json:"operation_id,omitempty"`
	ExitStatus  int            `json:"exit_status"`
}

// main confines the process entrypoint to wiring standard streams and returning the stable v1 status.
func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr, newAPIClient, newOperationID))
}

// newAPIClient constructs the public UDS client without performing a daemon connection during parsing.
func newAPIClient(config client.Config) (runtimeClient, error) {
	return client.New(config)
}

// run parses one command, invokes only the public client contract, and emits exactly one JSON result or error.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, factory clientFactory, idSource func() (string, error)) int {
	options, command, err := parseGlobalOptions(args)
	if err != nil {
		return writeError(stderr, "", err)
	}
	apiClient, err := factory(client.Config{
		SocketPath:       options.socketPath,
		Timeout:          options.timeout,
		TransportRetries: options.transportRetries,
	})
	if err != nil {
		return writeError(stderr, "", v1.WrapError(v1.CodeInvalidArgument, "client", err.Error(), false, err))
	}
	outcome := executeCommand(ctx, apiClient, command, options.operationID, stdin, idSource)
	if outcome.err != nil {
		return writeError(stderr, outcome.operationID, outcome.err)
	}
	if err := writeJSON(stdout, outcome.value); err != nil {
		return writeError(stderr, outcome.operationID, err)
	}
	return 0
}

// parseGlobalOptions accepts transport and idempotency flags only before the resource/action vocabulary.
func parseGlobalOptions(args []string) (globalOptions, []string, error) {
	options := globalOptions{}
	flags := flag.NewFlagSet("mydocker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.socketPath, "socket", defaultSocketPath, "absolute mydockerd Unix socket path")
	flags.DurationVar(&options.timeout, "timeout", 30*time.Second, "per-request timeout")
	flags.IntVar(&options.transportRetries, "transport-retries", 1, "bounded response-loss retries")
	flags.StringVar(&options.operationID, "operation-id", "", "durable mutation identity")
	if err := flags.Parse(args); err != nil {
		return globalOptions{}, nil, invalidArgument("arguments", err.Error())
	}
	if !filepath.IsAbs(options.socketPath) {
		return globalOptions{}, nil, invalidArgument("socket", "must be an absolute path")
	}
	if options.timeout <= 0 {
		return globalOptions{}, nil, invalidArgument("timeout", "must be greater than zero")
	}
	if options.transportRetries < 0 {
		return globalOptions{}, nil, invalidArgument("transport-retries", "must not be negative")
	}
	if options.operationID != "" {
		if err := v1.ValidateOperationID(options.operationID); err != nil {
			return globalOptions{}, nil, err
		}
	}
	if flags.NArg() == 0 {
		return globalOptions{}, nil, invalidArgument("command", "expected a resource and action")
	}
	return options, flags.Args(), nil
}

// executeCommand dispatches the bounded v1 CLI vocabulary without interpreting process argv or environment values.
func executeCommand(ctx context.Context, apiClient runtimeClient, args []string, explicitOperationID string, stdin io.Reader, idSource func() (string, error)) commandOutcome {
	if explicitOperationID != "" && !isMutationCommand(args) {
		return commandOutcome{err: invalidArgument("operation-id", "must be omitted for read-only commands")}
	}
	switch args[0] {
	case "sandbox":
		return executeSandbox(ctx, apiClient, args[1:], explicitOperationID, stdin, idSource)
	case "container":
		return executeContainer(ctx, apiClient, args[1:], explicitOperationID, stdin, idSource)
	case "operation":
		return executeOperation(ctx, apiClient, args[1:])
	case "events":
		return executeEvents(ctx, apiClient, args[1:])
	case "logs":
		return executeLogs(ctx, apiClient, args[1:])
	default:
		return commandOutcome{err: invalidArgument("command", fmt.Sprintf("unsupported resource %q", args[0]))}
	}
}

// isMutationCommand identifies the exact action forms that consume a durable operation identity.
func isMutationCommand(args []string) bool {
	if len(args) < 2 {
		return false
	}
	switch args[0] + " " + args[1] {
	case "sandbox create", "sandbox stop", "sandbox delete",
		"container create", "container start", "container kill", "container delete":
		return true
	default:
		return false
	}
}

// executeSandbox validates one Sandbox action and creates an operation identity only for mutations.
func executeSandbox(ctx context.Context, apiClient runtimeClient, args []string, explicitOperationID string, stdin io.Reader, idSource func() (string, error)) commandOutcome {
	if len(args) == 0 {
		return commandOutcome{err: invalidArgument("sandbox.action", "is required")}
	}
	switch args[0] {
	case "create":
		inputPath, err := parseInputAction("sandbox create", args[1:])
		if err != nil {
			return commandOutcome{err: err}
		}
		var input v1.CreateSandboxRequest
		if err := readJSONInput(inputPath, stdin, &input); err != nil {
			return commandOutcome{err: err}
		}
		if err := input.Validate(); err != nil {
			return commandOutcome{err: err}
		}
		operationID, err := mutationOperationID(explicitOperationID, idSource)
		if err != nil {
			return commandOutcome{err: err}
		}
		response, err := apiClient.CreateSandbox(ctx, operationID, input)
		return commandOutcome{value: response, operationID: operationID, err: err}
	case "get":
		id, err := parseSingleID("sandbox get", "sandbox_id", args[1:])
		if err != nil {
			return commandOutcome{err: err}
		}
		response, err := apiClient.GetSandbox(ctx, id)
		return commandOutcome{value: response, err: err}
	case "list":
		if len(args) != 1 {
			return commandOutcome{err: invalidArgument("sandbox list", "does not accept positional arguments")}
		}
		response, err := apiClient.ListSandboxes(ctx)
		return commandOutcome{value: response, err: err}
	case "stop", "delete":
		id, err := parseSingleID("sandbox "+args[0], "sandbox_id", args[1:])
		if err != nil {
			return commandOutcome{err: err}
		}
		operationID, err := mutationOperationID(explicitOperationID, idSource)
		if err != nil {
			return commandOutcome{err: err}
		}
		if args[0] == "stop" {
			response, callErr := apiClient.StopSandbox(ctx, operationID, id)
			return commandOutcome{value: response, operationID: operationID, err: callErr}
		}
		response, callErr := apiClient.DeleteSandbox(ctx, operationID, id)
		return commandOutcome{value: response, operationID: operationID, err: callErr}
	default:
		return commandOutcome{err: invalidArgument("sandbox.action", fmt.Sprintf("unsupported action %q", args[0]))}
	}
}

// executeContainer validates one Attempt action while preserving JSON argv and environment as supplied.
func executeContainer(ctx context.Context, apiClient runtimeClient, args []string, explicitOperationID string, stdin io.Reader, idSource func() (string, error)) commandOutcome {
	if len(args) == 0 {
		return commandOutcome{err: invalidArgument("container.action", "is required")}
	}
	switch args[0] {
	case "create":
		sandboxID, inputPath, err := parseContainerCreate(args[1:])
		if err != nil {
			return commandOutcome{err: err}
		}
		var input v1.CreateContainerRequest
		if err := readJSONInput(inputPath, stdin, &input); err != nil {
			return commandOutcome{err: err}
		}
		input.SandboxID = sandboxID
		if err := input.Validate(); err != nil {
			return commandOutcome{err: err}
		}
		operationID, err := mutationOperationID(explicitOperationID, idSource)
		if err != nil {
			return commandOutcome{err: err}
		}
		response, err := apiClient.CreateContainer(ctx, operationID, sandboxID, input)
		return commandOutcome{value: response, operationID: operationID, err: err}
	case "get":
		id, err := parseSingleID("container get", "container_id", args[1:])
		if err != nil {
			return commandOutcome{err: err}
		}
		response, err := apiClient.GetContainer(ctx, id)
		return commandOutcome{value: response, err: err}
	case "list":
		sandboxID, err := parseSingleID("container list", "sandbox_id", args[1:])
		if err != nil {
			return commandOutcome{err: err}
		}
		response, err := apiClient.ListContainers(ctx, sandboxID)
		return commandOutcome{value: response, err: err}
	case "start", "delete":
		id, err := parseSingleID("container "+args[0], "container_id", args[1:])
		if err != nil {
			return commandOutcome{err: err}
		}
		operationID, err := mutationOperationID(explicitOperationID, idSource)
		if err != nil {
			return commandOutcome{err: err}
		}
		if args[0] == "start" {
			response, callErr := apiClient.StartContainer(ctx, operationID, id)
			return commandOutcome{value: response, operationID: operationID, err: callErr}
		}
		response, callErr := apiClient.DeleteContainer(ctx, operationID, id)
		return commandOutcome{value: response, operationID: operationID, err: callErr}
	case "kill":
		containerID, policy, err := parseKill(args[1:])
		if err != nil {
			return commandOutcome{err: err}
		}
		if err := (v1.KillContainerRequest{ContainerID: containerID, Policy: policy}).Validate(); err != nil {
			return commandOutcome{err: err}
		}
		operationID, err := mutationOperationID(explicitOperationID, idSource)
		if err != nil {
			return commandOutcome{err: err}
		}
		response, callErr := apiClient.KillContainer(ctx, operationID, containerID, policy)
		return commandOutcome{value: response, operationID: operationID, err: callErr}
	default:
		return commandOutcome{err: invalidArgument("container.action", fmt.Sprintf("unsupported action %q", args[0]))}
	}
}

// executeOperation performs the read-only durable operation lookup form.
func executeOperation(ctx context.Context, apiClient runtimeClient, args []string) commandOutcome {
	if len(args) != 2 || args[0] != "get" {
		return commandOutcome{err: invalidArgument("operation", "expected: operation get OPERATION_ID")}
	}
	if err := v1.ValidateOperationID(args[1]); err != nil {
		return commandOutcome{err: err}
	}
	response, err := apiClient.GetOperation(ctx, args[1])
	return commandOutcome{value: response, err: err}
}

// executeEvents parses bounded resume pagination without creating a lifecycle operation.
func executeEvents(ctx context.Context, apiClient runtimeClient, args []string) commandOutcome {
	flags := flag.NewFlagSet("events", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	after := flags.String("after", "", "opaque resume token")
	limit := flags.Int("limit", 100, "page size")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		if err == nil {
			err = errors.New("unexpected positional arguments")
		}
		return commandOutcome{err: invalidArgument("events", err.Error())}
	}
	if _, err := v1.ParseResumeToken(v1.ResumeToken(*after)); err != nil {
		return commandOutcome{err: err}
	}
	if *limit <= 0 || *limit > 500 {
		return commandOutcome{err: invalidArgument("limit", "must be from 1 through 500")}
	}
	response, err := apiClient.Events(ctx, v1.ResumeToken(*after), *limit)
	return commandOutcome{value: response, err: err}
}

// executeLogs parses identity-bound log pagination without exposing daemon filesystem paths.
func executeLogs(ctx context.Context, apiClient runtimeClient, args []string) commandOutcome {
	flags := flag.NewFlagSet("logs", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	attemptID := flags.String("attempt-id", "", "exact Attempt identity")
	after := flags.String("after", "", "opaque log cursor")
	limit := flags.Int("limit", 100, "page size")
	if err := flags.Parse(args); err != nil {
		return commandOutcome{err: invalidArgument("logs", err.Error())}
	}
	if flags.NArg() != 1 {
		return commandOutcome{err: invalidArgument("logs", "expected: logs [flags] CONTAINER_ID")}
	}
	containerID := flags.Arg(0)
	if err := v1.ValidateResourceID("container_id", containerID); err != nil {
		return commandOutcome{err: err}
	}
	if err := v1.ValidateResourceID("attempt_id", *attemptID); err != nil {
		return commandOutcome{err: err}
	}
	if _, err := v1.ParseLogCursor(v1.LogCursor(*after), containerID, *attemptID); err != nil {
		return commandOutcome{err: err}
	}
	if *limit <= 0 || *limit > 100 {
		return commandOutcome{err: invalidArgument("limit", "must be from 1 through 100")}
	}
	response, err := apiClient.Logs(ctx, containerID, *attemptID, v1.LogCursor(*after), *limit)
	return commandOutcome{value: response, err: err}
}

// parseInputAction requires one JSON source and rejects all unrelated action flags.
func parseInputAction(name string, args []string) (string, error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input := flags.String("input", "-", "strict JSON request file or - for stdin")
	if err := flags.Parse(args); err != nil {
		return "", invalidArgument(name, err.Error())
	}
	if flags.NArg() != 0 {
		return "", invalidArgument(name, "does not accept positional arguments")
	}
	return *input, nil
}

// parseContainerCreate requires the parent Sandbox ID plus one strict JSON request source.
func parseContainerCreate(args []string) (string, string, error) {
	flags := flag.NewFlagSet("container create", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input := flags.String("input", "-", "strict JSON request file or - for stdin")
	if err := flags.Parse(args); err != nil {
		return "", "", invalidArgument("container create", err.Error())
	}
	if flags.NArg() != 1 {
		return "", "", invalidArgument("container create", "expected one SANDBOX_ID")
	}
	if err := v1.ValidateResourceID("sandbox_id", flags.Arg(0)); err != nil {
		return "", "", err
	}
	return flags.Arg(0), *input, nil
}

// parseKill requires one Container identity and a complete explicit graceful termination policy.
func parseKill(args []string) (string, v1.TerminationPolicy, error) {
	flags := flag.NewFlagSet("container kill", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	signal := flags.String("signal", "", "graceful signal name")
	gracePeriod := flags.String("grace-period", "", "explicit non-negative duration such as 5s or 0s")
	escalationSignal := flags.String("escalation-signal", "", "post-grace escalation signal name")
	if err := flags.Parse(args); err != nil {
		return "", v1.TerminationPolicy{}, invalidArgument("container kill", err.Error())
	}
	if flags.NArg() != 1 {
		return "", v1.TerminationPolicy{}, invalidArgument("container kill", "expected one CONTAINER_ID")
	}
	if err := v1.ValidateResourceID("container_id", flags.Arg(0)); err != nil {
		return "", v1.TerminationPolicy{}, err
	}
	if *signal == "" || *gracePeriod == "" || *escalationSignal == "" {
		return "", v1.TerminationPolicy{}, invalidArgument("container kill", "--signal, --grace-period, and --escalation-signal are all required")
	}
	duration, err := time.ParseDuration(*gracePeriod)
	if err != nil || duration < 0 {
		return "", v1.TerminationPolicy{}, invalidArgument("grace-period", "must be an explicit non-negative Go duration")
	}
	return flags.Arg(0), v1.TerminationPolicy{
		Signal:                 *signal,
		GracePeriodNanoseconds: duration.Nanoseconds(),
		EscalationSignal:       *escalationSignal,
	}, nil
}

// parseSingleID enforces the exact positional form used by simple resource actions.
func parseSingleID(name, field string, args []string) (string, error) {
	if len(args) != 1 {
		return "", invalidArgument(name, "expected exactly one resource ID")
	}
	if err := v1.ValidateResourceID(field, args[0]); err != nil {
		return "", err
	}
	return args[0], nil
}

// readJSONInput decodes one bounded object from stdin or an explicitly selected file with unknown-field rejection.
func readJSONInput(path string, stdin io.Reader, destination any) error {
	reader := stdin
	var file *os.File
	if path != "-" {
		opened, err := os.Open(path)
		if err != nil {
			return v1.WrapError(v1.CodeInvalidArgument, "input", "cannot open JSON input", false, err)
		}
		file = opened
		defer file.Close()
		reader = file
	}
	payload, err := io.ReadAll(io.LimitReader(reader, maxInputBytes+1))
	if err != nil {
		return v1.WrapError(v1.CodeInvalidArgument, "input", "cannot read JSON input", false, err)
	}
	if int64(len(payload)) > maxInputBytes {
		return invalidArgument("input", "exceeds the 1 MiB limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return v1.WrapError(v1.CodeInvalidArgument, "input", "invalid JSON request: "+err.Error(), false, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return invalidArgument("input", "must contain exactly one JSON value")
		}
		return v1.WrapError(v1.CodeInvalidArgument, "input", "invalid trailing JSON data", false, err)
	}
	return nil
}

// mutationOperationID validates an explicit identity or generates one before the first mutation attempt.
func mutationOperationID(explicit string, source func() (string, error)) (string, error) {
	if explicit != "" {
		return explicit, v1.ValidateOperationID(explicit)
	}
	operationID, err := source()
	if err != nil {
		return "", v1.WrapError(v1.CodeInternal, "operation_id", "cannot generate operation identity", false, err)
	}
	if err := v1.ValidateOperationID(operationID); err != nil {
		return "", v1.WrapError(v1.CodeInternal, "operation_id", "generated operation identity is invalid", false, err)
	}
	return operationID, nil
}

// newOperationID returns a cryptographically random 128-bit mutation identity suitable for safe retry reuse.
func newOperationID() (string, error) {
	value := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, value); err != nil {
		return "", err
	}
	return "op-" + hex.EncodeToString(value), nil
}

// invalidArgument creates one stable usage/representation failure for exit status two.
func invalidArgument(field, message string) error {
	return v1.NewError(v1.CodeInvalidArgument, field, message)
}

// writeJSON emits one compact JSON value followed by a newline for machine-readable command composition.
func writeJSON(writer io.Writer, value any) error {
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return v1.WrapError(v1.CodeInternal, "output", "cannot encode JSON output", false, err)
	}
	return nil
}

// writeError maps typed API and transport failures to the stable v1 process status and JSON error shape.
func writeError(writer io.Writer, operationID string, err error) int {
	code := client.CodeOf(err)
	detail := errorDetail(err, code)
	status := v1.ExitStatus(code)
	if encodeErr := json.NewEncoder(writer).Encode(errorOutput{Error: detail, OperationID: operationID, ExitStatus: status}); encodeErr != nil {
		return v1.ExitStatus(v1.CodeInternal)
	}
	return status
}

// errorDetail preserves validated remote detail and bounds all locally produced diagnostic categories.
func errorDetail(err error, code v1.ErrorCode) v1.ErrorDetail {
	var remote *client.RemoteError
	if errors.As(err, &remote) {
		return remote.Envelope.Error
	}
	var local *v1.Error
	if errors.As(err, &local) {
		return v1.ErrorDetailFrom(local)
	}
	messages := map[v1.ErrorCode]string{
		v1.CodeCanceled:         "request canceled",
		v1.CodeDeadlineExceeded: "request deadline exceeded",
		v1.CodeUnavailable:      "mydockerd is unavailable",
		v1.CodeInternal:         "internal client error",
	}
	message := messages[code]
	if message == "" {
		message = string(code)
	}
	return v1.ErrorDetail{Code: code, Message: message, Retryable: code == v1.CodeUnavailable}
}
