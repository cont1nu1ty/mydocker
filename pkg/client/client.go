package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	v1 "mydocker/api/runtime/v1"
	"mydocker/internal/strictjson"
)

const (
	defaultTimeout          = 30 * time.Second
	defaultDialTimeout      = 5 * time.Second
	defaultMaxResponseBytes = int64(32 << 20)
)

// Config controls the local socket, deadlines, bounded retries, and response limit.
type Config struct {
	SocketPath       string
	Timeout          time.Duration
	DialTimeout      time.Duration
	TransportRetries int
	MaxResponseBytes int64
}

// Client is safe for concurrent calls and retains no operation identity between calls.
type Client struct {
	httpClient       *http.Client
	transport        *http.Transport
	transportRetries int
	maxResponseBytes int64
	requestID        func() (string, error)
}

// New creates a strict local client whose caller must supply every mutation operation ID.
func New(config Config) (*Client, error) {
	config = clientDefaults(config)
	if !filepath.IsAbs(config.SocketPath) {
		return nil, errors.New("client Unix socket path must be absolute")
	}
	if config.Timeout <= 0 || config.DialTimeout <= 0 || config.MaxResponseBytes <= 0 || config.TransportRetries < 0 {
		return nil, errors.New("client timeouts and response limit must be positive and retries must not be negative")
	}
	dialer := &net.Dialer{Timeout: config.DialTimeout}
	transport := &http.Transport{
		DisableCompression: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", config.SocketPath)
		},
	}
	return newWithTransport(config, transport, newRequestID), nil
}

// clientDefaults fills zero-valued bounds without changing explicitly selected retry behavior.
func clientDefaults(config Config) Config {
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = defaultDialTimeout
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = defaultMaxResponseBytes
	}
	return config
}

// newWithTransport builds a client around an injected transport and request-ID source for deterministic tests.
func newWithTransport(config Config, roundTripper http.RoundTripper, requestID func() (string, error)) *Client {
	client := &Client{
		httpClient:       &http.Client{Transport: roundTripper, Timeout: config.Timeout},
		transportRetries: config.TransportRetries,
		maxResponseBytes: config.MaxResponseBytes,
		requestID:        requestID,
	}
	if transport, ok := roundTripper.(*http.Transport); ok {
		client.transport = transport
	}
	return client
}

// newRequestID creates a random transport-attempt identity without creating a durable operation ID.
func newRequestID() (string, error) {
	value := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, value); err != nil {
		return "", fmt.Errorf("generate request ID: %w", err)
	}
	return "req-" + hex.EncodeToString(value), nil
}

// CloseIdleConnections releases pooled local socket connections without affecting daemon-owned workloads.
func (c *Client) CloseIdleConnections() {
	if c == nil {
		return
	}
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	} else if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
}

// CreateSandbox submits immutable Sandbox intent using the caller's durable operation ID.
func (c *Client) CreateSandbox(ctx context.Context, operationID string, input v1.CreateSandboxRequest) (v1.SandboxResponse, error) {
	if err := input.Validate(); err != nil {
		return v1.SandboxResponse{}, err
	}
	var response v1.SandboxResponse
	if err := c.doJSON(ctx, http.MethodPost, v1.BasePath+"/sandboxes", operationID, input, &response); err != nil {
		return v1.SandboxResponse{}, err
	}
	if err := validateSandboxCall(response, input.SandboxID, operationID, "create", "ready"); err != nil {
		return v1.SandboxResponse{}, err
	}
	return response, nil
}

// StopSandbox requests a legal stop transition while preserving operation identity across retries.
func (c *Client) StopSandbox(ctx context.Context, operationID, sandboxID string) (v1.SandboxResponse, error) {
	path, err := resourcePath(v1.BasePath+"/sandboxes/", sandboxID, ":stop")
	if err != nil {
		return v1.SandboxResponse{}, err
	}
	var response v1.SandboxResponse
	if err = c.doJSON(ctx, http.MethodPost, path, operationID, v1.StopSandboxRequest{}, &response); err != nil {
		return v1.SandboxResponse{}, err
	}
	if err = validateSandboxCall(response, sandboxID, operationID, "stop", "stopped"); err != nil {
		return v1.SandboxResponse{}, err
	}
	return response, nil
}

// DeleteSandbox requests verified cleanup and metadata removal for one stopped Sandbox.
func (c *Client) DeleteSandbox(ctx context.Context, operationID, sandboxID string) (v1.OperationResponse, error) {
	path, err := resourcePath(v1.BasePath+"/sandboxes/", sandboxID, "")
	if err != nil {
		return v1.OperationResponse{}, err
	}
	var response v1.OperationResponse
	if err = c.doJSON(ctx, http.MethodDelete, path, operationID, v1.DeleteSandboxRequest{}, &response); err != nil {
		return v1.OperationResponse{}, err
	}
	if err = validateMutationOperation(response.Operation, operationID, "delete", "sandbox", sandboxID); err != nil {
		return v1.OperationResponse{}, err
	}
	return response, nil
}

// GetSandbox returns the current authoritative projection without creating an operation.
func (c *Client) GetSandbox(ctx context.Context, sandboxID string) (v1.SandboxResponse, error) {
	path, err := resourcePath(v1.BasePath+"/sandboxes/", sandboxID, "")
	if err != nil {
		return v1.SandboxResponse{}, err
	}
	var response v1.SandboxResponse
	if err = c.doJSON(ctx, http.MethodGet, path, "", nil, &response); err != nil {
		return v1.SandboxResponse{}, err
	}
	if err = validateSandboxCall(response, sandboxID, "", ""); err != nil {
		return v1.SandboxResponse{}, err
	}
	return response, nil
}

// ListSandboxes returns one deterministic server snapshot without creating an operation.
func (c *Client) ListSandboxes(ctx context.Context) (v1.SandboxListResponse, error) {
	var response v1.SandboxListResponse
	if err := c.doJSON(ctx, http.MethodGet, v1.BasePath+"/sandboxes", "", nil, &response); err != nil {
		return v1.SandboxListResponse{}, err
	}
	return response, nil
}

// CreateContainer creates one immutable Container/Attempt pair under a Ready Sandbox.
func (c *Client) CreateContainer(ctx context.Context, operationID, sandboxID string, input v1.CreateContainerRequest) (v1.ContainerResponse, error) {
	input.SandboxID = sandboxID
	if err := input.Validate(); err != nil {
		return v1.ContainerResponse{}, err
	}
	path, err := resourcePath(v1.BasePath+"/sandboxes/", sandboxID, "/containers")
	if err != nil {
		return v1.ContainerResponse{}, err
	}
	var response v1.ContainerResponse
	if err = c.doJSON(ctx, http.MethodPost, path, operationID, input, &response); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err = validateContainerCall(response, input.ContainerID, input.SandboxID, input.AttemptID, operationID, "create", "created"); err != nil {
		return v1.ContainerResponse{}, err
	}
	return response, nil
}

// StartContainer releases a prepared Attempt without changing its immutable create spec.
func (c *Client) StartContainer(ctx context.Context, operationID, containerID string) (v1.ContainerResponse, error) {
	path, err := resourcePath(v1.BasePath+"/containers/", containerID, ":start")
	if err != nil {
		return v1.ContainerResponse{}, err
	}
	var response v1.ContainerResponse
	if err = c.doJSON(ctx, http.MethodPost, path, operationID, v1.StartContainerRequest{}, &response); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err = validateContainerCall(response, containerID, "", "", operationID, "start", "running", "stopped"); err != nil {
		return v1.ContainerResponse{}, err
	}
	return response, nil
}

// KillContainer requests one complete explicit graceful policy for the Attempt's verified wrapper identity.
func (c *Client) KillContainer(ctx context.Context, operationID, containerID string, policy v1.TerminationPolicy) (v1.ContainerResponse, error) {
	input := v1.KillContainerRequest{ContainerID: containerID, Policy: policy}
	if err := input.Validate(); err != nil {
		return v1.ContainerResponse{}, err
	}
	path, err := resourcePath(v1.BasePath+"/containers/", containerID, ":kill")
	if err != nil {
		return v1.ContainerResponse{}, err
	}
	var response v1.ContainerResponse
	if err = c.doJSON(ctx, http.MethodPost, path, operationID, input, &response); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err = validateContainerCall(response, containerID, "", "", operationID, "kill", "stopped"); err != nil {
		return v1.ContainerResponse{}, err
	}
	return response, nil
}

// DeleteContainer requests verified teardown and atomic Container/Attempt metadata removal.
func (c *Client) DeleteContainer(ctx context.Context, operationID, containerID string) (v1.OperationResponse, error) {
	path, err := resourcePath(v1.BasePath+"/containers/", containerID, "")
	if err != nil {
		return v1.OperationResponse{}, err
	}
	var response v1.OperationResponse
	if err = c.doJSON(ctx, http.MethodDelete, path, operationID, v1.DeleteContainerRequest{}, &response); err != nil {
		return v1.OperationResponse{}, err
	}
	if err = validateMutationOperation(response.Operation, operationID, "delete", "container", containerID); err != nil {
		return v1.OperationResponse{}, err
	}
	return response, nil
}

// GetContainer returns one authoritative Container/Attempt projection without creating an operation.
func (c *Client) GetContainer(ctx context.Context, containerID string) (v1.ContainerResponse, error) {
	path, err := resourcePath(v1.BasePath+"/containers/", containerID, "")
	if err != nil {
		return v1.ContainerResponse{}, err
	}
	var response v1.ContainerResponse
	if err = c.doJSON(ctx, http.MethodGet, path, "", nil, &response); err != nil {
		return v1.ContainerResponse{}, err
	}
	if err = validateContainerCall(response, containerID, "", "", "", ""); err != nil {
		return v1.ContainerResponse{}, err
	}
	return response, nil
}

// ListContainers returns deterministic Container order within one Sandbox.
func (c *Client) ListContainers(ctx context.Context, sandboxID string) (v1.ContainerListResponse, error) {
	path, err := resourcePath(v1.BasePath+"/sandboxes/", sandboxID, "/containers")
	if err != nil {
		return v1.ContainerListResponse{}, err
	}
	var response v1.ContainerListResponse
	if err = c.doJSON(ctx, http.MethodGet, path, "", nil, &response); err != nil {
		return v1.ContainerListResponse{}, err
	}
	for _, container := range response.Containers {
		if container.SandboxID != sandboxID {
			return v1.ContainerListResponse{}, errors.New("mydocker API returned a Container from a different Sandbox")
		}
	}
	return response, nil
}

// GetOperation looks up the retained result or resumable stage for an operation ID.
func (c *Client) GetOperation(ctx context.Context, operationID string) (v1.OperationResponse, error) {
	if err := v1.ValidateOperationID(operationID); err != nil {
		return v1.OperationResponse{}, err
	}
	path := v1.BasePath + "/operations/" + url.PathEscape(operationID)
	var response v1.OperationResponse
	if err := c.doJSON(ctx, http.MethodGet, path, "", nil, &response); err != nil {
		return v1.OperationResponse{}, err
	}
	if response.Operation.ID != operationID {
		return v1.OperationResponse{}, errors.New("mydocker API returned a different operation identity")
	}
	return response, nil
}

// Events returns one strictly ordered page and its opaque resume token.
func (c *Client) Events(ctx context.Context, after v1.ResumeToken, limit int) (v1.EventListResponse, error) {
	if _, err := v1.ParseResumeToken(after); err != nil {
		return v1.EventListResponse{}, err
	}
	if limit <= 0 || limit > 500 {
		return v1.EventListResponse{}, v1.NewError(v1.CodeInvalidArgument, "limit", "must be from 1 through 500")
	}
	query := make(url.Values)
	if after != "" {
		query.Set("after", string(after))
	}
	query.Set("limit", strconv.Itoa(limit))
	var response v1.EventListResponse
	err := c.doJSON(ctx, http.MethodGet, v1.BasePath+"/events?"+query.Encode(), "", nil, &response)
	if err != nil {
		return v1.EventListResponse{}, err
	}
	if err := validateEventResponse(after, limit, response); err != nil {
		return v1.EventListResponse{}, err
	}
	return response, nil
}

// Logs returns one ordered stdout/stderr page for an exact Container/Attempt identity.
func (c *Client) Logs(ctx context.Context, containerID, attemptID string, after v1.LogCursor, limit int) (v1.LogListResponse, error) {
	if err := v1.ValidateResourceID("container_id", containerID); err != nil {
		return v1.LogListResponse{}, err
	}
	if err := v1.ValidateResourceID("attempt_id", attemptID); err != nil {
		return v1.LogListResponse{}, err
	}
	if _, err := v1.ParseLogCursor(after, containerID, attemptID); err != nil {
		return v1.LogListResponse{}, err
	}
	if limit <= 0 || limit > 100 {
		return v1.LogListResponse{}, v1.NewError(v1.CodeInvalidArgument, "limit", "must be from 1 through 100")
	}
	path, err := resourcePath(v1.BasePath+"/containers/", containerID, "/logs")
	if err != nil {
		return v1.LogListResponse{}, err
	}
	query := make(url.Values)
	query.Set("attempt_id", attemptID)
	if after != "" {
		query.Set("after", string(after))
	}
	query.Set("limit", strconv.Itoa(limit))
	var response v1.LogListResponse
	if err := c.doJSON(ctx, http.MethodGet, path+"?"+query.Encode(), "", nil, &response); err != nil {
		return v1.LogListResponse{}, err
	}
	if err := validateLogResponse(containerID, attemptID, after, limit, response); err != nil {
		return v1.LogListResponse{}, err
	}
	return response, nil
}

// resourcePath validates one public resource identity before escaping it as a path segment.
func resourcePath(prefix, id, suffix string) (string, error) {
	if err := v1.ValidateResourceID("resource_id", id); err != nil {
		return "", err
	}
	return prefix + url.PathEscape(id) + suffix, nil
}

// doJSON reuses the exact caller operation ID on every transport retry while assigning a fresh request ID.
func (c *Client) doJSON(ctx context.Context, method, path, operationID string, input, output any) error {
	if c == nil || c.httpClient == nil || c.requestID == nil {
		return errors.New("client is not initialized")
	}
	mutation := method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete
	if mutation {
		if err := v1.ValidateOperationID(operationID); err != nil {
			return err
		}
	} else if operationID != "" {
		return v1.NewError(v1.CodeInvalidArgument, "operation_id", "must be omitted for a read-only request")
	}
	var payload []byte
	var err error
	if input != nil {
		payload, err = json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode v1 request: %w", err)
		}
	}
	var lastErr error
	for attempt := 0; attempt <= c.transportRetries; attempt++ {
		requestID, err := c.requestID()
		if err != nil {
			return err
		}
		request, err := newHTTPRequest(ctx, method, path, requestID, operationID, payload, input != nil)
		if err != nil {
			return err
		}
		response, err := c.httpClient.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
			continue
		}
		decodeErr := c.decodeResponse(response, output, requestID, operationID)
		var transportErr *TransportError
		if errors.As(decodeErr, &transportErr) && attempt < c.transportRetries {
			lastErr = decodeErr
			continue
		}
		return decodeErr
	}
	return &TransportError{Cause: lastErr}
}

// validateSandboxCall binds a successful response to the requested Sandbox and,
// for mutations, to the exact terminal operation and endpoint-specific phase.
func validateSandboxCall(response v1.SandboxResponse, sandboxID, operationID, operationType string, phases ...string) error {
	if response.Sandbox.ID != sandboxID {
		return errors.New("mydocker API returned a different Sandbox identity")
	}
	if operationID == "" {
		if response.Operation != nil {
			return errors.New("mydocker API attached a mutation operation to a read-only Sandbox response")
		}
	} else {
		if response.Operation == nil {
			return errors.New("mydocker API omitted the mutation operation from a Sandbox response")
		}
		if err := validateMutationOperation(*response.Operation, operationID, operationType, "sandbox", sandboxID); err != nil {
			return err
		}
	}
	return validateReturnedPhase("Sandbox", response.Sandbox.Status.Phase, phases)
}

// validateContainerCall binds a successful response to the requested Container,
// optional create identities, exact mutation operation, and endpoint-specific phase.
func validateContainerCall(response v1.ContainerResponse, containerID, sandboxID, attemptID, operationID, operationType string, phases ...string) error {
	if response.Container.ID != containerID {
		return errors.New("mydocker API returned a different Container identity")
	}
	if sandboxID != "" && response.Container.SandboxID != sandboxID {
		return errors.New("mydocker API returned a Container from a different Sandbox")
	}
	if attemptID != "" && response.Container.AttemptID != attemptID {
		return errors.New("mydocker API returned a different Attempt identity")
	}
	if operationID == "" {
		if response.Operation != nil {
			return errors.New("mydocker API attached a mutation operation to a read-only Container response")
		}
	} else {
		if response.Operation == nil {
			return errors.New("mydocker API omitted the mutation operation from a Container response")
		}
		if err := validateMutationOperation(*response.Operation, operationID, operationType, "container", containerID); err != nil {
			return err
		}
	}
	return validateReturnedPhase("Container", response.Container.Status.Phase, phases)
}

// validateMutationOperation requires exact durable identity, verb, target, and a terminal successful result.
func validateMutationOperation(operation v1.Operation, operationID, operationType, targetKind, targetID string) error {
	if operation.ID != operationID {
		return errors.New("mydocker API returned a different mutation operation identity")
	}
	if operation.Type != operationType || operation.Target.Kind != targetKind || operation.Target.ID != targetID {
		return errors.New("mydocker API returned a mutation operation for a different verb or target")
	}
	if operation.State != "succeeded" || operation.Stage != "complete" || (operation.Result != "succeeded" && operation.Result != "noop") {
		return errors.New("mydocker API returned a nonterminal mutation operation in a success response")
	}
	return nil
}

// validateReturnedPhase accepts any structurally valid phase when no endpoint restriction is supplied.
func validateReturnedPhase(resourceKind, phase string, allowed []string) error {
	if len(allowed) == 0 {
		return nil
	}
	for _, candidate := range allowed {
		if phase == candidate {
			return nil
		}
	}
	return fmt.Errorf("mydocker API returned %s phase %q outside the endpoint result contract", resourceKind, phase)
}

// validateEventResponse requires non-overlapping order and a token matching the last returned sequence.
func validateEventResponse(after v1.ResumeToken, limit int, response v1.EventListResponse) error {
	previous, err := v1.ParseResumeToken(after)
	if err != nil {
		return err
	}
	if len(response.Events) > limit {
		return errors.New("mydocker API returned more events than requested")
	}
	for index, event := range response.Events {
		if event.Sequence <= previous {
			return errors.New("mydocker API returned overlapping or unordered events")
		}
		if (previous != 0 || index > 0) && event.Sequence != previous+1 {
			return errors.New("mydocker API returned an event page with a sequence gap")
		}
		previous = event.Sequence
	}
	expected := after
	if len(response.Events) > 0 {
		expected = v1.NewResumeToken(previous)
	}
	if response.NextResumeToken != expected {
		return errors.New("mydocker API returned an event resume token inconsistent with its page")
	}
	if response.HasMore && len(response.Events) != limit {
		return errors.New("mydocker API marked a short event page as having more results")
	}
	return nil
}

// validateLogResponse requires exact identity, global cursor order, per-stream sequence, and payload evidence.
func validateLogResponse(containerID, attemptID string, after v1.LogCursor, limit int, response v1.LogListResponse) error {
	previousCursor, err := v1.ParseLogCursor(after, containerID, attemptID)
	if err != nil {
		return err
	}
	if len(response.Frames) > limit {
		return errors.New("mydocker API returned more log frames than requested")
	}
	streamSequences := make(map[string]uint64, 2)
	for _, frame := range response.Frames {
		if frame.ContainerID != containerID || frame.AttemptID != attemptID {
			return errors.New("mydocker API returned a log frame for a different Container Attempt")
		}
		if frame.Stream != "stdout" && frame.Stream != "stderr" {
			return errors.New("mydocker API returned an unsupported log stream")
		}
		if frame.Cursor <= previousCursor || frame.Sequence == 0 || len(frame.Payload) == 0 {
			return errors.New("mydocker API returned invalid log cursor, sequence, or payload data")
		}
		if frame.Cursor != previousCursor+1 {
			return errors.New("mydocker API returned a workload log page with a cursor gap")
		}
		if previous := streamSequences[frame.Stream]; previous != 0 && frame.Sequence != previous+1 {
			return errors.New("mydocker API returned a non-contiguous per-stream log sequence")
		}
		digest := sha256.Sum256(frame.Payload)
		if frame.PayloadSHA256 != hex.EncodeToString(digest[:]) {
			return errors.New("mydocker API returned invalid log payload evidence")
		}
		previousCursor = frame.Cursor
		streamSequences[frame.Stream] = frame.Sequence
	}
	expected := after
	if len(response.Frames) > 0 {
		expected, err = v1.NewLogCursor(containerID, attemptID, previousCursor)
		if err != nil {
			return err
		}
	}
	if response.NextCursor != expected {
		return errors.New("mydocker API returned a log cursor inconsistent with its page")
	}
	if response.HasMore && len(response.Frames) != limit {
		return errors.New("mydocker API marked a short log page as having more results")
	}
	return nil
}

// newHTTPRequest constructs one transport attempt from immutable operation input and replayable bytes.
func newHTTPRequest(ctx context.Context, method, path, requestID, operationID string, payload []byte, hasBody bool) (*http.Request, error) {
	var body io.Reader
	if hasBody {
		body = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://mydocker"+path, body)
	if err != nil {
		return nil, fmt.Errorf("construct v1 request: %w", err)
	}
	request.Header.Set(v1.HeaderRequestID, requestID)
	if operationID != "" {
		request.Header.Set(v1.HeaderOperationID, operationID)
	}
	if hasBody {
		request.Header.Set("Content-Type", v1.MediaTypeJSON)
	}
	request.Header.Set("Accept", v1.MediaTypeJSON)
	return request, nil
}

// decodeResponse enforces correlation, bounded size, and strict JSON for success and typed failures.
func (c *Client) decodeResponse(response *http.Response, output any, requestID, operationID string) error {
	defer response.Body.Close()
	if response.Header.Get(v1.HeaderRequestID) != requestID || response.Header.Get(v1.HeaderOperationID) != operationID {
		return errors.New("mydocker API response correlation headers do not match the request")
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != v1.MediaTypeJSON {
		return errors.New("mydocker API returned a non-JSON response")
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
	if err != nil {
		return &TransportError{Cause: fmt.Errorf("read v1 response: %w", err)}
	}
	if int64(len(payload)) > c.maxResponseBytes {
		return errors.New("mydocker API response exceeds the configured size limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope v1.ErrorEnvelope
		if err := decodeStrict(payload, &envelope); err != nil {
			decodeErr := fmt.Errorf("decode v1 error response: %w", err)
			if isTruncatedJSONError(err) {
				return &TransportError{Cause: decodeErr}
			}
			return decodeErr
		}
		if err := envelope.Validate(); err != nil {
			return fmt.Errorf("validate v1 error response: %w", err)
		}
		if envelope.RequestID != requestID || envelope.OperationID != operationID {
			return errors.New("mydocker API error envelope correlation does not match the request")
		}
		if v1.HTTPStatus(envelope.Error.Code) != response.StatusCode {
			return errors.New("mydocker API error status does not match its stable code")
		}
		return &RemoteError{StatusCode: response.StatusCode, Envelope: envelope}
	}
	if output == nil {
		return nil
	}
	if err := decodeStrict(payload, output); err != nil {
		decodeErr := fmt.Errorf("decode v1 response: %w", err)
		if isTruncatedJSONError(err) {
			return &TransportError{Cause: decodeErr}
		}
		return decodeErr
	}
	if validator, ok := output.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return fmt.Errorf("validate v1 response: %w", err)
		}
	}
	return nil
}

// isTruncatedJSONError identifies only EOF-shaped incomplete framing so semantic
// schema failures and correlation conflicts remain fail-closed without retry.
func isTruncatedJSONError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// decodeStrict rejects lossy UTF-8, unknown fields, duplicate keys, and any second JSON value.
func decodeStrict(payload []byte, destination any) error {
	return strictjson.Decode(payload, destination)
}
