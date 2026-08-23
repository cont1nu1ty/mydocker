package shim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"mydocker/internal/ownership"
)

const (
	maxRememberedControlRequests = 4096
	defaultControlTimeout        = 30 * time.Second
)

// ControlAction is the bounded request vocabulary exposed over the private Unix socket.
type ControlAction string

const (
	// ActionInspect returns prepared, running, or terminal owner-scoped evidence without mutation.
	ActionInspect ControlAction = "inspect"
	// ActionRelease consumes the closed one-shot gate and starts the child at most once.
	ActionRelease ControlAction = "release"
	// ActionSignal asks the live Child to perform action-time verified signal delivery.
	ActionSignal ControlAction = "signal"
	// ActionPrepareRootfs asks init PID1 to execute and ACK its one-shot deferred pivot.
	ActionPrepareRootfs ControlAction = "prepare_rootfs"
)

// Valid reports whether the action belongs to the M3 wrapper protocol.
func (action ControlAction) Valid() bool {
	return action == ActionInspect || action == ActionRelease || action == ActionSignal || action == ActionPrepareRootfs
}

// ControlRequest is one owner-authenticated wrapper request whose exact request ID replay is idempotent.
type ControlRequest struct {
	SchemaVersion uint32             `json:"schema_version"`
	RequestID     string             `json:"request_id"`
	Owner         ownership.OwnerKey `json:"owner"`
	Action        ControlAction      `json:"action"`
	Signal        Signal             `json:"signal,omitempty"`
	Rootfs        *RootfsRequest     `json:"rootfs,omitempty"`
}

// Validate rejects unsupported schemas, unsafe request IDs, invalid owner keys, and ambiguous action fields.
func (request ControlRequest) Validate() error {
	if request.SchemaVersion != SchemaVersion {
		return newError(CodeInvalidArgument, "unsupported control schema version", nil)
	}
	if err := validateOpaque("control request ID", request.RequestID, 128); err != nil {
		return newError(CodeInvalidArgument, "invalid control request ID", err)
	}
	if err := request.Owner.Validate(); err != nil {
		return newError(CodeInvalidArgument, "invalid control owner", err)
	}
	if !request.Action.Valid() {
		return newError(CodeUnsupportedRequest, "unsupported control action", nil)
	}
	if request.Action == ActionSignal {
		if !request.Signal.Valid() {
			return newError(CodeInvalidArgument, "signal action requires a supported signal", nil)
		}
	} else if request.Signal != "" {
		return newError(CodeInvalidArgument, "non-signal action must not carry a signal", nil)
	}
	if request.Action == ActionPrepareRootfs {
		if request.Rootfs == nil {
			return newError(CodeInvalidArgument, "prepare_rootfs action requires a command", nil)
		}
		if err := request.Rootfs.Validate(); err != nil {
			return newError(CodeInvalidArgument, "invalid prepare_rootfs command", err)
		}
	} else if request.Rootfs != nil {
		return newError(CodeInvalidArgument, "non-rootfs action must not carry a rootfs command", nil)
	}
	return nil
}

// ControlResponse returns exactly one observation, signal-delivery fact, or typed error.
type ControlResponse struct {
	SchemaVersion uint32             `json:"schema_version"`
	RequestID     string             `json:"request_id,omitempty"`
	Observation   *Observation       `json:"observation,omitempty"`
	Delivery      *SignalDelivery    `json:"delivery,omitempty"`
	Rootfs        *RootfsPreparation `json:"rootfs,omitempty"`
	Error         *Error             `json:"error,omitempty"`
}

// Validate checks response schema and the exactly-one result contract.
func (response ControlResponse) Validate() error {
	if response.SchemaVersion != SchemaVersion {
		return errors.New("unsupported control response schema")
	}
	results := 0
	if response.Observation != nil {
		results++
		if err := response.Observation.Validate(); err != nil {
			return err
		}
	}
	if response.Delivery != nil {
		results++
		if err := response.Delivery.Validate(); err != nil {
			return err
		}
	}
	if response.Rootfs != nil {
		results++
		if err := response.Rootfs.Validate(); err != nil {
			return err
		}
	}
	if response.Error != nil {
		results++
		if response.Error.Code == "" || response.Error.Message == "" {
			return errors.New("control error requires code and message")
		}
	}
	if results != 1 {
		return errors.New("control response must contain exactly one result")
	}
	return nil
}

// controlEntry serializes one request ID and retains its immutable first response for exact replay.
type controlEntry struct {
	requestEvidence string
	response        ControlResponse
	done            chan struct{}
}

// HandleControl authenticates, serializes, and exactly replays one request without repeating side effects.
func (wrapper *Wrapper) HandleControl(request ControlRequest) ControlResponse {
	response := ControlResponse{SchemaVersion: SchemaVersion, RequestID: request.RequestID}
	if err := request.Validate(); err != nil {
		response.Error = controlError(err)
		return response
	}
	if request.Owner != wrapper.owner {
		response.Error = newError(CodeOwnerMismatch, "control owner does not match this wrapper", nil)
		return response
	}
	requestEvidence, err := controlRequestEvidence(request)
	if err != nil {
		response.Error = controlError(err)
		return response
	}
	// Inspect is read-only and may be polled for the wrapper's entire lifetime;
	// it must not consume the finite mutation replay journal.
	if request.Action == ActionInspect {
		return wrapper.dispatchControl(request)
	}
	wrapper.controlMu.Lock()
	if existing, found := wrapper.controlEntries[request.RequestID]; found {
		if existing.requestEvidence != requestEvidence {
			wrapper.controlMu.Unlock()
			response.Error = newError(CodeDuplicateRequest, "control request ID was reused with different content", nil)
			return response
		}
		done := existing.done
		wrapper.controlMu.Unlock()
		<-done
		wrapper.controlMu.Lock()
		response = cloneControlResponse(existing.response)
		wrapper.controlMu.Unlock()
		return response
	}
	if len(wrapper.controlEntries) >= maxRememberedControlRequests {
		wrapper.controlMu.Unlock()
		response.Error = newError(CodeUnavailable, "control replay cache is full", nil)
		return response
	}
	entry := &controlEntry{requestEvidence: requestEvidence, done: make(chan struct{})}
	wrapper.controlEntries[request.RequestID] = entry
	wrapper.controlMu.Unlock()

	response = wrapper.dispatchControl(request)
	wrapper.controlMu.Lock()
	entry.response = cloneControlResponse(response)
	close(entry.done)
	wrapper.controlMu.Unlock()
	return response
}

// controlRequestEvidence canonicalizes validated JSON content so retries from
// independent decodes compare semantic values rather than pointer addresses.
func controlRequestEvidence(request ControlRequest) (string, error) {
	digest, err := ownership.EvidenceDigest(request)
	if err != nil {
		return "", newError(CodeInvalidArgument, "encode control request evidence", err)
	}
	return digest, nil
}

// dispatchControl executes only the first occurrence of one validated owner-scoped control request.
func (wrapper *Wrapper) dispatchControl(request ControlRequest) ControlResponse {
	response := ControlResponse{SchemaVersion: SchemaVersion, RequestID: request.RequestID}
	switch request.Action {
	case ActionInspect:
		observation, err := wrapper.Inspect()
		if err != nil {
			response.Error = controlError(err)
		} else {
			response.Observation = &observation
		}
	case ActionRelease:
		observation, err := wrapper.Release()
		if err != nil {
			response.Error = controlError(err)
		} else {
			response.Observation = &observation
		}
	case ActionSignal:
		delivery, err := wrapper.ForwardSignal(request.Signal)
		if err != nil {
			response.Error = controlError(err)
		} else {
			response.Delivery = &delivery
		}
	case ActionPrepareRootfs:
		preparation, err := wrapper.PrepareRootfs(*request.Rootfs)
		if err != nil {
			response.Error = controlError(err)
		} else {
			response.Rootfs = &preparation
		}
	default:
		response.Error = newError(CodeUnsupportedRequest, "unsupported control action", nil)
	}
	return response
}

// cloneControlResponse prevents callers from mutating cached observation, delivery, or error values.
func cloneControlResponse(response ControlResponse) ControlResponse {
	clone := response
	if response.Observation != nil {
		observation := response.Observation.Clone()
		clone.Observation = &observation
	}
	if response.Delivery != nil {
		delivery := *response.Delivery
		clone.Delivery = &delivery
	}
	if response.Rootfs != nil {
		rootfs := *response.Rootfs
		clone.Rootfs = &rootfs
	}
	if response.Error != nil {
		clone.Error = &Error{Code: response.Error.Code, Message: response.Error.Message}
	}
	return clone
}

// controlError converts an arbitrary local error to a bounded wire-safe shim error.
func controlError(err error) *Error {
	var typed *Error
	if errors.As(err, &typed) {
		return &Error{Code: typed.Code, Message: typed.Message}
	}
	return &Error{Code: CodeUnavailable, Message: "wrapper request failed"}
}

// ControlServer owns one private Unix socket and serves independent one-request connections.
type ControlServer struct {
	path       string
	listener   *net.UnixListener
	wrapper    *Wrapper
	socketInfo os.FileInfo
	closeOnce  sync.Once
	closeErr   error
	wait       sync.WaitGroup
}

// NewControlServer binds an absent socket path in a private same-owner directory and applies mode 0600.
func NewControlServer(path string, wrapper *Wrapper) (*ControlServer, error) {
	if wrapper == nil {
		return nil, errors.New("control server wrapper must not be nil")
	}
	if err := validateAbsolutePath("control socket", path); err != nil {
		return nil, err
	}
	if err := validatePrivateDirectory(osTerminalFS{}, filepath.Dir(path)); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); err == nil {
		return nil, fmt.Errorf("%w: control socket path already exists", ErrUnsafeArtifact)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect control socket: %w", err)
	}
	address := &net.UnixAddr{Name: path, Net: "unix"}
	listener, err := net.ListenUnix("unix", address)
	if err != nil {
		return nil, fmt.Errorf("listen on control socket: %w", err)
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = listener.Close()
		}
	}()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("restrict control socket: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect bound control socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0o177 != 0 {
		return nil, fmt.Errorf("%w: bound control endpoint is not a private socket", ErrUnsafeArtifact)
	}
	listener.SetUnlinkOnClose(false)
	server := &ControlServer{path: path, listener: listener, wrapper: wrapper, socketInfo: info}
	succeeded = true
	return server, nil
}

// Serve accepts requests until context cancellation, keeping the wrapper resident across daemon disconnects.
func (server *ControlServer) Serve(ctx context.Context) error {
	if ctx == nil {
		return errors.New("control server context must not be nil")
	}
	cancelClose := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = server.listener.Close()
		case <-cancelClose:
		}
	}()
	defer close(cancelClose)
	for {
		connection, err := server.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				server.wait.Wait()
				return nil
			}
			server.wait.Wait()
			return fmt.Errorf("accept control connection: %w", err)
		}
		server.wait.Add(1)
		go server.handleConnection(connection)
	}
}

// Close stops accepting requests, removes only the exact socket inode created
// by this server, and replays the first cleanup result to every caller.
func (server *ControlServer) Close() error {
	server.closeOnce.Do(func() {
		server.closeErr = server.listener.Close()
		if errors.Is(server.closeErr, net.ErrClosed) {
			server.closeErr = nil
		}
		server.wait.Wait()
		info, err := os.Lstat(server.path)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		if err != nil {
			server.closeErr = errors.Join(server.closeErr, err)
			return
		}
		if !os.SameFile(server.socketInfo, info) {
			server.closeErr = errors.Join(server.closeErr, fmt.Errorf("%w: control socket path was replaced", ErrUnsafeArtifact))
			return
		}
		server.closeErr = errors.Join(server.closeErr, os.Remove(server.path))
	})
	return server.closeErr
}

// handleConnection decodes one bounded request, emits one response, and never keeps daemon connection state.
func (server *ControlServer) handleConnection(connection *net.UnixConn) {
	defer server.wait.Done()
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(30 * time.Second))
	request, err := decodeControlRequest(connection)
	var response ControlResponse
	if err != nil {
		response = ControlResponse{SchemaVersion: SchemaVersion, Error: controlError(err)}
	} else {
		response = server.wrapper.HandleControl(request)
	}
	_ = json.NewEncoder(connection).Encode(response)
}

// decodeControlRequest rejects unknown fields, trailing values, and payloads beyond the protocol bound.
func decodeControlRequest(reader io.Reader) (ControlRequest, error) {
	limited := &io.LimitedReader{R: reader, N: MaxControlBytes + 1}
	payload, err := io.ReadAll(limited)
	if err != nil {
		return ControlRequest{}, newError(CodeInvalidArgument, "read control request", err)
	}
	if len(payload) > MaxControlBytes {
		return ControlRequest{}, newError(CodeInvalidArgument, "control request exceeds size limit", nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request ControlRequest
	if err := decoder.Decode(&request); err != nil {
		return ControlRequest{}, newError(CodeInvalidArgument, "decode control request", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return ControlRequest{}, newError(CodeInvalidArgument, "control request has trailing data", err)
	}
	return request, nil
}

// DoControl sends one request over a fresh Unix connection and discards the
// optional transport peer identity used only by the production launcher.
func DoControl(ctx context.Context, socketPath string, request ControlRequest) (ControlResponse, error) {
	response, _, err := doControl(ctx, socketPath, request, false)
	return response, err
}

// DoControlWithPeer sends one bounded request and returns the authenticated
// Unix peer PID so launch/recovery can open strong process evidence without a persisted raw PID.
func DoControlWithPeer(ctx context.Context, socketPath string, request ControlRequest) (ControlResponse, int, error) {
	return doControl(ctx, socketPath, request, true)
}

// doControl performs one fresh-connection exchange. Every call receives a
// bounded client deadline, including background recovery contexts, so a
// connected but unresponsive wrapper cannot stall reconciliation.
func doControl(ctx context.Context, socketPath string, request ControlRequest, requirePeer bool) (ControlResponse, int, error) {
	if ctx == nil {
		return ControlResponse{}, 0, errors.New("control client context must not be nil")
	}
	if err := validateAbsolutePath("control socket", socketPath); err != nil {
		return ControlResponse{}, 0, err
	}
	if err := request.Validate(); err != nil {
		return ControlResponse{}, 0, err
	}
	controlContext, cancel := boundedControlContext(ctx, defaultControlTimeout)
	defer cancel()
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(controlContext, "unix", socketPath)
	if err != nil {
		return ControlResponse{}, 0, newError(CodeUnavailable, "connect to wrapper control socket", err)
	}
	defer connection.Close()
	peerPID := 0
	if requirePeer {
		unixConnection, ok := connection.(*net.UnixConn)
		if !ok {
			return ControlResponse{}, 0, newError(CodeUnavailable, "control connection is not Unix", nil)
		}
		peerPID, err = peerProcessID(unixConnection)
		if err != nil {
			return ControlResponse{}, 0, newError(CodeUnavailable, "authenticate wrapper control peer", err)
		}
	}
	deadline, _ := controlContext.Deadline()
	_ = connection.SetDeadline(deadline)
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return ControlResponse{}, 0, newError(CodeUnavailable, "send wrapper control request", err)
	}
	if unixConnection, ok := connection.(*net.UnixConn); ok {
		_ = unixConnection.CloseWrite()
	}
	limited := &io.LimitedReader{R: connection, N: MaxControlBytes + 1}
	payload, err := io.ReadAll(limited)
	if err != nil {
		return ControlResponse{}, 0, newError(CodeUnavailable, "read wrapper control response", err)
	}
	if len(payload) > MaxControlBytes {
		return ControlResponse{}, 0, newError(CodeUnavailable, "wrapper control response exceeds size limit", nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var response ControlResponse
	if err := decoder.Decode(&response); err != nil {
		return ControlResponse{}, 0, newError(CodeUnavailable, "decode wrapper control response", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return ControlResponse{}, 0, newError(CodeUnavailable, "wrapper control response has trailing data", err)
	}
	if err := response.Validate(); err != nil {
		return ControlResponse{}, 0, newError(CodeUnavailable, "invalid wrapper control response", err)
	}
	return response, peerPID, nil
}

// boundedControlContext preserves an earlier caller deadline and otherwise
// installs the protocol maximum used for dial, write, and read operations.
func boundedControlContext(ctx context.Context, maximum time.Duration) (context.Context, context.CancelFunc) {
	if deadline, exists := ctx.Deadline(); exists && time.Until(deadline) <= maximum {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, maximum)
}
