package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	v1 "mydocker/api/runtime/v1"
)

// roundTripFunc adapts a deterministic test function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip executes the injected transport behavior for one client attempt.
func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

// failingReadCloser models a response connection that disappears before any JSON can be read.
type failingReadCloser struct{}

// Read reports a truncated response body so the client must replay the same operation.
func (failingReadCloser) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

// Close completes the synthetic response body lifecycle without another error.
func (failingReadCloser) Close() error {
	return nil
}

// TestTransportRetryPreservesOperationID verifies a lost response gets a fresh request ID but the same body and operation identity.
func TestTransportRetryPreservesOperationID(t *testing.T) {
	var operationIDs []string
	var requestIDs []string
	var bodies []string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		operationIDs = append(operationIDs, request.Header.Get(v1.HeaderOperationID))
		requestIDs = append(requestIDs, request.Header.Get(v1.HeaderRequestID))
		bodies = append(bodies, string(payload))
		if len(operationIDs) == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		response := validSandboxMutationResponse("operation-one", "sandbox-one")
		return successfulResponse(t, request, http.StatusCreated, response), nil
	})
	requestIDsToIssue := []string{"request-one", "request-two"}
	nextRequestID := func() (string, error) {
		value := requestIDsToIssue[0]
		requestIDsToIssue = requestIDsToIssue[1:]
		return value, nil
	}
	config := Config{Timeout: time.Second, TransportRetries: 1, MaxResponseBytes: 4096}
	client := newWithTransport(config, transport, nextRequestID)
	input := v1.CreateSandboxRequest{SandboxID: "sandbox-one", Spec: v1.SandboxSpec{Network: v1.NetworkIntent{Mode: "none"}}}
	response, err := client.CreateSandbox(context.Background(), "operation-one", input)
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if response.Sandbox.ID != "sandbox-one" {
		t.Fatalf("sandbox ID = %q", response.Sandbox.ID)
	}
	if len(operationIDs) != 2 || operationIDs[0] != "operation-one" || operationIDs[1] != operationIDs[0] {
		t.Fatalf("operation IDs = %v, want exact replay", operationIDs)
	}
	if requestIDs[0] == requestIDs[1] || requestIDs[0] != "request-one" || requestIDs[1] != "request-two" {
		t.Fatalf("request IDs = %v, want distinct transport attempts", requestIDs)
	}
	if bodies[0] != bodies[1] {
		t.Fatalf("retry body changed: %q != %q", bodies[0], bodies[1])
	}
}

// TestResponseLossRetryPreservesOperationID verifies a broken response body is replayed with the original durable identity.
func TestResponseLossRetryPreservesOperationID(t *testing.T) {
	var operationIDs []string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		operationIDs = append(operationIDs, request.Header.Get(v1.HeaderOperationID))
		if len(operationIDs) == 1 {
			headers := make(http.Header)
			headers.Set("Content-Type", v1.MediaTypeJSON)
			headers.Set(v1.HeaderRequestID, request.Header.Get(v1.HeaderRequestID))
			headers.Set(v1.HeaderOperationID, request.Header.Get(v1.HeaderOperationID))
			return &http.Response{StatusCode: http.StatusCreated, Header: headers, Body: failingReadCloser{}}, nil
		}
		return successfulResponse(t, request, http.StatusCreated, validSandboxMutationResponse("operation-one", "sandbox-one")), nil
	})
	issued := 0
	client := newWithTransport(
		Config{Timeout: time.Second, TransportRetries: 1, MaxResponseBytes: 4096},
		transport,
		func() (string, error) {
			issued++
			return fmt.Sprintf("request-%d", issued), nil
		},
	)
	input := v1.CreateSandboxRequest{SandboxID: "sandbox-one", Spec: v1.SandboxSpec{Network: v1.NetworkIntent{Mode: "none"}}}
	if _, err := client.CreateSandbox(context.Background(), "operation-one", input); err != nil {
		t.Fatalf("CreateSandbox after response loss: %v", err)
	}
	if len(operationIDs) != 2 || operationIDs[0] != "operation-one" || operationIDs[1] != operationIDs[0] {
		t.Fatalf("operation IDs = %v, want exact replay", operationIDs)
	}
}

// TestTruncatedJSONRetryPreservesOperationID verifies orderly EOF in a partial
// success document is treated as ambiguous transport loss and replayed exactly.
func TestTruncatedJSONRetryPreservesOperationID(t *testing.T) {
	var operationIDs []string
	var requestIDs []string
	var bodies []string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		operationIDs = append(operationIDs, request.Header.Get(v1.HeaderOperationID))
		requestIDs = append(requestIDs, request.Header.Get(v1.HeaderRequestID))
		bodies = append(bodies, string(payload))
		if len(operationIDs) == 1 {
			return rawResponse(request, http.StatusCreated, `{"sandbox":{"id":"sandbox-one"`), nil
		}
		return successfulResponse(t, request, http.StatusCreated, validSandboxMutationResponse("operation-one", "sandbox-one")), nil
	})
	issued := 0
	client := newWithTransport(
		Config{Timeout: time.Second, TransportRetries: 1, MaxResponseBytes: 4096},
		transport,
		func() (string, error) {
			issued++
			return fmt.Sprintf("request-%d", issued), nil
		},
	)
	input := v1.CreateSandboxRequest{SandboxID: "sandbox-one", Spec: v1.SandboxSpec{Network: v1.NetworkIntent{Mode: "none"}}}
	if _, err := client.CreateSandbox(context.Background(), "operation-one", input); err != nil {
		t.Fatalf("CreateSandbox after truncated JSON: %v", err)
	}
	if len(operationIDs) != 2 || operationIDs[0] != "operation-one" || operationIDs[1] != operationIDs[0] {
		t.Fatalf("operation IDs = %v, want exact replay", operationIDs)
	}
	if requestIDs[0] == requestIDs[1] || requestIDs[0] != "request-1" || requestIDs[1] != "request-2" {
		t.Fatalf("request IDs = %v, want distinct transport attempts", requestIDs)
	}
	if bodies[0] != bodies[1] {
		t.Fatalf("retry body changed: %q != %q", bodies[0], bodies[1])
	}
}

// TestTruncatedErrorEnvelopeIsTransportError verifies an incomplete error
// document is transport ambiguity rather than a trusted remote classification.
func TestTruncatedErrorEnvelopeIsTransportError(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return rawResponse(request, http.StatusConflict, `{"error":{"code":"conflict"`), nil
	})
	client := newWithTransport(
		Config{Timeout: time.Second, MaxResponseBytes: 4096},
		transport,
		func() (string, error) { return "request-one", nil },
	)
	input := v1.CreateSandboxRequest{SandboxID: "sandbox-one", Spec: v1.SandboxSpec{Network: v1.NetworkIntent{Mode: "none"}}}
	_, err := client.CreateSandbox(context.Background(), "operation-one", input)
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("error = %v, want TransportError", err)
	}
}

// TestSemanticSuccessResponseFailuresDoNotRetry verifies complete zero, wrong
// resource, and wrong operation documents fail closed after one transport attempt.
func TestSemanticSuccessResponseFailuresDoNotRetry(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "zero response", payload: `{}`},
		{name: "wrong Sandbox", payload: marshalTestJSON(t, validSandboxMutationResponse("operation-one", "sandbox-other"))},
		{name: "wrong operation", payload: marshalTestJSON(t, validSandboxMutationResponse("operation-other", "sandbox-one"))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				attempts++
				return rawResponse(request, http.StatusCreated, test.payload), nil
			})
			issued := 0
			client := newWithTransport(
				Config{Timeout: time.Second, TransportRetries: 1, MaxResponseBytes: 4096},
				transport,
				func() (string, error) {
					issued++
					return fmt.Sprintf("request-%d", issued), nil
				},
			)
			input := v1.CreateSandboxRequest{SandboxID: "sandbox-one", Spec: v1.SandboxSpec{Network: v1.NetworkIntent{Mode: "none"}}}
			if _, err := client.CreateSandbox(context.Background(), "operation-one", input); err == nil {
				t.Fatal("CreateSandbox error = nil, want semantic response rejection")
			} else {
				var transportErr *TransportError
				if errors.As(err, &transportErr) {
					t.Fatalf("error = %v, want fail-closed semantic error", err)
				}
			}
			if attempts != 1 || issued != 1 {
				t.Fatalf("attempts/request IDs = %d/%d, want no retry", attempts, issued)
			}
		})
	}
}

// TestRemoteErrorMapping verifies a typed envelope reaches callers with stable code and exit status.
func TestRemoteErrorMapping(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		envelope := v1.ErrorEnvelope{
			Error:     v1.ErrorDetail{Code: v1.CodeNotFound, Message: "sandbox does not exist"},
			RequestID: request.Header.Get(v1.HeaderRequestID),
		}
		return successfulResponse(t, request, http.StatusNotFound, envelope), nil
	})
	client := newWithTransport(
		Config{Timeout: time.Second, MaxResponseBytes: 4096},
		transport,
		func() (string, error) { return "request-one", nil },
	)
	_, err := client.GetSandbox(context.Background(), "missing")
	var remote *RemoteError
	if !errors.As(err, &remote) {
		t.Fatalf("error = %v, want RemoteError", err)
	}
	if remote.Code() != v1.CodeNotFound || ExitStatus(err) != 3 {
		t.Fatalf("remote code/status = %q/%d", remote.Code(), ExitStatus(err))
	}
}

// TestRetentionErrorHelpers verifies callers can branch on bounded-history
// outcomes without parsing status codes or human-readable daemon messages.
func TestRetentionErrorHelpers(t *testing.T) {
	tests := []struct {
		name    string
		code    v1.ErrorCode
		matches func(error) bool
		exit    int
	}{
		{name: "operation expired", code: v1.CodeOperationExpired, matches: IsOperationExpired, exit: 4},
		{name: "resume gap", code: v1.CodeResumeGap, matches: IsResumeGap, exit: 4},
		{name: "resource exhausted", code: v1.CodeResourceExhausted, matches: IsResourceExhausted, exit: 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := &RemoteError{
				StatusCode: v1.HTTPStatus(test.code),
				Envelope: v1.ErrorEnvelope{Error: v1.ErrorDetail{
					Code: test.code, Message: string(test.code), Retryable: false,
				}},
			}
			if CodeOf(err) != test.code || !test.matches(err) || ExitStatus(err) != test.exit {
				t.Fatalf("retention error code/match/status = %q/%t/%d, want %q/true/%d",
					CodeOf(err), test.matches(err), ExitStatus(err), test.code, test.exit)
			}
		})
	}
}

// TestStrictResponseRejectsUnknownFields verifies schema drift cannot be silently ignored by clients.
func TestStrictResponseRejectsUnknownFields(t *testing.T) {
	attempts := 0
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		attempts++
		headers := make(http.Header)
		headers.Set("Content-Type", v1.MediaTypeJSON)
		headers.Set(v1.HeaderRequestID, request.Header.Get(v1.HeaderRequestID))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       io.NopCloser(strings.NewReader(`{"sandboxes":[],"unexpected EOF":true}`)),
		}, nil
	})
	client := newWithTransport(
		Config{Timeout: time.Second, TransportRetries: 1, MaxResponseBytes: 4096},
		transport,
		func() (string, error) { return fmt.Sprintf("request-%d", attempts+1), nil },
	)
	if _, err := client.ListSandboxes(context.Background()); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error = %v, want unknown-field rejection", err)
	}
	if attempts != 1 {
		t.Fatalf("transport attempts = %d, want semantic unknown-field failure without retry", attempts)
	}
}

// TestPaginationRejectsSkippedEventAndLogPositions verifies a response cannot
// advance an opaque resume position past data omitted from the returned page.
func TestPaginationRejectsSkippedEventAndLogPositions(t *testing.T) {
	eventAfter := v1.NewResumeToken(4)
	event := validClientEvent(6)
	eventResponse := v1.EventListResponse{
		Events: []v1.Event{event}, NextResumeToken: v1.NewResumeToken(event.Sequence),
	}
	if err := eventResponse.Validate(); err != nil {
		t.Fatalf("single-event DTO fixture should be valid without request context: %v", err)
	}
	if err := validateEventResponse(eventAfter, 10, eventResponse); err == nil {
		t.Fatal("validateEventResponse() accepted a missing sequence after the caller token")
	}

	logAfter, err := v1.NewLogCursor("container-one", "attempt-one", 4)
	if err != nil {
		t.Fatalf("NewLogCursor(after) error = %v", err)
	}
	frame := validClientLogFrame(6, 3, "stdout", []byte("skipped-frame"))
	next, err := v1.NewLogCursor(frame.ContainerID, frame.AttemptID, frame.Cursor)
	if err != nil {
		t.Fatalf("NewLogCursor(next) error = %v", err)
	}
	logResponse := v1.LogListResponse{Frames: []v1.LogFrame{frame}, NextCursor: next}
	if err := logResponse.Validate(); err != nil {
		t.Fatalf("single-frame DTO fixture should be valid without request context: %v", err)
	}
	if err := validateLogResponse(frame.ContainerID, frame.AttemptID, logAfter, 10, logResponse); err == nil {
		t.Fatal("validateLogResponse() accepted a missing cursor after the caller token")
	}
}

// successfulResponse builds a JSON response that echoes the transport correlation headers.
func successfulResponse(t *testing.T, request *http.Request, status int, value any) *http.Response {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	headers := make(http.Header)
	headers.Set("Content-Type", v1.MediaTypeJSON)
	headers.Set(v1.HeaderRequestID, request.Header.Get(v1.HeaderRequestID))
	if operationID := request.Header.Get(v1.HeaderOperationID); operationID != "" {
		headers.Set(v1.HeaderOperationID, operationID)
	}
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(string(payload)))}
}

// rawResponse builds a synthetic JSON response while allowing intentionally malformed body framing.
func rawResponse(request *http.Request, status int, payload string) *http.Response {
	headers := make(http.Header)
	headers.Set("Content-Type", v1.MediaTypeJSON)
	headers.Set(v1.HeaderRequestID, request.Header.Get(v1.HeaderRequestID))
	if operationID := request.Header.Get(v1.HeaderOperationID); operationID != "" {
		headers.Set(v1.HeaderOperationID, operationID)
	}
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(payload))}
}

// validSandboxMutationResponse constructs one complete terminal create result for client correlation tests.
func validSandboxMutationResponse(operationID, sandboxID string) v1.SandboxResponse {
	operation := v1.Operation{
		ID: operationID, Type: "create", Target: v1.ResourceRef{Kind: "sandbox", ID: sandboxID},
		Fingerprint: v1.RequestFingerprint{Version: 1, SHA256: strings.Repeat("a", 64)},
		State:       "succeeded",
		Stage:       "complete",
		Result:      "succeeded",
		Reason:      "none",
	}
	return v1.SandboxResponse{
		Sandbox: v1.Sandbox{
			ID: sandboxID, Spec: v1.SandboxSpec{Network: v1.NetworkIntent{Mode: "none"}},
			Status: v1.SandboxStatus{
				Phase: "ready", Generation: 1, ObservedGeneration: 1,
				LastObservation: v1.LifecycleObservation{OperationID: operationID, EventSequence: 1, Reason: "none"},
			},
		},
		Operation: &operation,
	}
}

// marshalTestJSON serializes a complete semantic test document without hiding encoding failures.
func marshalTestJSON(t *testing.T, value any) string {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test JSON: %v", err)
	}
	return string(payload)
}

// validClientEvent constructs one complete event for request-aware pagination tests.
func validClientEvent(sequence uint64) v1.Event {
	return v1.Event{
		Sequence: sequence, OperationID: "operation-event", Type: "create",
		Target:    v1.ResourceRef{Kind: "sandbox", ID: "sandbox-one"},
		Resources: []v1.ResourceRef{{Kind: "sandbox", ID: "sandbox-one"}},
		Stage:     "complete", Result: "succeeded", Reason: "none",
		OccurredAt: time.Unix(1, 0).UTC(), Generation: 1, ObservedGeneration: 1,
	}
}

// validClientLogFrame constructs one checksum-bound frame for request-aware cursor tests.
func validClientLogFrame(cursor, sequence uint64, stream string, payload []byte) v1.LogFrame {
	digest := sha256.Sum256(payload)
	return v1.LogFrame{
		ContainerID: "container-one", AttemptID: "attempt-one", Stream: stream,
		Cursor: cursor, Sequence: sequence, Payload: append([]byte(nil), payload...),
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}
}
