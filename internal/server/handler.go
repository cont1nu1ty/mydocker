package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	v1 "mydocker/api/runtime/v1"
)

const (
	defaultEventPageLimit = 100
	maximumEventPageLimit = 500
	defaultLogPageLimit   = 100
	maximumLogPageLimit   = 100
)

// route dispatches one canonical path without allowing ServeMux redirects or text error bodies.
func (s *Server) route(writer http.ResponseWriter, request *http.Request) error {
	path := request.URL.EscapedPath()
	switch {
	case path == v1.BasePath+"/sandboxes":
		return s.handleSandboxes(writer, request)
	case strings.HasPrefix(path, v1.BasePath+"/sandboxes/"):
		return s.handleSandboxPath(writer, request, strings.TrimPrefix(path, v1.BasePath+"/sandboxes/"))
	case strings.HasPrefix(path, v1.BasePath+"/containers/"):
		return s.handleContainerPath(writer, request, strings.TrimPrefix(path, v1.BasePath+"/containers/"))
	case strings.HasPrefix(path, v1.BasePath+"/operations/"):
		return s.handleOperationPath(writer, request, strings.TrimPrefix(path, v1.BasePath+"/operations/"))
	case path == v1.BasePath+"/events":
		return s.handleEvents(writer, request)
	default:
		return v1.NewError(v1.CodeNotFound, "path", "v1 endpoint does not exist")
	}
}

// handleSandboxes serves top-level Sandbox creation and deterministic listing.
func (s *Server) handleSandboxes(writer http.ResponseWriter, request *http.Request) error {
	if err := rejectUnknownQuery(request); err != nil {
		return err
	}
	switch request.Method {
	case http.MethodPost:
		requestContext, err := readRequestContext(request, true)
		if err != nil {
			return err
		}
		var input v1.CreateSandboxRequest
		if err := s.decodeJSON(writer, request, &input); err != nil {
			return err
		}
		if err := input.Validate(); err != nil {
			return err
		}
		response, err := s.service.CreateSandbox(request.Context(), requestContext, input)
		if err != nil {
			return err
		}
		writeSuccess(writer, requestContext, http.StatusCreated, response)
		return nil
	case http.MethodGet:
		if err := rejectReadBody(request); err != nil {
			return err
		}
		requestContext, err := readRequestContext(request, false)
		if err != nil {
			return err
		}
		response, err := s.service.ListSandboxes(request.Context(), requestContext, v1.ListSandboxesRequest{})
		if err != nil {
			return err
		}
		writeSuccess(writer, requestContext, http.StatusOK, response)
		return nil
	default:
		return requireMethod(writer, request, http.MethodGet, http.MethodPost)
	}
}

// handleSandboxPath routes one Sandbox resource, its stop action, or nested Containers.
func (s *Server) handleSandboxPath(writer http.ResponseWriter, request *http.Request, suffix string) error {
	if strings.HasSuffix(suffix, ":stop") && !strings.Contains(strings.TrimSuffix(suffix, ":stop"), "/") {
		id, err := decodeResourceID("sandbox_id", strings.TrimSuffix(suffix, ":stop"))
		if err != nil {
			return err
		}
		return s.handleStopSandbox(writer, request, id)
	}
	if strings.HasSuffix(suffix, "/containers") {
		idPart := strings.TrimSuffix(suffix, "/containers")
		if strings.Contains(idPart, "/") {
			return v1.NewError(v1.CodeNotFound, "path", "v1 endpoint does not exist")
		}
		id, err := decodeResourceID("sandbox_id", idPart)
		if err != nil {
			return err
		}
		return s.handleSandboxContainers(writer, request, id)
	}
	if strings.Contains(suffix, "/") {
		return v1.NewError(v1.CodeNotFound, "path", "v1 endpoint does not exist")
	}
	id, err := decodeResourceID("sandbox_id", suffix)
	if err != nil {
		return err
	}
	return s.handleSandboxResource(writer, request, id)
}

// handleStopSandbox validates an operation-scoped empty body before calling the service.
func (s *Server) handleStopSandbox(writer http.ResponseWriter, request *http.Request, id string) error {
	if err := requireMethod(writer, request, http.MethodPost); err != nil {
		return err
	}
	if err := rejectUnknownQuery(request); err != nil {
		return err
	}
	requestContext, err := readRequestContext(request, true)
	if err != nil {
		return err
	}
	var input v1.StopSandboxRequest
	if err := s.decodeJSON(writer, request, &input); err != nil {
		return err
	}
	input.SandboxID = id
	response, err := s.service.StopSandbox(request.Context(), requestContext, input)
	if err != nil {
		return err
	}
	writeSuccess(writer, requestContext, http.StatusOK, response)
	return nil
}

// handleSandboxResource serves authoritative read or operation-scoped deletion for one Sandbox.
func (s *Server) handleSandboxResource(writer http.ResponseWriter, request *http.Request, id string) error {
	if err := rejectUnknownQuery(request); err != nil {
		return err
	}
	switch request.Method {
	case http.MethodGet:
		if err := rejectReadBody(request); err != nil {
			return err
		}
		requestContext, err := readRequestContext(request, false)
		if err != nil {
			return err
		}
		response, err := s.service.GetSandbox(request.Context(), requestContext, v1.GetSandboxRequest{SandboxID: id})
		if err != nil {
			return err
		}
		writeSuccess(writer, requestContext, http.StatusOK, response)
		return nil
	case http.MethodDelete:
		requestContext, err := readRequestContext(request, true)
		if err != nil {
			return err
		}
		var input v1.DeleteSandboxRequest
		if err := s.decodeJSON(writer, request, &input); err != nil {
			return err
		}
		input.SandboxID = id
		response, err := s.service.DeleteSandbox(request.Context(), requestContext, input)
		if err != nil {
			return err
		}
		writeSuccess(writer, requestContext, http.StatusOK, response)
		return nil
	default:
		return requireMethod(writer, request, http.MethodGet, http.MethodDelete)
	}
}

// handleSandboxContainers serves pair creation and deterministic listing within one Sandbox.
func (s *Server) handleSandboxContainers(writer http.ResponseWriter, request *http.Request, sandboxID string) error {
	if err := rejectUnknownQuery(request); err != nil {
		return err
	}
	switch request.Method {
	case http.MethodPost:
		requestContext, err := readRequestContext(request, true)
		if err != nil {
			return err
		}
		var input v1.CreateContainerRequest
		if err := s.decodeJSON(writer, request, &input); err != nil {
			return err
		}
		input.SandboxID = sandboxID
		if err := input.Validate(); err != nil {
			return err
		}
		response, err := s.service.CreateContainer(request.Context(), requestContext, input)
		if err != nil {
			return err
		}
		writeSuccess(writer, requestContext, http.StatusCreated, response)
		return nil
	case http.MethodGet:
		if err := rejectReadBody(request); err != nil {
			return err
		}
		requestContext, err := readRequestContext(request, false)
		if err != nil {
			return err
		}
		response, err := s.service.ListContainers(request.Context(), requestContext, v1.ListContainersRequest{SandboxID: sandboxID})
		if err != nil {
			return err
		}
		writeSuccess(writer, requestContext, http.StatusOK, response)
		return nil
	default:
		return requireMethod(writer, request, http.MethodGet, http.MethodPost)
	}
}

// handleContainerPath routes one Container resource or its start and kill actions.
func (s *Server) handleContainerPath(writer http.ResponseWriter, request *http.Request, suffix string) error {
	if strings.HasSuffix(suffix, "/logs") {
		idPart := strings.TrimSuffix(suffix, "/logs")
		if strings.Contains(idPart, "/") {
			return v1.NewError(v1.CodeNotFound, "path", "v1 endpoint does not exist")
		}
		id, err := decodeResourceID("container_id", idPart)
		if err != nil {
			return err
		}
		return s.handleContainerLogs(writer, request, id)
	}
	action := ""
	idPart := suffix
	for _, candidate := range []string{"start", "kill"} {
		ending := ":" + candidate
		if strings.HasSuffix(suffix, ending) {
			action = candidate
			idPart = strings.TrimSuffix(suffix, ending)
			break
		}
	}
	if strings.Contains(idPart, "/") {
		return v1.NewError(v1.CodeNotFound, "path", "v1 endpoint does not exist")
	}
	id, err := decodeResourceID("container_id", idPart)
	if err != nil {
		return err
	}
	if action != "" {
		return s.handleContainerAction(writer, request, id, action)
	}
	return s.handleContainerResource(writer, request, id)
}

// handleContainerLogs returns a bounded page for one exact Container/Attempt log identity.
func (s *Server) handleContainerLogs(writer http.ResponseWriter, request *http.Request, containerID string) error {
	if err := requireMethod(writer, request, http.MethodGet); err != nil {
		return err
	}
	if err := rejectReadBody(request); err != nil {
		return err
	}
	if err := rejectUnknownQuery(request, "attempt_id", "after", "limit"); err != nil {
		return err
	}
	requestContext, err := readRequestContext(request, false)
	if err != nil {
		return err
	}
	attemptID := request.URL.Query().Get("attempt_id")
	if err := v1.ValidateResourceID("attempt_id", attemptID); err != nil {
		return err
	}
	afterToken := v1.LogCursor(request.URL.Query().Get("after"))
	after, err := v1.ParseLogCursor(afterToken, containerID, attemptID)
	if err != nil {
		return err
	}
	limit, err := parseLogLimit(request.URL.Query().Get("limit"))
	if err != nil {
		return err
	}
	input := v1.ListLogsRequest{ContainerID: containerID, AttemptID: attemptID, AfterCursor: after, Limit: limit + 1}
	frames, err := s.service.LogsAfter(request.Context(), requestContext, input)
	if err != nil {
		return err
	}
	if err := validateLogPage(input, frames); err != nil {
		return err
	}
	hasMore := len(frames) > limit
	if hasMore {
		frames = frames[:limit]
	}
	if frames == nil {
		frames = []v1.LogFrame{}
	}
	nextCursor := afterToken
	if len(frames) > 0 {
		nextCursor, err = v1.NewLogCursor(containerID, attemptID, frames[len(frames)-1].Cursor)
		if err != nil {
			return v1.WrapError(v1.CodeInternal, "logs", "could not encode the verified log cursor", false, err)
		}
	}
	response := v1.LogListResponse{Frames: frames, NextCursor: nextCursor, HasMore: hasMore}
	writeSuccess(writer, requestContext, http.StatusOK, response)
	return nil
}

// parseLogLimit accepts an omitted default or a bounded positive decimal page size.
func parseLogLimit(value string) (int, error) {
	if value == "" {
		return defaultLogPageLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 || limit > maximumLogPageLimit {
		return 0, v1.NewError(v1.CodeInvalidArgument, "limit", "must be an integer from 1 through 100")
	}
	return limit, nil
}

// validateLogPage fails closed on cross-Attempt frames, invalid checksums, or regressing stream positions.
func validateLogPage(input v1.ListLogsRequest, frames []v1.LogFrame) error {
	if len(frames) > input.Limit {
		return v1.NewError(v1.CodeInternal, "logs", "service returned more frames than requested")
	}
	previousCursor := input.AfterCursor
	streamSequences := make(map[string]uint64, 2)
	for _, frame := range frames {
		if frame.ContainerID != input.ContainerID || frame.AttemptID != input.AttemptID {
			return v1.NewError(v1.CodeInternal, "logs", "service returned a frame for a different Container Attempt")
		}
		if frame.Stream != "stdout" && frame.Stream != "stderr" {
			return v1.NewError(v1.CodeInternal, "logs", "service returned an unsupported stream")
		}
		if frame.Cursor <= previousCursor || frame.Sequence == 0 || len(frame.Payload) == 0 {
			return v1.NewError(v1.CodeInternal, "logs", "service returned an invalid cursor, sequence, or payload")
		}
		if previous := streamSequences[frame.Stream]; previous != 0 && frame.Sequence != previous+1 {
			return v1.NewError(v1.CodeInternal, "logs", "service returned a non-contiguous per-stream sequence")
		}
		digest := sha256.Sum256(frame.Payload)
		if frame.PayloadSHA256 != hex.EncodeToString(digest[:]) {
			return v1.NewError(v1.CodeInternal, "logs", "service returned a frame with invalid payload evidence")
		}
		previousCursor = frame.Cursor
		streamSequences[frame.Stream] = frame.Sequence
	}
	return nil
}

// handleContainerAction validates start or kill input before dispatching to the matching service method.
func (s *Server) handleContainerAction(writer http.ResponseWriter, request *http.Request, id, action string) error {
	if err := requireMethod(writer, request, http.MethodPost); err != nil {
		return err
	}
	if err := rejectUnknownQuery(request); err != nil {
		return err
	}
	requestContext, err := readRequestContext(request, true)
	if err != nil {
		return err
	}
	var response v1.ContainerResponse
	switch action {
	case "start":
		var input v1.StartContainerRequest
		if err := s.decodeJSON(writer, request, &input); err != nil {
			return err
		}
		input.ContainerID = id
		response, err = s.service.StartContainer(request.Context(), requestContext, input)
	case "kill":
		var input v1.KillContainerRequest
		if err := s.decodeJSON(writer, request, &input); err != nil {
			return err
		}
		input.ContainerID = id
		if err := input.Validate(); err != nil {
			return err
		}
		response, err = s.service.KillContainer(request.Context(), requestContext, input)
	default:
		return v1.NewError(v1.CodeNotFound, "action", "container action does not exist")
	}
	if err != nil {
		return err
	}
	writeSuccess(writer, requestContext, http.StatusOK, response)
	return nil
}

// handleContainerResource serves authoritative read or operation-scoped deletion for one Container.
func (s *Server) handleContainerResource(writer http.ResponseWriter, request *http.Request, id string) error {
	if err := rejectUnknownQuery(request); err != nil {
		return err
	}
	switch request.Method {
	case http.MethodGet:
		if err := rejectReadBody(request); err != nil {
			return err
		}
		requestContext, err := readRequestContext(request, false)
		if err != nil {
			return err
		}
		response, err := s.service.GetContainer(request.Context(), requestContext, v1.GetContainerRequest{ContainerID: id})
		if err != nil {
			return err
		}
		writeSuccess(writer, requestContext, http.StatusOK, response)
		return nil
	case http.MethodDelete:
		requestContext, err := readRequestContext(request, true)
		if err != nil {
			return err
		}
		var input v1.DeleteContainerRequest
		if err := s.decodeJSON(writer, request, &input); err != nil {
			return err
		}
		input.ContainerID = id
		response, err := s.service.DeleteContainer(request.Context(), requestContext, input)
		if err != nil {
			return err
		}
		writeSuccess(writer, requestContext, http.StatusOK, response)
		return nil
	default:
		return requireMethod(writer, request, http.MethodGet, http.MethodDelete)
	}
}

// handleOperationPath returns one retained operation addressed by its client-generated identity.
func (s *Server) handleOperationPath(writer http.ResponseWriter, request *http.Request, escapedID string) error {
	if err := requireMethod(writer, request, http.MethodGet); err != nil {
		return err
	}
	if err := rejectUnknownQuery(request); err != nil {
		return err
	}
	if err := rejectReadBody(request); err != nil {
		return err
	}
	id, err := url.PathUnescape(escapedID)
	if err != nil {
		return v1.NewError(v1.CodeInvalidArgument, "operation_id", "contains invalid path escaping")
	}
	if err := v1.ValidateOperationID(id); err != nil {
		return err
	}
	requestContext, err := readRequestContext(request, false)
	if err != nil {
		return err
	}
	response, err := s.service.GetOperation(request.Context(), requestContext, v1.GetOperationRequest{OperationID: id})
	if err != nil {
		return err
	}
	writeSuccess(writer, requestContext, http.StatusOK, response)
	return nil
}

// handleEvents validates an opaque resume token and derives has-more with a bounded extra read.
func (s *Server) handleEvents(writer http.ResponseWriter, request *http.Request) error {
	if err := requireMethod(writer, request, http.MethodGet); err != nil {
		return err
	}
	if err := rejectReadBody(request); err != nil {
		return err
	}
	if err := rejectUnknownQuery(request, "after", "limit"); err != nil {
		return err
	}
	requestContext, err := readRequestContext(request, false)
	if err != nil {
		return err
	}
	afterToken := v1.ResumeToken(request.URL.Query().Get("after"))
	after, err := v1.ParseResumeToken(afterToken)
	if err != nil {
		return err
	}
	limit, err := parseEventLimit(request.URL.Query().Get("limit"))
	if err != nil {
		return err
	}
	events, err := s.service.EventsAfter(request.Context(), requestContext, v1.ListEventsRequest{AfterSequence: after, Limit: limit + 1})
	if err != nil {
		return err
	}
	if err := validateEventPage(after, events); err != nil {
		return err
	}
	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	nextToken := afterToken
	if len(events) > 0 {
		nextToken = v1.NewResumeToken(events[len(events)-1].Sequence)
	}
	response := v1.EventListResponse{Events: events, NextResumeToken: nextToken, HasMore: hasMore}
	writeSuccess(writer, requestContext, http.StatusOK, response)
	return nil
}

// parseEventLimit accepts an omitted default or a bounded positive decimal page size.
func parseEventLimit(value string) (int, error) {
	if value == "" {
		return defaultEventPageLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 || limit > maximumEventPageLimit {
		return 0, v1.NewError(v1.CodeInvalidArgument, "limit", "must be an integer from 1 through 500")
	}
	return limit, nil
}

// validateEventPage fails closed when a Service returns unordered or non-resumable events.
func validateEventPage(after uint64, events []v1.Event) error {
	previous := after
	for _, event := range events {
		if event.Sequence <= previous {
			return v1.NewError(v1.CodeInternal, "events", "service returned unordered event sequences")
		}
		previous = event.Sequence
	}
	return nil
}

// decodeResourceID unescapes exactly one path segment and enforces the public identifier contract.
func decodeResourceID(field, escaped string) (string, error) {
	value, err := url.PathUnescape(escaped)
	if err != nil {
		return "", v1.NewError(v1.CodeInvalidArgument, field, "contains invalid path escaping")
	}
	if err := v1.ValidateResourceID(field, value); err != nil {
		return "", err
	}
	return value, nil
}

// readRequestContext requires exactly one request ID and, for mutations, exactly one operation ID.
func readRequestContext(request *http.Request, mutation bool) (v1.RequestContext, error) {
	requestValues := request.Header.Values(v1.HeaderRequestID)
	if len(requestValues) != 1 {
		return v1.RequestContext{}, v1.NewError(v1.CodeInvalidArgument, "request_id", "must be supplied exactly once")
	}
	operationValues := request.Header.Values(v1.HeaderOperationID)
	if len(operationValues) > 1 {
		return v1.RequestContext{}, v1.NewError(v1.CodeInvalidArgument, "operation_id", "must be supplied at most once")
	}
	requestContext := v1.RequestContext{RequestID: requestValues[0]}
	if len(operationValues) == 1 {
		requestContext.OperationID = operationValues[0]
	}
	if mutation {
		return requestContext, requestContext.ValidateMutation()
	}
	return requestContext, requestContext.ValidateRead()
}

// decodeJSON enforces media type, size, unknown-field, and single-value constraints.
func (s *Server) decodeJSON(writer http.ResponseWriter, request *http.Request, destination any) error {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != v1.MediaTypeJSON {
		return v1.NewError(v1.CodeInvalidArgument, "content_type", "must be application/json")
	}
	request.Body = http.MaxBytesReader(writer, request.Body, s.config.MaxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		var maximum *http.MaxBytesError
		if errors.As(err, &maximum) {
			return v1.NewError(v1.CodeRequestTooLarge, "body", "exceeds the configured request size limit")
		}
		return v1.NewError(v1.CodeInvalidArgument, "body", fmt.Sprintf("must contain one strict JSON object: %v", err))
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return v1.NewError(v1.CodeInvalidArgument, "body", "must contain exactly one JSON value")
		}
		var maximum *http.MaxBytesError
		if errors.As(err, &maximum) {
			return v1.NewError(v1.CodeRequestTooLarge, "body", "exceeds the configured request size limit")
		}
		return v1.NewError(v1.CodeInvalidArgument, "body", "contains invalid trailing data")
	}
	return nil
}

// rejectReadBody keeps GET request fingerprints and proxy behavior unambiguous.
func rejectReadBody(request *http.Request) error {
	if request.ContentLength > 0 || len(request.TransferEncoding) > 0 {
		return v1.NewError(v1.CodeInvalidArgument, "body", "must be empty for a read-only request")
	}
	return nil
}

// rejectUnknownQuery fails on duplicate or unrecognized values rather than silently changing semantics.
func rejectUnknownQuery(request *http.Request, allowed ...string) error {
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return v1.NewError(v1.CodeInvalidArgument, "query", "contains invalid escaping or separators")
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, entries := range values {
		if _, ok := allowedSet[key]; !ok {
			return v1.NewError(v1.CodeInvalidArgument, "query."+key, "is not recognized")
		}
		if len(entries) != 1 {
			return v1.NewError(v1.CodeInvalidArgument, "query."+key, "must be supplied at most once")
		}
	}
	return nil
}

// writeSuccess echoes request identities in headers and emits one strict JSON response.
func writeSuccess(writer http.ResponseWriter, requestContext v1.RequestContext, status int, value any) {
	writer.Header().Set(v1.HeaderRequestID, requestContext.RequestID)
	if requestContext.OperationID != "" {
		writer.Header().Set(v1.HeaderOperationID, requestContext.OperationID)
	}
	writeJSON(writer, status, value)
}
