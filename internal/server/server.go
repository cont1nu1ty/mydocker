package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	v1 "mydocker/api/runtime/v1"
)

const (
	defaultMaxRequestBytes = int64(1 << 20)
	defaultMaxHeaderBytes  = 32 << 10
	defaultReadTimeout     = 30 * time.Second
	defaultWriteTimeout    = 30 * time.Second
	defaultHeaderTimeout   = 5 * time.Second
	defaultIdleTimeout     = 60 * time.Second
	defaultHandlerTimeout  = 30 * time.Second
)

// Config controls bounded HTTP handling and the filesystem-visible UDS endpoint.
type Config struct {
	SocketPath        string
	SocketMode        os.FileMode
	MaxRequestBytes   int64
	MaxHeaderBytes    int
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
	HandlerTimeout    time.Duration
}

// Server owns one UDS listener and exposes a transport-independent lifecycle Service.
type Server struct {
	config   Config
	service  Service
	http     *http.Server
	mu       sync.Mutex
	listener net.Listener
	done     chan struct{}
	serveErr error
}

// New validates dependencies and prepares an unstarted v1 server without touching the filesystem.
func New(config Config, service Service) (*Server, error) {
	if service == nil {
		return nil, errors.New("server service must not be nil")
	}
	config = withDefaults(config)
	if config.SocketPath == "" {
		return nil, errors.New("server Unix socket path must not be empty")
	}
	if config.MaxRequestBytes <= 0 || config.MaxHeaderBytes <= 0 || config.HandlerTimeout <= 0 ||
		config.ReadTimeout <= 0 || config.WriteTimeout <= 0 || config.ReadHeaderTimeout <= 0 || config.IdleTimeout <= 0 {
		return nil, errors.New("server request bounds and timeouts must be positive")
	}
	server := &Server{config: config, service: service, done: make(chan struct{})}
	server.http = &http.Server{
		Handler:           server.requestTimeout(server),
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		IdleTimeout:       config.IdleTimeout,
		MaxHeaderBytes:    config.MaxHeaderBytes,
	}
	return server, nil
}

// withDefaults fills zero-valued operational bounds while preserving explicit test values.
func withDefaults(config Config) Config {
	if config.SocketMode == 0 {
		config.SocketMode = 0o660
	}
	if config.MaxRequestBytes == 0 {
		config.MaxRequestBytes = defaultMaxRequestBytes
	}
	if config.MaxHeaderBytes == 0 {
		config.MaxHeaderBytes = defaultMaxHeaderBytes
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = defaultReadTimeout
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = defaultWriteTimeout
	}
	if config.ReadHeaderTimeout == 0 {
		config.ReadHeaderTimeout = defaultHeaderTimeout
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = defaultIdleTimeout
	}
	if config.HandlerTimeout == 0 {
		config.HandlerTimeout = defaultHandlerTimeout
	}
	return config
}

// Start synchronously binds and verifies the UDS before serving in a background goroutine.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return errors.New("server is already started")
	}
	listener, err := listenUnix(s.config.SocketPath, s.config.SocketMode)
	if err != nil {
		return err
	}
	s.listener = listener
	go func() {
		err := s.http.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		s.mu.Lock()
		s.serveErr = err
		close(s.done)
		s.mu.Unlock()
	}()
	return nil
}

// Wait blocks until serving ends and returns the normalized Serve result.
func (s *Server) Wait() error {
	s.mu.Lock()
	if s.listener == nil {
		s.mu.Unlock()
		return errors.New("server is not started")
	}
	done := s.done
	s.mu.Unlock()
	<-done
	s.mu.Lock()
	err := s.serveErr
	s.mu.Unlock()
	return err
}

// Shutdown drains handlers, closes the listener, and waits for socket cleanup.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	started := s.listener != nil
	s.mu.Unlock()
	if !started {
		return nil
	}
	shutdownErr := s.http.Shutdown(ctx)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, s.http.Close())
	}
	serveErr := s.Wait()
	return errors.Join(shutdownErr, serveErr)
}

// requestTimeout bounds service execution in addition to the HTTP read and write deadlines.
func (s *Server) requestTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), s.config.HandlerTimeout)
		defer cancel()
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

// ServeHTTP routes only the explicit v1 path vocabulary and always returns JSON failures.
func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)
	if !isVersionOnePath(request.URL.EscapedPath()) {
		s.writeError(writer, request, v1.NewError(v1.CodeUnsupportedVersion, "version", "only /v1 is supported"), request.Header.Get(v1.HeaderOperationID))
		return
	}
	if err := s.route(writer, request); err != nil {
		s.writeError(writer, request, err, request.Header.Get(v1.HeaderOperationID))
	}
}

// isVersionOnePath requires a path-segment boundary instead of accepting prefixes such as /v10.
func isVersionOnePath(path string) bool {
	return path == v1.BasePath || len(path) > len(v1.BasePath) && path[:len(v1.BasePath)+1] == v1.BasePath+"/"
}

// setResponseHeaders establishes the only v1 response encoding and basic sniffing protection.
func setResponseHeaders(writer http.ResponseWriter) {
	writer.Header().Set("Content-Type", v1.MediaTypeJSON)
	writer.Header().Set("X-Content-Type-Options", "nosniff")
}

// writeError converts typed service failures to a stable envelope without exposing unknown causes.
func (s *Server) writeError(writer http.ResponseWriter, request *http.Request, err error, operationID string) {
	detail := v1.ErrorDetailFrom(err)
	if errors.Is(err, context.DeadlineExceeded) {
		detail = v1.ErrorDetail{Code: v1.CodeDeadlineExceeded, Message: "request deadline exceeded", Retryable: true}
	} else if errors.Is(err, context.Canceled) {
		detail = v1.ErrorDetail{Code: v1.CodeCanceled, Message: "request canceled", Retryable: true}
	}
	requestID := request.Header.Get(v1.HeaderRequestID)
	if requestID != "" {
		writer.Header().Set(v1.HeaderRequestID, requestID)
	}
	if operationID != "" {
		writer.Header().Set(v1.HeaderOperationID, operationID)
	}
	envelope := v1.ErrorEnvelope{Error: detail, RequestID: requestID, OperationID: operationID}
	writeJSON(writer, v1.HTTPStatus(detail.Code), envelope)
}

// writeJSON marshals before committing headers so invalid service output becomes a clean internal response.
func writeJSON(writer http.ResponseWriter, status int, value any) {
	payload, err := jsonMarshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		envelope := v1.ErrorEnvelope{
			Error:       v1.ErrorDetail{Code: v1.CodeInternal, Message: "internal server error"},
			RequestID:   writer.Header().Get(v1.HeaderRequestID),
			OperationID: writer.Header().Get(v1.HeaderOperationID),
		}
		payload, _ = json.Marshal(envelope)
	}
	writer.WriteHeader(status)
	_, _ = writer.Write(append(payload, '\n'))
}

// jsonMarshal is a narrow seam that keeps response encoding centralized and testable.
func jsonMarshal(value any) ([]byte, error) {
	return json.Marshal(value)
}

// requireMethod returns a typed error and Allow header for an unsupported method.
func requireMethod(writer http.ResponseWriter, request *http.Request, methods ...string) error {
	for _, method := range methods {
		if request.Method == method {
			return nil
		}
	}
	writer.Header().Set("Allow", strings.Join(methods, ", "))
	return v1.NewError(v1.CodeMethodNotAllowed, "method", "method is not allowed for this path")
}
