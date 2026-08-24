package client

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestResponseJSONRejectsNestedDuplicateKeysAndInvalidUTF8WithoutRetry verifies complete
// ambiguous responses are never trusted as typed remote outcomes or retried as transport loss.
func TestResponseJSONRejectsNestedDuplicateKeysAndInvalidUTF8WithoutRetry(t *testing.T) {
	tests := map[string]string{
		"nested duplicate":  `{"error":{"code":"not_found","code":"not_found","message":"missing","retryable":false},"request_id":"request-one"}`,
		"case-folded alias": `{"error":{"code":"not_found","CODE":"internal","message":"missing","retryable":false},"request_id":"request-one"}`,
		"invalid UTF-8":     "{\"error\":{\"code\":\"not_found\",\"message\":\"bad \xff\",\"retryable\":false},\"request_id\":\"request-one\"}",
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			attempts := 0
			transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				attempts++
				return rawResponse(request, http.StatusNotFound, payload), nil
			})
			apiClient := newWithTransport(
				Config{Timeout: time.Second, TransportRetries: 1, MaxResponseBytes: 4096},
				transport,
				func() (string, error) { return "request-one", nil },
			)
			_, err := apiClient.GetSandbox(context.Background(), "sandbox-one")
			if err == nil {
				t.Fatal("GetSandbox() accepted ambiguous response JSON")
			}
			var remote *RemoteError
			var transportErr *TransportError
			if errors.As(err, &remote) || errors.As(err, &transportErr) {
				t.Fatalf("GetSandbox() error = %T %v, want non-retryable response decode failure", err, err)
			}
			if attempts != 1 {
				t.Fatalf("transport attempts = %d, want one semantic rejection", attempts)
			}
			if !strings.Contains(err.Error(), "decode v1 error response") {
				t.Fatalf("GetSandbox() error = %v, want response decode classification", err)
			}
		})
	}
}
