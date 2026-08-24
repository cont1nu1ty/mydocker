package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	v1 "mydocker/api/runtime/v1"
)

// TestStrictRequestBytes verifies ambiguous wire bytes receive stable input
// errors before lifecycle service dispatch.
func TestStrictRequestBytes(t *testing.T) {
	service := newFakeService()
	_, _, socketPath := startTestServer(t, service, Config{})
	valid := `{"sandbox_id":"sandbox-one","spec":{"network":{"mode":"none"},"resources":{"requests":{},"limits":{}}}}`
	tests := map[string]string{
		"nested duplicate":  strings.Replace(valid, `"mode":"none"`, `"mode":"none","mode":"loopback"`, 1),
		"case-folded alias": strings.Replace(valid, `"mode":"none"`, `"mode":"none","MODE":"loopback"`, 1),
		"invalid UTF-8":     strings.Replace(valid, "sandbox-one", "sandbox-\xff", 1),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			response := postRawSandbox(t, socketPath, body)
			defer response.Body.Close()
			var envelope v1.ErrorEnvelope
			if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}
			if response.StatusCode != http.StatusBadRequest || envelope.Error.Code != v1.CodeInvalidArgument || envelope.Error.Field != "body" {
				t.Fatalf("status/error = %d/%#v, want invalid_argument body", response.StatusCode, envelope.Error)
			}
		})
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.createCalls != 0 {
		t.Fatalf("ambiguous requests reached service %d times", service.createCalls)
	}
}
