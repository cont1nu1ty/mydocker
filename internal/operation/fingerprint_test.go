package operation

import (
	"encoding/json"
	"testing"
)

// fingerprintRequest is semantic request content after the caller has removed
// operation, transport request, and trace metadata.
type fingerprintRequest struct {
	Image       string            `json:"image"`
	Argv        []string          `json:"argv"`
	Environment map[string]string `json:"environment"`
}

// reverseObjectJSON deliberately emits unsorted object keys to verify that
// CanonicalRequestJSON does not trust custom marshaler key order.
type reverseObjectJSON struct{}

// MarshalJSON returns a valid object with reverse lexical key order for the
// canonicalization regression test.
func (reverseObjectJSON) MarshalJSON() ([]byte, error) {
	return []byte(`{"z":1,"a":2}`), nil
}

// mustFingerprint creates a request fingerprint or fails the current test.
func mustFingerprint(t *testing.T, request any) RequestFingerprint {
	t.Helper()
	fingerprint, err := CanonicalRequestFingerprint(request)
	if err != nil {
		t.Fatalf("CanonicalRequestFingerprint() error = %v", err)
	}
	return fingerprint
}

// TestCanonicalRequestFingerprintStableMapOrder verifies that map insertion
// order and custom JSON object order do not change a semantic request digest.
func TestCanonicalRequestFingerprintStableMapOrder(t *testing.T) {
	first := fingerprintRequest{
		Image: "sha256:image",
		Argv:  []string{"/bin/echo", "hello"},
		Environment: map[string]string{
			"ZED":   "last",
			"ALPHA": "first",
		},
	}
	second := fingerprintRequest{
		Image: "sha256:image",
		Argv:  []string{"/bin/echo", "hello"},
		Environment: map[string]string{
			"ALPHA": "first",
			"ZED":   "last",
		},
	}
	if got, want := mustFingerprint(t, first), mustFingerprint(t, second); got != want {
		t.Fatalf("fingerprints differ by map order: got %+v want %+v", got, want)
	}

	canonical, err := CanonicalRequestJSON(reverseObjectJSON{})
	if err != nil {
		t.Fatalf("CanonicalRequestJSON() error = %v", err)
	}
	if got, want := string(canonical), `{"a":2,"z":1}`; got != want {
		t.Fatalf("CanonicalRequestJSON() = %s, want %s", got, want)
	}
}

// TestCanonicalRequestFingerprintChangesWithSemanticContent verifies that argv
// order and other semantic request changes produce a different binding.
func TestCanonicalRequestFingerprintChangesWithSemanticContent(t *testing.T) {
	base := fingerprintRequest{
		Image:       "sha256:image-a",
		Argv:        []string{"printf", "%s", "value"},
		Environment: map[string]string{"MODE": "test"},
	}
	argvChanged := base
	argvChanged.Argv = []string{"printf", "value", "%s"}
	imageChanged := base
	imageChanged.Image = "sha256:image-b"

	baseFingerprint := mustFingerprint(t, base)
	for name, request := range map[string]fingerprintRequest{
		"argv order": argvChanged,
		"image":      imageChanged,
	} {
		t.Run(name, func(t *testing.T) {
			if got := mustFingerprint(t, request); got == baseFingerprint {
				t.Fatalf("fingerprint did not change for %s", name)
			}
		})
	}
}

// TestCanonicalRequestFingerprintExcludesMetadataByInputBoundary documents that
// callers obtain retry-stable digests by passing only semantic request content.
func TestCanonicalRequestFingerprintExcludesMetadataByInputBoundary(t *testing.T) {
	type transportEnvelope struct {
		OperationID string             `json:"operation_id"`
		RequestID   string             `json:"request_id"`
		TraceID     string             `json:"trace_id"`
		Request     fingerprintRequest `json:"request"`
	}
	first := transportEnvelope{
		OperationID: "op-1",
		RequestID:   "request-1",
		TraceID:     "trace-1",
		Request:     fingerprintRequest{Image: "sha256:image", Argv: []string{"true"}},
	}
	second := first
	second.RequestID = "request-2"
	second.TraceID = "trace-2"

	if got, want := mustFingerprint(t, first.Request), mustFingerprint(t, second.Request); got != want {
		t.Fatalf("semantic request fingerprints differ: got %+v want %+v", got, want)
	}
	if got, want := mustFingerprint(t, first), mustFingerprint(t, second); got == want {
		t.Fatal("full transport envelopes unexpectedly have the same fingerprint")
	}
}

// TestCanonicalRequestFingerprintRejectsUnsupportedValue verifies that invalid
// JSON input is returned as an error rather than assigned an unstable digest.
func TestCanonicalRequestFingerprintRejectsUnsupportedValue(t *testing.T) {
	if _, err := CanonicalRequestFingerprint(map[string]any{"channel": make(chan int)}); err == nil {
		t.Fatal("CanonicalRequestFingerprint() error = nil, want unsupported value error")
	}
}

// TestRequestFingerprintValidate verifies schema, length, and hexadecimal checks.
func TestRequestFingerprintValidate(t *testing.T) {
	valid := mustFingerprint(t, map[string]string{"key": "value"})
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid fingerprint rejected: %v", err)
	}

	tests := map[string]RequestFingerprint{
		"version": {Version: CurrentFingerprintVersion + 1, SHA256: valid.SHA256},
		"length":  {Version: CurrentFingerprintVersion, SHA256: "short"},
		"case":    {Version: CurrentFingerprintVersion, SHA256: "A" + valid.SHA256[1:]},
	}
	for name, fingerprint := range tests {
		t.Run(name, func(t *testing.T) {
			if err := fingerprint.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want validation error")
			}
		})
	}
}

// TestCanonicalRequestJSONProducesValidJSON verifies the canonical bytes remain
// ordinary standard-library JSON suitable for hashing and diagnostics.
func TestCanonicalRequestJSONProducesValidJSON(t *testing.T) {
	canonical, err := CanonicalRequestJSON(map[string]any{"nested": map[string]int{"b": 2, "a": 1}})
	if err != nil {
		t.Fatalf("CanonicalRequestJSON() error = %v", err)
	}
	if !json.Valid(canonical) {
		t.Fatalf("CanonicalRequestJSON() produced invalid JSON: %q", canonical)
	}
}
