package main

import (
	"strings"
	"testing"

	v1 "mydocker/api/runtime/v1"
)

// TestReadJSONInputRejectsNestedDuplicateKeysAndInvalidUTF8 verifies files and
// stdin cannot silently overwrite request fields or replace malformed bytes.
func TestReadJSONInputRejectsNestedDuplicateKeysAndInvalidUTF8(t *testing.T) {
	valid := `{"sandbox_id":"sandbox-one","spec":{"network":{"mode":"none"},"resources":{"requests":{},"limits":{}}}}`
	tests := map[string]string{
		"nested duplicate":  strings.Replace(valid, `"mode":"none"`, `"mode":"none","mode":"loopback"`, 1),
		"case-folded alias": strings.Replace(valid, `"mode":"none"`, `"mode":"none","MODE":"loopback"`, 1),
		"invalid UTF-8":     strings.Replace(valid, "sandbox-one", "sandbox-\xff", 1),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			var destination v1.CreateSandboxRequest
			err := readJSONInput("-", strings.NewReader(payload), &destination)
			detail := v1.ErrorDetailFrom(err)
			if detail.Code != v1.CodeInvalidArgument || detail.Field != "input" {
				t.Fatalf("readJSONInput() detail = %#v for %v", detail, err)
			}
		})
	}
}
