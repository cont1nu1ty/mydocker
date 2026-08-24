package strictjson

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

type decodeFixture struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Nested struct {
		Value int `json:"value"`
	} `json:"nested"`
	Items []struct {
		Value int `json:"value"`
	} `json:"items"`
}

// TestDecodeRequiresExactStructFieldCase verifies encoding/json's fallback
// case folding cannot overwrite top-level, nested, or map-value struct fields.
func TestDecodeRequiresExactStructFieldCase(t *testing.T) {
	tests := map[string]struct {
		payload     string
		destination any
	}{
		"top-level alias": {
			payload:     `{"name":"first","NAME":"second","nested":{"value":1},"items":[]}`,
			destination: &decodeFixture{},
		},
		"nested alias": {
			payload:     `{"name":"first","nested":{"value":1,"VALUE":2},"items":[]}`,
			destination: &decodeFixture{},
		},
		"map value alias": {
			payload: `{"one":{"value":1,"VALUE":2}}`,
			destination: &map[string]struct {
				Value int `json:"value"`
			}{},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := Decode([]byte(test.payload), test.destination); !errors.Is(err, ErrNonCanonicalKey) {
				t.Fatalf("Decode(case alias) error = %v, want ErrNonCanonicalKey", err)
			}
		})
	}
}

// TestDecodePreservesCaseSensitiveMapKeys verifies strict struct matching does
// not collapse distinct application keys inside string-keyed maps.
func TestDecodePreservesCaseSensitiveMapKeys(t *testing.T) {
	payload := []byte(`{"name":"valid","labels":{"Role":"first","role":"second"},"nested":{"value":1},"items":[]}`)
	var destination decodeFixture
	if err := Decode(payload, &destination); err != nil {
		t.Fatalf("Decode(case-sensitive map) error = %v", err)
	}
	if len(destination.Labels) != 2 || destination.Labels["Role"] != "first" || destination.Labels["role"] != "second" {
		t.Fatalf("Decode(case-sensitive map) labels = %#v", destination.Labels)
	}
}

// TestDecodeAcceptsOneCanonicalDocument verifies strict checks preserve normal nested JSON decoding.
func TestDecodeAcceptsOneCanonicalDocument(t *testing.T) {
	payload := []byte(`{"name":"valid","nested":{"value":1},"items":[{"value":2}]}`)
	var destination decodeFixture
	if err := Decode(payload, &destination); err != nil {
		t.Fatalf("Decode(valid) error = %v", err)
	}
	if destination.Name != "valid" || destination.Nested.Value != 1 || len(destination.Items) != 1 || destination.Items[0].Value != 2 {
		t.Fatalf("Decode(valid) destination = %#v", destination)
	}
}

// TestDecodeRejectsDuplicateKeysAtAnyDepth verifies objects nested directly,
// beneath arrays, and through equivalent escaped names cannot overwrite values.
func TestDecodeRejectsDuplicateKeysAtAnyDepth(t *testing.T) {
	tests := map[string]string{
		"nested object":        `{"name":"value","nested":{"value":1,"value":2},"items":[]}`,
		"object beneath array": `{"name":"value","nested":{"value":1},"items":[{"value":1,"value":2}]}`,
		"escaped equivalent":   `{"name":"value","nested":{"value":1,"\u0076alue":2},"items":[]}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			var destination decodeFixture
			err := Decode([]byte(payload), &destination)
			if !errors.Is(err, ErrDuplicateKey) {
				t.Fatalf("Decode(duplicate) error = %v, want ErrDuplicateKey", err)
			}
		})
	}
}

// TestDecodeClassifiesStrictFailures verifies callers can distinguish ambiguous
// encoding and framing from ordinary schema and truncated-document failures.
func TestDecodeClassifiesStrictFailures(t *testing.T) {
	invalidUTF8 := append([]byte(`{"name":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`","nested":{"value":1},"items":[]}`)...)
	tests := []struct {
		name    string
		payload []byte
		want    error
	}{
		{name: "invalid UTF-8", payload: invalidUTF8, want: ErrInvalidUTF8},
		{name: "duplicate key", payload: []byte(`{"name":"first","name":"second","nested":{"value":1},"items":[]}`), want: ErrDuplicateKey},
		{name: "multiple values", payload: []byte(`{"name":"first","nested":{"value":1},"items":[]} {"name":"second"}`), want: ErrMultipleValues},
		{name: "truncated value", payload: []byte(`{"name":"first"`), want: io.ErrUnexpectedEOF},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var destination decodeFixture
			err := Decode(test.payload, &destination)
			if !errors.Is(err, test.want) {
				t.Fatalf("Decode() error = %v, want classification %v", err, test.want)
			}
		})
	}

	var destination decodeFixture
	err := Decode([]byte(`{"name":"valid","nested":{"value":1},"items":[],"future":true}`), &destination)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("Decode(unknown field) error = %v", err)
	}
	if errors.Is(err, ErrInvalidUTF8) || errors.Is(err, ErrDuplicateKey) || errors.Is(err, ErrMultipleValues) {
		t.Fatalf("unknown-field error was misclassified: %v", err)
	}
}

// FuzzDecodeRejectsGeneratedNestedDuplicateKeys checks arbitrary decoded key
// names cannot evade duplicate detection through JSON string escaping.
func FuzzDecodeRejectsGeneratedNestedDuplicateKeys(f *testing.F) {
	f.Add("field")
	f.Add("quote\"and\\slash")
	f.Add("\u4e2d\u6587")
	f.Fuzz(func(t *testing.T, key string) {
		if len(key) > 4096 {
			return
		}
		encodedKey, err := json.Marshal(key)
		if err != nil {
			t.Fatalf("json.Marshal(key) error = %v", err)
		}
		payload := append([]byte(`{"nested":{`), encodedKey...)
		payload = append(payload, []byte(`:1,`)...)
		payload = append(payload, encodedKey...)
		payload = append(payload, []byte(`:2}}`)...)
		var destination struct {
			Nested map[string]int `json:"nested"`
		}
		if err := Decode(payload, &destination); !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("Decode(generated duplicate %q) error = %v", key, err)
		}
	})
}
