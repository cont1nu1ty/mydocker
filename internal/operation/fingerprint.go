package operation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// CanonicalRequestFingerprint returns a versioned SHA-256 digest of the
// request's canonical JSON value. The caller must pass only semantic request
// content: operation ID, request ID, trace context, timestamps, and other
// transport metadata must be excluded before this function is called.
func CanonicalRequestFingerprint(request any) (RequestFingerprint, error) {
	canonical, err := CanonicalRequestJSON(request)
	if err != nil {
		return RequestFingerprint{}, err
	}
	digest := sha256.Sum256(canonical)
	return RequestFingerprint{
		Version: CurrentFingerprintVersion,
		SHA256:  hex.EncodeToString(digest[:]),
	}, nil
}

// CanonicalRequestJSON converts a JSON-compatible request into deterministic
// bytes: object keys are sorted by encoding/json and array order, including
// argv order, is preserved. Re-decoding also canonicalizes custom MarshalJSON
// output instead of trusting its object-key order.
func CanonicalRequestJSON(request any) ([]byte, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal request for canonical fingerprint: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode request for canonical fingerprint: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("canonical request contains more than one JSON value")
		}
		return nil, fmt.Errorf("check canonical request boundary: %w", err)
	}

	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode canonical request: %w", err)
	}
	return canonical, nil
}
