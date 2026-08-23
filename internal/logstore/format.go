package logstore

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"mydocker/internal/domain"
)

const (
	framePrefixBytes = 48
	frameFixedBytes  = 68
	frameCommitBytes = 40
	maxIdentityBytes = 128
)

var frameMagic = [4]byte{'M', 'D', 'L', 'G'}

var frameCommitMagic = [8]byte{'M', 'D', 'L', 'G', 'C', 'M', 'T', '1'}

// encodeFrame serializes one validated frame into a length-prefixed, deterministic binary record.
func encodeFrame(frame Frame) ([]byte, error) {
	if err := frame.Validate(); err != nil {
		return nil, err
	}
	container := []byte(frame.Identity.ContainerID)
	attempt := []byte(frame.Identity.AttemptID)
	bodyLength := frameFixedBytes + len(container) + len(attempt) + len(frame.Payload)
	encoded := make([]byte, framePrefixBytes+bodyLength+frameCommitBytes)
	binary.BigEndian.PutUint64(encoded[0:8], uint64(bodyLength))
	binary.BigEndian.PutUint64(encoded[8:16], ^uint64(bodyLength))
	body := encoded[framePrefixBytes : framePrefixBytes+bodyLength]
	copy(body[0:4], frameMagic[:])
	binary.BigEndian.PutUint32(body[4:8], frame.SchemaVersion)
	binary.BigEndian.PutUint16(body[8:10], uint16(len(container)))
	binary.BigEndian.PutUint16(body[10:12], uint16(len(attempt)))
	streamCode, err := encodeStream(frame.Stream)
	if err != nil {
		return nil, err
	}
	body[12] = streamCode
	binary.BigEndian.PutUint64(body[16:24], uint64(frame.Cursor))
	binary.BigEndian.PutUint64(body[24:32], frame.Sequence)
	binary.BigEndian.PutUint32(body[32:36], uint32(len(frame.Payload)))
	digest := sha256.Sum256(frame.Payload)
	copy(body[36:68], digest[:])
	offset := frameFixedBytes
	copy(body[offset:], container)
	offset += len(container)
	copy(body[offset:], attempt)
	offset += len(attempt)
	copy(body[offset:], frame.Payload)
	recordDigest := digestRecord(encoded[0:16], body)
	copy(encoded[16:48], recordDigest[:])
	commit := encoded[framePrefixBytes+bodyLength:]
	copy(commit[0:8], frameCommitMagic[:])
	copy(commit[8:40], recordDigest[:])
	return encoded, nil
}

// decodeFrame parses one complete body while rejecting unknown fields, malformed lengths, and checksum failures; Payload borrows body until the caller clones it.
func decodeFrame(body []byte) (Frame, error) {
	if len(body) < frameFixedBytes {
		return Frame{}, fmt.Errorf("%w: frame body is shorter than fixed header", ErrCorrupt)
	}
	if string(body[0:4]) != string(frameMagic[:]) {
		return Frame{}, fmt.Errorf("%w: frame magic does not match", ErrCorrupt)
	}
	schema := binary.BigEndian.Uint32(body[4:8])
	if schema != SchemaVersion {
		return Frame{}, fmt.Errorf("%w: version %d", ErrUnsupportedSchema, schema)
	}
	containerLength := int(binary.BigEndian.Uint16(body[8:10]))
	attemptLength := int(binary.BigEndian.Uint16(body[10:12]))
	if containerLength == 0 || containerLength > maxIdentityBytes || attemptLength == 0 || attemptLength > maxIdentityBytes {
		return Frame{}, fmt.Errorf("%w: identity length is out of bounds", ErrCorrupt)
	}
	if body[13] != 0 || body[14] != 0 || body[15] != 0 {
		return Frame{}, fmt.Errorf("%w: reserved header bytes are nonzero", ErrCorrupt)
	}
	stream, err := decodeStream(body[12])
	if err != nil {
		return Frame{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	payloadLength := int(binary.BigEndian.Uint32(body[32:36]))
	if payloadLength == 0 || payloadLength > MaxPayloadBytes {
		return Frame{}, fmt.Errorf("%w: payload length is out of bounds", ErrCorrupt)
	}
	expectedLength := frameFixedBytes + containerLength + attemptLength + payloadLength
	if len(body) != expectedLength {
		return Frame{}, fmt.Errorf("%w: frame body length does not match embedded lengths", ErrCorrupt)
	}
	offset := frameFixedBytes
	container := string(body[offset : offset+containerLength])
	offset += containerLength
	attempt := string(body[offset : offset+attemptLength])
	offset += attemptLength
	payload := body[offset:]
	digest := sha256.Sum256(payload)
	if !equalDigest(body[36:68], digest[:]) {
		return Frame{}, fmt.Errorf("%w: payload checksum does not match", ErrCorrupt)
	}
	frame := Frame{
		SchemaVersion: SchemaVersion,
		Identity: Identity{
			ContainerID: domain.ContainerID(container),
			AttemptID:   domain.AttemptID(attempt),
		},
		Stream:        stream,
		Cursor:        Cursor(binary.BigEndian.Uint64(body[16:24])),
		Sequence:      binary.BigEndian.Uint64(body[24:32]),
		Payload:       payload,
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}
	if err := frame.Validate(); err != nil {
		return Frame{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return frame, nil
}

// encodeStream maps the public stream vocabulary onto the stable one-byte disk representation.
func encodeStream(stream Stream) (byte, error) {
	switch stream {
	case StreamStdout:
		return 1, nil
	case StreamStderr:
		return 2, nil
	default:
		return 0, fmt.Errorf("unsupported workload log stream %q", stream)
	}
}

// decodeStream maps one disk stream code onto the bounded public vocabulary.
func decodeStream(code byte) (Stream, error) {
	switch code {
	case 1:
		return StreamStdout, nil
	case 2:
		return StreamStderr, nil
	default:
		return "", fmt.Errorf("unsupported workload log stream code %d", code)
	}
}

// equalDigest compares two fixed-size public checksums without accepting truncated evidence.
func equalDigest(left, right []byte) bool {
	if len(left) != sha256.Size || len(right) != sha256.Size {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

// digestRecord binds the complemented length and complete body so valid-looking metadata corruption cannot be accepted.
func digestRecord(lengthPrefix, body []byte) [sha256.Size]byte {
	hasher := sha256.New()
	_, _ = hasher.Write(lengthPrefix)
	_, _ = hasher.Write(body)
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}
