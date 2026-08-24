package logstore

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const maxFrameBodyBytes = frameFixedBytes + 2*maxIdentityBytes + MaxPayloadBytes

type openConfig struct {
	files FilePrimitives
}

// Store owns one exclusively opened append log and its recovery-built cursor index.
type Store struct {
	mu             sync.RWMutex
	file           File
	identity       Identity
	lastCursor     Cursor
	lastSequence   map[Stream]uint64
	confirmedSize  int64
	closed         bool
	appendPoisoned error
}

// Open validates and exclusively opens one private Attempt log, recovering only an incomplete final frame.
func Open(path string, identity Identity, options ...Option) (*Store, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	config := openConfig{files: osFilePrimitives{}}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("workload log open option %d must not be nil", index)
		}
		if err := option(&config); err != nil {
			return nil, fmt.Errorf("apply workload log open option %d: %w", index, err)
		}
	}
	before, err := validateSecurePath(config.files, path)
	if err != nil {
		return nil, err
	}
	file, err := config.files.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open workload log: %w", err)
	}
	locked := false
	succeeded := false
	defer func() {
		if succeeded {
			return
		}
		if locked {
			_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		}
		_ = file.Close()
	}()
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrInUse, path)
		}
		return nil, fmt.Errorf("lock workload log: %w", err)
	}
	locked = true
	if err := validateOpenedFile(config.files, path, before, file); err != nil {
		return nil, err
	}
	// An empty entry may be a fresh file or the result of retrying an uncertain
	// creation. Synchronize its inode and mode before publishing the directory
	// entry so a workload that never writes output still has a durable log.
	if before == nil || before.Size() == 0 {
		if err := file.Sync(); err != nil {
			return nil, fmt.Errorf("synchronize empty workload log: %w", err)
		}
	}
	// Synchronize on every open so retry after an uncertain first directory sync
	// cannot mistake an existing but not yet durable entry for completed creation.
	if err := syncParentDirectory(config.files, path); err != nil {
		return nil, err
	}
	if err := acquireCommitLock(file, unix.F_WRLCK, true); err != nil {
		return nil, fmt.Errorf("lock workload log recovery boundary: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect workload log recovery size: %w", err)
	}
	scan, err := scanFramePage(file, identity, info.Size(), 0, 0, false, true)
	if err != nil {
		return nil, err
	}
	if err := releaseCommitLock(file); err != nil {
		return nil, fmt.Errorf("unlock workload log recovery boundary: %w", err)
	}
	store := &Store{
		file:          file,
		identity:      identity,
		lastCursor:    scan.lastCursor,
		lastSequence:  scan.lastSequence,
		confirmedSize: scan.validSize,
	}
	succeeded = true
	return store, nil
}

// Identity returns the immutable Container Attempt binding established by Open.
func (store *Store) Identity() Identity {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.identity
}

// Append assigns the next global cursor and per-stream sequence, returning only after file synchronization succeeds.
// A write or sync failure makes further append completion unknown until the caller closes and reopens the Store.
func (store *Store) Append(stream Stream, payload []byte) (Frame, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Frame{}, ErrClosed
	}
	if store.appendPoisoned != nil {
		return Frame{}, fmt.Errorf("%w: %w", ErrAppendUnavailable, store.appendPoisoned)
	}
	if !stream.Valid() {
		return Frame{}, fmt.Errorf("unsupported workload log stream %q", stream)
	}
	if len(payload) == 0 {
		return Frame{}, errors.New("workload log payload must not be empty")
	}
	if len(payload) > MaxPayloadBytes {
		return Frame{}, fmt.Errorf("workload log payload exceeds %d bytes", MaxPayloadBytes)
	}
	if store.lastCursor == Cursor(math.MaxUint64) || store.lastSequence[stream] == math.MaxUint64 {
		return Frame{}, errors.New("workload log cursor or sequence space is exhausted")
	}
	frame := newFrame(store.identity, stream, store.lastCursor+1, store.lastSequence[stream]+1, payload)
	encoded, err := encodeFrame(frame)
	if err != nil {
		return Frame{}, err
	}
	if store.confirmedSize > math.MaxInt64-int64(len(encoded)) {
		return Frame{}, errors.New("workload log file size is exhausted")
	}
	if err := acquireCommitLock(store.file, unix.F_WRLCK, true); err != nil {
		return Frame{}, fmt.Errorf("lock workload log append boundary: %w", err)
	}
	dataEnd := len(encoded) - frameCommitBytes
	if err := writeFull(store.file, encoded[:dataEnd]); err != nil {
		store.appendPoisoned = fmt.Errorf("write prepared frame: %w", err)
		return Frame{}, fmt.Errorf("%w: %w", ErrAppendUnavailable, store.appendPoisoned)
	}
	if err := store.file.Sync(); err != nil {
		store.appendPoisoned = fmt.Errorf("synchronize prepared frame: %w", err)
		return Frame{}, fmt.Errorf("%w: %w", ErrAppendUnavailable, store.appendPoisoned)
	}
	if err := writeFull(store.file, encoded[dataEnd:]); err != nil {
		store.appendPoisoned = fmt.Errorf("write frame commit marker: %w", err)
		return Frame{}, fmt.Errorf("%w: %w", ErrAppendUnavailable, store.appendPoisoned)
	}
	if err := store.file.Sync(); err != nil {
		store.appendPoisoned = fmt.Errorf("synchronize frame commit marker: %w", err)
		return Frame{}, fmt.Errorf("%w: %w", ErrAppendUnavailable, store.appendPoisoned)
	}
	if err := releaseCommitLock(store.file); err != nil {
		store.appendPoisoned = fmt.Errorf("publish synchronized frame: %w", err)
		return Frame{}, fmt.Errorf("%w: %w", ErrAppendUnavailable, store.appendPoisoned)
	}
	store.lastCursor = frame.Cursor
	store.lastSequence[stream] = frame.Sequence
	store.confirmedSize += int64(len(encoded))
	return frame.Clone(), nil
}

// Read returns frames whose global cursor is strictly greater than after, in cursor order; zero means all remaining and a future cursor returns ErrCursorGap.
func (store *Store) Read(after Cursor, limit int) ([]Frame, error) {
	if limit < 0 {
		return nil, ErrInvalidLimit
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return nil, ErrClosed
	}
	scan, err := scanFramePage(store.file, store.identity, store.confirmedSize, after, limit, true, false)
	if err != nil {
		return nil, err
	}
	if scan.lastCursor != store.lastCursor || scan.validSize != store.confirmedSize ||
		scan.lastSequence[StreamStdout] != store.lastSequence[StreamStdout] ||
		scan.lastSequence[StreamStderr] != store.lastSequence[StreamStderr] {
		return nil, fmt.Errorf("%w: confirmed writer index differs from file prefix", ErrCorrupt)
	}
	return scan.frames, nil
}

// LastCursor returns the greatest synchronized cursor visible to this open Store.
func (store *Store) LastCursor() (Cursor, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return 0, ErrClosed
	}
	return store.lastCursor, nil
}

// Close releases the exclusive file lock and descriptor; it is idempotent.
func (store *Store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	if store.appendPoisoned != nil {
		if err := store.file.Truncate(store.confirmedSize); err != nil {
			return fmt.Errorf("truncate unconfirmed workload log tail before close: %w", err)
		}
		if err := store.file.Sync(); err != nil {
			return fmt.Errorf("synchronize unconfirmed workload log tail removal before close: %w", err)
		}
	}
	store.closed = true
	unlockErr := unix.Flock(int(store.file.Fd()), unix.LOCK_UN)
	closeErr := store.file.Close()
	if unlockErr != nil || closeErr != nil {
		return errors.Join(unlockErr, closeErr)
	}
	return nil
}

// newFrame copies caller bytes and computes checksum evidence for the next store-assigned positions.
func newFrame(identity Identity, stream Stream, cursor Cursor, sequence uint64, payload []byte) Frame {
	copyPayload := append([]byte(nil), payload...)
	digest := payloadDigest(copyPayload)
	return Frame{
		SchemaVersion: SchemaVersion,
		Identity:      identity,
		Stream:        stream,
		Cursor:        cursor,
		Sequence:      sequence,
		Payload:       copyPayload,
		PayloadSHA256: digest,
	}
}

// payloadDigest returns the lowercase SHA-256 representation persisted and exposed with each frame.
func payloadDigest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// writeFull prevents a short write from being mistaken for a complete append.
func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if written < 0 || written > len(payload) {
			return errors.New("workload log writer returned an invalid byte count")
		}
		payload = payload[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

type scanResult struct {
	frames       []Frame
	lastCursor   Cursor
	lastSequence map[Stream]uint64
	validSize    int64
}

// scanFramePage validates every frame in one fixed-size prefix while optionally retaining only the requested cursor page and at most one body scratch buffer.
func scanFramePage(file File, identity Identity, snapshotSize int64, after Cursor, limit int, collect bool, recoverTail bool) (scanResult, error) {
	if limit < 0 {
		return scanResult{}, ErrInvalidLimit
	}
	if snapshotSize < 0 {
		return scanResult{}, fmt.Errorf("%w: negative file size", ErrCorrupt)
	}
	fileSize := snapshotSize
	frames := make([]Frame, 0)
	lastSequence := map[Stream]uint64{StreamStdout: 0, StreamStderr: 0}
	var lastCursor Cursor
	var bodyScratch []byte
	for offset := int64(0); offset < fileSize; {
		remaining := fileSize - offset
		if remaining < framePrefixBytes {
			if recoverTail {
				if err := truncateIncompleteTail(file, offset); err != nil {
					return scanResult{}, err
				}
			}
			fileSize = offset
			break
		}
		var prefix [framePrefixBytes]byte
		if err := readFullAt(file, prefix[:], offset); err != nil {
			return scanResult{}, fmt.Errorf("%w: read frame length at offset %d: %v", ErrCorrupt, offset, err)
		}
		bodyLength := binary.BigEndian.Uint64(prefix[0:8])
		bodyLengthComplement := binary.BigEndian.Uint64(prefix[8:16])
		if bodyLengthComplement != ^bodyLength {
			return scanResult{}, fmt.Errorf("%w: frame length check at offset %d does not match", ErrCorrupt, offset)
		}
		if bodyLength < frameFixedBytes || bodyLength > maxFrameBodyBytes {
			return scanResult{}, fmt.Errorf("%w: frame length %d at offset %d is out of bounds", ErrCorrupt, bodyLength, offset)
		}
		dataLength := uint64(framePrefixBytes) + bodyLength
		totalLength := dataLength + frameCommitBytes
		if totalLength > math.MaxInt64 || int64(totalLength) > remaining {
			if recoverTail {
				if err := truncateIncompleteTail(file, offset); err != nil {
					return scanResult{}, err
				}
			}
			fileSize = offset
			break
		}
		if cap(bodyScratch) < int(bodyLength) {
			bodyScratch = make([]byte, int(bodyLength))
		}
		body := bodyScratch[:int(bodyLength)]
		if err := readFullAt(file, body, offset+framePrefixBytes); err != nil {
			return scanResult{}, fmt.Errorf("%w: read frame body at offset %d: %v", ErrCorrupt, offset, err)
		}
		frame, err := decodeFrame(body)
		if err != nil {
			return scanResult{}, fmt.Errorf("frame at offset %d: %w", offset, err)
		}
		recordDigest := digestRecord(prefix[0:16], body)
		if !equalDigest(prefix[16:48], recordDigest[:]) {
			return scanResult{}, fmt.Errorf("%w: complete frame digest at offset %d does not match", ErrCorrupt, offset)
		}
		var commit [frameCommitBytes]byte
		if err := readFullAt(file, commit[:], offset+int64(dataLength)); err != nil {
			return scanResult{}, fmt.Errorf("%w: read frame commit marker at offset %d: %v", ErrCorrupt, offset, err)
		}
		if string(commit[0:8]) != string(frameCommitMagic[:]) || !equalDigest(commit[8:40], recordDigest[:]) {
			// A full-size but invalid final marker can be the torn result of the
			// marker-write-to-fsync crash window. It never published this frame,
			// so writable recovery discards only that exact tail. The same bytes
			// before EOF remain durable-history corruption and fail closed.
			if recoverTail && int64(totalLength) == remaining {
				if err := truncateIncompleteTail(file, offset); err != nil {
					return scanResult{}, err
				}
				fileSize = offset
				break
			}
			return scanResult{}, fmt.Errorf("%w: frame commit marker at offset %d does not match", ErrCorrupt, offset)
		}
		if frame.Identity != identity {
			return scanResult{}, fmt.Errorf("%w: frame at offset %d belongs to container %q attempt %q", ErrIdentityMismatch, offset, frame.Identity.ContainerID, frame.Identity.AttemptID)
		}
		if lastCursor == Cursor(math.MaxUint64) || frame.Cursor != lastCursor+1 {
			return scanResult{}, fmt.Errorf("%w: cursor %d at offset %d does not follow %d", ErrCorrupt, frame.Cursor, offset, lastCursor)
		}
		previousSequence := lastSequence[frame.Stream]
		if previousSequence == math.MaxUint64 || frame.Sequence != previousSequence+1 {
			return scanResult{}, fmt.Errorf("%w: %s sequence %d at offset %d does not follow %d", ErrCorrupt, frame.Stream, frame.Sequence, offset, previousSequence)
		}
		if collect && frame.Cursor > after && (limit == 0 || len(frames) < limit) {
			frames = append(frames, frame.Clone())
		}
		lastCursor = frame.Cursor
		lastSequence[frame.Stream] = frame.Sequence
		offset += int64(totalLength)
	}
	finalInfo, err := file.Stat()
	if err != nil {
		return scanResult{}, fmt.Errorf("reinspect workload log size: %w", err)
	}
	if recoverTail {
		if finalInfo.Size() != fileSize {
			return scanResult{}, fmt.Errorf("%w: file size changed during recovery scan", ErrCorrupt)
		}
	} else if finalInfo.Size() < snapshotSize {
		return scanResult{}, fmt.Errorf("%w: file shrank during read-only scan", ErrCorrupt)
	}
	if after > lastCursor {
		return scanResult{}, &CursorGapError{Requested: after, LastAvailable: lastCursor}
	}
	return scanResult{frames: frames, lastCursor: lastCursor, lastSequence: lastSequence, validSize: fileSize}, nil
}

// truncateIncompleteTail removes an interrupted final append and synchronizes that recovery before Open succeeds.
func truncateIncompleteTail(file File, size int64) error {
	if err := file.Truncate(size); err != nil {
		return fmt.Errorf("truncate incomplete workload log tail to %d: %w", size, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("synchronize incomplete workload log tail recovery: %w", err)
	}
	return nil
}

// readFullAt requires exactly the requested bytes at an already size-checked frame position.
func readFullAt(reader io.ReaderAt, payload []byte, offset int64) error {
	for len(payload) > 0 {
		read, err := reader.ReadAt(payload, offset)
		if read < 0 || read > len(payload) {
			return errors.New("workload log reader returned an invalid byte count")
		}
		payload = payload[read:]
		offset += int64(read)
		if err != nil {
			if errors.Is(err, io.EOF) && len(payload) == 0 {
				return nil
			}
			return err
		}
		if read == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
