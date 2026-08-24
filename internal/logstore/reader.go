package logstore

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// Reader is an identity-bound read-only view that reopens the file for every snapshot and never competes for the shim writer lock.
type Reader struct {
	files    FilePrimitives
	path     string
	identity Identity
}

// OpenReader validates one internally resolved private log location and returns a descriptor-free source suitable for daemon requests.
func OpenReader(path string, identity Identity, options ...Option) (*Reader, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	config := openConfig{files: osFilePrimitives{}}
	for index, option := range options {
		if option == nil {
			return nil, fmt.Errorf("workload log reader option %d must not be nil", index)
		}
		if err := option(&config); err != nil {
			return nil, fmt.Errorf("apply workload log reader option %d: %w", index, err)
		}
	}
	reader := &Reader{files: config.files, path: path, identity: identity}
	if _, err := reader.snapshot(0, 0, false); err != nil {
		return nil, err
	}
	return reader, nil
}

// Identity returns the immutable Container Attempt binding supplied by the trusted locator.
func (reader *Reader) Identity() Identity {
	if reader == nil {
		return Identity{}
	}
	return reader.identity
}

// Read reopens and validates one fixed-size snapshot, returning frames strictly after the cursor and ErrCursorGap for a future position without retaining a file descriptor.
func (reader *Reader) Read(after Cursor, limit int) ([]Frame, error) {
	if reader == nil {
		return nil, ErrClosed
	}
	if limit < 0 {
		return nil, ErrInvalidLimit
	}
	return reader.snapshot(after, limit, true)
}

// snapshot captures a synchronized size boundary under a short shared commit lock, then validates the prefix while retaining only the requested page.
func (reader *Reader) snapshot(after Cursor, limit int, collect bool) (_ []Frame, resultErr error) {
	before, err := validateSecurePath(reader.files, reader.path)
	if err != nil {
		return nil, err
	}
	if before == nil {
		return nil, fmt.Errorf("%w: resolved Attempt log is absent", ErrNotFound)
	}
	file, err := reader.files.OpenFile(reader.path, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: resolved Attempt log disappeared", ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("open workload log reader: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close workload log reader: %w", closeErr))
		}
	}()
	if err := validateOpenedFile(reader.files, reader.path, before, file); err != nil {
		return nil, err
	}
	if err := acquireCommitLock(file, unix.F_RDLCK, false); err != nil {
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EACCES) {
			return nil, ErrReadUnavailable
		}
		return nil, fmt.Errorf("lock workload log committed boundary: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect workload log committed size: %w", err)
	}
	if err := releaseCommitLock(file); err != nil {
		return nil, fmt.Errorf("unlock workload log committed boundary: %w", err)
	}
	scan, err := scanFramePage(file, reader.identity, info.Size(), after, limit, collect, false)
	if err != nil {
		return nil, err
	}
	return scan.frames, nil
}
