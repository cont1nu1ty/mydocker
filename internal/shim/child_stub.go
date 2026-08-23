//go:build !linux

package shim

import (
	"errors"
	"io"

	"mydocker/internal/domain"
)

// OSChildRunner is unavailable outside the Linux-only production target.
type OSChildRunner struct{}

// Start fails closed because the M3 pidfd-backed wrapper supports Linux only.
func (OSChildRunner) Start(domain.ProcessSpec, io.Writer, io.Writer) (Child, error) {
	return nil, errors.New("OS child runner requires Linux pidfd support")
}
