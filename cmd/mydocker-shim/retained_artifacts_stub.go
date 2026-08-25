//go:build !linux

package main

import (
	"errors"
	"os"

	"mydocker/internal/shim"
)

// retainInitArtifacts is unavailable where procfs directory-FD paths and Linux
// no-follow directory opens cannot provide the production retention contract.
func retainInitArtifacts(shim.RuntimeConfig) (*os.File, shim.RuntimeConfig, error) {
	return nil, shim.RuntimeConfig{}, errors.New("retained init artifacts require Linux")
}
