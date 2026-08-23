package slim

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
)

// ProcessLaunchSpec contains only prevalidated exec, namespace descriptor, and
// clone-time cgroup inputs; argv and environment remain structured throughout.
type ProcessLaunchSpec struct {
	Executable  string
	Arguments   []string
	Environment []string
	CloneFlags  uintptr
	CgroupFD    int
	ExtraFDs    []int
	ReleaseFD   int
}

// Validate rejects ambient executable lookup, malformed argv/environment,
// invalid descriptors, and a release FD not matching ExtraFiles child numbering.
func (spec ProcessLaunchSpec) Validate() error {
	if !filepath.IsAbs(spec.Executable) || filepath.Clean(spec.Executable) != spec.Executable || spec.Executable == string(filepath.Separator) || strings.ContainsRune(spec.Executable, '\x00') {
		return errors.New("process launch executable must be a clean absolute non-root path")
	}
	if spec.CgroupFD < 0 || spec.CloneFlags == 0 {
		return errors.New("process launch requires a cgroup descriptor and clone flags")
	}
	seen := make(map[int]struct{}, len(spec.ExtraFDs))
	for _, fd := range spec.ExtraFDs {
		if fd < 3 {
			return errors.New("process launch extra descriptor must not alias standard streams")
		}
		if fd == spec.CgroupFD {
			return errors.New("process launch extra descriptor must not alias the borrowed cgroup descriptor")
		}
		if _, duplicate := seen[fd]; duplicate {
			return errors.New("process launch extra descriptors must be unique")
		}
		seen[fd] = struct{}{}
	}
	if spec.ReleaseFD != 3+len(spec.ExtraFDs) {
		return errors.New("process launch release descriptor does not match child ExtraFiles numbering")
	}
	if len(spec.Environment) == 0 {
		return errors.New("process launch requires an explicit non-inherited environment")
	}
	for _, value := range append(append([]string(nil), spec.Arguments...), spec.Environment...) {
		if strings.ContainsRune(value, '\x00') {
			return errors.New("process launch argv and environment must not contain NUL")
		}
	}
	for _, value := range spec.Environment {
		name, _, found := strings.Cut(value, "=")
		if !found || name == "" {
			return errors.New("process launch environment entries must contain a non-empty name")
		}
	}
	return nil
}

// ProcessFactory performs the only production fork/exec boundary. Start owns
// ExtraFDs and internally creates the parent-death release pipe.
type ProcessFactory interface {
	Preflight(context.Context) error
	Start(context.Context, ProcessLaunchSpec) (StartedProcess, error)
}

// StartedProcess retains two pidfd references and the release-pipe writer so
// callers can transfer identity, authorize execution, or abort exactly once.
type StartedProcess interface {
	PID() int
	TakePIDFD() (int, error)
	Release() error
	Abort(context.Context) error
	Commit() error
}
