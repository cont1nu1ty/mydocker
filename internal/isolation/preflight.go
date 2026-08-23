package isolation

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

// PreflightConfig selects read-only host checks and the explicit privileged-test safety gate.
type PreflightConfig struct {
	CgroupRoot            string
	Namespaces            []NamespaceType
	ForPrivilegedTest     bool
	AllowPrivilegedTest   bool
	DisposableEnvironment bool
}

// DefaultPreflightConfig returns the rootful cgroup-v2 and five-namespace M2 feature set.
func DefaultPreflightConfig() PreflightConfig {
	return PreflightConfig{
		CgroupRoot: "/sys/fs/cgroup",
		Namespaces: []NamespaceType{NamespaceUTS, NamespaceIPC, NamespaceNetwork, NamespacePID, NamespaceMount},
	}
}

// PreflightReport records only observed host capabilities and never claims a privileged test ran.
type PreflightReport struct {
	Rootful               bool
	CgroupV2              bool
	Pidfd                 bool
	Namespaces            map[NamespaceType]bool
	CgroupRoot            string
	PrivilegedTestAllowed bool
}

// ValidatePrivilegedTest enforces both explicit opt-in and a caller-verified disposable environment.
func ValidatePrivilegedTest(config PreflightConfig) error {
	if !config.ForPrivilegedTest {
		return nil
	}
	if !config.AllowPrivilegedTest {
		return ErrPrivilegedTestDenied
	}
	if !config.DisposableEnvironment {
		return ErrUnsafeTestEnvironment
	}
	return nil
}

// Preflight performs read-only root, cgroup2, namespace, and pidfd probes through Ops.
func Preflight(ctx context.Context, ops Ops, config PreflightConfig) (PreflightReport, error) {
	if err := validateContext(ctx); err != nil {
		return PreflightReport{}, err
	}
	if err := requireOps(ops); err != nil {
		return PreflightReport{}, err
	}
	if err := ValidatePrivilegedTest(config); err != nil {
		return PreflightReport{}, err
	}
	if config.CgroupRoot == "" {
		config.CgroupRoot = DefaultPreflightConfig().CgroupRoot
	}
	if !filepath.IsAbs(config.CgroupRoot) || filepath.Clean(config.CgroupRoot) == string(filepath.Separator) {
		return PreflightReport{}, fmt.Errorf("%w: cgroup root must be an absolute non-root path", ErrPreflight)
	}
	if len(config.Namespaces) == 0 {
		config.Namespaces = DefaultPreflightConfig().Namespaces
	}
	report := PreflightReport{
		Rootful:               ops.EffectiveUID() == 0,
		Namespaces:            make(map[NamespaceType]bool, len(config.Namespaces)),
		CgroupRoot:            filepath.Clean(config.CgroupRoot),
		PrivilegedTestAllowed: config.ForPrivilegedTest && config.AllowPrivilegedTest && config.DisposableEnvironment,
	}
	if !report.Rootful {
		return report, fmt.Errorf("%w: effective UID must be zero for the rootful runtime", ErrPreflight)
	}
	filesystem, err := ops.StatFS(report.CgroupRoot)
	if err != nil {
		return report, fmt.Errorf("%w: inspect cgroup root: %v", ErrPreflight, err)
	}
	report.CgroupV2 = filesystem.Type == Cgroup2FSMagic
	if !report.CgroupV2 {
		return report, fmt.Errorf("%w: %s is not a cgroup v2 filesystem", ErrPreflight, report.CgroupRoot)
	}
	seen := make(map[NamespaceType]struct{}, len(config.Namespaces))
	for _, namespaceType := range config.Namespaces {
		if !namespaceType.Valid() {
			return report, fmt.Errorf("%w: unsupported namespace %q", ErrPreflight, namespaceType)
		}
		if _, duplicate := seen[namespaceType]; duplicate {
			return report, fmt.Errorf("%w: duplicate namespace %q", ErrPreflight, namespaceType)
		}
		seen[namespaceType] = struct{}{}
		if err := probeNamespace(ops, fmt.Sprintf("/proc/self/ns/%s", namespaceType.procName()), namespaceType); err != nil {
			return report, fmt.Errorf("%w: probe %s namespace: %v", ErrPreflight, namespaceType, err)
		}
		report.Namespaces[namespaceType] = true
	}
	pidfd, err := ops.PidfdOpen(ops.ProcessID())
	if err != nil {
		return report, fmt.Errorf("%w: pidfd_open self: %v", ErrPreflight, err)
	}
	defer ops.Close(pidfd)
	if err := ops.PidfdSendSignal(pidfd, 0); err != nil {
		return report, fmt.Errorf("%w: pidfd liveness probe: %v", ErrPreflight, err)
	}
	report.Pidfd = true
	return report, nil
}

// PreflightSystem runs the read-only probes against the current Linux host implementation.
func PreflightSystem(ctx context.Context, config PreflightConfig) (PreflightReport, error) {
	if !platformSupported() {
		return PreflightReport{}, ErrUnsupportedPlatform
	}
	return Preflight(ctx, NewSystemOps(), config)
}

// probeNamespace verifies nsfs, namespace kind, and descriptor inode without joining it.
func probeNamespace(ops Ops, path string, expected NamespaceType) error {
	return probeNamespaceInode(ops, path, expected, 0)
}

// probeNamespaceInode verifies nsfs, namespace kind, and an optional exact inode without joining it.
func probeNamespaceInode(ops Ops, path string, expected NamespaceType, expectedInode uint64) error {
	fd, err := ops.OpenNamespace(path)
	if err != nil {
		return err
	}
	defer ops.Close(fd)
	return validateNamespaceFD(ops, fd, expected, expectedInode)
}

// closeError preserves the primary error while making a descriptor-close failure visible when no primary exists.
func closeError(primary error, closeErr error) error {
	if primary != nil {
		return primary
	}
	if closeErr != nil && !errors.Is(closeErr, ErrClosed) {
		return closeErr
	}
	return nil
}
