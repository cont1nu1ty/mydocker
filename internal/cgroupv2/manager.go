package cgroupv2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"mydocker/internal/domain"
)

const (
	controllerEnableCommand = "+cpu +memory +pids"
	keeperLeafName          = "keeper"
)

var requiredControllers = []string{"cpu", "memory", "pids"}

// ProcessReference supplies a PID only after action-time verification of strong evidence that already includes cgroup identity.
// Implementations are runtime-only, must never persist the PID as authority, and must not migrate it after evidence capture.
type ProcessReference interface {
	VerifiedPID(ctx context.Context) (int, error)
}

// Manager owns deterministic cgroup paths below one explicitly configured v2 root.
type Manager struct {
	config Config
	fs     FileSystem
	probe  HostProbe
}

// NewManager validates dependencies and the lexical ownership boundary without mutating the host.
func NewManager(config Config, filesystem FileSystem, probe HostProbe) (*Manager, error) {
	normalized, err := config.normalize()
	if err != nil {
		return nil, err
	}
	if filesystem == nil {
		return nil, errors.New("cgroup filesystem must not be nil")
	}
	if probe == nil {
		return nil, errors.New("cgroup host probe must not be nil")
	}
	return &Manager{config: normalized, fs: filesystem, probe: probe}, nil
}

// Root returns the canonical delegated ownership root used for every derived path.
func (m *Manager) Root() string {
	if m == nil {
		return ""
	}
	return m.config.Root
}

// Preflight verifies the exact root is a non-symlink directory on cgroup v2 with all required controllers.
func (m *Manager) Preflight(ctx context.Context) error {
	if m == nil {
		return errors.New("cgroup manager must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.verifyDirectory(m.config.Root); err != nil {
		return fmt.Errorf("verify cgroup root: %w", err)
	}
	supported, err := m.probe.IsCgroupV2(m.config.Root)
	if err != nil {
		return err
	}
	if !supported {
		return fmt.Errorf("configured root %q: %w", m.config.Root, ErrUnsupported)
	}
	if err := m.requireControllers(m.config.Root); err != nil {
		return err
	}
	return ctx.Err()
}

// SandboxPath derives a fixed-length hexadecimal component from SandboxID and proves containment below Root.
func (m *Manager) SandboxPath(id domain.SandboxID) (string, error) {
	if m == nil {
		return "", errors.New("cgroup manager must not be nil")
	}
	if err := id.Validate(); err != nil {
		return "", err
	}
	return m.joinOwned("sandbox-" + hexadecimalID(string(id)))
}

// KeeperPath derives the fixed keeper leaf below a Sandbox parent and proves containment below Root.
func (m *Manager) KeeperPath(id domain.SandboxID) (string, error) {
	sandboxPath, err := m.SandboxPath(id)
	if err != nil {
		return "", err
	}
	return m.joinOwned(filepath.Base(sandboxPath), keeperLeafName)
}

// AttemptPath derives an Attempt leaf that is a sibling of the fixed keeper below its process-free Sandbox parent.
func (m *Manager) AttemptPath(sandboxID domain.SandboxID, attemptID domain.AttemptID) (string, error) {
	sandboxPath, err := m.SandboxPath(sandboxID)
	if err != nil {
		return "", err
	}
	if err := attemptID.Validate(); err != nil {
		return "", err
	}
	return m.joinOwned(filepath.Base(sandboxPath), "attempt-"+hexadecimalID(string(attemptID)))
}

// CreateSandbox idempotently creates a process-free parent and enables cpu, memory, and pids for keeper and Attempt leaves.
func (m *Manager) CreateSandbox(ctx context.Context, id domain.SandboxID) (SandboxCgroup, error) {
	if err := m.Preflight(ctx); err != nil {
		return SandboxCgroup{}, err
	}
	path, err := m.SandboxPath(id)
	if err != nil {
		return SandboxCgroup{}, err
	}
	if err := m.enableControllers(m.config.Root); err != nil {
		return SandboxCgroup{}, fmt.Errorf("enable root controllers: %w", err)
	}
	created, err := m.ensureDirectory(path)
	if err != nil {
		return SandboxCgroup{}, fmt.Errorf("create Sandbox cgroup: %w", err)
	}
	if err := m.enableControllers(path); err != nil {
		if created {
			if cleanupErr := m.removeExact(path); cleanupErr != nil {
				return SandboxCgroup{}, errors.Join(fmt.Errorf("enable Sandbox controllers: %w", err), fmt.Errorf("rollback Sandbox cgroup: %w", cleanupErr))
			}
		}
		return SandboxCgroup{}, fmt.Errorf("enable Sandbox controllers: %w", err)
	}
	return SandboxCgroup{SandboxID: id, Path: path}, nil
}

// CreateKeeper idempotently creates the fixed process-bearing leaf while preserving an empty controller-owning Sandbox parent.
func (m *Manager) CreateKeeper(ctx context.Context, id domain.SandboxID) (KeeperCgroup, error) {
	if err := m.Preflight(ctx); err != nil {
		return KeeperCgroup{}, err
	}
	sandboxPath, err := m.SandboxPath(id)
	if err != nil {
		return KeeperCgroup{}, err
	}
	if err := m.verifyDirectory(sandboxPath); err != nil {
		return KeeperCgroup{}, fmt.Errorf("verify Sandbox cgroup: %w", err)
	}
	if err := m.enableControllers(sandboxPath); err != nil {
		return KeeperCgroup{}, fmt.Errorf("verify process-free Sandbox controller parent: %w", err)
	}
	path, err := m.KeeperPath(id)
	if err != nil {
		return KeeperCgroup{}, err
	}
	if _, err := m.ensureDirectory(path); err != nil {
		return KeeperCgroup{}, fmt.Errorf("create keeper leaf cgroup: %w", err)
	}
	return KeeperCgroup{SandboxID: id, Path: path}, nil
}

// CreateAttempt idempotently creates an Attempt child from immutable resolved Container limits, writes controls, and verifies host-canonical readback.
func (m *Manager) CreateAttempt(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID, limits domain.ResolvedResourceLimits) (AttemptCgroup, EffectiveLimits, error) {
	if err := limits.Validate(); err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, err
	}
	if err := m.Preflight(ctx); err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, err
	}
	pageSize, err := m.probe.PageSize()
	if err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, fmt.Errorf("read host page size: %w", err)
	}
	writes, expected, err := planLimits(limits, pageSize)
	if err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, err
	}
	sandboxPath, err := m.SandboxPath(sandboxID)
	if err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, err
	}
	if err := m.verifyDirectory(sandboxPath); err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, fmt.Errorf("verify Sandbox cgroup: %w", err)
	}
	if err := m.enableControllers(sandboxPath); err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, fmt.Errorf("enable Sandbox controllers: %w", err)
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, err
	}
	created, err := m.ensureDirectory(path)
	if err != nil {
		return AttemptCgroup{}, EffectiveLimits{}, fmt.Errorf("create Attempt cgroup: %w", err)
	}
	fail := func(primary error) (AttemptCgroup, EffectiveLimits, error) {
		if !created {
			return AttemptCgroup{}, EffectiveLimits{}, primary
		}
		if cleanupErr := m.removeExact(path); cleanupErr != nil {
			return AttemptCgroup{}, EffectiveLimits{}, errors.Join(primary, fmt.Errorf("rollback Attempt cgroup: %w", cleanupErr))
		}
		return AttemptCgroup{}, EffectiveLimits{}, primary
	}
	if err := m.writeLimits(path, writes); err != nil {
		return fail(err)
	}
	effective, err := m.readEffectiveLimitsPath(path)
	if err != nil {
		return fail(fmt.Errorf("read effective limits: %w", err))
	}
	if !effective.Equal(expected) {
		return fail(fmt.Errorf("got %+v, want %+v: %w", effective, expected, ErrEffectiveMismatch))
	}
	return AttemptCgroup{SandboxID: sandboxID, AttemptID: attemptID, Path: path}, effective, nil
}

// AttachProcess action-time verifies process identity and only confirms pre-established Attempt membership; it never migrates a process after evidence capture.
func (m *Manager) AttachProcess(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID, process ProcessReference) error {
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return err
	}
	return m.confirmProcessMembership(ctx, path, process, "Attempt")
}

// ConfirmKeeperProcess action-time verifies a keeper and only confirms membership in its fixed leaf; it never writes cgroup.procs.
func (m *Manager) ConfirmKeeperProcess(ctx context.Context, sandboxID domain.SandboxID, process ProcessReference) error {
	path, err := m.KeeperPath(sandboxID)
	if err != nil {
		return err
	}
	return m.confirmProcessMembership(ctx, path, process, "keeper")
}

// confirmProcessMembership brackets a read-only membership observation with strong identity verification so PID exit or reuse fails closed.
func (m *Manager) confirmProcessMembership(ctx context.Context, path string, process ProcessReference, resource string) error {
	if process == nil {
		return errors.New("verified process reference must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pid, err := process.VerifiedPID(ctx)
	if err != nil {
		return fmt.Errorf("verify process for cgroup membership confirmation: %w", err)
	}
	if pid <= 0 {
		return fmt.Errorf("verified process PID must be positive")
	}
	if err := m.verifyDirectory(path); err != nil {
		return err
	}
	members, err := m.readMembershipPath(path)
	if err != nil {
		return err
	}
	for _, member := range members {
		if member == pid {
			reverifiedPID, verifyErr := process.VerifiedPID(ctx)
			if verifyErr != nil {
				return fmt.Errorf("reverify process after cgroup membership confirmation: %w", verifyErr)
			}
			if reverifiedPID != pid {
				return fmt.Errorf("process PID changed from %d to %d during %s cgroup membership confirmation: %w", pid, reverifiedPID, resource, ErrEffectiveMismatch)
			}
			return nil
		}
	}
	return fmt.Errorf("verified PID %d was not launched in the %s cgroup: %w", pid, resource, ErrEffectiveMismatch)
}

// OpenAttempt opens the exact non-symlink Attempt cgroup for fd-relative runtime operations.
func (m *Manager) OpenAttempt(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) (DirectoryHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return nil, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return nil, err
	}
	handle, err := m.fs.OpenDir(path)
	if err != nil {
		return nil, fmt.Errorf("open Attempt cgroup: %w", err)
	}
	return handle, nil
}

// OpenKeeper opens the exact fixed keeper leaf for clone-time cgroup
// placement. The descriptor is runtime-only and does not authorize process
// migration or become persisted resource identity.
func (m *Manager) OpenKeeper(ctx context.Context, sandboxID domain.SandboxID) (DirectoryHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := m.KeeperPath(sandboxID)
	if err != nil {
		return nil, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return nil, err
	}
	handle, err := m.fs.OpenDir(path)
	if err != nil {
		return nil, fmt.Errorf("open keeper cgroup: %w", err)
	}
	return handle, nil
}

// KeeperProcessIDs returns the exact current members of one verified keeper
// leaf so launch recovery can distinguish an empty intent from an uncheckpointed wrapper.
func (m *Manager) KeeperProcessIDs(ctx context.Context, sandboxID domain.SandboxID) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := m.KeeperPath(sandboxID)
	if err != nil {
		return nil, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return nil, err
	}
	return m.readMembershipPath(path)
}

// AttemptProcessIDs returns the exact current members of one verified Attempt
// leaf for fail-closed init discovery after a daemon crash before receipt checkpointing.
func (m *Manager) AttemptProcessIDs(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return nil, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return nil, err
	}
	return m.readMembershipPath(path)
}

// InspectSandboxPresence performs a read-only exact-directory check below the configured delegated root.
func (m *Manager) InspectSandboxPresence(ctx context.Context, sandboxID domain.SandboxID) (bool, error) {
	if err := m.Preflight(ctx); err != nil {
		return false, err
	}
	path, err := m.SandboxPath(sandboxID)
	if err != nil {
		return false, err
	}
	return m.inspectExactPresence(path)
}

// InspectKeeperPresence performs a read-only check for the fixed process-bearing keeper leaf.
func (m *Manager) InspectKeeperPresence(ctx context.Context, sandboxID domain.SandboxID) (bool, error) {
	if err := m.Preflight(ctx); err != nil {
		return false, err
	}
	path, err := m.KeeperPath(sandboxID)
	if err != nil {
		return false, err
	}
	return m.inspectExactPresence(path)
}

// InspectAttemptPresence performs a read-only check for one deterministic Attempt leaf.
func (m *Manager) InspectAttemptPresence(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) (bool, error) {
	if err := m.Preflight(ctx); err != nil {
		return false, err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return false, err
	}
	return m.inspectExactPresence(path)
}

// inspectExactPresence distinguishes verified absence from unsafe metadata without creating or removing anything.
func (m *Manager) inspectExactPresence(path string) (bool, error) {
	if _, err := m.joinOwnedRelative(path); err != nil {
		return false, err
	}
	info, err := m.fs.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect cgroup presence %q: %w: %w", path, err, ErrUnknownState)
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return false, fmt.Errorf("path %q is not an owned non-symlink directory: %w", path, ErrUnknownState)
	}
	return true, nil
}

// RemoveAttempt idempotently removes only the exact Attempt cgroup after verified empty and childless observations.
func (m *Manager) RemoveAttempt(ctx context.Context, sandboxID domain.SandboxID, attemptID domain.AttemptID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := m.AttemptPath(sandboxID, attemptID)
	if err != nil {
		return err
	}
	return m.removeExact(path)
}

// RemoveKeeper idempotently removes only the exact empty keeper leaf and never traverses sibling Attempts or the parent.
func (m *Manager) RemoveKeeper(ctx context.Context, id domain.SandboxID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := m.KeeperPath(id)
	if err != nil {
		return err
	}
	return m.removeExact(path)
}

// RemoveSandbox idempotently removes only the exact Sandbox parent after all processes and child cgroups are absent.
func (m *Manager) RemoveSandbox(ctx context.Context, id domain.SandboxID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := m.SandboxPath(id)
	if err != nil {
		return err
	}
	return m.removeExact(path)
}

// writeLimits writes only cgroup enforcement controls and never consults or serializes request fields.
func (m *Manager) writeLimits(path string, limits EffectiveLimits) error {
	writes := []struct {
		name  string
		value string
	}{
		{name: "cpu.max", value: formatCPUMax(limits.CPU)},
		{name: "memory.max", value: formatScalarLimit(limits.Memory)},
		{name: "pids.max", value: formatScalarLimit(limits.Pids)},
	}
	for _, write := range writes {
		if err := m.fs.WriteFile(filepath.Join(path, write.name), []byte(write.value+"\n")); err != nil {
			return fmt.Errorf("write %s: %w", write.name, err)
		}
	}
	return nil
}

// enableControllers verifies controller availability, writes bounded enable commands, and verifies readback.
func (m *Manager) enableControllers(path string) error {
	if err := m.requireControllers(path); err != nil {
		return err
	}
	if err := m.requireEmptyMembership(path); err != nil {
		return err
	}
	controlPath := filepath.Join(path, "cgroup.subtree_control")
	if err := m.fs.WriteFile(controlPath, []byte(controllerEnableCommand+"\n")); err != nil {
		return err
	}
	value, err := m.fs.ReadFile(controlPath)
	if err != nil {
		return err
	}
	available := wordSet(string(value))
	for _, controller := range requiredControllers {
		if !available[controller] {
			return fmt.Errorf("controller %q missing after enable: %w", controller, ErrEffectiveMismatch)
		}
	}
	return nil
}

// requireEmptyMembership enforces cgroup v2's no-internal-process rule before enabling child controllers.
func (m *Manager) requireEmptyMembership(path string) error {
	members, err := m.readMembershipPath(path)
	if err != nil {
		return fmt.Errorf("read controller-parent membership: %w", err)
	}
	if len(members) != 0 {
		return fmt.Errorf("controller parent %q contains %d process(es): %w", path, len(members), ErrPopulated)
	}
	return nil
}

// requireControllers rejects a parent that cannot delegate every M2 controller.
func (m *Manager) requireControllers(path string) error {
	value, err := m.fs.ReadFile(filepath.Join(path, "cgroup.controllers"))
	if err != nil {
		return fmt.Errorf("read available controllers: %w", err)
	}
	available := wordSet(string(value))
	for _, controller := range requiredControllers {
		if !available[controller] {
			return fmt.Errorf("required controller %q is unavailable: %w", controller, ErrUnsupported)
		}
	}
	return nil
}

// ensureDirectory creates one exact path or validates an existing non-symlink directory.
func (m *Manager) ensureDirectory(path string) (bool, error) {
	err := m.fs.Mkdir(path, 0o755)
	created := err == nil
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return false, err
	}
	if err := m.verifyDirectory(path); err != nil {
		return false, err
	}
	return created, nil
}

// verifyDirectory rejects absent, non-directory, or symlink ownership paths.
func (m *Manager) verifyDirectory(path string) error {
	clean := filepath.Clean(path)
	if clean != m.config.Root {
		if _, err := m.joinOwnedRelative(clean); err != nil {
			return err
		}
	}
	info, err := m.fs.Lstat(clean)
	if err != nil {
		return fmt.Errorf("inspect cgroup directory %q: %w: %w", clean, err, ErrUnknownState)
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("path %q is not an owned non-symlink directory: %w", clean, ErrUnknownState)
	}
	return nil
}

// removeExact refuses unknown, populated, or child-owning cgroups and invokes only one non-recursive removal.
func (m *Manager) removeExact(path string) error {
	if _, err := m.joinOwnedRelative(path); err != nil {
		return err
	}
	if err := m.verifyDirectory(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("verify cgroup before removal: %w", err)
	}
	events, err := m.readEventsPath(path)
	if err != nil {
		return fmt.Errorf("observe cgroup before removal: %w", err)
	}
	if events.Populated {
		return ErrPopulated
	}
	entries, err := m.fs.ReadDir(path)
	if err != nil {
		return fmt.Errorf("list cgroup children: %w: %w", err, ErrUnknownState)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return fmt.Errorf("child cgroup %q remains: %w", entry.Name(), ErrBusy)
		}
	}
	if err := m.fs.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if errors.Is(err, syscall.EBUSY) || errors.Is(err, syscall.ENOTEMPTY) {
			return fmt.Errorf("remove exact cgroup %q: %w", path, ErrBusy)
		}
		return fmt.Errorf("remove exact cgroup %q: %w", path, err)
	}
	return nil
}

// joinOwned constructs a path from safe components and rejects lexical root escape.
func (m *Manager) joinOwned(components ...string) (string, error) {
	path := filepath.Join(append([]string{m.config.Root}, components...)...)
	if _, err := m.joinOwnedRelative(path); err != nil {
		return "", err
	}
	return path, nil
}

// joinOwnedRelative returns the relative owned name while rejecting the root itself and every escape form.
func (m *Manager) joinOwnedRelative(path string) (string, error) {
	clean := filepath.Clean(path)
	relative, err := filepath.Rel(m.config.Root, clean)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q relative to %q: %w", path, m.config.Root, ErrOutsideRoot)
	}
	return relative, nil
}

// hexadecimalID returns a fixed-length hexadecimal SHA-256 encoding safe for one filesystem component.
func hexadecimalID(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// wordSet parses whitespace-separated controller and status vocabularies.
func wordSet(value string) map[string]bool {
	result := make(map[string]bool)
	for _, word := range strings.Fields(value) {
		result[strings.TrimPrefix(word, "+")] = true
	}
	return result
}

// formatCPUMax serializes only the fixed-period cgroup v2 cpu.max grammar.
func formatCPUMax(value CPUMax) string {
	if value.Unlimited {
		return fmt.Sprintf("max %d", CPUPeriodMicros)
	}
	return fmt.Sprintf("%d %d", value.QuotaMicros, CPUPeriodMicros)
}

// formatScalarLimit serializes one numeric value or the cgroup v2 max token.
func formatScalarLimit(value ScalarLimit) string {
	if value.Unlimited {
		return "max"
	}
	return strconv.FormatUint(value.Value, 10)
}
