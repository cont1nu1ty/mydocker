package cgroupv2

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"

	"mydocker/internal/domain"
)

// TestInspectExactPresenceIsReadOnly verifies recovery can distinguish exact directories from absence without creating controller state.
func TestInspectExactPresenceIsReadOnly(t *testing.T) {
	manager, fake := newFakeManager(t)
	sandboxID := domain.SandboxID("sandbox-presence")
	attemptID := domain.AttemptID("attempt-presence")
	if present, err := manager.InspectSandboxPresence(context.Background(), sandboxID); err != nil || present {
		t.Fatalf("InspectSandboxPresence(absent) = (%v, %v)", present, err)
	}
	if _, err := manager.CreateSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	if _, err := manager.CreateKeeper(context.Background(), sandboxID); err != nil {
		t.Fatalf("CreateKeeper() error = %v", err)
	}
	if _, _, err := manager.CreateAttempt(context.Background(), sandboxID, attemptID, resolveTestLimits(t, domain.Resources{})); err != nil {
		t.Fatalf("CreateAttempt() error = %v", err)
	}
	writesBefore := len(fake.writes)
	checks := []struct {
		name string
		call func() (bool, error)
	}{
		{name: "sandbox", call: func() (bool, error) { return manager.InspectSandboxPresence(context.Background(), sandboxID) }},
		{name: "keeper", call: func() (bool, error) { return manager.InspectKeeperPresence(context.Background(), sandboxID) }},
		{name: "attempt", call: func() (bool, error) {
			return manager.InspectAttemptPresence(context.Background(), sandboxID, attemptID)
		}},
	}
	for _, check := range checks {
		present, err := check.call()
		if err != nil || !present {
			t.Fatalf("Inspect%sPresence() = (%v, %v)", check.name, present, err)
		}
	}
	if len(fake.writes) != writesBefore {
		t.Fatalf("presence inspection performed writes: before=%d after=%d", writesBefore, len(fake.writes))
	}
}

// TestNewManagerAndPreflightFailClosed verifies configuration, v2, controller, and root-identity gates before mutation.
func TestNewManagerAndPreflightFailClosed(t *testing.T) {
	t.Run("configuration", func(t *testing.T) {
		fake := newFakeFileSystem("/delegated/mydocker")
		probe := fakeHostProbe{supported: true}
		for _, root := range []string{"", ".", "relative", "/"} {
			if _, err := NewManager(Config{Root: root}, fake, probe); err == nil {
				t.Errorf("NewManager(Root: %q) error = nil", root)
			}
		}
		if _, err := NewManager(Config{Root: "/delegated/mydocker"}, nil, probe); err == nil {
			t.Error("NewManager(nil filesystem) error = nil")
		}
		if _, err := NewManager(Config{Root: "/delegated/mydocker"}, fake, nil); err == nil {
			t.Error("NewManager(nil probe) error = nil")
		}
	})

	t.Run("v1 or other filesystem", func(t *testing.T) {
		fake := newFakeFileSystem("/delegated/mydocker")
		manager, err := NewManager(Config{Root: "/delegated/mydocker"}, fake, fakeHostProbe{supported: false})
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Preflight(context.Background()); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("Preflight() error = %v, want ErrUnsupported", err)
		}
		if len(fake.writes) != 0 {
			t.Fatalf("Preflight() performed writes: %+v", fake.writes)
		}
	})

	t.Run("missing controller", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		fake.setFile(filepath.Join(manager.Root(), "cgroup.controllers"), "cpu memory\n")
		if err := manager.Preflight(context.Background()); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("Preflight() error = %v, want ErrUnsupported", err)
		}
	})

	t.Run("symlink root", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		fake.modes[manager.Root()] = fs.ModeSymlink | 0o777
		if err := manager.Preflight(context.Background()); !errors.Is(err, ErrUnknownState) {
			t.Fatalf("Preflight() error = %v, want ErrUnknownState", err)
		}
	})

	t.Run("probe failure", func(t *testing.T) {
		probeErr := errors.New("statfs denied")
		fake := newFakeFileSystem("/delegated/mydocker")
		manager, err := NewManager(Config{Root: "/delegated/mydocker"}, fake, fakeHostProbe{err: probeErr})
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Preflight(context.Background()); !errors.Is(err, probeErr) {
			t.Fatalf("Preflight() error = %v, want probe failure", err)
		}
	})

	t.Run("populated delegated root", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		fake.setFile(filepath.Join(manager.Root(), "cgroup.procs"), "51\n")
		sandboxID := domain.SandboxID("sandbox-populated-root")
		if _, err := manager.CreateSandbox(context.Background(), sandboxID); !errors.Is(err, ErrPopulated) {
			t.Fatalf("CreateSandbox() error = %v, want ErrPopulated", err)
		}
		if len(fake.writes) != 0 {
			t.Fatalf("populated delegated root caused writes: %v", fake.writes)
		}
		path, err := manager.SandboxPath(sandboxID)
		if err != nil {
			t.Fatal(err)
		}
		if fake.exists(path) {
			t.Fatalf("populated delegated root produced Sandbox parent %q", path)
		}
	})
}

// TestDerivedPathsUseBoundedHexComponents verifies hostile IDs remain bounded while keeper and Attempt leaves are deterministic siblings.
func TestDerivedPathsUseBoundedHexComponents(t *testing.T) {
	manager, _ := newFakeManager(t)
	sandboxID := domain.SandboxID("../../沙箱/alpha")
	attemptID := domain.AttemptID("../attempt/β")

	sandboxPath, err := manager.SandboxPath(sandboxID)
	if err != nil {
		t.Fatalf("SandboxPath() error = %v", err)
	}
	attemptPath, err := manager.AttemptPath(sandboxID, attemptID)
	if err != nil {
		t.Fatalf("AttemptPath() error = %v", err)
	}
	keeperPath, err := manager.KeeperPath(sandboxID)
	if err != nil {
		t.Fatalf("KeeperPath() error = %v", err)
	}
	hexComponent := regexp.MustCompile(`^(sandbox|attempt)-[0-9a-f]{64}$`)
	for _, path := range []string{sandboxPath, attemptPath} {
		relative, relErr := filepath.Rel(manager.Root(), path)
		if relErr != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			t.Fatalf("derived path %q is outside root %q", path, manager.Root())
		}
		if !hexComponent.MatchString(filepath.Base(path)) {
			t.Errorf("path component %q is not bounded hexadecimal encoding", filepath.Base(path))
		}
	}
	if filepath.Dir(attemptPath) != sandboxPath {
		t.Fatalf("Attempt parent = %q, want %q", filepath.Dir(attemptPath), sandboxPath)
	}
	if filepath.Dir(keeperPath) != sandboxPath || filepath.Base(keeperPath) != keeperLeafName {
		t.Fatalf("keeper path = %q, want fixed leaf below %q", keeperPath, sandboxPath)
	}
	if keeperPath == attemptPath {
		t.Fatal("keeper and Attempt paths collided")
	}
	if strings.Contains(sandboxPath, string(sandboxID)) || strings.Contains(attemptPath, string(attemptID)) {
		t.Fatalf("derived paths exposed raw IDs: %q, %q", sandboxPath, attemptPath)
	}
}

// TestKeeperLeafKeepsSandboxParentProcessFree verifies controllers stay on the empty parent and keeper processes have a dedicated sibling leaf.
func TestKeeperLeafKeepsSandboxParentProcessFree(t *testing.T) {
	manager, fake := newFakeManager(t)
	sandboxID := domain.SandboxID("sandbox-keeper")
	sandbox, err := manager.CreateSandbox(context.Background(), sandboxID)
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	keeper, err := manager.CreateKeeper(context.Background(), sandboxID)
	if err != nil {
		t.Fatalf("CreateKeeper() error = %v", err)
	}
	if filepath.Dir(keeper.Path) != sandbox.Path || filepath.Base(keeper.Path) != keeperLeafName {
		t.Fatalf("keeper = %#v, Sandbox = %#v", keeper, sandbox)
	}
	if got := strings.TrimSpace(string(fake.files[filepath.Join(sandbox.Path, "cgroup.subtree_control")])); got != "cpu memory pids" {
		t.Fatalf("Sandbox subtree controllers = %q", got)
	}
	if got := strings.TrimSpace(string(fake.files[filepath.Join(keeper.Path, "cgroup.subtree_control")])); got != "" {
		t.Fatalf("keeper leaf unexpectedly enables child controllers: %q", got)
	}
	if got := strings.TrimSpace(string(fake.files[filepath.Join(sandbox.Path, "cgroup.procs")])); got != "" {
		t.Fatalf("Sandbox parent membership = %q, want empty", got)
	}
	repeated, err := manager.CreateKeeper(context.Background(), sandboxID)
	if err != nil || repeated.Path != keeper.Path {
		t.Fatalf("repeated CreateKeeper() = %#v, %v", repeated, err)
	}

	fake.setFile(filepath.Join(sandbox.Path, "cgroup.procs"), "73\n")
	writesBefore := len(fake.writes)
	if _, err := manager.CreateKeeper(context.Background(), sandboxID); !errors.Is(err, ErrPopulated) {
		t.Fatalf("CreateKeeper(populated parent) error = %v, want ErrPopulated", err)
	}
	attemptID := domain.AttemptID("attempt-populated-parent")
	if _, _, err := manager.CreateAttempt(context.Background(), sandboxID, attemptID, resolveTestLimits(t, domain.Resources{})); !errors.Is(err, ErrPopulated) {
		t.Fatalf("CreateAttempt(populated parent) error = %v, want ErrPopulated", err)
	}
	if len(fake.writes) != writesBefore {
		t.Fatal("populated Sandbox parent caused a controller or leaf write")
	}
	path, err := manager.AttemptPath(sandboxID, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if fake.exists(path) {
		t.Fatalf("populated Sandbox parent produced Attempt leaf %q", path)
	}
}

// TestCPUQuotaConversionUsesFixedPeriod verifies milli-CPU maps to exact ceiling division with the immutable 100000us period.
func TestCPUQuotaConversionUsesFixedPeriod(t *testing.T) {
	tests := []struct {
		milli int64
		want  uint64
	}{
		{milli: 10, want: 1_000},
		{milli: 999, want: 99_900},
		{milli: 1_000, want: 100_000},
		{milli: 1_001, want: 100_100},
		{milli: 2_500, want: 250_000},
	}
	for _, test := range tests {
		got, err := cpuQuotaMicros(test.milli)
		if err != nil {
			t.Fatalf("cpuQuotaMicros(%d) error = %v", test.milli, err)
		}
		if got != test.want {
			t.Errorf("cpuQuotaMicros(%d) = %d, want %d", test.milli, got, test.want)
		}
	}
	for _, invalid := range []int64{-1, 0, 9, int64(^uint64(0) >> 1)} {
		if _, err := cpuQuotaMicros(invalid); err == nil {
			t.Errorf("cpuQuotaMicros(%d) error = nil", invalid)
		}
	}
}

// TestCreateHierarchyWritesOnlyLimits verifies parent/child creation, controller enablement, defaults, readback, and request isolation.
func TestCreateHierarchyWritesOnlyLimits(t *testing.T) {
	manager, fake := newFakeManager(t)
	ctx := context.Background()
	sandboxID := domain.SandboxID("sandbox/one")

	sandbox, err := manager.CreateSandbox(ctx, sandboxID)
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	if !fake.exists(sandbox.Path) {
		t.Fatalf("Sandbox cgroup %q was not created", sandbox.Path)
	}
	keeper, err := manager.CreateKeeper(ctx, sandboxID)
	if err != nil {
		t.Fatalf("CreateKeeper() error = %v", err)
	}
	if !fake.exists(keeper.Path) || filepath.Dir(keeper.Path) != sandbox.Path {
		t.Fatalf("keeper cgroup = %#v", keeper)
	}
	for _, path := range []string{manager.Root(), sandbox.Path} {
		got := strings.TrimSpace(string(fake.files[filepath.Join(path, "cgroup.subtree_control")]))
		if got != "cpu memory pids" {
			t.Errorf("%s subtree controls = %q, want cpu memory pids", path, got)
		}
	}

	cpuRequest, memoryRequest := int64(7), int64(13)
	defaultAttempt, defaults, err := manager.CreateAttempt(ctx, sandboxID, domain.AttemptID("attempt/default"), resolveTestLimits(t, domain.Resources{
		Requests: domain.ResourceRequests{CPURequestMilli: &cpuRequest, MemoryRequestBytes: &memoryRequest},
	}))
	if err != nil {
		t.Fatalf("CreateAttempt(defaults) error = %v", err)
	}
	if !defaults.Equal(EffectiveLimits{
		CPU:    CPUMax{Unlimited: true, PeriodMicros: CPUPeriodMicros},
		Memory: ScalarLimit{Unlimited: true},
		Pids:   ScalarLimit{Value: DefaultPidsLimit},
	}) {
		t.Fatalf("default effective limits = %+v", defaults)
	}
	if got := string(fake.files[filepath.Join(defaultAttempt.Path, "cpu.max")]); got != "max 100000\n" {
		t.Errorf("default cpu.max = %q", got)
	}
	if got := string(fake.files[filepath.Join(defaultAttempt.Path, "memory.max")]); got != "max\n" {
		t.Errorf("default memory.max = %q", got)
	}
	if got := string(fake.files[filepath.Join(defaultAttempt.Path, "pids.max")]); got != "1024\n" {
		t.Errorf("default pids.max = %q", got)
	}

	cpuLimit, memoryLimit, pidsLimit := int64(1_501), int64(8_193), int64(33)
	explicitID := domain.AttemptID("attempt/explicit")
	explicitPath, err := manager.AttemptPath(sandboxID, explicitID)
	if err != nil {
		t.Fatal(err)
	}
	fake.writeOverrides[filepath.Join(explicitPath, "memory.max")] = []byte("12288\n")
	explicitAttempt, effective, err := manager.CreateAttempt(ctx, sandboxID, explicitID, resolveTestLimits(t, domain.Resources{
		Requests: domain.ResourceRequests{CPURequestMilli: &cpuRequest, MemoryRequestBytes: &memoryRequest},
		Limits: domain.ResourceLimits{
			CPULimitMilli:    &cpuLimit,
			MemoryLimitBytes: &memoryLimit,
			PidsLimit:        &pidsLimit,
		},
	}))
	if err != nil {
		t.Fatalf("CreateAttempt(explicit) error = %v", err)
	}
	want := EffectiveLimits{
		CPU:    CPUMax{QuotaMicros: 150_100, PeriodMicros: CPUPeriodMicros},
		Memory: ScalarLimit{Value: 12_288},
		Pids:   ScalarLimit{Value: 33},
	}
	if !effective.Equal(want) {
		t.Fatalf("effective limits = %+v, want %+v", effective, want)
	}
	for name, value := range map[string]string{
		"cpu.max":    "150100 100000\n",
		"memory.max": "12288\n",
		"pids.max":   "33\n",
	} {
		if got := string(fake.files[filepath.Join(explicitAttempt.Path, name)]); got != value {
			t.Errorf("%s = %q, want %q", name, got, value)
		}
	}
	memoryWrites := fake.writesTo("memory.max")
	if len(memoryWrites) == 0 || memoryWrites[len(memoryWrites)-1].Data != "8193\n" {
		t.Fatalf("memory.max writes = %+v, want raw requested 8193 before canonical readback", memoryWrites)
	}
	for _, write := range fake.writes {
		if strings.HasPrefix(write.Path, explicitAttempt.Path+string(filepath.Separator)) {
			switch filepath.Base(write.Path) {
			case "cpu.max", "memory.max", "pids.max":
			default:
				t.Errorf("Attempt received non-enforcement write %+v", write)
			}
		}
	}
	if fake.exists(filepath.Join(explicitAttempt.Path, "cpu.weight")) || fake.exists(filepath.Join(explicitAttempt.Path, "memory.low")) {
		t.Fatal("request-only cgroup controls were created")
	}

	repeated, repeatedEffective, err := manager.CreateAttempt(ctx, sandboxID, explicitID, resolveTestLimits(t, domain.Resources{
		Requests: domain.ResourceRequests{CPURequestMilli: &cpuRequest, MemoryRequestBytes: &memoryRequest},
		Limits: domain.ResourceLimits{
			CPULimitMilli:    &cpuLimit,
			MemoryLimitBytes: &memoryLimit,
			PidsLimit:        &pidsLimit,
		},
	}))
	if err != nil {
		t.Fatalf("repeated CreateAttempt() error = %v", err)
	}
	if repeated.Path != explicitAttempt.Path || !repeatedEffective.Equal(effective) {
		t.Fatalf("repeated CreateAttempt() = (%+v, %+v), want stable result", repeated, repeatedEffective)
	}
}

// TestLeafCleanupPrecedesSandboxParent verifies keeper and Attempt leaves are removed explicitly before their parent with no recursive fallback.
func TestLeafCleanupPrecedesSandboxParent(t *testing.T) {
	manager, fake := newFakeManager(t)
	sandboxID := domain.SandboxID("sandbox-cleanup-order")
	sandbox, err := manager.CreateSandbox(context.Background(), sandboxID)
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}
	keeper, err := manager.CreateKeeper(context.Background(), sandboxID)
	if err != nil {
		t.Fatalf("CreateKeeper() error = %v", err)
	}
	attemptID := domain.AttemptID("attempt-cleanup-order")
	attempt, _, err := manager.CreateAttempt(context.Background(), sandboxID, attemptID, resolveTestLimits(t, domain.Resources{}))
	if err != nil {
		t.Fatalf("CreateAttempt() error = %v", err)
	}
	if err := manager.RemoveSandbox(context.Background(), sandboxID); !errors.Is(err, ErrBusy) {
		t.Fatalf("RemoveSandbox(with leaves) error = %v, want ErrBusy", err)
	}
	if len(fake.removes) != 0 {
		t.Fatalf("parent-first cleanup attempted removals: %v", fake.removes)
	}
	if err := manager.RemoveAttempt(context.Background(), sandboxID, attemptID); err != nil {
		t.Fatalf("RemoveAttempt() error = %v", err)
	}
	if err := manager.RemoveKeeper(context.Background(), sandboxID); err != nil {
		t.Fatalf("RemoveKeeper() error = %v", err)
	}
	if err := manager.RemoveSandbox(context.Background(), sandboxID); err != nil {
		t.Fatalf("RemoveSandbox() error = %v", err)
	}
	want := []string{attempt.Path, keeper.Path, sandbox.Path}
	if len(fake.removes) != len(want) {
		t.Fatalf("exact removals = %v, want %v", fake.removes, want)
	}
	for index := range want {
		if fake.removes[index] != want[index] {
			t.Fatalf("exact removals = %v, want %v", fake.removes, want)
		}
	}
	if err := manager.RemoveKeeper(context.Background(), sandboxID); err != nil {
		t.Fatalf("repeated RemoveKeeper() error = %v", err)
	}
	if len(fake.removes) != len(want) {
		t.Fatalf("absent keeper retry invoked removal again: %v", fake.removes)
	}
}

// TestCreateAttemptRejectsInvalidResourcesBeforeOwnership verifies resolved-limit validation prevents any child cgroup mutation.
func TestCreateAttemptRejectsInvalidResourcesBeforeOwnership(t *testing.T) {
	manager, fake := newFakeManager(t)
	sandboxID := domain.SandboxID("sandbox-invalid")
	if _, err := manager.CreateSandbox(context.Background(), sandboxID); err != nil {
		t.Fatal(err)
	}
	limit := int64(9)
	attemptID := domain.AttemptID("attempt-invalid")
	_, _, err := manager.CreateAttempt(context.Background(), sandboxID, attemptID, domain.ResolvedResourceLimits{
		CPULimitMilli: &limit, MemoryUnlimited: true, PidsLimit: domain.DefaultPidsLimit,
	})
	if err == nil {
		t.Fatal("CreateAttempt(invalid resources) error = nil")
	}
	path, pathErr := manager.AttemptPath(sandboxID, attemptID)
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	if fake.exists(path) {
		t.Fatalf("invalid resources created Attempt path %q", path)
	}
}

// TestCreateAttemptRejectsUnknownPageSizeBeforeOwnership verifies host-dependent memory canonicalization cannot begin with missing or unsafe page facts.
func TestCreateAttemptRejectsUnknownPageSizeBeforeOwnership(t *testing.T) {
	tests := []struct {
		name  string
		probe fakeHostProbe
	}{
		{name: "probe failure", probe: fakeHostProbe{supported: true, pageErr: errors.New("page size unavailable")}},
		{name: "non-power-of-two", probe: fakeHostProbe{supported: true, pageSize: 3_000}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const root = "/delegated/page-size"
			fake := newFakeFileSystem(root)
			manager, err := NewManager(Config{Root: root}, fake, test.probe)
			if err != nil {
				t.Fatalf("NewManager() error = %v", err)
			}
			sandboxID := domain.SandboxID("sandbox-page-size")
			if _, err := manager.CreateSandbox(context.Background(), sandboxID); err != nil {
				t.Fatalf("CreateSandbox() error = %v", err)
			}
			attemptID := domain.AttemptID("attempt-page-size")
			writesBefore := len(fake.writes)
			if _, _, err := manager.CreateAttempt(context.Background(), sandboxID, attemptID, resolveTestLimits(t, domain.Resources{})); err == nil {
				t.Fatal("CreateAttempt() error = nil")
			}
			path, err := manager.AttemptPath(sandboxID, attemptID)
			if err != nil {
				t.Fatal(err)
			}
			if fake.exists(path) || len(fake.writes) != writesBefore {
				t.Fatalf("invalid page size created or configured Attempt %q", path)
			}
		})
	}
}

// TestSetupFailuresRollbackOnlyNewCgroups verifies exact inverse cleanup and joined cleanup evidence under injected failures.
func TestSetupFailuresRollbackOnlyNewCgroups(t *testing.T) {
	t.Run("Attempt control write", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		sandboxID := domain.SandboxID("sandbox-write-failure")
		if _, err := manager.CreateSandbox(context.Background(), sandboxID); err != nil {
			t.Fatal(err)
		}
		attemptID := domain.AttemptID("attempt-write-failure")
		path, err := manager.AttemptPath(sandboxID, attemptID)
		if err != nil {
			t.Fatal(err)
		}
		writeErr := errors.New("injected memory.max failure")
		fake.setFailure("write", filepath.Join(path, "memory.max"), writeErr)
		if _, _, err := manager.CreateAttempt(context.Background(), sandboxID, attemptID, resolveTestLimits(t, domain.Resources{})); !errors.Is(err, writeErr) {
			t.Fatalf("CreateAttempt() error = %v, want injected write error", err)
		}
		if fake.exists(path) {
			t.Fatalf("failed setup leaked new Attempt path %q", path)
		}
		if len(fake.removes) != 1 || fake.removes[0] != path {
			t.Fatalf("rollback removals = %v, want only %q", fake.removes, path)
		}
	})

	t.Run("effective mismatch", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		sandboxID := domain.SandboxID("sandbox-readback")
		if _, err := manager.CreateSandbox(context.Background(), sandboxID); err != nil {
			t.Fatal(err)
		}
		attemptID := domain.AttemptID("attempt-readback")
		path, err := manager.AttemptPath(sandboxID, attemptID)
		if err != nil {
			t.Fatal(err)
		}
		fake.writeOverrides[filepath.Join(path, "pids.max")] = []byte("2048\n")
		if _, _, err := manager.CreateAttempt(context.Background(), sandboxID, attemptID, resolveTestLimits(t, domain.Resources{})); !errors.Is(err, ErrEffectiveMismatch) {
			t.Fatalf("CreateAttempt() error = %v, want ErrEffectiveMismatch", err)
		}
		if fake.exists(path) {
			t.Fatalf("mismatched setup leaked new Attempt path %q", path)
		}
	})

	t.Run("rollback busy is retained", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		sandboxID := domain.SandboxID("sandbox-busy-rollback")
		if _, err := manager.CreateSandbox(context.Background(), sandboxID); err != nil {
			t.Fatal(err)
		}
		attemptID := domain.AttemptID("attempt-busy-rollback")
		path, err := manager.AttemptPath(sandboxID, attemptID)
		if err != nil {
			t.Fatal(err)
		}
		writeErr := errors.New("injected cpu.max failure")
		fake.setFailure("write", filepath.Join(path, "cpu.max"), writeErr)
		fake.setFailure("remove", path, syscall.EBUSY)
		_, _, createErr := manager.CreateAttempt(context.Background(), sandboxID, attemptID, resolveTestLimits(t, domain.Resources{}))
		if !errors.Is(createErr, writeErr) || !errors.Is(createErr, ErrBusy) {
			t.Fatalf("CreateAttempt() error = %v, want joined setup and ErrBusy evidence", createErr)
		}
		if !fake.exists(path) {
			t.Fatalf("busy rollback incorrectly hid owned path %q", path)
		}
	})

	t.Run("Sandbox controller enable", func(t *testing.T) {
		manager, fake := newFakeManager(t)
		sandboxID := domain.SandboxID("sandbox-controller-failure")
		path, err := manager.SandboxPath(sandboxID)
		if err != nil {
			t.Fatal(err)
		}
		enableErr := errors.New("injected subtree control failure")
		fake.setFailure("write", filepath.Join(path, "cgroup.subtree_control"), enableErr)
		if _, err := manager.CreateSandbox(context.Background(), sandboxID); !errors.Is(err, enableErr) {
			t.Fatalf("CreateSandbox() error = %v, want injected enable failure", err)
		}
		if fake.exists(path) {
			t.Fatalf("failed setup leaked new Sandbox path %q", path)
		}
	})
}
