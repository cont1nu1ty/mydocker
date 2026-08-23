package isolation

import (
	"context"
	"errors"
	"testing"
)

// TestPreflightReadOnly verifies the complete M2 feature probe without recording any host mutation.
func TestPreflightReadOnly(t *testing.T) {
	ops := newFakeOps()
	report, err := Preflight(context.Background(), ops, DefaultPreflightConfig())
	if err != nil {
		t.Fatalf("Preflight() error = %v", err)
	}
	if !report.Rootful || !report.CgroupV2 || !report.Pidfd || len(report.Namespaces) != 5 {
		t.Fatalf("Preflight() report = %#v", report)
	}
	if len(ops.mutations) != 0 {
		t.Fatalf("Preflight() mutations = %v, want none", ops.mutations)
	}
}

// TestValidatePrivilegedTestPolicy verifies that opt-in cannot authorize a non-disposable host.
func TestValidatePrivilegedTestPolicy(t *testing.T) {
	tests := []struct {
		name   string
		config PreflightConfig
		want   error
	}{
		{name: "ordinary read-only preflight", config: PreflightConfig{}},
		{name: "missing explicit opt-in", config: PreflightConfig{ForPrivilegedTest: true, DisposableEnvironment: true}, want: ErrPrivilegedTestDenied},
		{name: "ordinary host rejected", config: PreflightConfig{ForPrivilegedTest: true, AllowPrivilegedTest: true}, want: ErrUnsafeTestEnvironment},
		{name: "explicit disposable test", config: PreflightConfig{ForPrivilegedTest: true, AllowPrivilegedTest: true, DisposableEnvironment: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidatePrivilegedTest(test.config)
			if !errors.Is(err, test.want) {
				t.Fatalf("ValidatePrivilegedTest() error = %v, want %v", err, test.want)
			}
		})
	}
}

// TestPreflightFailsClosed verifies root, cgroup2, and namespace failures stop the feature gate.
func TestPreflightFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeOps)
	}{
		{name: "not rootful", mutate: func(ops *fakeOps) { ops.euid = 1000 }},
		{name: "not cgroup2", mutate: func(ops *fakeOps) { ops.statfs["/sys/fs/cgroup"] = FileSystemInfo{Type: 0x1234} }},
		{name: "namespace unavailable", mutate: func(ops *fakeOps) { ops.fail["open-ns:/proc/self/ns/net"] = errors.New("missing") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ops := newFakeOps()
			test.mutate(ops)
			_, err := Preflight(context.Background(), ops, DefaultPreflightConfig())
			if !errors.Is(err, ErrPreflight) {
				t.Fatalf("Preflight() error = %v, want ErrPreflight", err)
			}
			if len(ops.mutations) != 0 {
				t.Fatalf("Preflight() mutations = %v, want none", ops.mutations)
			}
		})
	}
}
