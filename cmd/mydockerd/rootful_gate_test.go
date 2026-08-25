package main

import (
	"errors"
	"fmt"
	"testing"

	"mydocker/internal/isolation"
)

const (
	rootfulEnableEnvironment      = "MYDOCKER_ROOTFUL_TEST"
	rootfulAllowEnvironment       = "MYDOCKER_ALLOW_PRIVILEGED_TEST"
	rootfulDisposableEnvironment  = "MYDOCKER_DISPOSABLE_ENVIRONMENT"
	rootfulWorkRootEnvironment    = "MYDOCKER_ROOTFUL_WORK_ROOT"
	rootfulCgroupRootEnvironment  = "MYDOCKER_ROOTFUL_CGROUP_ROOT"
	rootfulRootfsEnvironment      = "MYDOCKER_ROOTFUL_ROOTFS"
	rootfulShimEnvironment        = "MYDOCKER_ROOTFUL_SHIM"
	rootfulDisposableAttestation  = "I_UNDERSTAND_MYDOCKER_MUTATES_THIS_DISPOSABLE_VM"
	rootfulWorkRootMarkerName     = ".mydocker-rootful-test-root"
	rootfulWorkRootMarkerContents = "mydocker disposable rootful test root\n"
)

// rootfulTestEnvironment contains only operator-supplied ownership boundaries;
// loading it validates policy strings but performs no filesystem operation.
type rootfulTestEnvironment struct {
	WorkRoot   string
	CgroupRoot string
	Rootfs     string
	Shim       string
}

// loadRootfulTestEnvironment keeps the tagged suite disabled unless both
// privileged-test opt-ins, the exact disposable-host attestation, and every
// dedicated path are present. It performs no host mutation or path lookup.
func loadRootfulTestEnvironment(lookup func(string) (string, bool)) (rootfulTestEnvironment, bool, error) {
	if lookup == nil {
		return rootfulTestEnvironment{}, false, errors.New("rootful environment lookup is nil")
	}
	enabled, exists := lookup(rootfulEnableEnvironment)
	if !exists || enabled == "" {
		return rootfulTestEnvironment{}, false, nil
	}
	if enabled != "1" {
		return rootfulTestEnvironment{}, false, fmt.Errorf("%s must equal 1", rootfulEnableEnvironment)
	}
	allowed, _ := lookup(rootfulAllowEnvironment)
	disposable, _ := lookup(rootfulDisposableEnvironment)
	gate := isolation.PreflightConfig{
		ForPrivilegedTest: true, AllowPrivilegedTest: allowed == "1",
		DisposableEnvironment: disposable == rootfulDisposableAttestation,
	}
	if err := isolation.ValidatePrivilegedTest(gate); err != nil {
		return rootfulTestEnvironment{}, false, err
	}
	values := make(map[string]string, 4)
	for _, name := range []string{rootfulWorkRootEnvironment, rootfulCgroupRootEnvironment, rootfulRootfsEnvironment, rootfulShimEnvironment} {
		value, present := lookup(name)
		if !present || value == "" {
			return rootfulTestEnvironment{}, false, fmt.Errorf("%s is required for the rootful suite", name)
		}
		values[name] = value
	}
	return rootfulTestEnvironment{
		WorkRoot: values[rootfulWorkRootEnvironment], CgroupRoot: values[rootfulCgroupRootEnvironment],
		Rootfs: values[rootfulRootfsEnvironment], Shim: values[rootfulShimEnvironment],
	}, true, nil
}

// TestLoadRootfulTestEnvironmentFailsClosed verifies ordinary test execution
// remains disabled and partial or malformed authorization never reaches paths.
func TestLoadRootfulTestEnvironmentFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		enabled bool
		want    error
	}{
		{name: "ordinary invocation", values: map[string]string{}},
		{name: "malformed master opt-in", values: map[string]string{rootfulEnableEnvironment: "yes"}, want: errors.New("malformed")},
		{name: "missing second opt-in", values: map[string]string{rootfulEnableEnvironment: "1", rootfulDisposableEnvironment: rootfulDisposableAttestation}, want: isolation.ErrPrivilegedTestDenied},
		{name: "missing disposable attestation", values: map[string]string{rootfulEnableEnvironment: "1", rootfulAllowEnvironment: "1"}, want: isolation.ErrUnsafeTestEnvironment},
		{name: "missing dedicated paths", values: map[string]string{
			rootfulEnableEnvironment: "1", rootfulAllowEnvironment: "1", rootfulDisposableEnvironment: rootfulDisposableAttestation,
		}, want: errors.New("paths")},
		{name: "complete explicit authorization", enabled: true, values: map[string]string{
			rootfulEnableEnvironment: "1", rootfulAllowEnvironment: "1", rootfulDisposableEnvironment: rootfulDisposableAttestation,
			rootfulWorkRootEnvironment: "/var/lib/mydocker-rootful-test", rootfulCgroupRootEnvironment: "/sys/fs/cgroup/mydocker-rootful-test-one",
			rootfulRootfsEnvironment: "/var/lib/mydocker-rootful-test/rootfs", rootfulShimEnvironment: "/var/lib/mydocker-rootful-test/bin/mydocker-shim",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookup := func(name string) (string, bool) {
				value, exists := test.values[name]
				return value, exists
			}
			environment, enabled, err := loadRootfulTestEnvironment(lookup)
			if test.want == nil && err != nil {
				t.Fatalf("loadRootfulTestEnvironment() error = %v", err)
			}
			if test.want != nil && err == nil {
				t.Fatalf("loadRootfulTestEnvironment() error = nil, want failure")
			}
			if errors.Is(test.want, isolation.ErrPrivilegedTestDenied) && !errors.Is(err, isolation.ErrPrivilegedTestDenied) {
				t.Fatalf("loadRootfulTestEnvironment() error = %v, want ErrPrivilegedTestDenied", err)
			}
			if errors.Is(test.want, isolation.ErrUnsafeTestEnvironment) && !errors.Is(err, isolation.ErrUnsafeTestEnvironment) {
				t.Fatalf("loadRootfulTestEnvironment() error = %v, want ErrUnsafeTestEnvironment", err)
			}
			if enabled != test.enabled {
				t.Fatalf("loadRootfulTestEnvironment() enabled = %t, want %t", enabled, test.enabled)
			}
			if enabled && (environment.WorkRoot == "" || environment.CgroupRoot == "" || environment.Rootfs == "" || environment.Shim == "") {
				t.Fatalf("loadRootfulTestEnvironment() returned incomplete paths: %+v", environment)
			}
		})
	}
}
