package slim

import "testing"

// TestProcessLaunchSpecBindsReleaseFDToExtraFiles verifies namespace descriptor
// count determines the only inherited release descriptor accepted by the child.
func TestProcessLaunchSpecBindsReleaseFDToExtraFiles(t *testing.T) {
	valid := ProcessLaunchSpec{
		Executable: "/usr/libexec/mydocker-shim", Arguments: []string{"-config", "/run/mydocker/shim.json"},
		Environment: []string{"PATH=/usr/bin"}, CloneFlags: 1, CgroupFD: 9,
		ExtraFDs: []int{20, 21, 22}, ReleaseFD: 6,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.ReleaseFD = 5
	if err := invalid.Validate(); err == nil {
		t.Fatal("release descriptor collision was accepted")
	}
}
