package shim

import (
	"reflect"
	"strings"
	"testing"

	"mydocker/internal/isolation"
)

// TestInitBootstrapRequiresExactSandboxNamespaceSet verifies PID/mount handles,
// duplicates, missing descriptors, and incomplete identities fail before setns.
func TestInitBootstrapRequiresExactSandboxNamespaceSet(t *testing.T) {
	valid := testInitBootstrap()
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid bootstrap: %v", err)
	}
	tests := []InitBootstrap{
		{SchemaVersion: InitBootstrapSchemaVersion, Executable: valid.Executable, ConfigPath: valid.ConfigPath, ConfigEvidence: valid.ConfigEvidence},
		func() InitBootstrap {
			value := testInitBootstrap()
			value.Namespaces[2] = value.Namespaces[0]
			return value
		}(),
		func() InitBootstrap {
			value := testInitBootstrap()
			value.Namespaces[0].Type = isolation.NamespacePID
			return value
		}(),
		func() InitBootstrap { value := testInitBootstrap(); value.Namespaces[0].FD = -1; return value }(),
		func() InitBootstrap { value := testInitBootstrap(); value.Namespaces[0].Inode = 0; return value }(),
	}
	for index, candidate := range tests {
		if err := candidate.Validate(); err == nil {
			t.Errorf("invalid bootstrap %d was accepted", index)
		}
	}
}

// TestInitBootstrapSortsDefensiveCopy verifies deterministic join ordering does
// not mutate launcher-owned descriptor metadata.
func TestInitBootstrapSortsDefensiveCopy(t *testing.T) {
	bootstrap := testInitBootstrap()
	before := append([]BootstrapNamespace(nil), bootstrap.Namespaces...)
	sorted := bootstrap.SortedNamespaces()
	got := []isolation.NamespaceType{sorted[0].Type, sorted[1].Type, sorted[2].Type}
	want := []isolation.NamespaceType{isolation.NamespaceIPC, isolation.NamespaceNetwork, isolation.NamespaceUTS}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("join order=%v, want %v", got, want)
	}
	if !reflect.DeepEqual(bootstrap.Namespaces, before) {
		t.Fatal("SortedNamespaces mutated the bootstrap")
	}
}

// TestValidateInitBootstrapCompletionRejectsKeeper verifies an ungated normal
// shim invocation cannot label a keeper config as a PID1 bootstrap re-exec.
func TestValidateInitBootstrapCompletionRejectsKeeper(t *testing.T) {
	spec := testKeeperSpec(t)
	config := RuntimeConfig{
		SchemaVersion: SchemaVersion, Mode: ModeKeeper, Owner: spec.Owner, SandboxID: spec.SandboxID,
		WrapperEvidence: spec.WrapperEvidence, ControlSocket: "/run/mydocker/control.sock",
	}
	if err := ValidateInitBootstrapCompletion(config, spec.WrapperEvidence, 1, 2); err == nil {
		t.Fatal("keeper bypassed the parent release gate")
	}
}

// testInitBootstrap supplies valid non-live descriptors for validation-only bootstrap tests.
func testInitBootstrap() InitBootstrap {
	return InitBootstrap{
		SchemaVersion: InitBootstrapSchemaVersion, Executable: "/usr/libexec/mydocker-shim",
		ConfigPath: "/run/mydocker/owner/shim.json", ConfigEvidence: strings.Repeat("a", 64),
		Namespaces: []BootstrapNamespace{
			{Type: isolation.NamespaceUTS, FD: 3, Inode: 101},
			{Type: isolation.NamespaceIPC, FD: 4, Inode: 102},
			{Type: isolation.NamespaceNetwork, FD: 5, Inode: 103},
		},
	}
}
