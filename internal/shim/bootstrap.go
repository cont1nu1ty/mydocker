package shim

import (
	"errors"
	"path/filepath"
	"sort"
	"strings"

	"mydocker/internal/isolation"
)

// InitBootstrapSchemaVersion is the only first-stage namespace bootstrap schema understood by this shim.
const InitBootstrapSchemaVersion uint32 = 1

// BootstrapNamespace transfers one exact keeper namespace descriptor and its
// expected nsfs inode into the Attempt PID1 bootstrap process.
type BootstrapNamespace struct {
	Type  isolation.NamespaceType
	FD    int
	Inode uint64
}

// Validate rejects unsupported, PID/mount, ambient, or identity-free bootstrap descriptors.
func (namespace BootstrapNamespace) Validate() error {
	if namespace.Type != isolation.NamespaceUTS && namespace.Type != isolation.NamespaceIPC && namespace.Type != isolation.NamespaceNetwork {
		return errors.New("init bootstrap accepts only Sandbox UTS, IPC, and network namespaces")
	}
	if namespace.FD < 3 || namespace.Inode == 0 {
		return errors.New("init bootstrap namespace requires an inherited descriptor and inode")
	}
	return nil
}

// InitBootstrap is the bounded first-stage command that joins the verified
// keeper namespaces before re-executing the init shim inside new PID/mount namespaces.
type InitBootstrap struct {
	SchemaVersion  uint32
	Executable     string
	ConfigPath     string
	ConfigEvidence string
	Namespaces     []BootstrapNamespace
}

// Validate requires one clean executable/config identity and exactly one UTS,
// IPC, and network descriptor, rejecting ambiguous or duplicate joins.
func (bootstrap InitBootstrap) Validate() error {
	if bootstrap.SchemaVersion != InitBootstrapSchemaVersion {
		return errors.New("unsupported init bootstrap schema")
	}
	for _, path := range []string{bootstrap.Executable, bootstrap.ConfigPath} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) || strings.ContainsRune(path, '\x00') {
			return errors.New("init bootstrap paths must be clean absolute non-root paths")
		}
	}
	if !validDigest(bootstrap.ConfigEvidence) {
		return errors.New("init bootstrap requires a canonical config evidence digest")
	}
	if len(bootstrap.Namespaces) != 3 {
		return errors.New("init bootstrap requires exactly three Sandbox namespaces")
	}
	seen := make(map[isolation.NamespaceType]struct{}, len(bootstrap.Namespaces))
	for _, namespace := range bootstrap.Namespaces {
		if err := namespace.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[namespace.Type]; duplicate {
			return errors.New("init bootstrap contains a duplicate namespace")
		}
		seen[namespace.Type] = struct{}{}
	}
	for _, required := range []isolation.NamespaceType{isolation.NamespaceUTS, isolation.NamespaceIPC, isolation.NamespaceNetwork} {
		if _, exists := seen[required]; !exists {
			return errors.New("init bootstrap is missing a required Sandbox namespace")
		}
	}
	return nil
}

// SortedNamespaces returns a defensive deterministic join order so launcher
// argument order cannot alter the bootstrap's namespace transition sequence.
func (bootstrap InitBootstrap) SortedNamespaces() []BootstrapNamespace {
	namespaces := append([]BootstrapNamespace(nil), bootstrap.Namespaces...)
	sort.Slice(namespaces, func(left, right int) bool { return namespaces[left].Type < namespaces[right].Type })
	return namespaces
}
