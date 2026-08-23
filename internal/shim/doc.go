// Package shim implements the M3 keeper and long-lived Attempt init wrapper.
//
// The package deliberately owns no namespace or cgroup setup. A provider must
// finish those steps while the init wrapper is held behind its one-shot gate.
// Releasing that gate starts a workload child; the wrapper itself never execs
// the workload and remains available for inspection after the child exits.
package shim
