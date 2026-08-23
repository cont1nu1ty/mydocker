// Package slim adapts the M2 isolation contracts to the M3 long-lived shim protocol.
//
// Host paths are derived only from a configured private runtime root and an
// ownership token. API values and receipt attributes never become paths.
// The production Linux launcher currently fails preflight because the existing
// M2 primitives do not yet expose cgroup-FD-at-fork for both wrapper roles or
// one safe parent-to-PID1 bootstrap that joins adopted Sandbox namespaces,
// checkpoints the final wrapper, later accepts the catalog-resolved rootfs,
// pivots root, and then enters the shim command. Pure tests use an explicit
// fake Launcher.
package slim
