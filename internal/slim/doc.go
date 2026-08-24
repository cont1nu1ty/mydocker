// Package slim adapts the M2 isolation contracts to the M3 long-lived shim protocol.
//
// Host paths are derived only from a configured private runtime root and an
// ownership token. API values and receipt attributes never become paths.
// The Linux launcher persists immutable intent before fork, places keeper and
// init wrappers in their verified cgroups at fork, retains pidfd-backed process
// evidence, and performs the parent-to-PID1 namespace bootstrap before the
// catalog-resolved rootfs is accepted. M3 sources are trusted prepared
// directories rather than per-Attempt snapshots, and the complete rootful path
// still requires isolated-host integration evidence.
package slim
