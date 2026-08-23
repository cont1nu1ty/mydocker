// Package logstore persists one Container Attempt's stdout and stderr as a
// checksummed, append-only frame log. A Store assigns a global cursor and a
// per-stream sequence only after the prepared frame and its commit marker have
// each been synchronized. A Reader reopens validated read-only snapshots
// without acquiring or leaking the shim's exclusive writer descriptor.
package logstore
