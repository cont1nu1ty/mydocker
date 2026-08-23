// Package cgroupv2 manages an explicitly delegated cgroup v2 subtree for
// Sandbox parents and Attempt children. It never falls back to cgroup v1 and
// never recursively removes host paths.
package cgroupv2
