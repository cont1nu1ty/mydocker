// Package provider defines the provider-neutral M2 host-resource boundary used
// by lifecycle orchestration and M3 recovery. The package contains contracts,
// validation, and rollback dispatch only; concrete host syscall providers live
// behind these interfaces.
package provider
