// Package isolation defines the Linux-only M2 host-isolation boundary.
//
// The package keeps process and namespace ownership evidence explicit, performs
// read-only host preflight, and routes every host mutation through Ops so unit
// tests never need privileges. Namespace creation and rootfs preparation also
// require a LockedHelper, which pins and then disposes a dedicated OS thread.
package isolation
