//go:build linux

package shim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"

	"mydocker/internal/isolation"
)

const dnsMountSourceName = ".mydocker-resolv.conf.mount-source"

// PID1RootfsPreparer executes the deferred pivot in the long-lived init
// wrapper and injects DNS from an exact descriptor opened beneath the retained
// private owner directory rather than trusting a mutable host path.
type PID1RootfsPreparer struct {
	ops              isolation.Ops
	ownerDirectoryFD int
}

// NewPID1RootfsPreparer constructs the production Linux preparer with a
// borrowed, already-verified owner-directory descriptor. The caller must keep
// the descriptor open until the wrapper stops; effects occur only on PrepareRootfs.
func NewPID1RootfsPreparer(ownerDirectoryFD int) *PID1RootfsPreparer {
	return &PID1RootfsPreparer{ops: isolation.NewSystemOps(), ownerDirectoryFD: ownerDirectoryFD}
}

// PrepareRootfs creates a private inode-backed resolv.conf source, verifies
// this process is the expected PID1/mount owner, then performs the one-shot
// bind and pivot sequence. The source name exists only while mount(2) needs a
// mountable filesystem inode and is removed before this method returns.
func (preparer *PID1RootfsPreparer) PrepareRootfs(request RootfsRequest) (resultErr error) {
	if preparer == nil || preparer.ops == nil || preparer.ownerDirectoryFD < 3 {
		return errors.New("PID1 rootfs preparer is not configured")
	}
	if err := request.Validate(); err != nil {
		return err
	}
	dnsFD, err := createDNSMountSource(preparer.ownerDirectoryFD, request.DNS)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, removeDNSMountSource(preparer.ownerDirectoryFD, dnsFD), unix.Close(dnsFD))
	}()
	bootstrap := isolation.PID1Bootstrap{
		SchemaVersion: isolation.PID1BootstrapSchemaVersion,
		Namespaces: isolation.CreatedNamespaceSet{Inodes: map[isolation.NamespaceType]uint64{
			isolation.NamespacePID: request.PIDNamespaceInode, isolation.NamespaceMount: request.MountNamespaceInode,
		}},
		Rootfs: request.Source,
	}
	return isolation.RunPID1Child(context.Background(), preparer.ops, bootstrap, func(ctx context.Context, helper *isolation.LockedHelper) error {
		return helper.PrepareRootWithDNS(ctx, request.Source, dnsFD)
	})
}

// createDNSMountSource publishes canonical nameserver lines as one exclusive
// regular file beneath the retained private directory. Linux cannot bind-mount
// an anonymous memfd, so the name remains present only until mount completion.
func createDNSMountSource(ownerDirectoryFD int, servers []string) (_ int, resultErr error) {
	if ownerDirectoryFD < 3 {
		return -1, errors.New("DNS mount source requires a retained owner-directory descriptor")
	}
	payload := renderResolvConf(servers)
	fd, err := unix.Openat(ownerDirectoryFD, dnsMountSourceName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return -1, fmt.Errorf("create exclusive resolv.conf mount source: %w", err)
	}
	succeeded := false
	defer func() {
		if !succeeded {
			resultErr = errors.Join(resultErr, removeDNSMountSource(ownerDirectoryFD, fd), unix.Close(fd))
		}
	}()
	for len(payload) > 0 {
		written, writeErr := unix.Write(fd, payload)
		if writeErr != nil {
			return -1, fmt.Errorf("write resolv.conf mount source: %w", writeErr)
		}
		if written <= 0 || written > len(payload) {
			return -1, errors.New("write resolv.conf mount source made no progress")
		}
		payload = payload[written:]
	}
	if err := unix.Fchmod(fd, 0o600); err != nil {
		return -1, fmt.Errorf("restrict resolv.conf mount source: %w", err)
	}
	if err := unix.Fsync(fd); err != nil {
		return -1, fmt.Errorf("synchronize resolv.conf mount source: %w", err)
	}
	succeeded = true
	return fd, nil
}

// removeDNSMountSource unlinks only the exact regular inode created by this
// preparer. It refuses to delete a replacement and leaves the path for operator
// inspection if identity can no longer be proven.
func removeDNSMountSource(ownerDirectoryFD, sourceFD int) error {
	if ownerDirectoryFD < 3 || sourceFD < 0 {
		return errors.New("DNS mount source cleanup descriptors are invalid")
	}
	var source unix.Stat_t
	if err := unix.Fstat(sourceFD, &source); err != nil {
		return fmt.Errorf("inspect resolv.conf mount source descriptor: %w", err)
	}
	var path unix.Stat_t
	if err := unix.Fstatat(ownerDirectoryFD, dnsMountSourceName, &path, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect resolv.conf mount source path: %w", err)
	}
	if source.Mode&unix.S_IFMT != unix.S_IFREG || path.Mode&unix.S_IFMT != unix.S_IFREG || source.Dev != path.Dev || source.Ino != path.Ino || path.Uid != uint32(os.Geteuid()) {
		return errors.New("resolv.conf mount source path no longer names the exact owned regular file")
	}
	if err := unix.Unlinkat(ownerDirectoryFD, dnsMountSourceName, 0); err != nil {
		return fmt.Errorf("unlink exact resolv.conf mount source: %w", err)
	}
	return nil
}

// renderResolvConf returns deterministic bytes for the ordered Sandbox DNS
// contract, including an intentionally empty managed file for network=none.
func renderResolvConf(servers []string) []byte {
	var builder strings.Builder
	builder.WriteString("# generated by mydocker\n")
	for _, server := range servers {
		builder.WriteString("nameserver ")
		builder.WriteString(server)
		builder.WriteByte('\n')
	}
	return []byte(builder.String())
}
