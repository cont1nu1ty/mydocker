package isolation

import (
	"fmt"
	"strconv"
	"strings"
)

// parseNamespaceLink extracts the bounded namespace kind and inode from Linux nsfs link text.
func parseNamespaceLink(value string) (NamespaceType, uint64, error) {
	open := strings.Index(value, ":[")
	if open <= 0 || !strings.HasSuffix(value, "]") {
		return "", 0, fmt.Errorf("invalid namespace link %q", value)
	}
	kind := NamespaceType(value[:open])
	if !kind.Valid() {
		return "", 0, fmt.Errorf("unsupported namespace link kind %q", kind)
	}
	inode, err := strconv.ParseUint(value[open+2:len(value)-1], 10, 64)
	if err != nil || inode == 0 {
		return "", 0, fmt.Errorf("invalid namespace inode in %q", value)
	}
	return kind, inode, nil
}

// validateNamespaceFD checks nsfs, link kind, descriptor inode, and an optional expected inode.
func validateNamespaceFD(ops Ops, fd int, expectedType NamespaceType, expectedInode uint64) error {
	filesystem, err := ops.FstatFS(fd)
	if err != nil {
		return fmt.Errorf("fstatfs namespace: %w", err)
	}
	if filesystem.Type != NamespaceFSMagic {
		return fmt.Errorf("%w: namespace descriptor is not nsfs", ErrUnsafeIdentity)
	}
	stat, err := ops.Fstat(fd)
	if err != nil {
		return fmt.Errorf("fstat namespace: %w", err)
	}
	link, err := ops.ReadlinkFD(fd)
	if err != nil {
		return fmt.Errorf("read namespace descriptor link: %w", err)
	}
	actualType, actualInode, err := parseNamespaceLink(link)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnsafeIdentity, err)
	}
	if actualType != expectedType || actualInode != stat.Ino || (expectedInode != 0 && actualInode != expectedInode) {
		return fmt.Errorf("%w: namespace kind/inode evidence changed", ErrUnsafeIdentity)
	}
	return nil
}
