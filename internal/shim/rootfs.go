package shim

import (
	"errors"
	"fmt"
	"net"
	"time"

	"mydocker/internal/isolation"
	"mydocker/internal/ownership"
)

// RootfsRequest is the one-shot trusted daemon-to-PID1 preparation command;
// it contains a catalog-resolved source and the exact PID/mount namespace inodes
// already checkpointed for this wrapper.
type RootfsRequest struct {
	SourceID            string                 `json:"source_id"`
	Source              isolation.RootfsConfig `json:"source"`
	DNS                 []string               `json:"dns,omitempty"`
	PIDNamespaceInode   uint64                 `json:"pid_namespace_inode"`
	MountNamespaceInode uint64                 `json:"mount_namespace_inode"`
	ConfigurationSHA256 string                 `json:"configuration_sha256"`
}

// Clone copies the ordered DNS list so replay comparison cannot alias caller memory.
func (request RootfsRequest) Clone() RootfsRequest {
	clone := request
	clone.DNS = append([]string(nil), request.DNS...)
	return clone
}

// Validate rejects path-shaped source identities, malformed DNS, missing
// namespace receipts, or a non-canonical configuration fingerprint.
func (request RootfsRequest) Validate() error {
	if err := validateOpaque("rootfs source ID", request.SourceID, 256); err != nil {
		return err
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if request.PIDNamespaceInode == 0 || request.MountNamespaceInode == 0 {
		return errors.New("rootfs request requires PID and mount namespace inodes")
	}
	if len(request.DNS) > 3 {
		return errors.New("rootfs request supports at most three DNS servers")
	}
	for index, server := range request.DNS {
		if net.ParseIP(server) == nil {
			return fmt.Errorf("rootfs DNS server %d must be an IP literal", index)
		}
	}
	if !validDigest(request.ConfigurationSHA256) {
		return errors.New("rootfs request requires a canonical configuration digest")
	}
	return nil
}

// RootfsPreparation is the immutable ACK retained by PID1 and replayed after
// daemon reconnect without repeating mount or pivot side effects.
type RootfsPreparation struct {
	RequestSHA256  string    `json:"request_sha256"`
	EvidenceSHA256 string    `json:"evidence_sha256"`
	PreparedAt     time.Time `json:"prepared_at"`
}

// Validate checks that a preparation ACK contains canonical request/effect
// evidence and a wrapper-stamped completion time.
func (preparation RootfsPreparation) Validate() error {
	if !validDigest(preparation.RequestSHA256) || !validDigest(preparation.EvidenceSHA256) || preparation.PreparedAt.IsZero() {
		return errors.New("rootfs preparation requires request/effect digests and completion time")
	}
	expected, err := rootfsPreparationEvidence(preparation.RequestSHA256, preparation.PreparedAt)
	if err != nil {
		return err
	}
	if preparation.EvidenceSHA256 != expected {
		return errors.New("rootfs preparation evidence does not match its request and completion time")
	}
	return nil
}

// ValidateFor binds an ACK to the exact command sent by the daemon, rejecting
// a well-formed preparation replayed from another source, DNS, or namespace set.
func (preparation RootfsPreparation) ValidateFor(request RootfsRequest) error {
	if err := preparation.Validate(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	expected, err := ownership.EvidenceDigest(request.Clone())
	if err != nil {
		return err
	}
	if preparation.RequestSHA256 != expected {
		return errors.New("rootfs preparation ACK belongs to another request")
	}
	return nil
}

// RootfsPreparer performs the privileged one-shot mount sequence inside the
// already-created PID1 wrapper; a returned error permanently seals that wrapper.
type RootfsPreparer interface {
	PrepareRootfs(RootfsRequest) error
}

// newRootfsPreparation binds a successful completion to the exact immutable
// command so semantic replay can distinguish retries from conflicting input.
func newRootfsPreparation(request RootfsRequest, preparedAt time.Time) (RootfsPreparation, error) {
	requestDigest, err := ownership.EvidenceDigest(request.Clone())
	if err != nil {
		return RootfsPreparation{}, err
	}
	preparation := RootfsPreparation{RequestSHA256: requestDigest, PreparedAt: preparedAt.Round(0).UTC()}
	preparation.EvidenceSHA256, err = rootfsPreparationEvidence(preparation.RequestSHA256, preparation.PreparedAt)
	if err != nil {
		return RootfsPreparation{}, err
	}
	return preparation, preparation.ValidateFor(request)
}

// rootfsPreparationEvidence recomputes the canonical effect ACK without
// recursively including its own digest.
func rootfsPreparationEvidence(requestSHA256 string, preparedAt time.Time) (string, error) {
	return ownership.EvidenceDigest(struct {
		RequestSHA256 string    `json:"request_sha256"`
		PreparedAt    time.Time `json:"prepared_at"`
	}{requestSHA256, preparedAt})
}
