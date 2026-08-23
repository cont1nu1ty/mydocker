package slim

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"mydocker/internal/isolation"
	"mydocker/internal/provider"
	"mydocker/internal/shim"
)

// StaticSourceCatalog is an immutable configured mapping from opaque IDs to prepared rootfs paths.
type StaticSourceCatalog struct {
	sources map[provider.OpaqueID]isolation.RootfsConfig
}

// NewStaticSourceCatalog validates and copies trusted prepared-rootfs configuration.
func NewStaticSourceCatalog(sources map[provider.OpaqueID]isolation.RootfsConfig) (*StaticSourceCatalog, error) {
	if len(sources) == 0 {
		return nil, errors.New("prepared-rootfs catalog must not be empty")
	}
	copySources := make(map[provider.OpaqueID]isolation.RootfsConfig, len(sources))
	for id, source := range sources {
		if err := id.Validate(); err != nil {
			return nil, fmt.Errorf("prepared-rootfs ID %q: %w", id, err)
		}
		if err := source.Validate(); err != nil {
			return nil, fmt.Errorf("prepared-rootfs %q: %w", id, err)
		}
		copySources[id] = source
	}
	return &StaticSourceCatalog{sources: copySources}, nil
}

// Resolve returns only a configured rootfs and rejects unknown or path-shaped API identifiers.
func (catalog *StaticSourceCatalog) Resolve(ctx context.Context, id provider.OpaqueID) (isolation.RootfsConfig, error) {
	if ctx == nil {
		return isolation.RootfsConfig{}, errors.New("source catalog context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return isolation.RootfsConfig{}, err
	}
	if err := id.Validate(); err != nil {
		return isolation.RootfsConfig{}, err
	}
	source, found := catalog.sources[id]
	if !found {
		return isolation.RootfsConfig{}, fmt.Errorf("prepared-rootfs ID %q is not configured", id)
	}
	return source, nil
}

// unixShimClient uses the production fresh-connection control implementation.
type unixShimClient struct{}

// Do delegates one owner-scoped exchange to shim.DoControl.
func (unixShimClient) Do(ctx context.Context, path string, request shim.ControlRequest) (shim.ControlResponse, error) {
	return shim.DoControl(ctx, path, request)
}

// sessionRequestIDs combines one random daemon-session prefix with a monotonic local counter.
type sessionRequestIDs struct {
	prefix string
	mu     sync.Mutex
	next   uint64
}

// Next returns a bounded request identity that is not reused within this daemon session.
func (ids *sessionRequestIDs) Next(action shim.ControlAction) string {
	ids.mu.Lock()
	defer ids.mu.Unlock()
	ids.next++
	return fmt.Sprintf("%s-%s-%d", ids.prefix, action, ids.next)
}
