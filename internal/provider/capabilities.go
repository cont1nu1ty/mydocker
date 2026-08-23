package provider

import (
	"errors"
	"fmt"
	"sort"

	"mydocker/internal/isolation"
)

// SchemaVersion is the only provider contract schema understood by M2 and M3.
const SchemaVersion uint32 = 1

// CgroupController identifies one cgroup v2 controller required by a workload.
type CgroupController string

const (
	// ControllerCPU identifies the cpu controller used to enforce cpu.max.
	ControllerCPU CgroupController = "cpu"
	// ControllerMemory identifies the memory controller used to enforce memory.max and observe OOM events.
	ControllerMemory CgroupController = "memory"
	// ControllerPids identifies the pids controller used to enforce pids.max.
	ControllerPids CgroupController = "pids"
)

// Valid reports whether the controller belongs to the bounded M2 controller set.
func (c CgroupController) Valid() bool {
	return c == ControllerCPU || c == ControllerMemory || c == ControllerPids
}

// CgroupRequirements states the cgroup features the engine needs without naming a host path.
type CgroupRequirements struct {
	UnifiedV2   bool               `json:"unified_v2"`
	Delegated   bool               `json:"delegated"`
	Controllers []CgroupController `json:"controllers"`
}

// Validate rejects duplicate, unsupported, or path-shaped cgroup requirements.
func (r CgroupRequirements) Validate() error {
	if !r.UnifiedV2 || !r.Delegated {
		return errors.New("cgroup requirements must request a delegated unified v2 hierarchy")
	}
	if err := validateControllers(r.Controllers, "cgroup requirements"); err != nil {
		return err
	}
	return nil
}

// IsolationRequirements states the rootful process and namespace features required by the engine.
type IsolationRequirements struct {
	Rootful    bool                      `json:"rootful"`
	PIDFD      bool                      `json:"pidfd"`
	PivotRoot  bool                      `json:"pivot_root"`
	StartGate  bool                      `json:"start_gate"`
	Streams    bool                      `json:"streams"`
	Namespaces []isolation.NamespaceType `json:"namespaces"`
}

// Validate rejects incomplete or duplicate isolation requirements before provider preflight.
func (r IsolationRequirements) Validate() error {
	if !r.Rootful || !r.PIDFD || !r.PivotRoot || !r.StartGate || !r.Streams {
		return errors.New("isolation requirements must request rootful pidfd, pivot_root, start-gate, and stream support")
	}
	return validateNamespaces(r.Namespaces, "isolation requirements")
}

// Requirements is the complete typed host contract passed to M2 preflight.
type Requirements struct {
	SchemaVersion uint32                `json:"schema_version"`
	Cgroup        CgroupRequirements    `json:"cgroup"`
	Isolation     IsolationRequirements `json:"isolation"`
}

// M2Requirements returns the exact rootful cgroup-v2 and namespace feature set required by M2.
func M2Requirements() Requirements {
	return Requirements{
		SchemaVersion: SchemaVersion,
		Cgroup: CgroupRequirements{
			UnifiedV2:   true,
			Delegated:   true,
			Controllers: []CgroupController{ControllerCPU, ControllerMemory, ControllerPids},
		},
		Isolation: IsolationRequirements{
			Rootful:   true,
			PIDFD:     true,
			PivotRoot: true,
			StartGate: true,
			Streams:   true,
			Namespaces: []isolation.NamespaceType{
				isolation.NamespaceUTS,
				isolation.NamespaceIPC,
				isolation.NamespaceNetwork,
				isolation.NamespacePID,
				isolation.NamespaceMount,
			},
		},
	}
}

// Validate rejects an unknown schema or either incomplete provider requirement group.
func (r Requirements) Validate() error {
	if r.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported provider requirements schema version %d", r.SchemaVersion)
	}
	if err := r.Cgroup.Validate(); err != nil {
		return err
	}
	return r.Isolation.Validate()
}

// CgroupCapabilities records read-only provider preflight facts without exposing the delegated root path.
type CgroupCapabilities struct {
	UnifiedV2   bool               `json:"unified_v2"`
	Delegated   bool               `json:"delegated"`
	Controllers []CgroupController `json:"controllers"`
}

// Validate rejects duplicate or unsupported cgroup capability claims.
func (c CgroupCapabilities) Validate() error {
	return validateControllers(c.Controllers, "cgroup capabilities")
}

// IsolationCapabilities records read-only rootful, pidfd, rootfs, namespace, gate, and stream facts.
type IsolationCapabilities struct {
	Rootful    bool                      `json:"rootful"`
	PIDFD      bool                      `json:"pidfd"`
	PivotRoot  bool                      `json:"pivot_root"`
	StartGate  bool                      `json:"start_gate"`
	Streams    bool                      `json:"streams"`
	Namespaces []isolation.NamespaceType `json:"namespaces"`
}

// Validate rejects duplicate or unsupported namespace capability claims.
func (c IsolationCapabilities) Validate() error {
	return validateNamespaces(c.Namespaces, "isolation capabilities")
}

// Capabilities combines provider observations that can be checked against one Requirements value.
type Capabilities struct {
	SchemaVersion uint32                `json:"schema_version"`
	Cgroup        CgroupCapabilities    `json:"cgroup"`
	Isolation     IsolationCapabilities `json:"isolation"`
}

// Validate rejects an unknown schema or malformed provider capability observation.
func (c Capabilities) Validate() error {
	if c.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported provider capabilities schema version %d", c.SchemaVersion)
	}
	if err := c.Cgroup.Validate(); err != nil {
		return err
	}
	return c.Isolation.Validate()
}

// Satisfies verifies that every requested host fact was observed and fails closed on missing support.
func (c Capabilities) Satisfies(requirements Requirements) error {
	if err := requirements.Validate(); err != nil {
		return fmt.Errorf("validate requirements: %w", err)
	}
	if err := c.Validate(); err != nil {
		return fmt.Errorf("validate capabilities: %w", err)
	}
	if requirements.Cgroup.UnifiedV2 && !c.Cgroup.UnifiedV2 {
		return errors.New("provider does not report a unified cgroup v2 hierarchy")
	}
	if requirements.Cgroup.Delegated && !c.Cgroup.Delegated {
		return errors.New("provider does not report a delegated cgroup v2 hierarchy")
	}
	if missing := missingControllers(requirements.Cgroup.Controllers, c.Cgroup.Controllers); len(missing) > 0 {
		return fmt.Errorf("provider is missing cgroup controllers %v", missing)
	}
	checks := []struct {
		name      string
		required  bool
		available bool
	}{
		{name: "rootful", required: requirements.Isolation.Rootful, available: c.Isolation.Rootful},
		{name: "pidfd", required: requirements.Isolation.PIDFD, available: c.Isolation.PIDFD},
		{name: "pivot_root", required: requirements.Isolation.PivotRoot, available: c.Isolation.PivotRoot},
		{name: "start gate", required: requirements.Isolation.StartGate, available: c.Isolation.StartGate},
		{name: "streams", required: requirements.Isolation.Streams, available: c.Isolation.Streams},
	}
	for _, check := range checks {
		if check.required && !check.available {
			return fmt.Errorf("provider is missing %s capability", check.name)
		}
	}
	if missing := missingNamespaces(requirements.Isolation.Namespaces, c.Isolation.Namespaces); len(missing) > 0 {
		return fmt.Errorf("provider is missing namespaces %v", missing)
	}
	return nil
}

// validateControllers rejects an empty, duplicate, or unsupported controller list.
func validateControllers(controllers []CgroupController, field string) error {
	if len(controllers) == 0 {
		return fmt.Errorf("%s must contain at least one controller", field)
	}
	seen := make(map[CgroupController]struct{}, len(controllers))
	for _, controller := range controllers {
		if !controller.Valid() {
			return fmt.Errorf("%s contains unsupported controller %q", field, controller)
		}
		if _, duplicate := seen[controller]; duplicate {
			return fmt.Errorf("%s contains duplicate controller %q", field, controller)
		}
		seen[controller] = struct{}{}
	}
	return nil
}

// validateNamespaces rejects an empty, duplicate, or unsupported namespace list.
func validateNamespaces(namespaces []isolation.NamespaceType, field string) error {
	if len(namespaces) == 0 {
		return fmt.Errorf("%s must contain at least one namespace", field)
	}
	seen := make(map[isolation.NamespaceType]struct{}, len(namespaces))
	for _, namespaceType := range namespaces {
		if !namespaceType.Valid() {
			return fmt.Errorf("%s contains unsupported namespace %q", field, namespaceType)
		}
		if _, duplicate := seen[namespaceType]; duplicate {
			return fmt.Errorf("%s contains duplicate namespace %q", field, namespaceType)
		}
		seen[namespaceType] = struct{}{}
	}
	return nil
}

// missingControllers returns a deterministic set difference for actionable preflight errors.
func missingControllers(required, available []CgroupController) []CgroupController {
	set := make(map[CgroupController]struct{}, len(available))
	for _, controller := range available {
		set[controller] = struct{}{}
	}
	var missing []CgroupController
	for _, controller := range required {
		if _, found := set[controller]; !found {
			missing = append(missing, controller)
		}
	}
	sort.Slice(missing, func(left, right int) bool { return missing[left] < missing[right] })
	return missing
}

// missingNamespaces returns a deterministic set difference for actionable preflight errors.
func missingNamespaces(required, available []isolation.NamespaceType) []isolation.NamespaceType {
	set := make(map[isolation.NamespaceType]struct{}, len(available))
	for _, namespaceType := range available {
		set[namespaceType] = struct{}{}
	}
	var missing []isolation.NamespaceType
	for _, namespaceType := range required {
		if _, found := set[namespaceType]; !found {
			missing = append(missing, namespaceType)
		}
	}
	sort.Slice(missing, func(left, right int) bool { return missing[left] < missing[right] })
	return missing
}
