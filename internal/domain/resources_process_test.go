package domain

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// TestResourcesValidation verifies optional, positive, and request-versus-limit semantics.
func TestResourcesValidation(t *testing.T) {
	one, two := int64(1), int64(2)
	nine, ten, eleven := int64(9), int64(10), int64(11)
	zero, negative := int64(0), int64(-1)
	tests := []struct {
		name    string
		value   Resources
		wantErr bool
	}{
		{name: "all omitted"},
		{name: "valid", value: Resources{
			Requests: ResourceRequests{CPURequestMilli: &nine, MemoryRequestBytes: &one},
			Limits:   ResourceLimits{CPULimitMilli: &ten, MemoryLimitBytes: &two, PidsLimit: &one},
		}},
		{name: "zero", value: Resources{Limits: ResourceLimits{PidsLimit: &zero}}, wantErr: true},
		{name: "negative", value: Resources{Limits: ResourceLimits{CPULimitMilli: &negative}}, wantErr: true},
		{name: "CPU below kernel quota floor", value: Resources{
			Limits: ResourceLimits{CPULimitMilli: &nine},
		}, wantErr: true},
		{name: "CPU at kernel quota floor", value: Resources{
			Limits: ResourceLimits{CPULimitMilli: &ten},
		}},
		{name: "cpu request exceeds limit", value: Resources{
			Requests: ResourceRequests{CPURequestMilli: &eleven}, Limits: ResourceLimits{CPULimitMilli: &ten},
		}, wantErr: true},
		{name: "memory request exceeds limit", value: Resources{
			Requests: ResourceRequests{MemoryRequestBytes: &two}, Limits: ResourceLimits{MemoryLimitBytes: &one},
		}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.value.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Resources.Validate() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

// TestResolveResourceLimitsPersistsDefaults verifies Container policy makes max/default semantics explicit and survives JSON persistence.
func TestResolveResourceLimitsPersistsDefaults(t *testing.T) {
	resolved, err := ResolveResourceLimits(Resources{})
	if err != nil {
		t.Fatalf("ResolveResourceLimits(defaults) error = %v", err)
	}
	if !resolved.CPUUnlimited || resolved.CPULimitMilli != nil || !resolved.MemoryUnlimited ||
		resolved.MemoryLimitBytes != nil || resolved.PidsLimit != DefaultPidsLimit {
		t.Fatalf("resolved defaults = %#v", resolved)
	}
	encoded, err := json.Marshal(resolved)
	if err != nil {
		t.Fatalf("json.Marshal(resolved defaults) error = %v", err)
	}
	wantJSON := `{"cpu_unlimited":true,"cpu_limit_milli":null,"memory_unlimited":true,"memory_limit_bytes":null,"pids_limit":1024}`
	if string(encoded) != wantJSON {
		t.Fatalf("resolved defaults JSON = %s, want %s", encoded, wantJSON)
	}
	var restored ResolvedResourceLimits
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("json.Unmarshal(resolved defaults) error = %v", err)
	}
	if err := restored.Validate(); err != nil || !reflect.DeepEqual(restored, resolved) {
		t.Fatalf("restored resolved limits = %#v, validation error = %v", restored, err)
	}
}

// TestResolvedResourceLimitsClone verifies finite resolved values remain immutable across Container/store clone boundaries.
func TestResolvedResourceLimitsClone(t *testing.T) {
	cpu, memory, pids := int64(10), int64(8_193), int64(33)
	resolved, err := ResolveResourceLimits(Resources{Limits: ResourceLimits{
		CPULimitMilli: &cpu, MemoryLimitBytes: &memory, PidsLimit: &pids,
	}})
	if err != nil {
		t.Fatalf("ResolveResourceLimits(explicit) error = %v", err)
	}
	clone := resolved.Clone()
	*clone.CPULimitMilli = 20
	*clone.MemoryLimitBytes = 16_384
	clone.PidsLimit = 44
	if *resolved.CPULimitMilli != 10 || *resolved.MemoryLimitBytes != 8_193 || resolved.PidsLimit != 33 {
		t.Fatalf("ResolvedResourceLimits.Clone() retained aliases: %#v", resolved)
	}
}

// TestResolvedResourceLimitsRequiresExplicitSemantics verifies missing or contradictory max/default fields cannot be accepted as hidden policy.
func TestResolvedResourceLimitsRequiresExplicitSemantics(t *testing.T) {
	ten := int64(10)
	one := int64(1)
	tests := []ResolvedResourceLimits{
		{},
		{CPUUnlimited: true, CPULimitMilli: &ten, MemoryUnlimited: true, PidsLimit: DefaultPidsLimit},
		{CPULimitMilli: &one, MemoryUnlimited: true, PidsLimit: DefaultPidsLimit},
		{CPUUnlimited: true, MemoryUnlimited: false, PidsLimit: DefaultPidsLimit},
		{CPUUnlimited: true, MemoryUnlimited: true, PidsLimit: 0},
	}
	for _, limits := range tests {
		if err := limits.Validate(); !IsCode(err, CodeInvalidArgument) {
			t.Errorf("ResolvedResourceLimits.Validate(%#v) error = %v, want invalid argument", limits, err)
		}
	}
}

// TestResourcesClone verifies immutable specs cannot be modified through optional-value aliases.
func TestResourcesClone(t *testing.T) {
	cpu, memory := int64(500), int64(1024)
	original := Resources{
		Requests: ResourceRequests{CPURequestMilli: &cpu},
		Limits:   ResourceLimits{MemoryLimitBytes: &memory},
	}
	clone := original.Clone()
	*clone.Requests.CPURequestMilli = 900
	*clone.Limits.MemoryLimitBytes = 2048
	if *original.Requests.CPURequestMilli != 500 || *original.Limits.MemoryLimitBytes != 1024 {
		t.Fatalf("Resources.Clone() retained pointer aliases: %#v", original)
	}
}

// TestProcessSpecFidelity verifies argv/env special values and duplicates survive structured cloning.
func TestProcessSpecFidelity(t *testing.T) {
	original := ProcessSpec{
		Argv: []string{"/bin/tool", "", "space value", `quote"value`},
		Environment: []EnvVar{
			{Name: "TOKEN", Value: "a=b=c"},
			{Name: "TOKEN", Value: "duplicate-preserved"},
			{Name: "EMPTY", Value: ""},
		},
		WorkingDirectory: "/work dir",
		Termination:      TerminationPolicy{Signal: "SIGTERM", GracePeriod: 3 * time.Second, EscalationSignal: "SIGKILL"},
	}
	if err := original.Validate(); err != nil {
		t.Fatalf("ProcessSpec.Validate() error = %v", err)
	}
	clone := original.Clone()
	if !reflect.DeepEqual(clone, original) {
		t.Fatalf("ProcessSpec.Clone() = %#v, want %#v", clone, original)
	}
	clone.Argv[1] = "changed"
	clone.Environment[0].Value = "changed"
	if original.Argv[1] != "" || original.Environment[0].Value != "a=b=c" {
		t.Fatal("ProcessSpec.Clone() retained slice aliases")
	}
}

// TestTerminationPolicyRequiresExplicitPair verifies M1 does not invent signal defaults.
func TestTerminationPolicyRequiresExplicitPair(t *testing.T) {
	if err := (TerminationPolicy{}).Validate(); err != nil {
		t.Fatalf("unspecified TerminationPolicy.Validate() error = %v", err)
	}
	if _, err := NewKillPlan(TerminationPolicy{}); !IsCode(err, CodeInvalidArgument) {
		t.Fatalf("NewKillPlan(unspecified) error = %v, want invalid argument", err)
	}
	policy := TerminationPolicy{Signal: "SIGTERM", GracePeriod: time.Second, EscalationSignal: "SIGKILL"}
	plan, err := NewKillPlan(policy)
	if err != nil {
		t.Fatalf("NewKillPlan(explicit) error = %v", err)
	}
	if plan.Signal != "SIGTERM" || plan.EscalationSignal != "SIGKILL" || plan.GracePeriod != time.Second {
		t.Fatalf("NewKillPlan(explicit) = %#v", plan)
	}
}

// TestProcessSpecRejectsUnrepresentableValues verifies NUL and invalid env names fail before persistence.
func TestProcessSpecRejectsUnrepresentableValues(t *testing.T) {
	tests := []ProcessSpec{
		{},
		{Argv: []string{""}},
		{Argv: []string{"bad\x00arg"}},
		{Argv: []string{"relative-tool"}},
		{Argv: []string{"/bin/../bin/tool"}},
		{Argv: []string{"/bin/tool"}, WorkingDirectory: "relative-work"},
		{Argv: []string{"/bin/tool"}, WorkingDirectory: "/work/../work"},
		{Argv: []string{"ok"}, Environment: []EnvVar{{Name: "BAD=NAME", Value: "x"}}},
		{Argv: []string{"ok"}, Environment: []EnvVar{{Name: "OK", Value: "bad\x00value"}}},
	}
	for _, value := range tests {
		if err := value.Validate(); !IsCode(err, CodeInvalidArgument) {
			t.Fatalf("ProcessSpec.Validate(%#v) error = %v, want invalid argument", value, err)
		}
	}
}
