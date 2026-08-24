package server

import (
	"context"
	"testing"

	v1 "mydocker/api/runtime/v1"
)

// TestEventDurationAvailabilityCrossesUDS verifies transport decoding preserves the distinction between an omitted duration and an explicit measured zero.
func TestEventDurationAvailabilityCrossesUDS(t *testing.T) {
	service := newFakeService()
	missing := newTestEvent(1)
	measuredZero := newTestEvent(2)
	zero := int64(0)
	measuredZero.DurationNanoseconds = &zero
	service.events = []v1.Event{missing, measuredZero}
	_, apiClient, _ := startTestServer(t, service, Config{})
	response, err := apiClient.Events(context.Background(), "", 2)
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}
	if len(response.Events) != 2 {
		t.Fatalf("Events() count = %d, want 2", len(response.Events))
	}
	if response.Events[0].DurationNanoseconds != nil {
		t.Fatalf("missing UDS duration = %d, want nil", *response.Events[0].DurationNanoseconds)
	}
	if response.Events[1].DurationNanoseconds == nil || *response.Events[1].DurationNanoseconds != 0 {
		t.Fatalf("measured-zero UDS duration = %#v, want explicit zero", response.Events[1].DurationNanoseconds)
	}
}
