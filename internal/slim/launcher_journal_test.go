package slim

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"mydocker/internal/domain"
	"mydocker/internal/operation"
	"mydocker/internal/ownership"
	"mydocker/internal/shim"
)

// TestLaunchStoreRetriesUncertainDirectorySync verifies a visible config is
// never treated as durable until a retry confirms its owner directory entry.
func TestLaunchStoreRetriesUncertainDirectorySync(t *testing.T) {
	store, config := testLaunchStore(t)
	injected := errors.New("injected directory sync failure")
	calls := 0
	store.syncDirectory = func(string) error {
		calls++
		if calls <= 2 {
			return injected
		}
		return nil
	}
	if _, _, err := store.EnsureIntent(config); !errors.Is(err, injected) {
		t.Fatalf("first EnsureIntent error=%v", err)
	}
	if _, _, err := store.EnsureIntent(config); !errors.Is(err, injected) {
		t.Fatalf("second EnsureIntent error=%v", err)
	}
	if _, journal, err := store.EnsureIntent(config); err != nil || journal.Phase != launchPhaseIntent {
		t.Fatalf("third EnsureIntent=(%+v,%v)", journal, err)
	}
	if calls < 4 {
		t.Fatalf("sync calls=%d, want config confirmation plus journal commit", calls)
	}
}

// TestLaunchStorePersistsIntentBeforeStrongProcessTransitions verifies config
// and journal replay remain exact across intent, authorized, and ready phases.
func TestLaunchStorePersistsIntentBeforeStrongProcessTransitions(t *testing.T) {
	store, config := testLaunchStore(t)
	prepared, intent, err := store.EnsureIntent(config)
	if err != nil {
		t.Fatal(err)
	}
	if intent.Phase != launchPhaseIntent || prepared.WrapperEvidence == "" {
		t.Fatalf("prepared=%+v intent=%+v", prepared, intent)
	}
	evidence := fakeProcessEvidence(config.Owner)
	authorized, err := intent.withProcess(launchPhaseAuthorized, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(intent, authorized); err != nil {
		t.Fatal(err)
	}
	ready, err := authorized.withProcess(launchPhaseReady, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(authorized, ready); err != nil {
		t.Fatal(err)
	}
	_, recovered, err := store.EnsureIntent(config)
	if err != nil || recovered.Phase != launchPhaseReady || recovered.ProcessEvidence == nil || *recovered.ProcessEvidence != evidence {
		t.Fatalf("recovered=(%+v,%v)", recovered, err)
	}
}

// TestLaunchJournalRejectsSkippedRegressedOrReboundTransitions verifies the
// state machine cannot claim readiness directly, regress, or swap process identity.
func TestLaunchJournalRejectsSkippedRegressedOrReboundTransitions(t *testing.T) {
	store, config := testLaunchStore(t)
	_, intent, err := store.EnsureIntent(config)
	if err != nil {
		t.Fatal(err)
	}
	evidence := fakeProcessEvidence(config.Owner)
	if _, err := intent.withProcess(launchPhaseReady, evidence); err == nil {
		t.Fatal("intent advanced directly to ready")
	}
	authorized, err := intent.withProcess(launchPhaseAuthorized, evidence)
	if err != nil {
		t.Fatal(err)
	}
	rebound := evidence
	rebound.PID++
	rebound.StartTime++
	if _, err := authorized.withProcess(launchPhaseReady, rebound); err == nil {
		t.Fatal("ready transition replaced authorized process evidence")
	}
	ready, err := authorized.withProcess(launchPhaseReady, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLaunchJournalTransition(ready, authorized); err == nil {
		t.Fatal("ready journal regressed to authorized")
	}
	if _, err := intent.resetIntentAfterVerifiedAbsence(); err == nil {
		t.Fatal("empty intent reset without a recorded process")
	}
}

// TestLaunchStoreCASRejectsConcurrentAndStaleWriters verifies only one gated
// process can win authorization and an old absent observation cannot erase ready state.
func TestLaunchStoreCASRejectsConcurrentAndStaleWriters(t *testing.T) {
	store, config := testLaunchStore(t)
	_, intent, err := store.EnsureIntent(config)
	if err != nil {
		t.Fatal(err)
	}
	evidenceA := fakeProcessEvidence(config.Owner)
	evidenceB := evidenceA
	evidenceB.PID++
	evidenceB.StartTime++
	authorizedA, err := intent.withProcess(launchPhaseAuthorized, evidenceA)
	if err != nil {
		t.Fatal(err)
	}
	authorizedB, err := intent.withProcess(launchPhaseAuthorized, evidenceB)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(intent, authorizedA); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(intent, authorizedB); err == nil {
		t.Fatal("second concurrent authorization overwrote first process")
	}
	readyA, err := authorizedA.withProcess(launchPhaseReady, evidenceA)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(authorizedA, readyA); err != nil {
		t.Fatal(err)
	}
	staleReset, err := authorizedA.resetIntentAfterVerifiedAbsence()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(authorizedA, staleReset); err == nil {
		t.Fatal("stale authorized writer erased ready process evidence")
	}
	recovered, found, err := store.Read()
	if err != nil || !found || recovered.ChecksumSHA256 != readyA.ChecksumSHA256 {
		t.Fatalf("recovered journal=(%+v,%t,%v), want ready A", recovered, found, err)
	}
}

// TestLaunchStoreCASReplaysUncertainDirectorySync verifies authorized, ready,
// and reset renames become durable through exact retries after parent sync failure.
func TestLaunchStoreCASReplaysUncertainDirectorySync(t *testing.T) {
	for _, phase := range []launchPhase{launchPhaseAuthorized, launchPhaseReady, launchPhaseIntent} {
		t.Run(string(phase), func(t *testing.T) {
			store, config := testLaunchStore(t)
			_, intent, err := store.EnsureIntent(config)
			if err != nil {
				t.Fatal(err)
			}
			evidence := fakeProcessEvidence(config.Owner)
			authorized, err := intent.withProcess(launchPhaseAuthorized, evidence)
			if err != nil {
				t.Fatal(err)
			}
			ready, err := authorized.withProcess(launchPhaseReady, evidence)
			if err != nil {
				t.Fatal(err)
			}
			reset, err := ready.resetIntentAfterVerifiedAbsence()
			if err != nil {
				t.Fatal(err)
			}
			var expected, next launchJournal
			switch phase {
			case launchPhaseAuthorized:
				expected, next = intent, authorized
			case launchPhaseReady:
				if err := store.Write(intent, authorized); err != nil {
					t.Fatal(err)
				}
				expected, next = authorized, ready
			case launchPhaseIntent:
				if err := store.Write(intent, authorized); err != nil {
					t.Fatal(err)
				}
				if err := store.Write(authorized, ready); err != nil {
					t.Fatal(err)
				}
				expected, next = ready, reset
			}
			injected := errors.New("injected transition directory sync failure")
			calls := 0
			store.syncDirectory = func(string) error {
				calls++
				if calls == 1 {
					return injected
				}
				return nil
			}
			if err := store.Write(expected, next); !errors.Is(err, injected) {
				t.Fatalf("first Write error=%v, want injected sync failure", err)
			}
			persisted, found, err := store.Read()
			if err != nil || !found || !reflect.DeepEqual(persisted, next) {
				t.Fatalf("post-rename journal=(%+v,%t,%v), want next", persisted, found, err)
			}
			if err := store.Write(expected, next); err != nil {
				t.Fatalf("exact retry error=%v", err)
			}
			if calls != 2 {
				t.Fatalf("directory sync calls=%d, want failed write plus exact retry", calls)
			}
		})
	}
}

// TestLaunchStoreRejectsConfigDriftAndJournalCorruption verifies owner files
// cannot be silently repurposed after the immutable intent is durable.
func TestLaunchStoreRejectsConfigDriftAndJournalCorruption(t *testing.T) {
	store, config := testLaunchStore(t)
	if _, _, err := store.EnsureIntent(config); err != nil {
		t.Fatal(err)
	}
	drift := config
	drift.ControlSocket = filepath.Join(config.ControlSocket, "different")
	if _, _, err := store.EnsureIntent(drift); err == nil {
		t.Fatal("runtime config drift was accepted")
	}
	journal, found, err := store.Read()
	if err != nil || !found {
		t.Fatal(err)
	}
	journal.ChecksumSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	payload, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.paths.LaunchJournal, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Read(); err == nil {
		t.Fatal("corrupt launch journal was accepted")
	}
}

// testLaunchStore creates a private owner directory and one valid keeper config without launching a process.
func testLaunchStore(t *testing.T) (*launchStore, shim.RuntimeConfig) {
	t.Helper()
	root := privateSlimRoot(t)
	owner, err := ownership.NewOwnerKey("op-launch-store", operation.Target{Kind: operation.TargetSandbox, ID: "sandbox-launch-store"}, domain.InitialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := newArtifactStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := artifacts.ensureOwnerDirectory(owner); err != nil {
		t.Fatal(err)
	}
	paths, err := deriveArtifactPaths(root, owner)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newLaunchStore(paths, owner)
	if err != nil {
		t.Fatal(err)
	}
	config := shim.RuntimeConfig{
		SchemaVersion: shim.SchemaVersion, Mode: shim.ModeKeeper, Owner: owner,
		SandboxID: "sandbox-launch-store", ControlSocket: paths.ControlSocket,
	}
	return store, config
}
