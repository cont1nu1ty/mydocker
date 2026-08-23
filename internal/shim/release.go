package shim

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// launchParentGuard supplies the only Pdeathsig mutation needed after release;
// tests inject it so default verification never calls prctl.
type launchParentGuard interface {
	ClearParentDeathSignal() error
}

// LaunchReleaseByte is the only parent authorization accepted by a newly
// exec'd shim before it may create namespaces, bind its socket, or serve.
const LaunchReleaseByte byte = 0xa5

// WaitLaunchRelease consumes and closes one inherited pipe descriptor. A
// parent crash produces EOF and therefore makes the unreconciled child exit;
// success requires exactly one authorization byte followed by pipe closure.
func WaitLaunchRelease(fd int) error {
	return waitLaunchRelease(fd, systemLaunchParentGuard{})
}

// waitLaunchRelease consumes exact durable authorization and clears Pdeathsig;
// parent exit before the clear kills this child and is recovered through the journal.
func waitLaunchRelease(fd int, guard launchParentGuard) error {
	if fd < 3 {
		return errors.New("launch release descriptor must be an inherited child descriptor")
	}
	if guard == nil {
		return errors.New("launch parent guard must not be nil")
	}
	file := os.NewFile(uintptr(fd), "mydocker-launch-release")
	if file == nil {
		return errors.New("launch release descriptor is invalid")
	}
	payload, readErr := io.ReadAll(io.LimitReader(file, 2))
	closeErr := file.Close()
	if readErr != nil {
		return errors.Join(fmt.Errorf("read launch release gate: %w", readErr), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close launch release gate: %w", closeErr)
	}
	if len(payload) != 1 || payload[0] != LaunchReleaseByte {
		return errors.New("launch release gate closed without exact parent authorization")
	}
	if err := guard.ClearParentDeathSignal(); err != nil {
		return fmt.Errorf("clear authorized launch parent death signal: %w", err)
	}
	return nil
}
