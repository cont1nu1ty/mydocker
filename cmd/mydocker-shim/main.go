// mydocker-shim is the restricted M3 keeper and long-lived Attempt init wrapper.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"mydocker/internal/isolation"
	"mydocker/internal/logstore"
	"mydocker/internal/shim"
)

// main converts bootstrap or serving failure to a stable non-zero command status.
func main() {
	if len(os.Args) > 1 && os.Args[1] == "bootstrap-init" {
		if err := runBootstrap(os.Args[2:], os.Stderr); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "mydocker-shim bootstrap: %v\n", err)
			os.Exit(1)
		}
		return
	}
	context, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(context, os.Args[1:], os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "mydocker-shim: %v\n", err)
		os.Exit(1)
	}
}

// run loads one private config and serves the selected wrapper until process shutdown.
func run(ctx context.Context, arguments []string, stderr io.Writer) error {
	if ctx == nil {
		return errors.New("shim command context must not be nil")
	}
	flags := flag.NewFlagSet("mydocker-shim", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "absolute path to private wrapper configuration")
	configEvidence := flags.String("config-evidence", "", "expected immutable wrapper configuration evidence")
	releaseFD := flags.Int("release-fd", -1, "parent-owned launch release pipe descriptor")
	bootstrapComplete := flags.Bool("bootstrap-complete", false, "validated PID1 bootstrap second exec")
	pidInode := flags.Uint64("pid-inode", 0, "bootstrap PID namespace inode")
	mountInode := flags.Uint64("mount-inode", 0, "bootstrap mount namespace inode")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *configPath == "" || flags.NArg() != 0 {
		return errors.New("exactly one -config path is required")
	}
	if *releaseFD >= 0 && *bootstrapComplete {
		return errors.New("launch release gate and bootstrap completion are mutually exclusive")
	}
	if *releaseFD >= 0 {
		if err := shim.WaitLaunchRelease(*releaseFD); err != nil {
			return err
		}
	} else if !*bootstrapComplete {
		return errors.New("shim requires a parent release gate or validated bootstrap completion")
	}
	config, err := shim.LoadRuntimeConfig(*configPath)
	if err != nil {
		return err
	}
	if *releaseFD >= 0 && config.Mode != shim.ModeKeeper {
		return errors.New("parent release gate entry accepts only keeper mode; init requires PID1 bootstrap completion")
	}
	if *configEvidence != "" {
		observed, err := shim.RuntimeConfigEvidence(config)
		if err != nil {
			return err
		}
		if observed != *configEvidence || config.WrapperEvidence != *configEvidence {
			return errors.New("runtime config differs from bootstrap evidence")
		}
	}
	if *bootstrapComplete {
		if err := shim.ValidateInitBootstrapCompletion(config, *configEvidence, *pidInode, *mountInode); err != nil {
			return err
		}
	}
	if config.Mode == shim.ModeKeeper {
		wrapper, err := shim.NewKeeper(config.KeeperSpec())
		if err != nil {
			return err
		}
		return serve(ctx, config.ControlSocket, wrapper)
	}
	return runInit(ctx, config)
}

// runBootstrap parses only fixed descriptor/inode arguments emitted by the
// production launcher and delegates the non-returning namespace reattach stage.
func runBootstrap(arguments []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("mydocker-shim bootstrap-init", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "absolute path to private wrapper configuration")
	configEvidence := flags.String("config-evidence", "", "expected immutable wrapper configuration evidence")
	utsFD := flags.Int("uts-fd", -1, "inherited UTS namespace descriptor")
	utsInode := flags.Uint64("uts-inode", 0, "expected UTS namespace inode")
	ipcFD := flags.Int("ipc-fd", -1, "inherited IPC namespace descriptor")
	ipcInode := flags.Uint64("ipc-inode", 0, "expected IPC namespace inode")
	networkFD := flags.Int("net-fd", -1, "inherited network namespace descriptor")
	networkInode := flags.Uint64("net-inode", 0, "expected network namespace inode")
	releaseFD := flags.Int("release-fd", -1, "parent-owned launch release pipe descriptor")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("bootstrap-init does not accept positional arguments")
	}
	if err := shim.WaitLaunchRelease(*releaseFD); err != nil {
		return err
	}
	return shim.RunInitBootstrap(shim.InitBootstrap{
		SchemaVersion: shim.InitBootstrapSchemaVersion, Executable: os.Args[0],
		ConfigPath: *configPath, ConfigEvidence: *configEvidence,
		Namespaces: []shim.BootstrapNamespace{
			{Type: isolation.NamespaceUTS, FD: *utsFD, Inode: *utsInode},
			{Type: isolation.NamespaceIPC, FD: *ipcFD, Inode: *ipcInode},
			{Type: isolation.NamespaceNetwork, FD: *networkFD, Inode: *networkInode},
		},
	})
}

// runInit opens durable output and terminal stores before exposing the gated
// wrapper socket and reports every reverse-order descriptor cleanup failure.
func runInit(ctx context.Context, config shim.RuntimeConfig) (resultErr error) {
	ownerDirectory, retainedConfig, err := retainInitArtifacts(config)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, ownerDirectory.Close())
	}()
	logs, err := logstore.Open(config.LogPath, logstore.Identity{ContainerID: config.ContainerID, AttemptID: config.AttemptID})
	if err != nil {
		return fmt.Errorf("open workload log: %w", err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, logs.Close())
	}()
	stdout, err := shim.NewLogWriter(logs, logstore.StreamStdout)
	if err != nil {
		return err
	}
	stderr, err := shim.NewLogWriter(logs, logstore.StreamStderr)
	if err != nil {
		return err
	}
	terminal, err := shim.NewFileTerminalStore(retainedConfig.TerminalPath)
	if err != nil {
		return err
	}
	wrapper, err := shim.NewInitWithRootfs(config.InitSpec(), shim.OSChildRunner{}, stdout, stderr, terminal, shim.NewPID1RootfsPreparer(int(ownerDirectory.Fd())))
	if err != nil {
		return err
	}
	return serve(ctx, retainedConfig.ControlSocket, wrapper)
}

// serve binds the private control endpoint, keeps the wrapper resident across
// daemon connections, and joins socket cleanup failure into its final result.
func serve(ctx context.Context, socketPath string, wrapper *shim.Wrapper) (resultErr error) {
	server, err := shim.NewControlServer(socketPath, wrapper)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, server.Close())
	}()
	return server.Serve(ctx)
}
