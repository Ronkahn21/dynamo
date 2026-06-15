// Package main provides the snapshot-agent DaemonSet entrypoint.
// The agent runs the node-local snapshot controller and delegates CRIU/CUDA
// execution to the snapshot executor workflows.
package main

import (
	"cmp"
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-logr/logr"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/controller"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/logging"
	snapshotruntime "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/runtime"
)

func main() {
	runtimeType := flag.String("runtime", cmp.Or(os.Getenv("RUNTIME_TYPE"), snapshotruntime.RuntimeContainerd),
		"Container runtime backend: containerd or crio")
	runtimeSocket := flag.String("runtime-socket", os.Getenv("RUNTIME_SOCKET"),
		"Path to the container runtime socket (defaults to per-runtime convention)")
	flag.Parse()

	rootLog := logging.ConfigureLogger("stdout")
	agentLog := rootLog.WithName("agent")

	cfg, err := LoadConfigOrDefault(ConfigMapPath)
	if err != nil {
		fatal(agentLog, err, "Failed to load configuration")
	}
	if err := cfg.Validate(); err != nil {
		fatal(agentLog, err, "Invalid configuration")
	}

	rt, err := snapshotruntime.New(*runtimeType, *runtimeSocket)
	if err != nil {
		fatal(agentLog, err, "Failed to initialize container runtime",
			"runtime", *runtimeType, "socket", *runtimeSocket)
	}
	defer func() {
		if closeErr := rt.Close(); closeErr != nil {
			agentLog.Error(closeErr, "Failed to close runtime client")
		}
	}()

	// rootCtx is cancelled on signal. The restore informer's lifetime is bound to
	// informerCtx, which is only cancelled after the manager's Start returns, so the
	// restore path keeps running until the capture manager has fully shut down.
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	informerCtx, stopInformer := context.WithCancel(context.Background())
	defer stopInformer()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		agentLog.Info("Shutting down")
		cancel()
	}()

	agentLog.Info("Starting snapshot agent",
		"node", cfg.NodeName,
		"restricted_namespace", cfg.RestrictedNamespace,
		"runtime", *runtimeType,
	)

	// Restore path: the existing node-local client-go controller.
	nodeController, err := controller.NewNodeController(cfg, rt, rootLog.WithName("controller"))
	if err != nil {
		fatal(agentLog, err, "Failed to create snapshot node controller")
	}
	restoreDone := make(chan error, 1)
	go func() {
		agentLog.Info("Snapshot restore controller started")
		restoreDone <- nodeController.Run(informerCtx)
	}()

	// Capture path: the per-node SnapshotContent controller-runtime manager.
	mgr, err := controller.NewSnapshotContentManager(cfg, rt)
	if err != nil {
		fatal(agentLog, err, "Failed to create snapshot-content manager")
	}

	agentLog.Info("Starting snapshot-content manager")
	startErr := mgr.Start(rootCtx)

	// Manager has returned; now tear down the restore informer.
	stopInformer()
	if restoreErr := <-restoreDone; restoreErr != nil {
		agentLog.Error(restoreErr, "Snapshot restore controller exited with error")
	}
	if startErr != nil {
		fatal(agentLog, startErr, "Snapshot-content manager exited with error")
	}

	agentLog.Info("Agent stopped")
}

func fatal(log logr.Logger, err error, msg string, keysAndValues ...interface{}) {
	if err != nil {
		log.Error(err, msg, keysAndValues...)
	} else {
		log.Info(msg, keysAndValues...)
	}
	os.Exit(1)
}
