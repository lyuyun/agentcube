/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/klog/v2"

	"github.com/volcano-sh/agentcube/pkg/picod"
)

func main() {
	port := flag.Int("port", 8080, "Port for the PicoD server to listen on")
	workspace := flag.String("workspace", "", "Root directory for file operations (default: current working directory)")

	// Initialize klog flags
	klog.InitFlags(nil)
	flag.Parse()

	config := picod.Config{
		Port:      *port,
		Workspace: *workspace,
	}

	// Create server
	server := picod.NewServer(config)

	// Setup signal handling with context cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Start PicoD server in goroutine
	errCh := make(chan error, 1)
	go func() {
		klog.Infof("Starting PicoD server on port %d", *port)
		if err := server.Start(ctx); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	if server.SessionGateEnabled() {
		// Wait until the HTTP listener is bound (bootstrap complete) before
		// opening the inject socket. The Kuasar sandboxer requires the workload
		// to be in a quiescent state at this point.
		select {
		case <-ctx.Done():
			klog.Info("Received shutdown signal before bootstrap complete")
			<-errCh
			return
		case <-server.ListenerReady():
		case err := <-errCh:
			klog.Fatalf("Server error before bootstrap complete: %v", err)
		}

		// Open the inject socket and run the WarmFork handshake.
		//
		// Initial snapshot path: blocks indefinitely at Accept(); the VM is captured
		// while picod waits here. sessionGate=true is baked into the snapshot memory.
		//
		// Restore path: the process resumes from Accept() in the restored VM, completes
		// the handshake with Kuasar, and reaches the code below. This is the mechanism
		// for "after restore, inject fresh session state" (design §7.4):
		//   - Autonomous mode (Phase 1): Kuasar sends COMMIT directly; EnvOverrides is
		//     empty, so no session-specific env vars are applied.
		//   - Injection mode (Phase 2): Kuasar sends PREPARE with per-session env vars
		//     before COMMIT; those overrides are applied below via os.Setenv.
		result, err := picod.WaitForHandshake(ctx, "")
		if err != nil {
			klog.Fatalf("WarmFork handshake error: %v", err)
		}

		// Apply per-session env overrides from PREPARE. Empty in Phase 1 autonomous mode.
		for k, v := range result.EnvOverrides {
			if err := os.Setenv(k, v); err != nil {
				klog.Warningf("Failed to apply env override %s: %v", k, err)
			}
		}

		// Open the session gate: user requests can now be served.
		server.OpenSessionGate()
		klog.V(2).InfoS("picod: session gate open, HTTP API ready", "taskID", result.TaskID)
	}

	// Wait for signal or fatal error
	select {
	case <-ctx.Done():
		klog.Info("Received shutdown signal, shutting down gracefully...")
		<-errCh
	case err := <-errCh:
		klog.Fatalf("Server error: %v", err)
	}

	klog.Info("PicoD server stopped")
}
