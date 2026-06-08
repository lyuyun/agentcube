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

// Package kuasar implements the Driver for the Kuasar VMM sandboxer.
//
// The driver communicates with the node-local Kuasar sandboxer via the admin
// Unix socket (/run/vmm-sandboxer-admin.sock) using a newline-delimited JSON
// protocol (one request/response per connection).
//
// Build path (this driver):
//
//	template-create → the sandboxer probes the workload readiness socket
//	(CheckInjectSocket), pauses the VM, captures the snapshot, and resumes;
//	returns template_id on success.
//
// Supported snapshot modes:
//   - Fork: snapshot a quiescent workload for multi-restore.
package kuasar

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"k8s.io/klog/v2"

	agentdriver "github.com/volcano-sh/agentcube/pkg/agentd/driver"
	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

const (
	// ProviderName is the stable provider identifier for the Kuasar SnapStart driver.
	ProviderName = "snapstart.kuasar.io"

	// defaultSocketPath is the default Kuasar admin socket path.
	// Matches the --admin-listen default in the kuasar-vmm sandboxer.
	defaultSocketPath = "/run/vmm-sandboxer-admin.sock"

	// snapshotTimeout is the deadline for a template-create call. The sandboxer
	// must probe the workload readiness socket and capture the VM before this expires.
	snapshotTimeout = 10 * time.Minute

	// adminTimeout is the deadline for lightweight admin calls
	// (pool-gc, template-list, template-get).
	adminTimeout = 30 * time.Second
)

// Driver implements agentdriver.SnapshotDriver for Kuasar WarmFork snapshots.
// Pass an empty socketPath to NewDriver to use the default admin socket path.
type Driver struct {
	socketPath string
}

// NewDriver creates a Driver. Pass an empty string to use the default socket path.
func NewDriver(socketPath string) *Driver {
	if socketPath == "" {
		socketPath = defaultSocketPath
	}
	return &Driver{socketPath: socketPath}
}

func (d *Driver) Name() string { return ProviderName }

func (d *Driver) Capabilities(_ context.Context) agentdriver.SnapshotDriverCapabilities {
	return agentdriver.SnapshotDriverCapabilities{
		SnapshotModes: []runtimev1alpha1.SandboxSnapshotMode{
			runtimev1alpha1.SandboxSnapshotModeFork,
		},
	}
}

// Create dispatches to the mode-specific snapshot implementation.
func (d *Driver) Create(ctx context.Context, req agentdriver.SnapshotDriverCreateRequest) (*agentdriver.SnapshotDriverArtifact, error) {
	switch req.SnapshotMode {
	case runtimev1alpha1.SandboxSnapshotModeFork:
		return d.createSnapshotFork(ctx, req)
	default:
		return nil, fmt.Errorf("kuasar driver: unsupported snapshot mode %q", req.SnapshotMode)
	}
}

// Delete removes the physical artifact identified by snapshotKey.
// It resolves snapshotKey to the driver-internal reference by listing local artifacts.
func (d *Driver) Delete(ctx context.Context, snapshotKey string) error {
	a, err := d.findKuasarArtifact(ctx, snapshotKey)
	if err != nil {
		return fmt.Errorf("kuasar driver: delete %s: %w", snapshotKey, err)
	}
	if a == nil {
		return nil // already absent
	}
	switch a.Mode {
	case runtimev1alpha1.SandboxSnapshotModeFork:
		return d.deleteSnapshotFork(ctx, a.TemplateID)
	default:
		return fmt.Errorf("kuasar driver: cannot delete artifact with unknown mode %q", a.Mode)
	}
}

// List returns all Fork and Resume artifacts managed by this driver on this node.
func (d *Driver) List(ctx context.Context) ([]agentdriver.SnapshotDriverArtifact, error) {
	internal, err := d.listSnapshotFork(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]agentdriver.SnapshotDriverArtifact, len(internal))
	for i, a := range internal {
		result[i] = agentdriver.SnapshotDriverArtifact{
			ProviderName: ProviderName,
			SnapshotKey:  a.SnapshotKey,
		}
	}
	return result, nil
}

// Inspect returns the current status of the artifact identified by snapshotKey.
// It resolves snapshotKey to the driver-internal reference by listing local artifacts.
func (d *Driver) Inspect(ctx context.Context, snapshotKey string) (*agentdriver.SnapshotDriverArtifactStatus, error) {
	a, err := d.findKuasarArtifact(ctx, snapshotKey)
	if err != nil || a == nil {
		return &agentdriver.SnapshotDriverArtifactStatus{Phase: runtimev1alpha1.SnapshotArtifactPhaseUnavailable}, nil
	}
	switch a.Mode {
	case runtimev1alpha1.SandboxSnapshotModeFork:
		return d.inspectSnapshotFork(ctx, a.TemplateID)
	default:
		return &agentdriver.SnapshotDriverArtifactStatus{Phase: runtimev1alpha1.SnapshotArtifactPhaseUnavailable}, nil
	}
}

// adminRPC sends one JSON request to the Kuasar admin socket and populates resp
// with the parsed response fields. Pass nil for resp when the caller needs no
// response fields beyond ok/error. Each call opens and closes its own connection
// (one request per connection per the admin protocol).
func (d *Driver) adminRPC(ctx context.Context, timeout time.Duration, req, resp any) error {
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", d.socketPath)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", d.socketPath, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	if _, err := fmt.Fprintf(conn, "%s\n", data); err != nil {
		return fmt.Errorf("write request: %w", err)
	}

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	var base struct {
		Ok    bool   `json:"ok"`
		Error string `json:"error,omitempty"`
	}
	if err := json.Unmarshal([]byte(line), &base); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}
	if !base.Ok {
		return fmt.Errorf("kuasar error: %s", base.Error)
	}
	if resp != nil {
		if err := json.Unmarshal([]byte(line), resp); err != nil {
			return fmt.Errorf("unmarshal response fields: %w", err)
		}
	}
	return nil
}

// adminTemplate is one entry in template-list / template-get / continuation-list responses.
type adminTemplate struct {
	TemplateID   string `json:"template_id"`
	SnapshotType string `json:"snapshot_type"`
	Key          string `json:"key"`
	// Continuation-specific fields.
	PodUID     string `json:"pod_uid,omitempty"`
	Generation uint32 `json:"generation,omitempty"`
}

// kuasarArtifact is a driver-internal representation that carries mode-specific
// references (e.g. TemplateID) alongside the logical SnapshotKey.
// It is never exposed outside the kuasar package.
type kuasarArtifact struct {
	Mode        runtimev1alpha1.SandboxSnapshotMode
	SnapshotKey string
	TemplateID  string
}

// findKuasarArtifact returns the internal artifact for snapshotKey, or nil if not found.
// Used by Delete and Inspect to resolve SnapshotKey → TemplateID without exposing
// driver-internal references through the public SnapshotDriverArtifact type.
func (d *Driver) findKuasarArtifact(ctx context.Context, snapshotKey string) (*kuasarArtifact, error) {
	artifacts, err := d.listSnapshotFork(ctx)
	if err != nil {
		return nil, err
	}
	for i := range artifacts {
		if artifacts[i].SnapshotKey == snapshotKey {
			return &artifacts[i], nil
		}
	}
	return nil, nil
}

func init() {
	agentdriver.RegisterDriverFactory(func() agentdriver.Driver {
		return NewDriver("")
	})
	klog.V(2).InfoS("kuasar snapshot driver registered", "provider", ProviderName)
}
