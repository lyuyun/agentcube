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

package driver

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	driverproto "github.com/volcano-sh/agentcube/pkg/agentd/driver/proto"
	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

const (
	// defaultGRPCSocket is the default sandbox gRPC northbound socket path.
	defaultGRPCSocket = "/run/vmm-sandboxer-service.sock"

	// snapshotTimeout is the deadline for CreateSandboxSnapshot.
	snapshotTimeout = 10 * time.Minute

	// rpcTimeout is the deadline for lightweight gRPC calls.
	rpcTimeout = 30 * time.Second
)

// grpcDriver implements SnapshotDriver against the sandbox gRPC northbound API
// (SandboxSnapshotController service defined in proto/snapshot.proto).
type grpcDriver struct {
	grpcSocket string
}

// NewGRPCDriver returns a SnapshotDriver that talks to the sandbox northbound API
// at grpcSocket. Pass an empty string to use the default socket path.
func NewGRPCDriver(grpcSocket string) SnapshotDriver {
	if grpcSocket == "" {
		grpcSocket = defaultGRPCSocket
	}
	return &grpcDriver{grpcSocket: grpcSocket}
}

// ---------------------------------------------------------------------------
// Connection helpers
// ---------------------------------------------------------------------------

func (d *grpcDriver) dial() (*grpc.ClientConn, error) {
	// "unix://" + "/run/foo.sock" = "unix:///run/foo.sock" — the standard URI
	// for an absolute Unix socket path in grpc-go. This sets :authority to
	// "localhost" so tonic's HTTP/2 header validation passes.
	return grpc.NewClient(
		"unix://"+d.grpcSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func (d *grpcDriver) snapshotClient() (driverproto.SandboxSnapshotControllerClient, *grpc.ClientConn, error) {
	conn, err := d.dial()
	if err != nil {
		return nil, nil, fmt.Errorf("connect to sandbox socket %s: %w", d.grpcSocket, err)
	}
	return driverproto.NewSandboxSnapshotControllerClient(conn), conn, nil
}

// ---------------------------------------------------------------------------
// Identity Service (Driver interface)
// ---------------------------------------------------------------------------

// GetPluginInfo returns the plugin name and version from the sandbox.
func (d *grpcDriver) GetPluginInfo(ctx context.Context) (*PluginInfo, error) {
	client, conn, err := d.snapshotClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	resp, err := client.GetPluginInfo(ctx, &driverproto.GetPluginInfoRequest{})
	if err != nil {
		return nil, fmt.Errorf("GetPluginInfo: %w", err)
	}
	return &PluginInfo{
		Name:    resp.Name,
		Version: resp.Version,
	}, nil
}

// GetPluginCapabilities returns the snapshot modes supported by the sandbox.
func (d *grpcDriver) GetPluginCapabilities(ctx context.Context) ([]PluginCapability, error) {
	client, conn, err := d.snapshotClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	resp, err := client.GetPluginCapabilities(ctx, &driverproto.GetPluginCapabilitiesRequest{})
	if err != nil {
		return nil, fmt.Errorf("GetPluginCapabilities: %w", err)
	}

	caps := make([]PluginCapability, 0, len(resp.Capabilities))
	for _, c := range resp.Capabilities {
		caps = append(caps, PluginCapability{Type: protoCapToAgent(c.Type)})
	}
	return caps, nil
}

// Probe checks whether the sandbox is ready.
func (d *grpcDriver) Probe(ctx context.Context) error {
	client, conn, err := d.snapshotClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	resp, err := client.Probe(ctx, &driverproto.ProbeRequest{})
	if err != nil {
		return fmt.Errorf("Probe: %w", err)
	}
	if !resp.Ready {
		return fmt.Errorf("sandbox not ready")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Snapshot operations (SnapshotDriver interface)
// ---------------------------------------------------------------------------

// CreateSnapshot creates a WarmFork or Continuation snapshot of the running sandbox.
func (d *grpcDriver) CreateSnapshot(ctx context.Context, req CreateSnapshotRequest) (*Snapshot, error) {
	mode, err := agentModeToProto(req.Mode)
	if err != nil {
		return nil, fmt.Errorf("CreateSnapshot: %w", err)
	}
	if req.PodUID == "" {
		return nil, fmt.Errorf("CreateSnapshot: PodUID is required")
	}

	client, conn, err := d.snapshotClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
	defer cancel()

	resp, err := client.CreateSandboxSnapshot(ctx, &driverproto.CreateSandboxSnapshotRequest{
		PodUid:       req.PodUID,
		SnapshotName: req.SnapshotName,
		Mode:         mode,
	})
	if err != nil {
		return nil, fmt.Errorf("CreateSandboxSnapshot %q (pod=%s): %w",
			req.SnapshotName, req.PodUID, err)
	}

	name := req.SnapshotName
	snapshotMode := req.Mode
	if s := resp.Snapshot; s != nil {
		if s.SnapshotName != "" {
			name = s.SnapshotName
		}
		snapshotMode = protoModeToAgent(driverproto.SnapshotMode(s.Mode))
	}

	return &Snapshot{
		SnapshotName: name,
		SnapshotHash: req.SnapshotHash,
		IsReadyToUse: true,
		Mode:         snapshotMode,
	}, nil
}

// DeleteSnapshot removes the snapshot identified by snapshotName.
func (d *grpcDriver) DeleteSnapshot(ctx context.Context, snapshotName string) error {
	client, conn, err := d.snapshotClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	_, err = client.DeleteSandboxSnapshot(ctx, &driverproto.DeleteSandboxSnapshotRequest{
		SnapshotName: snapshotName,
	})
	if err != nil {
		return fmt.Errorf("DeleteSandboxSnapshot %q: %w", snapshotName, err)
	}
	return nil
}

// ListSnapshots returns snapshots matching req.
// When req.SnapshotName is non-empty, GetSandboxSnapshot is used for a targeted
// lookup; otherwise ListSandboxSnapshots returns all snapshots.
func (d *grpcDriver) ListSnapshots(ctx context.Context, req ListSnapshotsRequest) (*ListSnapshotsResponse, error) {
	client, conn, err := d.snapshotClient()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	if req.SnapshotName != "" {
		return d.getSnapshot(ctx, client, req.SnapshotName)
	}
	return d.listAllSnapshots(ctx, client)
}

func (d *grpcDriver) getSnapshot(ctx context.Context, client driverproto.SandboxSnapshotControllerClient, name string) (*ListSnapshotsResponse, error) {
	resp, err := client.GetSandboxSnapshot(ctx, &driverproto.GetSandboxSnapshotRequest{
		SnapshotName: name,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return &ListSnapshotsResponse{}, nil
		}
		return nil, fmt.Errorf("GetSandboxSnapshot %q: %w", name, err)
	}
	if resp.Snapshot == nil {
		return &ListSnapshotsResponse{}, nil
	}
	return &ListSnapshotsResponse{
		Snapshots: []Snapshot{protoSnapshotToAgent(resp.Snapshot)},
	}, nil
}

func (d *grpcDriver) listAllSnapshots(ctx context.Context, client driverproto.SandboxSnapshotControllerClient) (*ListSnapshotsResponse, error) {
	resp, err := client.ListSandboxSnapshots(ctx, &driverproto.ListSandboxSnapshotsRequest{
		Mode: driverproto.SnapshotMode_UNSPECIFIED,
	})
	if err != nil {
		return nil, fmt.Errorf("ListSandboxSnapshots: %w", err)
	}
	snapshots := make([]Snapshot, 0, len(resp.Snapshots))
	for _, s := range resp.Snapshots {
		snapshots = append(snapshots, protoSnapshotToAgent(s))
	}
	return &ListSnapshotsResponse{Snapshots: snapshots}, nil
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

func protoSnapshotToAgent(s *driverproto.SandboxSnapshot) Snapshot {
	return Snapshot{
		SnapshotName: s.SnapshotName,
		IsReadyToUse: true, // sandbox only returns snapshots that exist and are usable
		Mode:         protoModeToAgent(driverproto.SnapshotMode(s.Mode)),
	}
}

func agentModeToProto(mode runtimev1alpha1.SandboxSnapshotMode) (driverproto.SnapshotMode, error) {
	switch mode {
	case runtimev1alpha1.SandboxSnapshotModeFork:
		return driverproto.SnapshotMode_FORK, nil
	case runtimev1alpha1.SandboxSnapshotModeResume:
		return driverproto.SnapshotMode_RESUME, nil
	default:
		return 0, fmt.Errorf("unsupported snapshot mode %q", mode)
	}
}

func protoModeToAgent(mode driverproto.SnapshotMode) runtimev1alpha1.SandboxSnapshotMode {
	switch mode {
	case driverproto.SnapshotMode_RESUME:
		return runtimev1alpha1.SandboxSnapshotModeResume
	default:
		return runtimev1alpha1.SandboxSnapshotModeFork
	}
}

func protoCapToAgent(t driverproto.PluginCapabilityType) PluginCapabilityType {
	switch t {
	case driverproto.PluginCapabilityType_PLUGIN_FORK:
		return PluginCapabilityWarmFork
	case driverproto.PluginCapabilityType_PLUGIN_RESUME:
		return PluginCapabilityContinuation
	default:
		return PluginCapabilityUnknown
	}
}
