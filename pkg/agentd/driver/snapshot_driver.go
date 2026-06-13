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

	corev1 "k8s.io/api/core/v1"

	runtimev1alpha1 "github.com/volcano-sh/agentcube/pkg/apis/runtime/v1alpha1"
)

// Restore is intentionally absent from SnapshotDriver.
// Per the design (§7.6), restore is handled by the runtime compatibility layer
// (CRI shim / VMM integration) that consumes the agentcube.volcano.sh/snapshot-key
// annotation directly during Pod sandbox creation. The node agent stays on the
// snapshot build path only.

// SnapshotDriver extends Driver with snapshot lifecycle operations.
// The interface mirrors the CSI Controller Service snapshot RPCs:
//   - CreateSnapshot  ↔  CSI CreateSnapshot
//   - DeleteSnapshot  ↔  CSI DeleteSnapshot
//   - ListSnapshots   ↔  CSI ListSnapshots (with optional snapshot_name filter)
//
// Separate Inspect/Get methods are intentionally absent: pass a non-empty
// ListSnapshotsRequest.SnapshotName to perform a targeted single-snapshot lookup,
// following CSI's ListSnapshots(snapshot_id=x) convention.
type SnapshotDriver interface {
	Driver

	// CreateSnapshot creates a snapshot of the running sandbox and returns the
	// resulting artifact. The returned Snapshot has IsReadyToUse=true on success.
	CreateSnapshot(ctx context.Context, req CreateSnapshotRequest) (*Snapshot, error)

	// DeleteSnapshot removes the snapshot identified by snapshotName.
	DeleteSnapshot(ctx context.Context, snapshotName string) error

	// ListSnapshots returns snapshots matching req.
	// When req.SnapshotName is non-empty, the driver performs a targeted lookup
	// (analogous to CSI ListSnapshots(snapshot_id=x)) and returns at most one entry.
	// An empty req.SnapshotName returns all snapshots.
	// Each returned Snapshot carries IsReadyToUse reflecting its current state.
	ListSnapshots(ctx context.Context, req ListSnapshotsRequest) (*ListSnapshotsResponse, error)
}

// CreateSnapshotRequest carries the inputs for a snapshot creation call.
type CreateSnapshotRequest struct {
	// TaskRef is the Kubernetes object reference to the SandboxSnapshotTask.
	TaskRef corev1.ObjectReference

	// TargetSandboxRef identifies the Sandbox to snapshot.
	TargetSandboxRef corev1.TypedLocalObjectReference

	// TargetNodeName is the node where the build Sandbox is running.
	TargetNodeName string

	// SnapshotName is the caller-chosen stable name for this snapshot artifact.
	SnapshotName string

	// SnapshotHash is the hash of the snapshot inputs.
	SnapshotHash string

	// Mode is the snapshot mode (Fork or Resume).
	Mode runtimev1alpha1.SandboxSnapshotMode

	// PodUID is the UID of the Kubernetes Pod backing the target sandbox.
	// Passed to the VMM sandboxer to identify the running VM.
	PodUID string
}

// Snapshot is the driver's representation of a snapshot artifact.
// It embeds IsReadyToUse so that a ListSnapshots result is self-contained,
// analogous to CSI embedding is_ready_to_use inside each ListSnapshots entry.
type Snapshot struct {
	// SnapshotName is the stable caller-chosen name for this artifact.
	SnapshotName string

	// SnapshotHash is the hash of the snapshot inputs, if known.
	SnapshotHash string

	// IsReadyToUse indicates the snapshot is fully created and usable for restore.
	// Mirrors CSI Snapshot.is_ready_to_use.
	IsReadyToUse bool

	// Mode is the snapshot mode (Fork or Resume).
	Mode runtimev1alpha1.SandboxSnapshotMode
}

// ListSnapshotsRequest is the input for ListSnapshots.
type ListSnapshotsRequest struct {
	// SnapshotName, when non-empty, restricts results to the named snapshot.
	// Implementations use GetSandboxSnapshot when the backend supports targeted
	// lookup; otherwise they fall back to list + client-side filter.
	SnapshotName string
}

// ListSnapshotsResponse is the output of ListSnapshots.
type ListSnapshotsResponse struct {
	Snapshots []Snapshot
}
