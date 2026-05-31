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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SnapStart configures WarmForkSnapshot acceleration for a CodeInterpreter or AgentRuntime.
// SnapStart is an independent CRD that references the runtime via runtimeRef.
// CodeInterpreter / AgentRuntime objects are NOT modified by SnapStart.
//
// Phase 1: only runtimeRef.kind=CodeInterpreter is supported.
// Phase 2 (TODO): AgentRuntime (BrowserAgent WarmForkSnapshot).
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="ActiveMode",type="string",JSONPath=".status.activeMode"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.snapshot.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type SnapStart struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapStartSpec   `json:"spec"`
	Status SnapStartStatus `json:"status,omitempty"`
}

// ArtifactDistributionMode controls how snapshot artifacts are distributed across nodes.
// The mode captures distribution semantics (who gets the artifact and when), not the
// underlying storage backend.
// +kubebuilder:validation:Enum=NodeLocal;LazyRemote;PreDistribute
type ArtifactDistributionMode string

const (
	// DistributionModeNodeLocal is the Phase 1 default: the snapshot artifact is local to
	// the node that built it. Restores are pinned to that specific node.
	DistributionModeNodeLocal ArtifactDistributionMode = "NodeLocal"
	// DistributionModeLazyRemote (Phase 2, reserved): a global artifact exists in remote
	// storage; a node pulls or materialises it only when a restore request arrives.
	DistributionModeLazyRemote ArtifactDistributionMode = "LazyRemote"
	// DistributionModePreDistribute (Phase 2, reserved): SnapshotController proactively
	// replicates or materialises the artifact to selected placements before restores arrive.
	DistributionModePreDistribute ArtifactDistributionMode = "PreDistribute"
)

// SnapStartArtifactSpec configures snapshot artifact distribution.
type SnapStartArtifactSpec struct {
	// Distribution is the artifact distribution mode.
	// NodeLocal (default, Phase 1): snapshot artifact stays on the node that built it.
	// LazyRemote (Phase 2, reserved): artifact is pulled from remote storage on-demand.
	// PreDistribute (Phase 2, reserved): artifact is proactively pushed to target nodes.
	// +kubebuilder:validation:Enum=NodeLocal;LazyRemote;PreDistribute
	// +kubebuilder:default="NodeLocal"
	// +optional
	Distribution ArtifactDistributionMode `json:"distribution,omitempty"`
}

// CacheState is the per-node snapshot artifact cache state.
// +kubebuilder:validation:Enum=Building;LocalReady;RemoteAvailable;Failed;Unavailable;Invalidated
type CacheState string

const (
	// CacheStateBuilding: snapshot build is in progress on this node.
	CacheStateBuilding CacheState = "Building"
	// CacheStateLocalReady: snapshot artifact is on this node's local disk and ready for restore.
	CacheStateLocalReady CacheState = "LocalReady"
	// CacheStateRemoteAvailable: snapshot is available from remote storage (Phase 2 reserved).
	CacheStateRemoteAvailable CacheState = "RemoteAvailable"
	// CacheStateFailed: snapshot build failed on this node.
	CacheStateFailed CacheState = "Failed"
	// CacheStateUnavailable: node went NotReady; snapshot may still exist but is not served.
	CacheStateUnavailable CacheState = "Unavailable"
	// CacheStateInvalidated: snapshot was invalidated on this node; deletion in progress.
	CacheStateInvalidated CacheState = "Invalidated"
)

// SnapshotArtifactStatus reports the effective artifact distribution mode.
type SnapshotArtifactStatus struct {
	// Distribution is the effective artifact distribution mode.
	Distribution ArtifactDistributionMode `json:"distribution"`
}

// SnapStartSpec configures a WarmForkSnapshot for the referenced runtime.
type SnapStartSpec struct {
	// RuntimeRef references the CodeInterpreter or AgentRuntime this SnapStart accelerates.
	// +kubebuilder:validation:Required
	RuntimeRef RuntimeReference `json:"runtimeRef"`

	// Checkpoint is the name the runtime reports via GET /runtime/status when it is
	// safe to snapshot. The controller polls this endpoint until safeToSnapshot=true.
	// Standard values: InterpreterReady (picod), BrowserReady (browser agent).
	// +kubebuilder:default="InterpreterReady"
	// +optional
	Checkpoint string `json:"checkpoint,omitempty"`

	// Invalidation controls when an existing snapshot is considered stale.
	// +optional
	Invalidation *SnapStartInvalidation `json:"invalidation,omitempty"`

	// Artifact configures snapshot artifact storage.
	// Defaults to NodeLocal (Phase 1).
	// +optional
	Artifact *SnapStartArtifactSpec `json:"artifact,omitempty"`
}

// RuntimeReference identifies a CodeInterpreter or AgentRuntime in the same namespace.
type RuntimeReference struct {
	// Kind is CodeInterpreter or AgentRuntime.
	// +kubebuilder:validation:Enum=CodeInterpreter;AgentRuntime
	Kind string `json:"kind"`
	// Name is the name of the referenced runtime object.
	Name string `json:"name"`
}

// SnapStartInvalidation controls when an existing snapshot is considered stale.
type SnapStartInvalidation struct {
	// OnImageDigestChange invalidates the snapshot when the container image reference changes.
	// For digest-pinned images (image@sha256:...), this detects digest changes via the image
	// field. For tagged images, this detects tag string changes (e.g. nginx:1.0 → nginx:2.0).
	// To also detect same-tag digest changes (nginx:latest pointing to a new digest), configure
	// maxAge or trigger a manual rebuild via the force-rebuild annotation.
	// Defaults to true. Use *bool (pointer) so that explicit false can be represented in JSON;
	// a non-pointer bool with omitempty would silently omit false and revert to the default.
	// +kubebuilder:default=true
	// +optional
	OnImageDigestChange *bool `json:"onImageDigestChange,omitempty"`
	// OnArgsChange invalidates the snapshot when container command or args change.
	// Defaults to true. Same *bool rationale as OnImageDigestChange.
	// +kubebuilder:default=true
	// +optional
	OnArgsChange *bool `json:"onArgsChange,omitempty"`
	// MaxAge is the maximum lifetime of a snapshot.
	// +kubebuilder:default="24h"
	MaxAge *metav1.Duration `json:"maxAge,omitempty"`
}

// SessionStartupMode is the effective startup path for new sessions.
// +kubebuilder:validation:Enum=Cold;Snapshot
type SessionStartupMode string

const (
	// SessionStartupModeCold: sessions cold-start (snapshot not ready or not configured).
	SessionStartupModeCold SessionStartupMode = "Cold"
	// SessionStartupModeSnapshot: sessions restore from WarmForkSnapshot template.
	SessionStartupModeSnapshot SessionStartupMode = "Snapshot"
)

// SnapStartStatus reports the current snapshot lifecycle state.
type SnapStartStatus struct {
	// ActiveMode is the authoritative answer to "what happens when I create a session?".
	// Cold: sessions cold-start (snapshot not ready or failed).
	// Snapshot: sessions restore from WarmForkSnapshot template.
	// +kubebuilder:validation:Enum=Cold;Snapshot
	ActiveMode SessionStartupMode `json:"activeMode"`

	// Snapshot reports the WarmForkSnapshot template lifecycle.
	// +optional
	Snapshot *SnapshotStatus `json:"snapshot,omitempty"`

	// Conditions reports detailed sub-state as standard Kubernetes conditions.
	// Known condition types:
	//   Degraded            — True when readyNodes < eligibleNodes; see snapshot.readyNodes/eligibleNodes.
	//   ProtocolNotSupported — True when the runtime does not implement /runtime/status
	//                         (persistent 404); snapshot build will not be retried.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Message is a human-readable description of the current state and next steps.
	// +optional
	Message string `json:"message,omitempty"`
}

// SnapshotPhase is the CRD-level (aggregate, cross-node) snapshot lifecycle phase.
// Per-node state is tracked as CacheState in Redis (see SnapshotInfo.CacheState).
// +kubebuilder:validation:Enum=Pending;Creating;Ready;Failed;Invalidated
type SnapshotPhase string

const (
	SnapshotPhasePending     SnapshotPhase = "Pending"
	SnapshotPhaseCreating    SnapshotPhase = "Creating"
	SnapshotPhaseReady       SnapshotPhase = "Ready"
	SnapshotPhaseFailed      SnapshotPhase = "Failed"
	SnapshotPhaseInvalidated SnapshotPhase = "Invalidated"
)

// SnapshotStatus reports snapshot lifecycle for a SnapStart.
// Per-node details (templateId, nodeIP, CacheState) are stored in Redis only;
// the CRD carries aggregate counts to avoid O(nodes) growth of the status object.
type SnapshotStatus struct {
	// Phase is the aggregate CRD-level snapshot lifecycle phase across all eligible nodes.
	// Per-node CacheState is stored in Redis (SnapshotInfo.CacheState).
	// +kubebuilder:validation:Enum=Pending;Creating;Ready;Failed;Invalidated
	Phase SnapshotPhase `json:"phase"`
	// Artifact reports the effective artifact storage type.
	// +optional
	Artifact *SnapshotArtifactStatus `json:"artifact,omitempty"`
	// EligibleNodes is the total number of nodes targeted for snapshot builds.
	// +optional
	EligibleNodes int32 `json:"eligibleNodes,omitempty"`
	// ReadyNodes is the number of nodes with a LocalReady snapshot (available for restore).
	// +optional
	ReadyNodes int32 `json:"readyNodes,omitempty"`
	// FailedNodes is the number of nodes where the snapshot build has permanently failed.
	// +optional
	FailedNodes int32 `json:"failedNodes,omitempty"`
	// UnavailableNodes is the number of nodes that are currently unreachable or invalidated.
	// +optional
	UnavailableNodes int32 `json:"unavailableNodes,omitempty"`
	// ReadyAt is the time the aggregate snapshot first became Ready.
	// +optional
	ReadyAt *metav1.Time `json:"readyAt,omitempty"`
	// FailedReason contains the error when Phase=Failed.
	// +optional
	FailedReason string `json:"failedReason,omitempty"`
	// RestoreFailureCount tracks consecutive restore failures across all nodes.
	// SnapshotController marks the snapshot Invalidated when this reaches 3.
	// Reset to 0 on successful rebuild.
	// +optional
	RestoreFailureCount int32 `json:"restoreFailureCount,omitempty"`
	// BuildFailureCount tracks consecutive retryable snapshot build failures.
	// SnapshotController stops automatic retries after three failed attempts.
	// +optional
	BuildFailureCount int32 `json:"buildFailureCount,omitempty"`
	// NextBuildRetryAt is the earliest time for the next automatic build retry.
	// +optional
	NextBuildRetryAt *metav1.Time `json:"nextBuildRetryAt,omitempty"`
}

// SnapStartList contains a list of SnapStart
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
type SnapStartList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SnapStart `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SnapStart{}, &SnapStartList{})
}
