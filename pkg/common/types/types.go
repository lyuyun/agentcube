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

package types

import "time"

const (
	// invoke kind
	AgentRuntimeKind    = "AgentRuntime"
	CodeInterpreterKind = "CodeInterpreter"

	// indicates what kind of api the underlying sandbox is created by
	SandboxKind       = "Sandbox"
	SandboxClaimsKind = "SandboxClaim"
)

// Annotation and label constants for SnapStart feature.
const (
	// AnnotationSnapStartRef is the AgentCube-layer logical SnapStart intent set on a newly
	// created direct Sandbox. Value is "<namespace>/<snapstart-name>". SandboxReconciler reads
	// this and propagates the Kuasar-facing protocol annotations before Pod creation.
	// Must never be set on SandboxClaim-bound Sandboxes (WarmPool hit path).
	AnnotationSnapStartRef = "agentcube.volcano.sh/snapstart-ref"
	// AnnotationSnapStartUID guards against stale intent after a SnapStart is deleted and
	// recreated with the same name. Value is the UID of the owning SnapStart object.
	AnnotationSnapStartUID = "agentcube.volcano.sh/snapstart-uid"
	// AnnotationRestoredFromSnapshot records the template key of the snapshot that was used
	// to restore this sandbox. Reserved for Kuasar to set after a successful restore;
	// WM reads it for observability once the Kuasar→AgentCube reporting channel is wired up.
	AnnotationRestoredFromSnapshot = "agentcube.volcano.sh/restored-from-snapshot"
	// AnnotationTemplateSandbox marks a sandbox as a snapshot build sandbox (Job semantics).
	// The sandbox is deleted after the snapshot is created.
	AnnotationTemplateSandbox = "agentcube.volcano.sh/template-sandbox"
	// AnnotationForceRebuild triggers a one-time forced snapshot rebuild when set to "true"
	// on a SnapStart object. Consumed (deleted) after rebuild succeeds.
	AnnotationForceRebuild = "agentcube.volcano.sh/force-rebuild"
	// LabelKuasarSnapstart is the node label that marks a node as supporting Kuasar SnapStart.
	// SnapshotController only builds snapshots on nodes with this label.
	LabelKuasarSnapstart = "agentcube.volcano.sh/kuasar-snapstart"
	// SnapshotFinalizer is the finalizer attached to SnapStart objects to ensure
	// snapshot files and Redis metadata are cleaned up before the object is deleted.
	SnapshotFinalizer = "agentcube.volcano.sh/snapshot-cleanup"
)

// SnapshotInfo is the per-node snapshot metadata stored in Redis Hash fields.
// CacheState is the authoritative per-node state (Building/LocalReady/Failed/Unavailable/Invalidated).
// SnapStartUID prevents stale data reuse after a SnapStart is deleted and recreated.
//
// Version fields (SpecHash, ImageRef, ImageDigest, Checkpoint, ProtocolVersion) are
// mandatory for restore selection. Entries missing any of these fields (built before
// version gating was introduced) are rejected by Workload Manager's restore selector.
type SnapshotInfo struct {
	TemplateID   string `json:"templateId"`
	TemplateKey  string `json:"templateKey"`
	SnapStartUID string `json:"snapStartUID,omitempty"` // UID of the owning SnapStart object
	CacheState   string `json:"cacheState"`             // CacheState values: Building/LocalReady/Failed/Unavailable/Invalidated
	// SpecHash is a hash of the non-image spec fields (args, env, resources, runtimeClass).
	// Used for restore-time and reconcile-time drift detection.
	SpecHash string `json:"specHash,omitempty"`
	// ImageRef is the container image string from CI.spec.template.image at build time.
	// Detects image reference changes (e.g. tag bump, digest-pinned image update).
	ImageRef string `json:"imageRef,omitempty"`
	// ImageDigest is the resolved container image digest (sha256:...) observed from the
	// build pod's containerStatuses[].imageID. Stored for audit and future digest-level
	// invalidation; not compared at restore time in Phase 1.
	ImageDigest string `json:"imageDigest,omitempty"`
	// Checkpoint is the SnapStart.spec.checkpoint value at build time
	// (e.g. "InterpreterReady", "BrowserReady").
	Checkpoint string `json:"checkpoint,omitempty"`
	// ProtocolVersion is the Kuasar WarmFork readiness protocol version used during build.
	// Currently always "1".
	ProtocolVersion string `json:"protocolVersion,omitempty"`
	// RuntimeGeneration is the CodeInterpreter metadata.generation at build time.
	// Informational; spec drift is detected via SpecHash rather than generation.
	RuntimeGeneration int64 `json:"runtimeGeneration,omitempty"`
	NodeName  string    `json:"nodeName"`
	NodeIP    string    `json:"nodeIP"`
	StartedAt time.Time `json:"startedAt,omitempty"`
	// FailedAt is set when CacheState transitions to Failed.
	// Used by SnapshotController to enforce the per-node incremental retry delay.
	// Separate from StartedAt (which records when the build began) to avoid semantic ambiguity.
	FailedAt  time.Time `json:"failedAt,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
}
